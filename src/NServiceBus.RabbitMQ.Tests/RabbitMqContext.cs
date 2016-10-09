namespace NServiceBus.Transport.RabbitMQ.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Settings;
    using RabbitMqNext;

    class RabbitMqContext
    {
        protected async Task MakeSureQueueAndExchangeExists(string queueName)
        {
            var connection = await connectionFactory.CreateAdministrationConnection();
            using (var channel = await connection.CreateChannel())
            {
                await channel.QueueDeclare(queueName, true, false, false, false, null, true);
                await channel.QueuePurge(queueName, true);

                //to make sure we kill old subscriptions
                await DeleteExchange(queueName);

                await routingTopology.Initialize(channel, queueName);
            }
        }

        async Task DeleteExchange(string exchangeName)
        {
            var connection = await connectionFactory.CreateAdministrationConnection();
            using (var channel = await connection.CreateChannel())
            {
                try
                {
                    await channel.ExchangeDelete(exchangeName, null, true);
                }
                // ReSharper disable EmptyGeneralCatchClause
                catch (Exception)
                // ReSharper restore EmptyGeneralCatchClause
                {
                }
            }
        }

        public virtual int MaximumConcurrency => 1;

        [SetUp]
        public async Task SetUp()
        {
            routingTopology = new ConventionalRoutingTopology(true);
            receivedMessages = new BlockingCollection<IncomingMessage>();

            var settings = new SettingsHolder();
            settings.Set("NServiceBus.Routing.EndpointName", "endpoint");

            var connectionString = Environment.GetEnvironmentVariable("RabbitMQTransport.ConnectionString");

            ConnectionConfiguration config;

            if (connectionString != null)
            {
                var parser = new ConnectionStringParser(settings);
                config = parser.Parse(connectionString);
            }
            else
            {
                config = new ConnectionConfiguration(settings);
                config.Host = "localhost";
            }
            connectionFactory = new RabbitMQ.ConnectionFactory(settings, config);
            channelProvider = new ChannelProvider(connectionFactory, routingTopology, true);

            messageDispatcher = new MessageDispatcher(channelProvider);

            var purger = new QueuePurger(connectionFactory);

            messagePump = new MessagePump(await connectionFactory.CreateConnection("Receiver"), new MessageConverter(), "Unit test", channelProvider, purger, TimeSpan.FromMinutes(2), 3, 0);

            await MakeSureQueueAndExchangeExists(ReceiverQueue);

            subscriptionManager = new SubscriptionManager(connectionFactory, routingTopology, ReceiverQueue);

            messagePump.Init(messageContext =>
            {
                receivedMessages.Add(new IncomingMessage(messageContext.MessageId, messageContext.Headers, messageContext.Body));
                return Task.CompletedTask;
            },
                ErrorContext => Task.FromResult(ErrorHandleResult.Handled),
                new CriticalError(_ => Task.CompletedTask),
                new PushSettings(ReceiverQueue, "error", true, TransportTransactionMode.ReceiveOnly)
            ).GetAwaiter().GetResult();

            messagePump.Start(new PushRuntimeSettings(MaximumConcurrency));
        }

        [TearDown]
        public void TearDown()
        {
            messagePump?.Stop().GetAwaiter().GetResult();

            channelProvider?.Dispose();
        }

        protected IncomingMessage WaitForMessage()
        {
            var waitTime = TimeSpan.FromSeconds(1);

            if (Debugger.IsAttached)
            {
                waitTime = TimeSpan.FromMinutes(10);
            }

            IncomingMessage message;
            receivedMessages.TryTake(out message, waitTime);

            return message;
        }

        protected const string ReceiverQueue = "testreceiver";
        protected MessageDispatcher messageDispatcher;
        protected RabbitMQ.ConnectionFactory connectionFactory;
        private ChannelProvider channelProvider;
        protected MessagePump messagePump;
        BlockingCollection<IncomingMessage> receivedMessages;

        protected ConventionalRoutingTopology routingTopology;
        protected SubscriptionManager subscriptionManager;
    }
}
