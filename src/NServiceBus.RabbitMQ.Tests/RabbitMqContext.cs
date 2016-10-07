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
        protected async void MakeSureQueueAndExchangeExists(string queueName)
        {
            using (var channel = await connection.CreateChannel())
            {
                await channel.QueueDeclare(queueName, true, false, false, false, null, true);
                await channel.QueuePurge(queueName, true);

                //to make sure we kill old subscriptions
                DeleteExchange(queueName);

                await routingTopology.Initialize(channel, queueName);
            }
        }

        async void DeleteExchange(string exchangeName)
        {
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

            connection = await global::RabbitMqNext.ConnectionFactory.Connect(config.Host, config.VirtualHost, config.UserName, config.Password);
            channelProvider = new ChannelProvider(connection, routingTopology, true);

            messageDispatcher = new MessageDispatcher(channelProvider);

            var purger = new QueuePurger(connection);

            messagePump = new MessagePump(connection, new MessageConverter(), "Unit test", channelProvider, purger, TimeSpan.FromMinutes(2), 3, 0);

            MakeSureQueueAndExchangeExists(ReceiverQueue);

            subscriptionManager = new SubscriptionManager(connection, routingTopology, ReceiverQueue);

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
        protected IConnection connection;
        private ChannelProvider channelProvider;
        protected MessagePump messagePump;
        BlockingCollection<IncomingMessage> receivedMessages;

        protected ConventionalRoutingTopology routingTopology;
        protected SubscriptionManager subscriptionManager;
    }
}
