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
            using (var connection = await connectionFactory.CreateAdministrationConnection())
            using (var channel = await connection.CreateChannelWithPublishConfirmation())
            {
                await channel.QueueDeclare(queueName, false, true, false, false, null, true);
                await channel.QueuePurge(queueName, true);

                await routingTopology.Initialize(channel, queueName);
            }
        }

        async Task DeleteExchange(string exchangeName)
        {
            using (var connection = await connectionFactory.CreateAdministrationConnection())
            using (var channel = await connection.CreateChannelWithPublishConfirmation())
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
            LogAdapter.IsDebugEnabled = true;
            LogAdapter.ProtocolLevelLogEnabled = true;

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
            connectionFactory = new RabbitMQ.ConnectionFactory(settings, config, 30);
            receiveConnection = await connectionFactory.CreateConnection("Receiver");
            channelProvider = new ChannelProvider(connectionFactory, routingTopology, true, 30, 100);

            messageDispatcher = new MessageDispatcher(channelProvider);

            var purger = new QueuePurger(connectionFactory);

            messagePump = new MessagePump(receiveConnection, new MessageConverter(), "Unit test", channelProvider, purger, TimeSpan.FromMinutes(2), 3, 0);

            //to make sure we kill old subscriptions
            await DeleteExchange(ReceiverQueue);
            await MakeSureQueueAndExchangeExists(ReceiverQueue);

            subscriptionManager = new SubscriptionManager(connectionFactory, routingTopology, ReceiverQueue);

            await messagePump.Init(messageContext =>
                {
                    receivedMessages.Add(new IncomingMessage(messageContext.MessageId, messageContext.Headers, messageContext.Body));
                    return Task.CompletedTask;
                },
                ErrorContext => Task.FromResult(ErrorHandleResult.Handled),
                new CriticalError(_ => Task.CompletedTask),
                new PushSettings(ReceiverQueue, "error", true, TransportTransactionMode.ReceiveOnly)
            );

            messagePump.Start(new PushRuntimeSettings(MaximumConcurrency));
        }

        [TearDown]
        public void TearDown()
        {
            messagePump?.Stop().GetAwaiter().GetResult();

            channelProvider?.Dispose();
            receiveConnection.Dispose();
        }

        protected IncomingMessage WaitForMessage()
        {
            var waitTime = TimeSpan.FromSeconds(5);

            //if (Debugger.IsAttached)
            //{
            //    waitTime = TimeSpan.FromMinutes(10);
            //}

            IncomingMessage message;
            receivedMessages.TryTake(out message, waitTime);

            return message;
        }

        protected const string ReceiverQueue = "testreceiver";
        protected MessageDispatcher messageDispatcher;
        protected RabbitMQ.ConnectionFactory connectionFactory;
        protected IConnection receiveConnection;
        protected ChannelProvider channelProvider;
        protected MessagePump messagePump;
        BlockingCollection<IncomingMessage> receivedMessages;

        protected ConventionalRoutingTopology routingTopology;
        protected SubscriptionManager subscriptionManager;
    }
}
