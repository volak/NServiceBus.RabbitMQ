namespace NServiceBus.Transport.RabbitMQ.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using RabbitMqNext;
    using ConnectionFactory = NServiceBus.Transport.RabbitMQ.ConnectionFactory;

    class RabbitMqContext
    {
        protected async Task MakeSureQueueAndExchangeExists(string queueName)
        {
            using (var connection = await connectionFactory.CreateAdministrationConnection())
            using (var channel = await connection.CreateChannel())
            {
                await channel.QueueDeclare(queueName, false, false, false, false, null, true);
                await channel.QueuePurge(queueName, true);

                //to make sure we kill old subscriptions
                await DeleteExchange(queueName);

                await routingTopology.Initialize(channel, queueName);
            }
        }

        async Task DeleteExchange(string exchangeName)
        {
            using (var connection = await connectionFactory.CreateAdministrationConnection())
            using (var channel = await connection.CreateChannel())
            {
                try
                {
                    await channel.ExchangeDelete(exchangeName, false);
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

            var connectionString = Environment.GetEnvironmentVariable("RabbitMQTransport.ConnectionString");

            ConnectionConfiguration config;

            if (connectionString != null)
            {
                var parser = new ConnectionStringParser(ReceiverQueue);
                config = parser.Parse(connectionString);
            }
            else
            {
                config = new ConnectionConfiguration(ReceiverQueue);
                config.Host = "localhost";
                config.VirtualHost = "nsb-rabbitmq-test";
            }

            connectionFactory = new ConnectionFactory(() => "unit test", config);
            channelProvider = new ChannelProvider(connectionFactory, routingTopology, true);
            connection = await connectionFactory.CreateConnection("unit test");

            messageDispatcher = new MessageDispatcher(channelProvider);

            var purger = new QueuePurger(connectionFactory);

            messagePump = new MessagePump(connection, new MessageConverter(), "Unit test", channelProvider, purger, TimeSpan.FromMinutes(2), 3, 0);

            await MakeSureQueueAndExchangeExists(ReceiverQueue);
            await MakeSureQueueAndExchangeExists(ErrorQueue);


            subscriptionManager = new SubscriptionManager(connectionFactory, routingTopology, ReceiverQueue);

            messagePump.Init(messageContext =>
            {
                receivedMessages.Add(new IncomingMessage(messageContext.MessageId, messageContext.Headers, messageContext.Body));
                return Task.CompletedTask;
            },
                ErrorContext => Task.FromResult(ErrorHandleResult.Handled),
                new CriticalError(_ => Task.CompletedTask),
                new PushSettings(ReceiverQueue, ErrorQueue, true, TransportTransactionMode.ReceiveOnly)
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

        protected async Task<MessageDelivery> GetOne(IChannel channel, string queueToReceiveOn)
        {
            var tsc = new TaskCompletionSource<MessageDelivery>();


            await channel.BasicConsume(ConsumeMode.ParallelWithBufferCopy, (msg) =>
            {
                tsc.SetResult(msg);
                return Task.CompletedTask;
            }, queueToReceiveOn, "test", true, false, null, true);

            return await tsc.Task;
        }

        protected const string ReceiverQueue = "testreceiver";
        protected const string ErrorQueue = "error";
        protected MessageDispatcher messageDispatcher;
        protected ConnectionFactory connectionFactory;
        protected IConnection connection;
        private ChannelProvider channelProvider;
        protected MessagePump messagePump;
        BlockingCollection<IncomingMessage> receivedMessages;

        protected ConventionalRoutingTopology routingTopology;
        protected SubscriptionManager subscriptionManager;
    }
}
