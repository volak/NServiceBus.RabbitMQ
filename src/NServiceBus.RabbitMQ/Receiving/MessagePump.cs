namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Extensibility;
    using global::RabbitMqNext;
    using Logging;
    using System.Collections.Concurrent;

    class MessagePump : IPushMessages, IDisposable
    {
        static readonly ILog Logger = LogManager.GetLogger(typeof(MessagePump));

        IConnection connection;
        ConnectionFactory connectionFactory;
        readonly MessageConverter messageConverter;
        readonly string consumerTag;
        readonly IChannelProvider channelProvider;
        readonly QueuePurger queuePurger;
        readonly TimeSpan timeToWaitBeforeTriggeringCircuitBreaker;
        readonly int prefetchMultiplier;
        readonly ushort overriddenPrefetchCount;


        // Init
        Func<MessageContext, Task> onMessage;
        Func<ErrorContext, Task<ErrorHandleResult>> onError;
        PushSettings settings;
        MessagePumpConnectionFailedCircuitBreaker circuitBreaker;

        // Start
        int maxConcurrency;
        ConcurrentBag<IChannel> channels;


        public MessagePump(ConnectionFactory connectionFactory, MessageConverter messageConverter, string consumerTag, IChannelProvider channelProvider, QueuePurger queuePurger, TimeSpan timeToWaitBeforeTriggeringCircuitBreaker, int prefetchMultiplier, ushort overriddenPrefetchCount)
        {
            this.connectionFactory = connectionFactory;
            this.messageConverter = messageConverter;
            this.consumerTag = consumerTag;
            this.channelProvider = channelProvider;
            this.queuePurger = queuePurger;
            this.timeToWaitBeforeTriggeringCircuitBreaker = timeToWaitBeforeTriggeringCircuitBreaker;
            this.prefetchMultiplier = prefetchMultiplier;
            this.overriddenPrefetchCount = overriddenPrefetchCount;
            this.channels = new ConcurrentBag<IChannel>();
        }

        public async Task Init(Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError, CriticalError criticalError, PushSettings settings)
        {
            this.onMessage = onMessage;
            this.onError = onError;
            this.settings = settings;

            circuitBreaker = new MessagePumpConnectionFailedCircuitBreaker($"'{settings.InputQueue} MessagePump'", timeToWaitBeforeTriggeringCircuitBreaker, criticalError);


            if (settings.PurgeOnStartup)
            {
                await queuePurger.Purge(settings.InputQueue).ConfigureAwait(false);
            }
        }

        public void Start(PushRuntimeSettings limitations)
        {
            maxConcurrency = limitations.MaxConcurrency;

            connection = connectionFactory.CreateConnection($"{settings.InputQueue} MessagePump").Result;
            var connectionsTasks = new List<Task>();
            for (var i = 0; i < maxConcurrency; i++)
            {
                connectionsTasks.Add(TaskEx.StartNew(i, async (state) =>
                {
                    var channel = await connection.CreateChannel().ConfigureAwait(false);
                    await channel.BasicQos(0, (ushort)Math.Min(prefetchMultiplier, ushort.MaxValue), false).ConfigureAwait(false);

                    var consumer = new MessageConsumer(channel, onMessage, onError, channelProvider, settings, messageConverter, circuitBreaker);
                    await channel.BasicConsume(ConsumeMode.SerializedWithBufferCopy, consumer, settings.InputQueue, $"{consumerTag}.{(int)state}", false, false, null, true).ConfigureAwait(false);

                    channels.Add(channel);
                }));
            }

            Task.WhenAll(connectionsTasks).Wait();
        }

        public async Task Stop()
        {
            while (channels.Count > 0)
            {
                IChannel chan;
                if (!channels.TryTake(out chan))
                    continue;

                if (!chan.IsClosed)
                {
                    await chan.Close().ConfigureAwait(false);
                    chan.Dispose();
                }
            }
            
            if (!connection.IsClosed)
                connection.Dispose();
        }



        public void Dispose()
        {
            // Injected
        }

    }
}
