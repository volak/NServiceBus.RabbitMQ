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


        public MessagePump(IConnection connection, MessageConverter messageConverter, string consumerTag, IChannelProvider channelProvider, QueuePurger queuePurger, TimeSpan timeToWaitBeforeTriggeringCircuitBreaker, int prefetchMultiplier, ushort overriddenPrefetchCount)
        {
            this.connection = connection;
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
            Logger.Info($"Starting message pump {settings.InputQueue}");
            maxConcurrency = limitations.MaxConcurrency;

            // Todo: MaxConcurrency
            // I could use MaxConcurrency to specify the number of channels like here
            // or I would use it to specify how many connections aka threads should exist

            for (var i = 0; i < maxConcurrency; i++)
            {
                CreateConsumer(i).Wait();
            }

        }

        private async Task CreateConsumer(int num)
        {

            Logger.Debug($"Creating consumer {consumerTag}.{num} with prefetch {prefetchMultiplier}");
            var channel = await connection.CreateChannel().ConfigureAwait(false);
            await channel.BasicQos(0, (ushort)Math.Min(prefetchMultiplier, ushort.MaxValue), false).ConfigureAwait(false);

            var consumer = new MessageConsumer(channel, onMessage, onError, channelProvider, settings, messageConverter, circuitBreaker);
            await channel.BasicConsume(ConsumeMode.SerializedWithBufferCopy, consumer, settings.InputQueue, $"{consumerTag}.{num}", false, false, null, true).ConfigureAwait(false);

            channels.Add(channel);
        }

        public async Task Stop()
        {
            Logger.Info($"Stopping message pump {settings.InputQueue}");
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
