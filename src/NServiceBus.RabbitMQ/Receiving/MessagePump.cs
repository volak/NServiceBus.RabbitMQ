namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using global::RabbitMqNext;
    using Logging;
    using NServiceBus.Extensibility;

    class MessagePump : IPushMessages, IDisposable, IQueueConsumer
    {
        static readonly ILog Logger = LogManager.GetLogger(typeof(MessagePump));
        static readonly TransportTransaction transportTranaction = new TransportTransaction();
        static readonly ContextBag contextBag = new ContextBag();

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
        CancellationTokenSource messageProcessing;
        SemaphoreSlim semaphore;
        IChannel channel;


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
            semaphore = new SemaphoreSlim(limitations.MaxConcurrency, limitations.MaxConcurrency);
            maxConcurrency = limitations.MaxConcurrency;
            messageProcessing = new CancellationTokenSource();
            long prefetchCount;

            if (overriddenPrefetchCount > 0)
            {
                prefetchCount = overriddenPrefetchCount;

                if (prefetchCount < limitations.MaxConcurrency)
                {
                    Logger.Warn($"The specified prefetch count '{prefetchCount}' is smaller than the specified maximum concurrency '{maxConcurrency}'. The maximum concurrency value will be used as the prefetch count instead.");
                    prefetchCount = maxConcurrency;
                }
            }
            else
            {
                prefetchCount = (long)maxConcurrency * prefetchMultiplier;
            }

            Logger.Debug($"Creating consumer {consumerTag} with prefetch {prefetchCount}");
            channel = connection.CreateChannel().Result;
            channel.BasicQos(0, (ushort)Math.Min(prefetchCount, ushort.MaxValue), false).Wait();

            channel.BasicConsume(ConsumeMode.ParallelWithBufferCopy, this, settings.InputQueue, consumerTag, false, false, null, true).Wait();

        }

        public async Task Stop()
        {
            Logger.Info($"Stopping message pump {settings.InputQueue}");
            messageProcessing.Cancel();
            while (semaphore.CurrentCount != maxConcurrency)
            {
                await Task.Delay(50).ConfigureAwait(false);
            }

            await channel.Close().ConfigureAwait(false);
            channel.Dispose();

            if (!connection.IsClosed)
                connection.Dispose();
        }

        public void Dispose()
        {
            // Injected
        }

        public void Broken()
        {
            circuitBreaker.Failure(new Exception("Broken"));
        }

        public void Recovered()
        {
            circuitBreaker.Success();
        }

        public void Cancelled()
        {
            circuitBreaker.Failure(new Exception("Canceled"));
        }

        public async Task Consume(MessageDelivery delivery)
        {

            string messageId;

            try
            {
                messageId = messageConverter.RetrieveMessageId(delivery);
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to retrieve ID from poison message. Moving message to queue '{settings.ErrorQueue}'...", ex);
                await MovePoisonMessage(delivery, settings.ErrorQueue).ConfigureAwait(false);

                return;
            }

            Dictionary<string, string> headers;

            try
            {
                headers = messageConverter.RetrieveHeaders(delivery);
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to retrieve headers from poison message '{messageId}'. Moving message to queue '{settings.ErrorQueue}'...", ex);
                await MovePoisonMessage(delivery, settings.ErrorQueue, messageId).ConfigureAwait(false);

                return;
            }


            using (var tokenSource = new CancellationTokenSource())
            {
                var processed = false;
                var errorHandled = false;
                var numberOfDeliveryAttempts = 0;

                if (delivery.stream.Length > Int32.MaxValue)
                    throw new InvalidOperationException("Your message is huge...");

                var body = new byte[delivery.stream.Length];
                try
                {
                    await delivery.stream.ReadAsync(body, 0, (Int32)delivery.stream.Length, messageProcessing.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { return; }

                try
                {
                    await semaphore.WaitAsync(messageProcessing.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { return; }
                try
                {
                    while (!processed && !errorHandled)
                    {
                        try
                        {
                            var messageContext = new MessageContext(messageId, headers, body, transportTranaction, tokenSource, contextBag);
                            await onMessage(messageContext).ConfigureAwait(false);
                            processed = true;
                        }
                        catch (Exception ex)
                        {
                            ++numberOfDeliveryAttempts;
                            var errorContext = new ErrorContext(ex, headers, messageId, body, transportTranaction, numberOfDeliveryAttempts);
                            errorHandled = await onError(errorContext).ConfigureAwait(false) == ErrorHandleResult.Handled;
                        }
                    }
                }
                finally
                {
                    semaphore.Release();
                }

                if (processed && tokenSource.IsCancellationRequested)
                {
                    channel.BasicNAck(delivery.deliveryTag, false, true);
                }
                else
                {
                    try
                    {
                        channel.BasicAck(delivery.deliveryTag, false);
                    }
                    catch (ObjectDisposedException ex)
                    {
                        Logger.Warn($"Failed to acknowledge message '{messageId}' because the channel was closed. The message was returned to the queue.", ex);
                    }
                }
            }
        }
        async Task MovePoisonMessage(MessageDelivery delivery, string queue, string messageId = null)
        {
            try
            {
                var poisonChannel = await channelProvider.GetPublishChannel().ConfigureAwait(false);

                var body = new byte[delivery.stream.Length];
                await delivery.stream.ReadAsync(body, 0, (Int32)delivery.stream.Length).ConfigureAwait(false);

                await poisonChannel.RawSendInCaseOfFailure(queue, body, delivery.properties.Clone()).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to move poison message{(messageId == null ? "" : $" '{messageId}'")} to queue '{queue}'. Returning message to original queue...", ex);

                channel.BasicNAck(delivery.deliveryTag, false, true);
                return;
            }

            try
            {
                channel.BasicAck(delivery.deliveryTag, false);
            }
            catch (ObjectDisposedException ex)
            {
                Logger.Warn($"Failed to acknowledge poison message{(messageId == null ? "" : $" '{messageId}'")} because the channel was closed. The message was sent to queue '{queue}' but also returned to the original queue.", ex);
            }
        }
    }
}
