namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Extensibility;
    using global::RabbitMqNext;
    using Logging;

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
        IChannel channel;

        // Stop
        TaskCompletionSource<bool> connectionShutdownCompleted;

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
            maxConcurrency = limitations.MaxConcurrency;
            messageProcessing = new CancellationTokenSource();
            
            channel = connection.CreateChannel().Result;

            long prefetchCount;

            if (overriddenPrefetchCount > 0)
            {
                prefetchCount = overriddenPrefetchCount;

                if (prefetchCount < maxConcurrency)
                {
                    Logger.Warn($"The specified prefetch count '{prefetchCount}' is smaller than the specified maximum concurrency '{maxConcurrency}'. The maximum concurrency value will be used as the prefetch count instead.");
                    prefetchCount = maxConcurrency;
                }
            }
            else
            {
                prefetchCount = (long)maxConcurrency * prefetchMultiplier;
            }

            channel.BasicQos(0, (ushort)Math.Min(prefetchCount, ushort.MaxValue), false);
            
            var startTasks = new List<Task>();
            for (var i = 0; i < maxConcurrency; i++)
                startTasks.Add(channel.BasicConsume(ConsumeMode.ParallelWithBufferCopy, this, settings.InputQueue, consumerTag, false, false, null, true));
            Task.WhenAll(startTasks).Wait();
        }

        public async Task Stop()
        {
            //consumer.Received -= Consumer_Received;
            messageProcessing.Cancel();

            connectionShutdownCompleted = new TaskCompletionSource<bool>();

            if (!connection.IsClosed)
            {
                connection.Dispose();
            }
            else
            {
                connectionShutdownCompleted.SetResult(true);
            }

            await connectionShutdownCompleted.Task.ConfigureAwait(false);
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
                await delivery.stream.ReadAsync(body, 0, (Int32)delivery.stream.Length).ConfigureAwait(false);
                
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
                if (processed && tokenSource.IsCancellationRequested)
                {
                    throw new NotImplementedException("Currently the library does not support rejecting messages");
                    //await consumer.Model.BasicRejectAndRequeueIfOpen(message.DeliveryTag).ConfigureAwait(false);
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

        
        
        
        async Task MovePoisonMessage(MessageDelivery delivery, string queue, string messageId = null)
        {
            try
            {
                var channel = await channelProvider.GetPublishChannel().ConfigureAwait(false);

                try
                {
                    var body = new byte[delivery.stream.Length];
                    await delivery.stream.ReadAsync(body, 0, (Int32)delivery.stream.Length).ConfigureAwait(false);

                    await channel.RawSendInCaseOfFailure(queue, body, delivery.properties).ConfigureAwait(false);
                }
                finally
                {
                    channelProvider.ReturnPublishChannel(channel);
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to move poison message{(messageId == null ? "" : $" '{messageId}'")} to queue '{queue}'. Returning message to original queue...", ex);

                throw new NotImplementedException("Currently the library does not support rejecting messages");
                //await consumer.Model.BasicRejectAndRequeueIfOpen(message.DeliveryTag).ConfigureAwait(false);

                //return;
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

        public void Dispose()
        {
            // Injected
        }

    }
}
