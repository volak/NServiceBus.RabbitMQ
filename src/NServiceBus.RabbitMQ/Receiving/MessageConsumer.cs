using NServiceBus.Extensibility;
using NServiceBus.Logging;
using RabbitMqNext;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NServiceBus.Transport.RabbitMQ
{
    class MessageConsumer : IQueueConsumer
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(MessageConsumer));
        static readonly ContextBag contextBag = new ContextBag();
        static readonly TransportTransaction transportTranaction = new TransportTransaction();

        private readonly IChannel _channel;
        private readonly IChannelProvider _channelProvider;
        private MessagePumpConnectionFailedCircuitBreaker _circutBreaker;
        private readonly MessageConverter _converter;
        private readonly Func<MessageContext, Task> _onMessage;
        private readonly Func<ErrorContext, Task<ErrorHandleResult>> _onError;
        private readonly PushSettings _settings;

        public MessageConsumer(IChannel channel, Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError, IChannelProvider channelProvider, PushSettings settings, MessageConverter messageConverter, MessagePumpConnectionFailedCircuitBreaker circutBreaker)
        {
            _channel = channel;
            _onMessage = onMessage;
            _onError = onError;
            _channelProvider = channelProvider;
            _settings = settings;
            _converter = messageConverter;
            _circutBreaker = circutBreaker;
        }

        public void Broken()
        {
            _circutBreaker.Failure(new Exception("Broken"));
        }

        public void Recovered()
        {
            _circutBreaker.Success();
        }

        public void Cancelled()
        {
            _circutBreaker.Failure(new Exception("Canceled"));
        }

        public async Task Consume(MessageDelivery delivery)
        {
            string messageId;

            try
            {
                messageId = _converter.RetrieveMessageId(delivery);
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to retrieve ID from poison message. Moving message to queue '{_settings.ErrorQueue}'...", ex);
                await MovePoisonMessage(delivery, _settings.ErrorQueue).ConfigureAwait(false);

                return;
            }

            Dictionary<string, string> headers;

            try
            {
                headers = _converter.RetrieveHeaders(delivery);
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to retrieve headers from poison message '{messageId}'. Moving message to queue '{_settings.ErrorQueue}'...", ex);
                await MovePoisonMessage(delivery, _settings.ErrorQueue, messageId).ConfigureAwait(false);

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
                        await _onMessage(messageContext).ConfigureAwait(false);
                        processed = true;
                    }
                    catch (Exception ex)
                    {
                        ++numberOfDeliveryAttempts;
                        var errorContext = new ErrorContext(ex, headers, messageId, body, transportTranaction, numberOfDeliveryAttempts);
                        errorHandled = await _onError(errorContext).ConfigureAwait(false) == ErrorHandleResult.Handled;
                    }
                }
                if (processed && tokenSource.IsCancellationRequested)
                {
                    _channel.BasicNAck(delivery.deliveryTag, false, true);
                }
                else
                {
                    try
                    {
                        _channel.BasicAck(delivery.deliveryTag, false);
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
                var channel = await _channelProvider.GetPublishChannel().ConfigureAwait(false);

                var body = new byte[delivery.stream.Length];
                await delivery.stream.ReadAsync(body, 0, (Int32)delivery.stream.Length).ConfigureAwait(false);
                
                await channel.RawSendInCaseOfFailure(queue, body, delivery.properties).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to move poison message{(messageId == null ? "" : $" '{messageId}'")} to queue '{queue}'. Returning message to original queue...", ex);

                _channel.BasicNAck(delivery.deliveryTag, false, true);
                return;
            }

            try
            {
                _channel.BasicAck(delivery.deliveryTag, false);
            }
            catch (ObjectDisposedException ex)
            {
                Logger.Warn($"Failed to acknowledge poison message{(messageId == null ? "" : $" '{messageId}'")} because the channel was closed. The message was sent to queue '{queue}' but also returned to the original queue.", ex);
            }
        }

    }
}
