namespace NServiceBus.Transport.RabbitMQ
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Extensibility;
    using RabbitMqNext;

    class MessageDispatcher : IDispatchMessages
    {
        readonly IChannelProvider channelProvider;

        public MessageDispatcher(IChannelProvider channelProvider)
        {
            this.channelProvider = channelProvider;
        }

        public async Task Dispatch(TransportOperations outgoingMessages, TransportTransaction transaction, ContextBag context)
        {
            var channel = await channelProvider.GetPublishChannel().ConfigureAwait(false);

            try
            {
                var unicastTransportOperations = outgoingMessages.UnicastTransportOperations;
                var multicastTransportOperations = outgoingMessages.MulticastTransportOperations;

                var tasks = new List<Task>(unicastTransportOperations.Count + multicastTransportOperations.Count);

                foreach (var operation in unicastTransportOperations)
                {
                    tasks.Add(SendMessage(operation, channel));
                }

                foreach (var operation in multicastTransportOperations)
                {
                    tasks.Add(PublishMessage(operation, channel));
                }

                await (tasks.Count == 1 ? tasks[0] : Task.WhenAll(tasks)).ConfigureAwait(false);
            }
            finally
            {
                channelProvider.ReturnPublishChannel(channel);
            }
        }

        Task SendMessage(UnicastTransportOperation transportOperation, ConfirmsAwareChannel channel)
        {
            var message = transportOperation.Message;
            
            var properties = channel.RentBasicProperties();
            properties.Fill(message, transportOperation.DeliveryConstraints);

            return channel.SendMessage(transportOperation.Destination, message, properties);
        }

        Task PublishMessage(MulticastTransportOperation transportOperation, ConfirmsAwareChannel channel)
        {
            var message = transportOperation.Message;

            var properties = channel.RentBasicProperties();
            properties.Fill(message, transportOperation.DeliveryConstraints);

            return channel.PublishMessage(transportOperation.MessageType, message, properties);
        }
    }
}
