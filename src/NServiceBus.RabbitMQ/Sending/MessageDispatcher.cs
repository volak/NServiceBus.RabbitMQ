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

        async Task SendMessage(UnicastTransportOperation transportOperation, NextChannel channel)
        {
            var message = transportOperation.Message;

            var properties = channel.RentBasicProperties();
            properties.Fill(message, transportOperation.DeliveryConstraints);

            await channel.SendMessage(transportOperation.Destination, message, properties).ConfigureAwait(false);

            channel.ReturnBasicProperties(properties);
        }

        async Task PublishMessage(MulticastTransportOperation transportOperation, NextChannel channel)
        {
            var message = transportOperation.Message;

            var properties = channel.RentBasicProperties();
            properties.Fill(message, transportOperation.DeliveryConstraints);

            await channel.PublishMessage(transportOperation.MessageType, message, properties).ConfigureAwait(false);

            channel.ReturnBasicProperties(properties);
        }
    }
}
