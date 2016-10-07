namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;
    using Logging;
    using RabbitMqNext;

    class ConfirmsAwareChannel : IDisposable
    {
        public ConfirmsAwareChannel(IConnection connection, IRoutingTopology routingTopology, bool usePublisherConfirms)
        {
            if (usePublisherConfirms)
                channel = connection.CreateChannelWithPublishConfirmation().Result;
            else
                channel = connection.CreateChannel().Result;

            
            this.routingTopology = routingTopology;
            this.usePublisherConfirms = usePublisherConfirms;
            
            
        }

        public BasicProperties CreateBasicProperties() => channel.RentBasicProperties();

        public bool IsOpen => !channel.IsClosed;

        public bool IsClosed => channel.IsClosed;

        public Task SendMessage(string address, OutgoingMessage message, BasicProperties properties)
        {
            return routingTopology.Send(channel, address, message, properties);
        }

        public Task PublishMessage(Type type, OutgoingMessage message, BasicProperties properties)
        {
            return routingTopology.Publish(channel, type, message, properties);
        }

        public Task RawSendInCaseOfFailure(string address, byte[] body, BasicProperties properties)
        {
            return routingTopology.RawSendInCaseOfFailure(channel, address, body, properties);
        }
        
        public void Dispose()
        {
            //injected
        }

        IChannel channel;
        readonly IRoutingTopology routingTopology;
        readonly bool usePublisherConfirms;

        static readonly ILog Logger = LogManager.GetLogger(typeof(ConfirmsAwareChannel));
    }
}
