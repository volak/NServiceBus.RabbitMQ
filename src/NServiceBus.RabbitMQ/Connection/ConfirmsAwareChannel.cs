namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Threading.Tasks;
    using Logging;
    using RabbitMqNext;

    [Janitor.SkipWeaving]
    class ConfirmsAwareChannel : IDisposable
    {
        public ConfirmsAwareChannel(IConnection connection, IRoutingTopology routingTopology, bool usePublisherConfirms)
        {
            if (usePublisherConfirms)
                channel = connection.CreateChannelWithPublishConfirmation().Result;
            else
                channel = connection.CreateChannel().Result;

            
            this.routingTopology = routingTopology;
            
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
            if (!disposed)
            {
                channel.Close();
                channel.Dispose();
            }
            disposed = true;
        }

        IChannel channel;
        readonly IRoutingTopology routingTopology;
        bool disposed;

        static readonly ILog Logger = LogManager.GetLogger(typeof(ConfirmsAwareChannel));
    }
}
