namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Threading.Tasks;
    using Logging;
    using RabbitMqNext;

    [Janitor.SkipWeaving]
    class NextChannel : IDisposable
    {
        public NextChannel(IConnection connection, IRoutingTopology routingTopology, bool usePublisherConfirms, int maxUnconfirmed = 100)
        {
            if (usePublisherConfirms)
                channel = connection.CreateChannelWithPublishConfirmation(maxunconfirmedMessages: maxUnconfirmed).Result;
            else
                channel = connection.CreateChannel().Result;
            
            this.routingTopology = routingTopology;
            
        }

        public BasicProperties RentBasicProperties() => channel.RentBasicProperties();
        public void ReturnBasicProperties(BasicProperties props) => channel.Return(props);

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

        static readonly ILog Logger = LogManager.GetLogger(typeof(NextChannel));
    }
}
