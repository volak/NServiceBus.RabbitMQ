namespace NServiceBus.Transport.RabbitMQ
{
    using RabbitMqNext;
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;

    class ChannelProvider : IChannelProvider, IDisposable
    {
        public ChannelProvider(ConnectionFactory connectionFactory, IRoutingTopology routingTopology, bool usePublisherConfirms)
        {
            this.connection = new Lazy<IConnection>(() => connectionFactory.CreatePublishConnection().Result);
            this.routingTopology = routingTopology;
            this.usePublisherConfirms = usePublisherConfirms;

            channels = new ConcurrentQueue<ConfirmsAwareChannel>();
        }

        public Task<ConfirmsAwareChannel> GetPublishChannel()
        {
            ConfirmsAwareChannel channel;
            if (!channels.TryDequeue(out channel) || channel.IsClosed)
            {
                channel?.Dispose();
                
                channel = new ConfirmsAwareChannel(connection.Value, routingTopology, usePublisherConfirms);
            }

            return Task.FromResult(channel);
        }

        public void ReturnPublishChannel(ConfirmsAwareChannel channel)
        {
            if (!channel.IsClosed)
            {
                channels.Enqueue(channel);
            }
            else
            {
                channel.Dispose();
            }
        }

        public void Dispose()
        {
            //injected
        }
        
        readonly Lazy<IConnection> connection;
        readonly IRoutingTopology routingTopology;
        readonly bool usePublisherConfirms;
        readonly ConcurrentQueue<ConfirmsAwareChannel> channels;
    }
}
