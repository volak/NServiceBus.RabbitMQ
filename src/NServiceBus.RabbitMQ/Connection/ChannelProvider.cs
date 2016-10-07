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
            connection = new Lazy<Task<IConnection>>(() => connectionFactory.CreatePublishConnection());

            this.routingTopology = routingTopology;
            this.usePublisherConfirms = usePublisherConfirms;

            channels = new ConcurrentQueue<ConfirmsAwareChannel>();
        }

        public async Task<ConfirmsAwareChannel> GetPublishChannel()
        {
            ConfirmsAwareChannel channel;
            if (!channels.TryDequeue(out channel) || channel.IsClosed)
            {
                channel?.Dispose();

                channel = new ConfirmsAwareChannel(await connection.Value.ConfigureAwait(false), routingTopology, usePublisherConfirms);
            }

            return channel;
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

        void DisposeManaged()
        {
            if (connection.IsValueCreated)
            {
                connection.Value.Dispose();
            }
        }

        readonly Lazy<Task<IConnection>> connection;
        readonly IRoutingTopology routingTopology;
        readonly bool usePublisherConfirms;
        readonly ConcurrentQueue<ConfirmsAwareChannel> channels;
    }
}
