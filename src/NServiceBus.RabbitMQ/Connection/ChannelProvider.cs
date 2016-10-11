namespace NServiceBus.Transport.RabbitMQ
{
    using RabbitMqNext;
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;
    using Logging;

    class ChannelProvider : IChannelProvider, IDisposable
    {
        static readonly ILog Logger = LogManager.GetLogger(typeof(ChannelProvider));
        public ChannelProvider(ConnectionFactory connectionFactory, IRoutingTopology routingTopology, bool usePublisherConfirms)
        {

            this.connection = new Lazy<IConnection>(() => connectionFactory.CreatePublishConnection().Result);
            this.routingTopology = routingTopology;
            this.usePublisherConfirms = usePublisherConfirms;
            channels = new ConcurrentQueue<NextChannel>();
        }

        public Task<NextChannel> GetPublishChannel()
        {
            // Now we can connect
            var connection = this.connection.Value;
            if(connection.IsClosed)
                throw new InvalidOperationException("Tried to create a channel on a closed connection");
            
            NextChannel channel;
            if (!channels.TryDequeue(out channel) || channel.IsClosed)
            {
                channel?.Dispose();

                channel = new NextChannel(connection, routingTopology, usePublisherConfirms);
            }

            return Task.FromResult(channel);
        }

        public void ReturnPublishChannel(NextChannel channel)
        {
            if (channel.IsClosed)
            {
                channel.Dispose();
                return;
            }
            channels.Enqueue(channel);
        }
        

        public void Dispose()
        {
            //injected
        }
        
        
        // Its lazy because when ChannelProvider is created the transport is not in the proper state yet
        readonly Lazy<IConnection> connection;
        readonly IRoutingTopology routingTopology;
        readonly bool usePublisherConfirms;
        readonly ConcurrentQueue<NextChannel> channels;
    }
}
