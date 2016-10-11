namespace NServiceBus.Transport.RabbitMQ
{
    using RabbitMqNext;
    using System;
    using System.Threading.Tasks;
    using Logging;

    class ChannelProvider : IChannelProvider, IDisposable
    {
        static readonly ILog Logger = LogManager.GetLogger(typeof(ChannelProvider));
        public ChannelProvider(ConnectionFactory connectionFactory, IRoutingTopology routingTopology, bool usePublisherConfirms, ushort keepChannels, int maxUnconfirmed)
        {

            this.connection = new Lazy<IConnection>(() => connectionFactory.CreatePublishConnection().Result);
            this.routingTopology = routingTopology;
            this.usePublisherConfirms = usePublisherConfirms;
            this.keepChannels = keepChannels;
            this.maxUnconfirmed = maxUnconfirmed;

            channels = new NextChannel[keepChannels];
        }

        public Task<NextChannel> GetPublishChannel()
        {
            // Now we can connect
            var connection = this.connection.Value;
            if(connection.IsClosed)
                throw new InvalidOperationException("Tried to create a channel on a closed connection");
            
            var index = (++lastSeq) % keepChannels;

            NextChannel channel;
            lock (channelLock)
            {
                channel = channels[index];
                if (channel == null || channel.IsClosed)
                {
                    Logger.Debug($"Creating a new publish channel {nameof(usePublisherConfirms)}: {usePublisherConfirms} {nameof(maxUnconfirmed)}: {maxUnconfirmed}");
                    channel = new NextChannel(connection, routingTopology, usePublisherConfirms, maxUnconfirmed);
                    channels[index] = channel;
                }
            }
            
            return Task.FromResult(channel);
        }
        
        

        public void Dispose()
        {
            //injected
        }

        ulong lastSeq = 0;
        readonly NextChannel[] channels;

        readonly object channelLock = new object();
        // Its lazy because when ChannelProvider is created the transport is not in the proper state yet
        readonly Lazy<IConnection> connection;
        readonly ushort keepChannels;
        readonly int maxUnconfirmed;
        readonly IRoutingTopology routingTopology;
        readonly bool usePublisherConfirms;
    }
}
