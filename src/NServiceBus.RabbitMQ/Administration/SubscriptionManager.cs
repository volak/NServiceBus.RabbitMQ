namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Threading.Tasks;
    using Extensibility;
    using RabbitMqNext;

    class SubscriptionManager : IManageSubscriptions
    {
        readonly IConnection connection;
        readonly IRoutingTopology routingTopology;
        readonly string localQueue;

        public SubscriptionManager(IConnection connection, IRoutingTopology routingTopology, string localQueue)
        {
            this.connection = connection;
            this.routingTopology = routingTopology;
            this.localQueue = localQueue;
        }

        public async Task Subscribe(Type eventType, ContextBag context)
        {
            using (var channel = await connection.CreateChannel().ConfigureAwait(false))
            {
                await routingTopology.SetupSubscription(channel, eventType, localQueue).ConfigureAwait(false);
            }
            
        }

        public async Task Unsubscribe(Type eventType, ContextBag context)
        {
            using (var channel = await connection.CreateChannel().ConfigureAwait(false))
            {
                await routingTopology.TeardownSubscription(channel, eventType, localQueue).ConfigureAwait(false);
            }
            
        }
    }
}