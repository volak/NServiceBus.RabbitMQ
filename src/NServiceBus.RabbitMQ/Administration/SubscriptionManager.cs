namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Threading.Tasks;
    using Extensibility;

    class SubscriptionManager : IManageSubscriptions
    {
        readonly ConnectionFactory connectionFactory;
        readonly IRoutingTopology routingTopology;
        readonly string localQueue;

        public SubscriptionManager(ConnectionFactory connectionFactory, IRoutingTopology routingTopology, string localQueue)
        {
            this.connectionFactory = connectionFactory;
            this.routingTopology = routingTopology;
            this.localQueue = localQueue;
        }

        public async Task Subscribe(Type eventType, ContextBag context)
        {
            using (var connection = await connectionFactory.CreateAdministrationConnection().ConfigureAwait(false))
            using (var channel = await connection.CreateChannel().ConfigureAwait(false))
            {
                await routingTopology.SetupSubscription(channel, eventType, localQueue).ConfigureAwait(false);
            }
            
        }

        public async Task Unsubscribe(Type eventType, ContextBag context)
        {
            using (var connection = await connectionFactory.CreateAdministrationConnection().ConfigureAwait(false))
            using (var channel = await connection.CreateChannel().ConfigureAwait(false))
            {
                await routingTopology.TeardownSubscription(channel, eventType, localQueue).ConfigureAwait(false);
            }
            
        }
    }
}