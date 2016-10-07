namespace NServiceBus.Transport.RabbitMQ
{
    using RabbitMqNext;
    using System.Threading.Tasks;

    class QueueCreator : ICreateQueues
    {
        readonly IConnection connection;
        readonly IRoutingTopology routingTopology;
        readonly bool durableMessagesEnabled;

        public QueueCreator(IConnection connection, IRoutingTopology routingTopology, bool durableMessagesEnabled)
        {
            this.connection = connection;
            this.routingTopology = routingTopology;
            this.durableMessagesEnabled = durableMessagesEnabled;
        }

        public async Task CreateQueueIfNecessary(QueueBindings queueBindings, string identity)
        {
            foreach (var receivingAddress in queueBindings.ReceivingAddresses)
            {
                await CreateQueueIfNecessary(receivingAddress).ConfigureAwait(false);
            }

            foreach (var sendingAddress in queueBindings.SendingAddresses)
            {
                await CreateQueueIfNecessary(sendingAddress).ConfigureAwait(false);
            }
            
        }

        async Task CreateQueueIfNecessary(string receivingAddress)
        {
            using (var channel = await connection.CreateChannel().ConfigureAwait(false))
            {
                await channel.QueueDeclare(receivingAddress, false, durableMessagesEnabled, false, false, null, true).ConfigureAwait(false);

                await routingTopology.Initialize(channel, receivingAddress).ConfigureAwait(false);
            }
        }
    }
}