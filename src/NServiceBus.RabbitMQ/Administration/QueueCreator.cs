namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using RabbitMqNext;
    using System.Threading.Tasks;
    using global::RabbitMQ.Client;

    class QueueCreator : ICreateQueues
    {
        readonly ConnectionFactory connectionFactory;
        readonly IRoutingTopology routingTopology;
        readonly bool durableMessagesEnabled;

        public QueueCreator(ConnectionFactory connectionFactory, IRoutingTopology routingTopology, bool durableMessagesEnabled)
        {
            this.connectionFactory = connectionFactory;
            this.routingTopology = routingTopology;
            this.durableMessagesEnabled = durableMessagesEnabled;
        }

        public async Task CreateQueueIfNecessary(QueueBindings queueBindings, string identity)
        {
            using (var connection = connectionFactory.CreateAdministrationConnection())
            using (var channel = connection.CreateModel())
            {
                var queueDeclaringTopology = routingTopology as IDeclareQueues;

                if (queueDeclaringTopology != null)
                {
                    queueDeclaringTopology.DeclareAndInitialize(channel, queueBindings.ReceivingAddresses, queueBindings.SendingAddresses);
                }
                else
                {
                    foreach (var receivingAddress in queueBindings.ReceivingAddresses)
                    {
                		await CreateQueueIfNecessary(channel, receivingAddress);
                    }

                    foreach (var sendingAddress in queueBindings.SendingAddresses)
                    {
                		await CreateQueueIfNecessary(channel, sendingAddress);
                    }
                }
            }

        }

        async Task CreateQueueIfNecessary(IChannel channel, string receivingAddress)
        {
                await channel.QueueDeclare(receivingAddress, false, durableMessagesEnabled, false, false, null, true).ConfigureAwait(false);

                await routingTopology.Initialize(channel, receivingAddress).ConfigureAwait(false);

                await channel.Close().ConfigureAwait(false);
        }
    }
}