﻿namespace NServiceBus.Transport.RabbitMQ
{
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

        public Task CreateQueueIfNecessary(QueueBindings queueBindings, string identity)
        {
            using (var connection = connectionFactory.CreateAdministrationConnection())
            using (var channel = connection.CreateModel())
            {
                foreach (var receivingAddress in queueBindings.ReceivingAddresses)
                {
                    CreateQueueIfNecessary(channel, receivingAddress);
                }

                foreach (var sendingAddress in queueBindings.SendingAddresses)
                {
                    CreateQueueIfNecessary(channel, sendingAddress);
                }
            }

            return TaskEx.CompletedTask;
        }

        void CreateQueueIfNecessary(IModel channel, string receivingAddress)
        {


            routingTopology.Initialize(channel, receivingAddress);
        }
    }
}