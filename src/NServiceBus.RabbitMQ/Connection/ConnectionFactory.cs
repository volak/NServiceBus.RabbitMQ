
namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Threading.Tasks;
    using global::RabbitMqNext;

    class ConnectionFactory
    {
        readonly Func<String, Task<IConnection>> connectionFactory;
        readonly Func<string> instanceSpecificQueue;

        public ConnectionFactory(Func<string> instanceSpecificQueue, ConnectionConfiguration connectionConfiguration)
        {
            if (instanceSpecificQueue == null)
            {
                throw new ArgumentException(nameof(instanceSpecificQueue));
            }
            if (connectionConfiguration == null)
            {
                throw new ArgumentNullException(nameof(connectionConfiguration));
            }

            if (connectionConfiguration.Host == null)
            {
                throw new ArgumentException("The connectionConfiguration has a null Host.", nameof(connectionConfiguration));
            }
            this.instanceSpecificQueue = instanceSpecificQueue;
            this.connectionFactory = (connectionName) => RabbitMqNext.ConnectionFactory.Connect(
                connectionConfiguration.Host,
                vhost: connectionConfiguration.VirtualHost,
                username: connectionConfiguration.UserName,
                password: connectionConfiguration.Password,
                recoverySettings: AutoRecoverySettings.All,
                maxChannels: ushort.MaxValue,
                connectionName: connectionName
            );
        }


        public Task<IConnection> CreatePublishConnection() => CreateConnection("Publish");

        public Task<IConnection> CreateAdministrationConnection() => CreateConnection("Administration");

        public Task<IConnection> CreateConnection(string connectionName)
        {
            //connectionFactory.ClientProperties["connected"] = DateTime.Now.ToString("G");

            return connectionFactory($"{instanceSpecificQueue()} - {connectionName}");

        }

    }
}