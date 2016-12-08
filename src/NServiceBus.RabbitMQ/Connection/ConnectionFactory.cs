
namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Threading.Tasks;
    using global::RabbitMqNext;
    using Settings;
    
    class ConnectionFactory
    {
        readonly Func<String, Task<IConnection>> connectionFactory;
        readonly ReadOnlySettings settings;

        public ConnectionFactory(ReadOnlySettings settings, ConnectionConfiguration connectionConfiguration)
        {
            if (settings == null)
            {
                throw new ArgumentException(nameof(settings));
            }
            if (connectionConfiguration == null)
            {
                throw new ArgumentNullException(nameof(connectionConfiguration));
            }

            if (connectionConfiguration.Host == null)
            {
                throw new ArgumentException("The connectionConfiguration has a null Host.", nameof(connectionConfiguration));
            }
            this.settings = settings;
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

            return connectionFactory($"{settings.InstanceSpecificQueue()} - {connectionName}");

        }

    }
}