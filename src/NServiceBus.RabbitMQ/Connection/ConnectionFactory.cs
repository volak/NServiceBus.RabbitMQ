
namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Security.Authentication;
    using System.Threading.Tasks;
    using global::RabbitMqNext;

    class ConnectionFactory
    {
        readonly Func<String, Task<IConnection>> connectionFactory;

        public ConnectionFactory(ConnectionConfiguration connectionConfiguration)
        {
            if (connectionConfiguration == null)
            {
                throw new ArgumentNullException(nameof(connectionConfiguration));
            }

            if (connectionConfiguration.Host == null)
            {
                throw new ArgumentException("The connectionConfiguration has a null Host.", nameof(connectionConfiguration));
            }

            connectionFactory = (connectionName) => global::RabbitMqNext.ConnectionFactory.Connect(
                connectionConfiguration.Host,
                vhost: connectionConfiguration.VirtualHost,
                username: connectionConfiguration.UserName,
                password: connectionConfiguration.Password,
                recoverySettings: new RabbitMqNext.AutoRecoverySettings { Enabled = true, RecoverBindings = true },
                maxChannels: connectionConfiguration.MaxChannels,
                connectionName: connectionName
                );
        }

        public Task<IConnection> CreatePublishConnection() => CreateConnection("Publish");

        public Task<IConnection> CreateAdministrationConnection() => CreateConnection("Administration");

        public Task<IConnection> CreateConnection(string connectionName)
        {
            //connectionFactory.ClientProperties["connected"] = DateTime.Now.ToString("G");

            return connectionFactory(connectionName);

        }
    }
}