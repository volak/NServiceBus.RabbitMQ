
namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using global::RabbitMqNext;

    [Janitor.SkipWeaving]
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
            connectionFactory = (connectionName) => RabbitMqNext.ConnectionFactory.Connect(
                connectionConfiguration.Host,
                vhost: connectionConfiguration.VirtualHost,
                username: connectionConfiguration.UserName,
                password: connectionConfiguration.Password,
                //recoverySettings: AutoRecoverySettings.All,
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