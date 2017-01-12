using System;
using System.Data.Common;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Settings;
using NServiceBus.TransportTests;
using NServiceBus.Transport;
using RabbitMqNext;

class ConfigureRabbitMQTransportInfrastructure : IConfigureTransportInfrastructure
{
    public TransportConfigurationResult Configure(SettingsHolder settings, TransportTransactionMode transactionMode)
    {
        var result = new TransportConfigurationResult();
        var transport = new RabbitMQTransport();

        var connectionString = Environment.GetEnvironmentVariable("RabbitMQTransport.ConnectionString");

        if (string.IsNullOrEmpty(connectionString))
        {
            throw new Exception("The 'RabbitMQTransport.ConnectionString' environment variable is not set.");
        }

        connectionStringBuilder = new DbConnectionStringBuilder { ConnectionString = connectionString };

        queueBindings = settings.Get<QueueBindings>();

        result.TransportInfrastructure = transport.Initialize(settings, connectionStringBuilder.ConnectionString);
        isTransportInitialized = true;
        result.PurgeInputQueueOnStartup = true;

        transportTransactionMode = result.TransportInfrastructure.TransactionMode;
        requestedTransactionMode = transactionMode;

        return result;
    }

    public async Task Cleanup()
    {
        if (isTransportInitialized && transportTransactionMode >= requestedTransactionMode)
        {
            await PurgeQueues(connectionStringBuilder, queueBindings);
        }
        
    }

    static async Task PurgeQueues(DbConnectionStringBuilder connectionStringBuilder, QueueBindings queueBindings)
    {
        if (connectionStringBuilder == null)
        {
            return;
        }
        
        object value;

        var hostname = "";
        var virtualhost = "";
        var username = "";
        var password = "";

        if (connectionStringBuilder.TryGetValue("username", out value))
        {
            username = value.ToString();
        }

        if (connectionStringBuilder.TryGetValue("password", out value))
        {
            password = value.ToString();
        }

        if (connectionStringBuilder.TryGetValue("virtualhost", out value))
        {
            virtualhost = value.ToString();
        }

        if (connectionStringBuilder.TryGetValue("host", out value))
        {
            hostname = value.ToString();
        }
        else
        {
            throw new Exception("The connection string doesn't contain a value for 'host'.");
        }
        Func<string, Task<IConnection>> factory = name => ConnectionFactory.Connect(hostname, virtualhost, username, password, connectionName: name);

        using (var connection = await factory("Test Queue Purger"))
        using (var channel = await connection.CreateChannel())
        {
            foreach (var queue in queueBindings.ReceivingAddresses)
            {
                await PurgeQueue(channel, queue);
            }

            foreach (var queue in queueBindings.SendingAddresses)
            {
                await PurgeQueue(channel, queue);
            }
        }
    }

    static async Task PurgeQueue(IChannel channel, string queue)
    {
        try
        {
            await channel.QueuePurge(queue, true);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Unable to clear queue '{queue}': {ex}");
        }
    }

    DbConnectionStringBuilder connectionStringBuilder;
    QueueBindings queueBindings;
    TransportTransactionMode transportTransactionMode;
    TransportTransactionMode requestedTransactionMode;
    bool isTransportInitialized;
}
