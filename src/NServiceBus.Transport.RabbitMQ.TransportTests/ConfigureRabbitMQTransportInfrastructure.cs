using System;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Settings;
using NServiceBus.TransportTests;
using NServiceBus.Transport;
using System.Text.RegularExpressions;
using RabbitMqNext;

class ConfigureRabbitMQTransportInfrastructure : IConfigureTransportInfrastructure
{

    public TransportConfigurationResult Configure(SettingsHolder settings, TransportTransactionMode transactionMode)
    {
        var result = new TransportConfigurationResult();
        var transport = new RabbitMQTransport();

        connectionString = Environment.GetEnvironmentVariable("RabbitMQTransport.ConnectionString");

        if (connectionString == null)
        {
            connectionString = "host=localhost";
        }

        queueBindings = settings.Get<QueueBindings>();

        result.TransportInfrastructure = transport.Initialize(settings, connectionString);
        result.PurgeInputQueueOnStartup = true;

        transportTransactionMode = result.TransportInfrastructure.TransactionMode;
        requestedTransactionMode = transactionMode;

        return result;
    }

    public async Task Cleanup()
    {
        if (transportTransactionMode >= requestedTransactionMode)
        {
            await PurgeQueues();
        }
    }

    async Task PurgeQueues()
    {
        var connectionFactory = CreateConnectionFactory();

        using (var connection = await connectionFactory("Test Queue Purger"))
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


    Func<String, Task<IConnection>> CreateConnectionFactory()
    {
        var match = Regex.Match(connectionString, string.Format("[^\\w]*{0}=(?<{0}>[^;]+)", "host"), RegexOptions.IgnoreCase);

        username = match.Groups["UserName"].Success ? match.Groups["UserName"].Value : "guest";
        password = match.Groups["Password"].Success ? match.Groups["Password"].Value : "guest";
        host = match.Groups["host"].Success ? match.Groups["host"].Value : "localhost";
        virtualHost = match.Groups["VirtualHost"].Success ? match.Groups["VirtualHost"].Value : "/";

        return (name) => global::RabbitMqNext.ConnectionFactory.Connect(
            host,
            vhost: virtualHost,
            username: username,
            password: password,
            recoverySettings: new RabbitMqNext.AutoRecoverySettings { Enabled = true, RecoverBindings = true },
            connectionName: name
            );
    }

    string connectionString;
    string username;
    string password;
    string host;
    string virtualHost;
    QueueBindings queueBindings;
    TransportTransactionMode transportTransactionMode;
    TransportTransactionMode requestedTransactionMode;
}

