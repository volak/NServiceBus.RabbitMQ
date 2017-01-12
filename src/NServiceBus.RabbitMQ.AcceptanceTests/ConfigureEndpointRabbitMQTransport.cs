using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NServiceBus;
using NServiceBus.AcceptanceTesting.Support;
using NServiceBus.AcceptanceTests.ScenarioDescriptors;
using NServiceBus.Transport.RabbitMQ.AcceptanceTests;
using RabbitMqNext;

class ConfigureScenariosForRabbitMQTransport : IConfigureSupportedScenariosForTestExecution
{
    public IEnumerable<Type> UnsupportedScenarioDescriptorTypes => new[] { typeof(AllDtcTransports), typeof(AllNativeMultiQueueTransactionTransports), typeof(AllTransportsWithMessageDrivenPubSub) };
}

class ConfigureEndpointRabbitMQTransport : IConfigureEndpointTestExecution
{
    DbConnectionStringBuilder connectionStringBuilder;
    string username;
    string password;
    string virtualhost;
    string hostname;

    public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings)
    {
        var connectionString = Environment.GetEnvironmentVariable("RabbitMQTransport.ConnectionString");

        if (string.IsNullOrEmpty(connectionString))
        {
            throw new Exception("The 'RabbitMQTransport.ConnectionString' environment variable is not set.");
        }

        connectionStringBuilder = new DbConnectionStringBuilder { ConnectionString = connectionString };

        configuration.UseTransport<RabbitMQTransport>().ConnectionString(connectionStringBuilder.ConnectionString);

        return TaskEx.CompletedTask;
    }

    public Task Cleanup() => PurgeQueues();

    async Task PurgeQueues()
    {
        if (connectionStringBuilder == null)
        {
            return;
        }
        
        object value;

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

        var queues = await GetQueues(factory);

        using (var connection = await factory("Test Queue Purger"))
        using (var channel = await connection.CreateChannelWithPublishConfirmation())
        {
            foreach (var queue in queues)
            {
                try
                {
                    await channel.QueuePurge(queue.Name, true);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Unable to clear queue {0}: {1}", queue.Name, ex);
                }
            }
        }
    }

    // Requires that the RabbitMQ Management API has been enabled: https://www.rabbitmq.com/management.html
    async Task<IEnumerable<Queue>> GetQueues(Func<string, Task<IConnection>> connectionFactory)
    {
        var httpClient = CreateHttpClient();

        var queueResult = await httpClient.GetAsync(string.Format(CultureInfo.InvariantCulture, "api/queues/{0}", Uri.EscapeDataString(virtualhost)));
        queueResult.EnsureSuccessStatusCode();

        var content = await queueResult.Content.ReadAsStringAsync();

        return JsonConvert.DeserializeObject<List<Queue>>(content);
    }

    HttpClient CreateHttpClient()
    {
        var handler = new HttpClientHandler
        {
            Credentials = new NetworkCredential(username, password)
        };

        var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri(string.Format(CultureInfo.InvariantCulture, "http://{0}:15672/", hostname))
        };

        httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        return httpClient;
    }

    class Queue
    {
        public string Name { get; set; }
    }
}