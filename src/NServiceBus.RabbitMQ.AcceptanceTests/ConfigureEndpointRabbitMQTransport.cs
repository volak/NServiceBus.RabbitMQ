using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NServiceBus;
using NServiceBus.AcceptanceTesting.Support;
using NServiceBus.AcceptanceTests.ScenarioDescriptors;
using NServiceBus.Transport.RabbitMQ.AcceptanceTests;
using RabbitMqNext;

class ConfigureScenariosForRabbitMQTransport : IConfigureSupportedScenariosForTestExecution
{
    public IEnumerable<Type> UnsupportedScenarioDescriptorTypes => new[] { typeof(AllDtcTransports), typeof(AllNativeMultiQueueTransactionTransports), typeof(AllTransportsWithMessageDrivenPubSub), typeof(AllTransportsWithoutNativeDeferralAndWithAtomicSendAndReceive) };
}

class ConfigureEndpointRabbitMQTransport : IConfigureEndpointTestExecution
{
    string connectionString;
    string username;
    string password;
    string host;
    string virtualHost;

    public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings)
    {
        connectionString = settings.Get<string>("Transport.ConnectionString");
        configuration.UseTransport<RabbitMQTransport>().ConnectionString(connectionString);

        return TaskEx.CompletedTask;
    }

    public Task Cleanup() => PurgeQueues();

    async Task PurgeQueues()
    {
        var connectionFactory = CreateConnectionFactory();

        var queues = await GetQueues(connectionFactory);

        using (var connection = await connectionFactory())
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

    Func<Task<IConnection>> CreateConnectionFactory()
    {
        var match = Regex.Match(connectionString, string.Format("[^\\w]*{0}=(?<{0}>[^;]+)", "host"), RegexOptions.IgnoreCase);

        username = match.Groups["UserName"].Success ? match.Groups["UserName"].Value : "guest";
        password = match.Groups["Password"].Success ? match.Groups["Password"].Value : "guest";
        host = match.Groups["host"].Success ? match.Groups["host"].Value : "localhost";
        virtualHost = match.Groups["VirtualHost"].Success ? match.Groups["VirtualHost"].Value : "/";

        return () => global::RabbitMqNext.ConnectionFactory.Connect(
            host,
            vhost: virtualHost,
            username: username,
            password: password,
            recoverySettings: AutoRecoverySettings.All,
            maxChannels: ushort.MaxValue,
            connectionName: "testing"
            );
    }

    // Requires that the RabbitMQ Management API has been enabled: https://www.rabbitmq.com/management.html
    async Task<IEnumerable<Queue>> GetQueues(Func<Task<IConnection>> connectionFactory)
    {
        var httpClient = CreateHttpClient();

        var queueResult = await httpClient.GetAsync(string.Format(CultureInfo.InvariantCulture, "api/queues/{0}", Uri.EscapeDataString(virtualHost)));
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
            BaseAddress = new Uri(string.Format(CultureInfo.InvariantCulture, "http://{0}:15672/", host))
        };

        httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        return httpClient;
    }

    class Queue
    {
        public string Name { get; set; }
    }
}