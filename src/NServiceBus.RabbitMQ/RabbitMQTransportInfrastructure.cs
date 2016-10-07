namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using Janitor;
    using Performance.TimeToBeReceived;
    using Routing;
    using Settings;
    using RabbitMqNext;
    using Logging;

    [SkipWeaving]
    class RabbitMQTransportInfrastructure : TransportInfrastructure, IDisposable
    {
        readonly static ILog Logger = LogManager.GetLogger("RabbitMqTransport");
        readonly SettingsHolder settings;
        readonly ChannelProvider channelProvider;
        IRoutingTopology routingTopology;
        IConnection connection;

        public RabbitMQTransportInfrastructure(SettingsHolder settings, string connectionString)
        {
            RabbitMqNext.LogAdapter.LogDebugFn = (@class, message, ex) => Logger.Debug($"{@class} {message}", ex);
            RabbitMqNext.LogAdapter.LogWarnFn= (@class, message, ex) => Logger.Warn($"{@class} {message}", ex);
            RabbitMqNext.LogAdapter.LogErrorFn = (@class, message, ex) => Logger.Error($"{@class} {message}", ex);

            this.settings = settings;

            var connectionConfiguration = new ConnectionStringParser(settings).Parse(connectionString);
            connection = global::RabbitMqNext.ConnectionFactory.Connect(
                connectionConfiguration.Host,
                vhost: connectionConfiguration.VirtualHost,
                username: connectionConfiguration.UserName,
                password: connectionConfiguration.Password,
                recoverySettings: new RabbitMqNext.AutoRecoverySettings { Enabled = true, RecoverBindings = true },
                maxChannels: connectionConfiguration.MaxChannels
                ).Result;

            CreateTopology();

            bool usePublisherConfirms;
            if (!settings.TryGet(SettingsKeys.UsePublisherConfirms, out usePublisherConfirms))
            {
                usePublisherConfirms = true;
            }

            channelProvider = new ChannelProvider(connection, routingTopology, usePublisherConfirms);

            RequireOutboxConsent = false;
        }

        public override IEnumerable<Type> DeliveryConstraints => new[] { typeof(DiscardIfNotReceivedBefore), typeof(NonDurableDelivery) };

        public override OutboundRoutingPolicy OutboundRoutingPolicy => new OutboundRoutingPolicy(OutboundRoutingType.Unicast, OutboundRoutingType.Multicast, OutboundRoutingType.Unicast);

        public override TransportTransactionMode TransactionMode => TransportTransactionMode.ReceiveOnly;

        public override EndpointInstance BindToLocalEndpoint(EndpointInstance instance) => instance;

        public override TransportReceiveInfrastructure ConfigureReceiveInfrastructure()
        {
            return new TransportReceiveInfrastructure(
                    () => CreateMessagePump(),
                    () => new QueueCreator(connection, routingTopology, settings.DurableMessagesEnabled()),
                    () => Task.FromResult(ObsoleteAppSettings.Check()));
        }

        public override TransportSendInfrastructure ConfigureSendInfrastructure()
        {
            return new TransportSendInfrastructure(
                () => new MessageDispatcher(channelProvider),
                () => Task.FromResult(StartupCheckResult.Success));
        }

        public override TransportSubscriptionInfrastructure ConfigureSubscriptionInfrastructure()
        {
            return new TransportSubscriptionInfrastructure(() => new SubscriptionManager(connection, routingTopology, settings.LocalAddress()));
        }

        public override string ToTransportAddress(LogicalAddress logicalAddress)
        {
            var queue = new StringBuilder(logicalAddress.EndpointInstance.Endpoint);

            if (logicalAddress.EndpointInstance.Discriminator != null)
            {
                queue.Append("-" + logicalAddress.EndpointInstance.Discriminator);
            }

            if (logicalAddress.Qualifier != null)
            {
                queue.Append("." + logicalAddress.Qualifier);
            }

            return queue.ToString();
        }

        public void Dispose()
        {
            channelProvider.Dispose();
        }

        void CreateTopology()
        {
            if (settings.HasSetting<IRoutingTopology>())
            {
                routingTopology = settings.Get<IRoutingTopology>();
            }
            else
            {
                var durable = settings.DurableMessagesEnabled();

                DirectRoutingTopology.Conventions conventions;

                if (settings.TryGet(out conventions))
                {
                    routingTopology = new DirectRoutingTopology(conventions, durable);
                }
                else
                {
                    routingTopology = new ConventionalRoutingTopology(durable);
                }
            }
        }

        IPushMessages CreateMessagePump()
        {
            MessageConverter messageConverter;

            if (settings.HasSetting(SettingsKeys.CustomMessageIdStrategy))
            {
                messageConverter = new MessageConverter(settings.Get<Func<MessageDelivery, string>>(SettingsKeys.CustomMessageIdStrategy));
            }
            else
            {
                messageConverter = new MessageConverter();
            }

            string hostDisplayName;
            if (!settings.TryGet("NServiceBus.HostInformation.DisplayName", out hostDisplayName))
            {
                hostDisplayName = Support.RuntimeEnvironment.MachineName;
            }

            var consumerTag = $"{hostDisplayName} - {settings.EndpointName()}";

            var queuePurger = new QueuePurger(connection);

            TimeSpan timeToWaitBeforeTriggeringCircuitBreaker;
            if (!settings.TryGet(SettingsKeys.TimeToWaitBeforeTriggeringCircuitBreaker, out timeToWaitBeforeTriggeringCircuitBreaker))
            {
                timeToWaitBeforeTriggeringCircuitBreaker = TimeSpan.FromMinutes(2);
            }

            int prefetchMultiplier;
            if (!settings.TryGet(SettingsKeys.PrefetchMultiplier, out prefetchMultiplier))
            {
                prefetchMultiplier = 3;
            }

            ushort prefetchCount;
            if (!settings.TryGet(SettingsKeys.PrefetchCount, out prefetchCount))
            {
                prefetchCount = 0;
            }

            return new MessagePump(connection, messageConverter, consumerTag, channelProvider, queuePurger, timeToWaitBeforeTriggeringCircuitBreaker, prefetchMultiplier, prefetchCount);
        }
    }
}
