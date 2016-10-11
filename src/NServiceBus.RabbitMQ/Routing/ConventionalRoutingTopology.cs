namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Collections.Concurrent;
    using global::RabbitMqNext;
    using System.Threading.Tasks;
    using Logging;

    /// <summary>
    /// Implements the RabbitMQ routing topology as described at http://codebetter.com/drusellers/2011/05/08/brain-dump-conventional-routing-in-rabbitmq/
    /// take 4:
    /// <list type="bullet">
    /// <item><description>we generate an exchange for each queue so that we can do direct sends to the queue. it is bound as a fanout exchange</description></item>
    /// <item><description> for each message published we generate series of exchanges that go from concrete class to each of its subclass
    /// / interfaces these are linked together from most specific to least specific. This way if you subscribe to the base interface you get
    /// all the messages. or you can be more selective. all exchanges in this situation are bound as fanouts.</description></item>
    /// <item><description>the subscriber declares his own queue and his queue exchange –
    /// he then also declares/binds his exchange to each of the message type exchanges desired</description></item>
    /// <item><description> the publisher discovers all of the exchanges needed for a given message, binds them all up
    /// and then pushes the message into the most specific queue letting RabbitMQ do the fanout for him. (One publish, multiple receivers!)</description></item>
    /// <item><description>we generate an exchange for each queue so that we can do direct sends to the queue. it is bound as a fanout exchange</description></item>
    /// </list>
    /// </summary>
    class ConventionalRoutingTopology : IRoutingTopology
    {
        readonly static ILog Logger = LogManager.GetLogger("ConventionalRoutingTopology");
        readonly bool useDurableExchanges;

        public ConventionalRoutingTopology(bool useDurableExchanges)
        {
            this.useDurableExchanges = useDurableExchanges;
        }

        public async Task SetupSubscription(IChannel channel, Type type, string subscriberName)
        {
            if (type == typeof(IEvent))
            {
                // Make handlers for IEvent handle all events whether they extend IEvent or not
                type = typeof(object);
            }

            await SetupTypeSubscriptions(channel, type).ConfigureAwait(false);
            await channel.ExchangeBind(ExchangeName(type), subscriberName, string.Empty, null, true).ConfigureAwait(false);
        }

        public async Task TeardownSubscription(IChannel channel, Type type, string subscriberName)
        {
            try
            {
                await channel.ExchangeUnbind(ExchangeName(type), subscriberName, string.Empty, null, true).ConfigureAwait(false);
            }
            // ReSharper disable EmptyGeneralCatchClause
            catch (Exception)
            // ReSharper restore EmptyGeneralCatchClause
            {
                // TODO: Any better way to make this idempotent?
            }
        }

        public async Task Publish(IChannel channel, Type type, OutgoingMessage message, BasicProperties properties)
        {
            await SetupTypeSubscriptions(channel, type).ConfigureAwait(false);
            if (channel.IsConfirmationEnabled)
                await channel.BasicPublishWithConfirmation(ExchangeName(type), String.Empty, false, properties, new ArraySegment<byte>(message.Body)).ConfigureAwait(false);
            else
                await channel.BasicPublish(ExchangeName(type), String.Empty, false, properties, message.Body).ConfigureAwait(false);
        }

        public async Task Send(IChannel channel, string address, OutgoingMessage message, BasicProperties properties)
        {
            if (channel.IsConfirmationEnabled)
                await channel.BasicPublishWithConfirmation(address, String.Empty, true, properties, new ArraySegment<byte>(message.Body)).ConfigureAwait(false);
            else
                await channel.BasicPublish(address, String.Empty, true, properties, message.Body).ConfigureAwait(false);
        }

        public async Task RawSendInCaseOfFailure(IChannel channel, string address, byte[] body, BasicProperties properties)
        {
            if (channel.IsConfirmationEnabled)
                await channel.BasicPublishWithConfirmation(address, String.Empty, true, properties, new ArraySegment<byte>(body)).ConfigureAwait(false);
            else
                await channel.BasicPublish(address, String.Empty, true, properties, body).ConfigureAwait(false);
        }

        public async Task Initialize(IChannel channel, string mainQueue)
        {
            await CreateExchange(channel, mainQueue).ConfigureAwait(false);
            await channel.QueueBind(mainQueue, mainQueue, string.Empty, null, true).ConfigureAwait(false);
        }

        static string ExchangeName(Type type) => type.Namespace + ":" + type.Name;

        async Task SetupTypeSubscriptions(IChannel channel, Type type)
        {
            if (type == typeof(Object) || IsTypeTopologyKnownConfigured(type))
            {
                return;
            }

            var typeToProcess = type;
            Logger.Info($"Creating exchange for {typeToProcess.FullName}");
            await CreateExchange(channel, ExchangeName(typeToProcess)).ConfigureAwait(false);
            var baseType = typeToProcess.BaseType;

            while (baseType != null)
            {
                Logger.Info($"Creating exchange for base type {baseType.FullName}");
                await CreateExchange(channel, ExchangeName(baseType)).ConfigureAwait(false);
                await channel.ExchangeBind(ExchangeName(typeToProcess), ExchangeName(baseType), string.Empty, null, true).ConfigureAwait(false);
                typeToProcess = baseType;
                baseType = typeToProcess.BaseType;
            }
            if (type.IsInterface)
            {
                foreach (var interfaceType in type.GetInterfaces())
                {
                    Logger.Info($"Creating exchange for interface type {interfaceType.FullName}");
                    var exchangeName = ExchangeName(interfaceType);

                    await CreateExchange(channel, exchangeName).ConfigureAwait(false);
                    await channel.ExchangeBind(ExchangeName(type), exchangeName, string.Empty, null, true).ConfigureAwait(false);
                }
            }

            MarkTypeConfigured(type);
        }

        void MarkTypeConfigured(Type eventType)
        {
            typeTopologyConfiguredSet[eventType] = null;
        }

        bool IsTypeTopologyKnownConfigured(Type eventType) => typeTopologyConfiguredSet.ContainsKey(eventType);

        async Task CreateExchange(IChannel channel, string exchangeName)
        {
            try
            {
                await channel.ExchangeDeclare(exchangeName, "fanout", useDurableExchanges, false, null, true).ConfigureAwait(false);
            }
            // ReSharper disable EmptyGeneralCatchClause
            catch (Exception)
            // ReSharper restore EmptyGeneralCatchClause
            {
                // TODO: Any better way to make this idempotent?
            }
        }

        readonly ConcurrentDictionary<Type, string> typeTopologyConfiguredSet = new ConcurrentDictionary<Type, string>();
    }
}