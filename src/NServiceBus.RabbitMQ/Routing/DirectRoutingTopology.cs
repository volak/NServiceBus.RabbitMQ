namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using global::RabbitMqNext;
    using System.Threading.Tasks;

    /// <summary>
    /// Route using a static routing convention for routing messages from publishers to subscribers using routing keys.
    /// </summary>
    class DirectRoutingTopology : IRoutingTopology, IDeclareQueues
    {
        public DirectRoutingTopology(Conventions conventions, bool useDurableExchanges)
        {
            this.conventions = conventions;
            this.useDurableExchanges = useDurableExchanges;
        }

        public async Task SetupSubscription(IChannel channel, Type type, string subscriberName)
        {
            await CreateExchange(channel, ExchangeName()).ConfigureAwait(false);
            await channel.QueueBind(subscriberName, ExchangeName(), GetRoutingKeyForBinding(type), null, true).ConfigureAwait(false);
        }

        public async Task TeardownSubscription(IChannel channel, Type type, string subscriberName)
        {
            await channel.QueueUnbind(subscriberName, ExchangeName(), GetRoutingKeyForBinding(type), null).ConfigureAwait(false);
        }

        public async Task Publish(IChannel channel, Type type, OutgoingMessage message, BasicProperties properties)
        {
            if (channel.IsConfirmationEnabled)
                await channel.BasicPublishWithConfirmation(ExchangeName(), GetRoutingKeyForPublish(type), false, properties, new ArraySegment<byte>(message.Body)).ConfigureAwait(false);
            else
                await channel.BasicPublish(ExchangeName(), GetRoutingKeyForPublish(type), false, properties, message.Body).ConfigureAwait(false);
        }

        public async Task Send(IChannel channel, string address, OutgoingMessage message, BasicProperties properties)
        {
            if (channel.IsConfirmationEnabled)
                await channel.BasicPublishWithConfirmation(string.Empty, address, true, properties, new ArraySegment<byte>(message.Body)).ConfigureAwait(false);
            else
                await channel.BasicPublish(string.Empty, address, true, properties, message.Body).ConfigureAwait(false);
        }

        public async Task RawSendInCaseOfFailure(IChannel channel, string address, byte[] body, BasicProperties properties)
        {
            if (channel.IsConfirmationEnabled)
                await channel.BasicPublishWithConfirmation(string.Empty, address, true, properties, new ArraySegment<byte>(body)).ConfigureAwait(false);
            else
                await channel.BasicPublish(string.Empty, address, true, properties, body).ConfigureAwait(false);
        }
		

        public async Task DeclareAndInitialize(IChannel channel, IEnumerable<string> receivingAddresses, IEnumerable<string> sendingAddresses)
        {
            foreach (var address in receivingAddresses.Concat(sendingAddresses))
            {
                await channel.QueueDeclare(address, useDurableExchanges, false, false, false, null, true).ConfigureAwait(false);
            }
        }

        public Task Initialize(IChannel channel, string main)
        {
            return Task.CompletedTask;
            //nothing needs to be done for direct routing
        }

        string ExchangeName() => conventions.ExchangeName(null, null);

        async Task CreateExchange(IChannel channel, string exchangeName)
        {
            if (exchangeName == AmqpTopicExchange)
            {
                return;
            }

            try
            {
                await channel.ExchangeDeclare(exchangeName, "topic", useDurableExchanges, false, null, true).ConfigureAwait(false);
            }
            // ReSharper disable EmptyGeneralCatchClause
            catch (Exception)
            // ReSharper restore EmptyGeneralCatchClause
            {

            }
        }

        string GetRoutingKeyForPublish(Type eventType) => conventions.RoutingKey(eventType);

        string GetRoutingKeyForBinding(Type eventType)
        {
            if (eventType == typeof(IEvent) || eventType == typeof(object))
            {
                return "#";
            }

            return conventions.RoutingKey(eventType) + ".#";
        }

        const string AmqpTopicExchange = "amq.topic";

        readonly Conventions conventions;
        readonly bool useDurableExchanges;

        public class Conventions
        {
            public Conventions(Func<string, Type, string> exchangeName, Func<Type, string> routingKey)
            {
                ExchangeName = exchangeName;
                RoutingKey = routingKey;
            }

            public Func<string, Type, string> ExchangeName { get; }

            public Func<Type, string> RoutingKey { get; }
        }
    }
}