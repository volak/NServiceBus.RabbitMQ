namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using RabbitMqNext;
    using System.Threading.Tasks;

    /// <summary>
    /// Topology for routing messages on the transport.
    /// </summary>
    public interface IRoutingTopology
    {
        /// <summary>
        /// Sets up a subscription for the subscriber to the specified type.
        /// </summary>
        /// <param name="channel">The RabbitMQ channel to operate on.</param>
        /// <param name="type">The type to subscribe to.</param>
        /// <param name="subscriberName">The name of the subscriber.</param>
        Task SetupSubscription(IChannel channel, Type type, string subscriberName);

        /// <summary>
        /// Removes a subscription for the subscriber to the specified type.
        /// </summary>
        /// <param name="channel">The RabbitMQ channel to operate on.</param>
        /// <param name="type">The type to unsubscribe from.</param>
        /// <param name="subscriberName">The name of the subscriber.</param>
        Task TeardownSubscription(IChannel channel, Type type, string subscriberName);

        /// <summary>
        /// Publishes a message of the specified type.
        /// </summary>
        /// <param name="channel">The RabbitMQ channel to operate on.</param>
        /// <param name="type">The type of the message to be published.</param>
        /// <param name="message">The message to publish.</param>
        /// <param name="properties">The RabbitMQ properties of the message to publish.</param>
        Task Publish(IChannel channel, Type type, OutgoingMessage message, BasicProperties properties);

        /// <summary>
        /// Sends a message to the specified endpoint.
        /// </summary>
        /// <param name="channel">The RabbitMQ channel to operate on.</param>
        /// <param name="address">The address of the destination endpoint.</param>
        /// <param name="message">The message to send.</param>
        /// <param name="properties">The RabbitMQ properties of the message to send.</param>
        Task Send(IChannel channel, string address, OutgoingMessage message, BasicProperties properties);

        /// <summary>
        /// Sends a raw message body to the specified endpoint.
        /// </summary>
        /// <param name="channel">The RabbitMQ channel to operate on.</param>
        /// <param name="address">The address of the destination endpoint.</param>
        /// <param name="body">The raw message body to send.</param>
        /// <param name="properties">The RabbitMQ properties of the message to send.</param>
        Task RawSendInCaseOfFailure(IChannel channel, string address, byte[] body, BasicProperties properties);

        /// <summary>
        /// Performs any initialization logic needed (e.g., creating exchanges and bindings).
        /// </summary>
        /// <param name="channel">The RabbitMQ channel to operate on.</param>
        /// <param name="main">The name of the queue to perform initialization on.</param>
        Task Initialize(IChannel channel, string main);
    }
}