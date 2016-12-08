namespace NServiceBus.Transport.RabbitMQ
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using RabbitMqNext;

    /// <summary>
    /// An extension for <see cref="IRoutingTopology"/> implementations which allows
    /// control over the declaration of queues as well as other initialization.
    /// The <see cref="DeclareAndInitialize(IChannel, IEnumerable{string}, IEnumerable{string})"/> method will be called
    /// on all implementations instead of the <see cref="IRoutingTopology.Initialize(IChannel, string)"/> method.
    /// </summary>
    public interface IDeclareQueues
    {
        /// <summary>
        /// Declares queues and performs any other initialization logic needed (e.g. creating exchanges and bindings).
        /// </summary>
        /// <param name="channel">The RabbitMQ channel to operate on.</param>
        /// <param name="receivingAddresses">
        /// The addresses of the queues to declare and perform initialization for, that this endpoint is receiving from.
        /// </param>
        /// <param name="sendingAddresses">
        /// The addresses of the queues to declare and perform initialization for, that this endpoint is sending to.
        /// </param>
        Task DeclareAndInitialize(IChannel channel, IEnumerable<string> receivingAddresses, IEnumerable<string> sendingAddresses);
    }
}
