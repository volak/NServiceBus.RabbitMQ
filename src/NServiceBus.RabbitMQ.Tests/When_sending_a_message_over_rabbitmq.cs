namespace NServiceBus.Transport.RabbitMQ.Tests
{
    using System;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Extensibility;
    using NUnit.Framework;
    using RabbitMqNext;
    using System.IO;

    [TestFixture]
    class When_sending_a_message_over_rabbitmq : RabbitMqContext
    {
        [Test]
        public Task Should_populate_the_body()
        {
            var body = Encoding.UTF8.GetBytes("<TestMessage/>");

            return Verify(new OutgoingMessageBuilder().WithBody(body), (IncomingMessage received) => Assert.AreEqual(body, received.Body));
        }

        [Test]
        public Task Should_set_the_content_type()
        {
            return Verify(new OutgoingMessageBuilder().WithHeader(Headers.ContentType, "application/json"), received => Assert.AreEqual("application/json", received.properties.ContentType));
        }

        [Test]
        public Task Should_default_the_content_type_to_octet_stream_when_no_content_type_is_specified()
        {
            return Verify(new OutgoingMessageBuilder(), received => Assert.AreEqual("application/octet-stream", received.properties.ContentType));
        }

        [Test]
        public Task Should_set_the_message_type_based_on_the_encoded_message_types_header()
        {
            var messageType = typeof(MyMessage);

            return Verify(new OutgoingMessageBuilder().WithHeader(Headers.EnclosedMessageTypes, messageType.AssemblyQualifiedName), received => Assert.AreEqual(messageType.FullName, received.properties.Type));
        }

        [Test]
        public Task Should_set_the_time_to_be_received()
        {
            var timeToBeReceived = TimeSpan.FromDays(1);

            return Verify(new OutgoingMessageBuilder().TimeToBeReceived(timeToBeReceived), received => Assert.AreEqual(timeToBeReceived.TotalMilliseconds.ToString(), received.properties.Expiration));
        }

        [Test]
        public Task Should_set_the_reply_to_address()
        {
            var address = "myAddress";

            return Verify(new OutgoingMessageBuilder().ReplyToAddress(address),
                (t, r) =>
                {
                    Assert.AreEqual(address, t.Headers[Headers.ReplyToAddress]);
                    Assert.AreEqual(address, r.properties.ReplyTo);
                });
        }

        [Test]
        public Task Should_set_correlation_id_if_present()
        {
            var correlationId = Guid.NewGuid().ToString();

            return Verify(new OutgoingMessageBuilder().CorrelationId(correlationId), result => Assert.AreEqual(correlationId, result.Headers[Headers.CorrelationId]));
        }

        [Test]
        public Task Should_preserve_the_recoverable_setting_if_set_to_durable()
        {
            return Verify(new OutgoingMessageBuilder(), result => Assert.True(result.Headers[Headers.NonDurableMessage] == "False"));
        }

        [Test]
        public Task Should_preserve_the_recoverable_setting_if_set_to_non_durable()
        {
            return Verify(new OutgoingMessageBuilder().NonDurable(), result => Assert.True(result.Headers[Headers.NonDurableMessage] == "True"));
        }

        [Test]
        public Task Should_transmit_all_transportMessage_headers()
        {
            return Verify(new OutgoingMessageBuilder().WithHeader("h1", "v1").WithHeader("h2", "v2"),
                result =>
                {
                    Assert.AreEqual("v1", result.Headers["h1"]);
                    Assert.AreEqual("v2", result.Headers["h2"]);
                });
        }

        async Task Verify(OutgoingMessageBuilder builder, Action<IncomingMessage, MessageDelivery> assertion, string queueToReceiveOn = "testEndPoint")
        {
            var operations = builder.SendTo(queueToReceiveOn).Build();

            await MakeSureQueueAndExchangeExists(queueToReceiveOn);

            await messageDispatcher.Dispatch(operations, new TransportTransaction(), new ContextBag());

            var messageId = operations.MulticastTransportOperations.FirstOrDefault()?.Message.MessageId ?? operations.UnicastTransportOperations.FirstOrDefault()?.Message.MessageId;

            var result = await Consume(messageId, queueToReceiveOn);

            var converter = new MessageConverter();

            var body = new byte[result.bodySize];
            await result.stream.ReadAsync(body, 0, result.bodySize);
            var incomingMessage = new IncomingMessage(
                converter.RetrieveMessageId(result),
                converter.RetrieveHeaders(result),
                body
            );

            assertion(incomingMessage, result);
        }

        Task Verify(OutgoingMessageBuilder builder, Action<IncomingMessage> assertion, string queueToReceiveOn = "testEndPoint") => Verify(builder, (t, r) => assertion(t), queueToReceiveOn);

        Task Verify(OutgoingMessageBuilder builder, Action<MessageDelivery> assertion, string queueToReceiveOn = "testEndPoint") => Verify(builder, (t, r) => assertion(r), queueToReceiveOn);

        async Task<MessageDelivery> Consume(string id, string queueToReceiveOn)
        {
            var connection = await connectionFactory.CreateAdministrationConnection().ConfigureAwait(false);
            using (var channel = await connection.CreateChannel())
            {
                // Doesnt support BasicGet like this
                MessageDelivery message = null;// channel.BasicGet(queueToReceiveOn, false);

                if (message == null)
                {
                    throw new InvalidOperationException("No message found in queue");
                }

                if (message.properties.MessageId != id)
                {
                    throw new InvalidOperationException("Unexpected message found in queue");
                }

                channel.BasicAck(message.deliveryTag, false);

                var clone = new MemoryStream();
                message.stream.CopyTo(clone);
                return new MessageDelivery() { deliveryTag = message.deliveryTag, redelivered = message.redelivered, routingKey = message.routingKey, properties = message.properties, stream = clone };
            }
        }

        class MyMessage
        {

        }
    }
}