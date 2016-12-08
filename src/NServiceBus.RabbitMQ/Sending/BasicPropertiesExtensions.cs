﻿namespace NServiceBus.Transport.RabbitMQ
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using global::RabbitMqNext;
    using DeliveryConstraints;
    using Performance.TimeToBeReceived;

    static class BasicPropertiesExtensions
    {
        public static void Fill(this BasicProperties properties, OutgoingMessage message, List<DeliveryConstraint> deliveryConstraints)
        {
            properties.MessageId = message.MessageId;

            if (message.Headers.ContainsKey(NServiceBus.Headers.CorrelationId))
            {
                properties.CorrelationId = message.Headers[NServiceBus.Headers.CorrelationId];
            }

            DiscardIfNotReceivedBefore timeToBeReceived;

            if (TryGet(deliveryConstraints, out timeToBeReceived) && timeToBeReceived.MaxTime < TimeSpan.MaxValue)
            {
                properties.Expiration = timeToBeReceived.MaxTime.TotalMilliseconds.ToString(CultureInfo.InvariantCulture);
            }

            if(deliveryConstraints.Any(c => c is NonDurableDelivery))
            {
                properties.DeliveryMode = 0x1;
            }
            else
            {
                properties.DeliveryMode = 0x2;
            }
            
            foreach(var header in message.Headers)
                properties.Headers[header.Key] = header.Value;

            string messageTypesHeader;
            if (message.Headers.TryGetValue(NServiceBus.Headers.EnclosedMessageTypes, out messageTypesHeader))
            {
                var index = messageTypesHeader.IndexOf(',');

                if (index > -1)
                {
                    properties.Type = messageTypesHeader.Substring(0, index);
                }
                else
                {
                    properties.Type = messageTypesHeader;
                }
            }

            if (message.Headers.ContainsKey(NServiceBus.Headers.ContentType))
            {
                properties.ContentType = message.Headers[NServiceBus.Headers.ContentType];
            }
            else
            {
                properties.ContentType = "application/octet-stream";
            }

            if (message.Headers.ContainsKey(NServiceBus.Headers.ReplyToAddress))
            {
                properties.ReplyTo = message.Headers[NServiceBus.Headers.ReplyToAddress];
            }
        }

        public static void SetConfirmationId(this BasicProperties properties, ulong confirmationId)
        {
            properties.Headers[confirmationIdHeader] = confirmationId.ToString();
        }

        public static bool TryGetConfirmationId(this BasicProperties properties, out ulong confirmationId)
        {
            confirmationId = 0;

            if (properties.Headers.ContainsKey(confirmationIdHeader))
            {
                var headerBytes = properties.Headers[confirmationIdHeader] as byte[];
                var headerString = Encoding.UTF8.GetString(headerBytes ?? new byte[0]);

                return UInt64.TryParse(headerString, out confirmationId);
            }

            return false;
        }

        static bool TryGet<T>(List<DeliveryConstraint> list, out T constraint) where T : DeliveryConstraint
        {
            constraint = list.OfType<T>().FirstOrDefault();

            return constraint != null;
        }

        const string confirmationIdHeader = "NServiceBus.Transport.RabbitMQ.ConfirmationId";
    }
}
