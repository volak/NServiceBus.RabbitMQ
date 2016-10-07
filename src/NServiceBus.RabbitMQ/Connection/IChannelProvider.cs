using RabbitMqNext;
using System.Threading.Tasks;

namespace NServiceBus.Transport.RabbitMQ
{
    interface IChannelProvider
    {
        Task<ConfirmsAwareChannel> GetPublishChannel();

        void ReturnPublishChannel(ConfirmsAwareChannel channel);
    }
}