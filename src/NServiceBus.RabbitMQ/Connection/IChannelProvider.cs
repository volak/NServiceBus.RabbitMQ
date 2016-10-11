using RabbitMqNext;
using System.Threading.Tasks;

namespace NServiceBus.Transport.RabbitMQ
{
    interface IChannelProvider
    {
        Task<NextChannel> GetPublishChannel();
        void ReturnPublishChannel(NextChannel channel);
    }
}