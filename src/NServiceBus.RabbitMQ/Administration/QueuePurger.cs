using RabbitMqNext;
using System.Threading.Tasks;

namespace NServiceBus.Transport.RabbitMQ
{
    class QueuePurger
    {
        readonly IConnection connection;

        public QueuePurger(IConnection connection)
        {
            this.connection = connection;
        }

        public async Task Purge(string queue)
        {
            using (var channel = await connection.CreateChannel().ConfigureAwait(false))
            {
                await channel.QueuePurge(queue, true).ConfigureAwait(false);
            }
        }
    }
}
