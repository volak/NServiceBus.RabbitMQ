using System.Threading.Tasks;

namespace NServiceBus.Transport.RabbitMQ
{
    class QueuePurger
    {
        readonly ConnectionFactory connectionFactory;

        public QueuePurger(ConnectionFactory connectionFactory)
        {
            this.connectionFactory = connectionFactory;
        }

        public async Task Purge(string queue)
        {
            using (var connection = await connectionFactory.CreateAdministrationConnection().ConfigureAwait(false))
            using (var channel = await connection.CreateChannel().ConfigureAwait(false))
            {
                await channel.QueuePurge(queue, true).ConfigureAwait(false);
            }
        }
    }
}
