using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace Consumer
{
    class Program
    {
        private const string QueueName = "orders-queue";

        private static int receivedCount = 0;

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: QueueName,
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);

                consumer.Received += (sender, e) =>
                {
                    var message = Encoding.UTF8.GetString(e.Body.ToArray());
                    int count = Interlocked.Increment(ref receivedCount);
                    Console.WriteLine($" Received message #{count}: {message}");
                };

                channel.BasicConsume(queue: QueueName,
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine($"Subscribed to the queue '{QueueName}'. Press Enter to exit.");

                Console.ReadLine();
                Console.WriteLine($"Total messages received: {receivedCount}");
            }
        }
    }
}
