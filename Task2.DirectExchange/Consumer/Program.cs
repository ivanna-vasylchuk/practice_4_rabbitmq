using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace Consumer
{
    class Program
    {
        private static readonly string[] AllowedQueues = { "new-order", "order-update", "order-cancelled" };
        private static int receivedCount = 0;

        static void Main(string[] args)
        {
            if (args.Length != 1 || Array.IndexOf(AllowedQueues, args[0]) < 0)
            {
                Console.WriteLine("Usage: dotnet run -- <" + string.Join("|", AllowedQueues) + ">");
                return;
            }

            string queueName = args[0];
            Console.Title = $"Consumer [{queueName}]";

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: queueName,
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);

                consumer.Received += (sender, e) =>
                {
                    var message = Encoding.UTF8.GetString(e.Body.ToArray());
                    int count = Interlocked.Increment(ref receivedCount);
                    Console.WriteLine($" [{queueName}] #{count,2} (key '{e.RoutingKey}'): {message}");
                };

                channel.BasicConsume(queue: queueName,
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine($"Subscribed to the queue '{queueName}'. Press Enter to exit.");

                Console.ReadLine();
                Console.WriteLine($"Total messages received from '{queueName}': {receivedCount}");
            }
        }
    }
}
