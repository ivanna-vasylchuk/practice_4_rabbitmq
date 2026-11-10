using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace Consumer
{
    class Program
    {
        private const string ExchangeName = "products_topic";
        private static int receivedCount = 0;

        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Usage: dotnet run -- <category>   (electronics | clothing | beauty | books)");
                return;
            }

            string category = args[0].ToLowerInvariant();
            string queueName = $"{category}-queue";
            string bindingKey = $"product.{category}";
            Console.Title = $"Consumer [{bindingKey}]";

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: ExchangeName, type: ExchangeType.Topic);
                channel.QueueDeclare(queue: queueName,
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);
                channel.QueueBind(queue: queueName, exchange: ExchangeName, routingKey: bindingKey);

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

                Console.WriteLine($"Subscribed to the queue '{queueName}'");
                Console.WriteLine($"Listening to [{bindingKey}]. Press Enter to exit.");

                Console.ReadLine();
                Console.WriteLine($"Total messages received from '{queueName}': {receivedCount}");
            }
        }
    }
}
