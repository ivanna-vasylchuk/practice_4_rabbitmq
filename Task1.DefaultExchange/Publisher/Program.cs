using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading;

namespace Publisher
{
    class Program
    {
        private const string QueueName = "orders-queue";

        private const int IntervalMs = 1500;

        private static readonly string[] Products = { "Laptop", "Smartphone", "Headphones", "Monitor", "Keyboard" };
        private static readonly Random random = new Random();

        static void Main(string[] args)
        {
            var counter = 0;
            do
            {
                Thread.Sleep(IntervalMs);

                var factory = new ConnectionFactory() { HostName = "localhost" };
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: QueueName,
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);

                    string product = Products[random.Next(Products.Length)];
                    int quantity = random.Next(1, 4);
                    string message = $"Order #{1000 + counter}: {product} x{quantity} " +
                                     $"(created at {DateTime.Now:HH:mm:ss})";

                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "",
                                         routingKey: QueueName,
                                         basicProperties: null,
                                         body: body);

                    Console.WriteLine($"[N:{counter++}] Sent to '{QueueName}': {message}");
                }
            } while (true);
        }
    }
}
