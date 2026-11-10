using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Producer
{
    class Program
    {
        private const string ExchangeName = "orders_direct";
        private const int MessagesToSend = 20;
        private const int IntervalMs = 1000;

        private static readonly Dictionary<string, string[]> Bindings = new Dictionary<string, string[]>
        {
            ["new-order"]       = new[] { "new-order", "order" },
            ["order-update"]    = new[] { "order-update" },
            ["order-cancelled"] = new[] { "order-cancelled" },
        };

        private static readonly string[] RoutingKeys = { "new-order", "order", "order-update", "order-cancelled" };
        private static readonly string[] Products = { "Laptop", "Smartphone", "Headphones", "Monitor", "Keyboard" };
        private static readonly string[] Statuses = { "paid", "packed", "shipped", "delivered" };
        private static readonly Random random = new Random();

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: ExchangeName, type: ExchangeType.Direct);

                foreach (var queue in Bindings)
                {
                    channel.QueueDeclare(queue: queue.Key,
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);

                    foreach (var routingKey in queue.Value)
                    {
                        channel.QueueBind(queue: queue.Key, exchange: ExchangeName, routingKey: routingKey);
                        Console.WriteLine($"Binding: {ExchangeName} --[{routingKey}]--> {queue.Key}");
                    }
                }
                Console.WriteLine();

                var keys = Enumerable.Range(0, MessagesToSend)
                                     .Select(i => RoutingKeys[i % RoutingKeys.Length])
                                     .OrderBy(_ => random.Next())
                                     .ToList();

                for (int counter = 0; counter < keys.Count; counter++)
                {
                    Thread.Sleep(IntervalMs);

                    string routingKey = keys[counter];
                    int orderId = 1000 + counter;
                    string message = BuildMessage(routingKey, orderId);

                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: ExchangeName,
                                         routingKey: routingKey,
                                         basicProperties: null,
                                         body: body);

                    Console.WriteLine($"[N:{counter + 1,2}] key '{routingKey}' -> {message}");
                }

                Console.WriteLine();
                Console.WriteLine($"Done: {MessagesToSend} messages sent. Per key: " +
                                  string.Join(", ", keys.GroupBy(k => k).Select(g => $"{g.Key}={g.Count()}")));
                Console.WriteLine("Press Enter to exit.");
                Console.ReadLine();
            }
        }

        private static string BuildMessage(string routingKey, int orderId)
        {
            switch (routingKey)
            {
                case "new-order":
                    return $"Order #{orderId} created: {Products[random.Next(Products.Length)]} x{random.Next(1, 4)}";
                case "order":
                    return $"Order #{orderId}: general order event";
                case "order-update":
                    return $"Order #{orderId} status changed to '{Statuses[random.Next(Statuses.Length)]}'";
                case "order-cancelled":
                    return $"Order #{orderId} cancelled by customer";
                default:
                    return $"Order #{orderId}: unknown event";
            }
        }
    }
}
