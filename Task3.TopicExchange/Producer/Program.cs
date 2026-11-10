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
        private const string ExchangeName = "products_topic";
        private const int IntervalMs = 1000;

        private static readonly Dictionary<string, string[]> Categories = new Dictionary<string, string[]>
        {
            ["electronics"] = new[] { "Laptop", "Smartphone", "Headphones", "Smart watch" },
            ["clothing"]    = new[] { "Jacket", "Jeans", "T-shirt", "Sneakers" },
            ["beauty"]      = new[] { "Lipstick", "Face cream", "Perfume", "Shampoo" },
            ["books"]       = new[] { "Kobzar", "Clean Code", "Dune", "1984" },
        };

        private static readonly Random random = new Random();

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: ExchangeName, type: ExchangeType.Topic);

                foreach (var category in Categories.Keys)
                {
                    string queueName = $"{category}-queue";
                    string bindingKey = $"product.{category}";

                    channel.QueueDeclare(queue: queueName,
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);
                    channel.QueueBind(queue: queueName, exchange: ExchangeName, routingKey: bindingKey);

                    Console.WriteLine($"Binding: {ExchangeName} --[{bindingKey}]--> {queueName}");
                }
                Console.WriteLine();

                int unroutedCount = 0;
                channel.BasicReturn += (sender, e) =>
                {
                    Interlocked.Increment(ref unroutedCount);
                    Console.WriteLine($"   !! NOT ROUTED (key '{e.RoutingKey}'): no queue matches this key");
                };

                var routingKeys = new List<string>();
                routingKeys.AddRange(Enumerable.Repeat("product.electronics", 5));
                routingKeys.AddRange(Enumerable.Repeat("product.clothing", 5));
                routingKeys.AddRange(Enumerable.Repeat("product.beauty", 4));
                routingKeys.AddRange(Enumerable.Repeat("product.books", 4));
                routingKeys.Add("product.toys");
                routingKeys.Add("product.electronics.sale");
                routingKeys = routingKeys.OrderBy(_ => random.Next()).ToList();

                for (int counter = 0; counter < routingKeys.Count; counter++)
                {
                    Thread.Sleep(IntervalMs);

                    string routingKey = routingKeys[counter];
                    string message = BuildMessage(routingKey, counter + 1);
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: ExchangeName,
                                         routingKey: routingKey,
                                         mandatory: true,
                                         basicProperties: null,
                                         body: body);

                    Console.WriteLine($"[N:{counter + 1,2}] key '{routingKey}' -> {message}");
                }

                Thread.Sleep(1000);

                Console.WriteLine();
                Console.WriteLine($"Done: {routingKeys.Count} messages sent, {unroutedCount} not routed.");
                Console.WriteLine("Press Enter to exit.");
                Console.ReadLine();
            }
        }

        private static string BuildMessage(string routingKey, int number)
        {
            string category = routingKey.Split('.')[1];
            string product = Categories.TryGetValue(category, out var products)
                ? products[random.Next(products.Length)]
                : "Unknown item";
            int price = random.Next(100, 5000);
            return $"Product #{number}: {product} ({category}), price {price} UAH";
        }
    }
}
