using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading.Channels;

namespace RabbitMqTester
{
    internal class Program
    {
        static void Main(string[] args)
        {
            BuildConnectionClient(out var connection, out var channel, out var exchangeName, out var routingKey, out var queue);

            if (args.Length > 0 && args[0] == "writer")
            {
                PublishMessage(channel, exchangeName, routingKey, queue);
            }
            else
            {
                ReadQueue(channel, queue);
            }
        }

        private static void ReadQueue(IModel channel, string queue)
        {
            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Received {0}", message);
            };

            channel.BasicConsume(queue: queue,
                                 autoAck: true,
                                 consumer: consumer);

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        public static void PublishMessage(IModel channel, string exchangeName, string routingKey, string queue)
        {
            // Binding queue to the exchange
            channel.QueueBind(queue: queue, exchange: exchangeName, routingKey: routingKey);

            string message = "Hello World!";
            var body = Encoding.UTF8.GetBytes(message);

            channel.BasicPublish(exchange: exchangeName,
                                 routingKey: routingKey,
                                 basicProperties: null,
                                 body: body);

            Console.WriteLine(" [x] Sent {0}", message);
        }

        private static void BuildConnectionClient(out IConnection connection,
            out IModel channel,
            out string exchangeName,
            out string routingKey,
            out string queue)
        {
            queue = "_queue_name_";
            exchangeName = "_exchangeName_";
            routingKey = "_routingKey_";

            var factory = new ConnectionFactory()
            {
                HostName = "_hostname_",
                Port = 1234,
                VirtualHost = "_virtualhost_", // replace with your RabbitMQ host if needed
                UserName = "_username_", // replace with your RabbitMQ username
                Password = "_password_" // replace with your RabbitMQ password
            };

            connection = factory.CreateConnection();
            channel = connection.CreateModel();

            // Declaring the exchange as topic
            channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Topic, durable: true);

            channel.QueueDeclare(queue: queue,
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);
        }
    }
}
