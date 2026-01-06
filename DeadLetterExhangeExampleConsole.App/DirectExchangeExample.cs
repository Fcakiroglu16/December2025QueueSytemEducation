using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace DeadLetterExhangeExampleConsole.App
{
    internal class DirectExchangeExample
    {

        public async Task Example()
        {

            const string mainExchange = "main.exchange-direct";
            const string mainQueue = "main-direct.queue";

            const string deadLetterExchange = "dead.letter.direct.exchange";
            const string deadLetterQueue = "dead.letter.direct.queue";

            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                Uri = new Uri("amqps://enudhixi:gaGLgyyYazzQvgD8ohyn2ayfg8AzqQp5@gorilla.lmq.cloudamqp.com/enudhixi")
            };


            using IConnection connection = await connectionFactory.CreateConnectionAsync();

            using IChannel channel = await connection.CreateChannelAsync();



            await channel.ExchangeDeclareAsync(mainExchange, ExchangeType.Direct, true, false, null);


            Dictionary<string, object> arguments = new Dictionary<string, object>()
            {
                { "x-dead-letter-exchange", deadLetterExchange },
                {"x-dead-letter-routing-key","route-key-error"}
            };

            await channel.QueueDeclareAsync(mainQueue, true, false, false, arguments);


            await channel.QueueBindAsync(mainQueue, mainExchange, "abc", null);


            await channel.ExchangeDeclareAsync(deadLetterExchange, ExchangeType.Direct, true, false, null);

            await channel.QueueDeclareAsync(deadLetterQueue, true, false, false, null);

            await channel.QueueBindAsync(deadLetterQueue, deadLetterExchange, "route-key-error", null);



            // publish

            string messageBody = "Hello, World!";


            BasicProperties properties = new BasicProperties();

            properties.Persistent = true;


            byte[] body = System.Text.Encoding.UTF8.GetBytes(messageBody);
            await channel.BasicPublishAsync(mainExchange, "abc", true, body);


            Console.WriteLine("Message sent to main exchange");





            AsyncEventingBasicConsumer consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += async (sender, eventArgs) =>
            {
                try
                {
                    string receivedMessage = System.Text.Encoding.UTF8.GetString(eventArgs.Body.ToArray());


                    Console.WriteLine($"Received Message: {receivedMessage}");

                    await channel.BasicAckAsync(eventArgs.DeliveryTag, false);
                }
                catch (Exception)
                {
                    await channel!.BasicNackAsync(eventArgs.DeliveryTag, false, false);
                }

            };

            await channel.BasicConsumeAsync(mainQueue, false, consumer);
            Console.ReadLine();
        }
    }
}
