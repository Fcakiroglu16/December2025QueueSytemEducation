using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DeadLetterExhangeExampleConsole.App;

internal class DirectExchangeExample
{
    public async Task Example()
    {
        const string mainExchange = "main.exchange-direct";
        const string mainQueue = "main-direct.queue";

        const string deadLetterExchange = "dead.letter.direct.exchange";
        const string deadLetterQueue = "dead.letter.direct.queue";

        var connectionFactory = new ConnectionFactory
        {
            Uri = new Uri("amqps://enudhixi:gaGLgyyYazzQvgD8ohyn2ayfg8AzqQp5@gorilla.lmq.cloudamqp.com/enudhixi")
        };


        using var connection = await connectionFactory.CreateConnectionAsync();

        using var channel = await connection.CreateChannelAsync();


        await channel.ExchangeDeclareAsync(mainExchange, ExchangeType.Direct, true, false);


        var arguments = new Dictionary<string, object>
        {
            { "x-dead-letter-exchange", deadLetterExchange },
            { "x-dead-letter-routing-key", "route-key-error" }
        };

        await channel.QueueDeclareAsync(mainQueue, true, false, false, arguments);


        await channel.QueueBindAsync(mainQueue, mainExchange, "abc");


        await channel.ExchangeDeclareAsync(deadLetterExchange, ExchangeType.Direct, true, false);

        await channel.QueueDeclareAsync(deadLetterQueue, true, false, false);

        await channel.QueueBindAsync(deadLetterQueue, deadLetterExchange, "route-key-error");


        // publish

        var messageBody = "Hello, World!";


        var properties = new BasicProperties();

        properties.Persistent = true;


        var body = Encoding.UTF8.GetBytes(messageBody);
        await channel.BasicPublishAsync(mainExchange, "abc", true, body);


        Console.WriteLine("Message sent to main exchange");


        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (sender, eventArgs) =>
        {
            try
            {
                var receivedMessage = Encoding.UTF8.GetString(eventArgs.Body.ToArray());


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