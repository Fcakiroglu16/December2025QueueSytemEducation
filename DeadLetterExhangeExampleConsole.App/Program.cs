// See https://aka.ms/new-console-template for more information
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

await DeadLetterExchangeExampleMethod();

async Task DeadLetterExchangeExampleMethod()
{
    Console.WriteLine("Dead Letter Exchange");


    const string mainExchange = "main.exchange";
    const string mainQueue = "main.queue";

    const string deadLetterExchange = "dead.letter.exchange";
    const string deadLetterQueue = "dead.letter.queue";

    ConnectionFactory connectionFactory = new ConnectionFactory
    {
        Uri = new Uri("amqps://enudhixi:gaGLgyyYazzQvgD8ohyn2ayfg8AzqQp5@gorilla.lmq.cloudamqp.com/enudhixi")
    };


    using IConnection connection = await connectionFactory.CreateConnectionAsync();

    using IChannel channel = await connection.CreateChannelAsync();



    await channel.ExchangeDeclareAsync(mainExchange, ExchangeType.Fanout, true, false, null);


    Dictionary<string, object> arguments = new Dictionary<string, object>()
    {
        { "x-dead-letter-exchange", deadLetterExchange },
        {"x-message-ttl",10000}
    };

    await channel.QueueDeclareAsync(mainQueue, true, false, false, arguments);





    await channel.QueueBindAsync(mainQueue, mainExchange, string.Empty, null);


// Declare Dead Letter Exchange and Queue

    await channel.ExchangeDeclareAsync(deadLetterExchange, ExchangeType.Fanout, true, false, null);

    await channel.QueueDeclareAsync(deadLetterQueue, true, false, false, null);

    await channel.QueueBindAsync(deadLetterQueue, deadLetterExchange, string.Empty, null);


// send message to main exchange
    string messageBody = "Hello, World!";


    BasicProperties properties = new BasicProperties();

    properties.Persistent = true;
//properties.Expiration = "10000"; // 10 saniye (milisaniye cinsinden)


    byte[] body = System.Text.Encoding.UTF8.GetBytes(messageBody);
    await channel.BasicPublishAsync(mainExchange, string.Empty, true, body);


    Console.WriteLine("Message sent to main exchange. It will be dead-lettered after TTL expires if not consumed.");

    AsyncEventingBasicConsumer consumer = new AsyncEventingBasicConsumer(channel);

    consumer.ReceivedAsync += async (sender, eventArgs) =>
    {
        try
        {
            var receivedMessage = System.Text.Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            Console.WriteLine($"Received message from dead-letter queue: {receivedMessage}");
            // Acknowledge the message
            await channel.BasicAckAsync(eventArgs.DeliveryTag, false);

        }
        catch (Exception)
        {
            await channel!.BasicNackAsync(eventArgs.DeliveryTag, false, false);
        }

    };

    await channel.BasicConsumeAsync(deadLetterQueue, false, consumer);
}


