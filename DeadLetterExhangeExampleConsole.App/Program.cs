// See https://aka.ms/new-console-template for more information

using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

//var directExchangeExample = new DirectExchangeExample();

//await directExchangeExample.Example();

//await DeadLetterExchangeExampleMethod();
await DeadLetterExchangeExampleWithRequeueMethod();


async Task DeadLetterExchangeExampleWithRequeueMethod()
{
    Console.WriteLine("Dead Letter Exchange With Requeue");


    const string mainExchange = "main.exchange";
    const string mainQueue = "main.queue";

    const string deadLetterExchange = "dead.letter.exchange";
    const string deadLetterQueue = "dead.letter.queue";

    var connectionFactory = new ConnectionFactory
    {
        Uri = new Uri("amqps://enudhixi:gaGLgyyYazzQvgD8ohyn2ayfg8AzqQp5@gorilla.lmq.cloudamqp.com/enudhixi")
    };


    using var connection = await connectionFactory.CreateConnectionAsync();

    using var channel = await connection.CreateChannelAsync();


    await channel.ExchangeDeclareAsync(mainExchange, ExchangeType.Fanout, true, false);


    // RabbitMQ Queue Types


    var arguments = new Dictionary<string, object>
    {
        { "x-queue-type", "quorum" },
        { "x-dead-letter-exchange", deadLetterExchange },
        { "x-message-ttl", 10000 },
        { "x-delivery-limit", 3 }
    };

    await channel.QueueDeclareAsync(mainQueue, true, false, false, arguments);


    await channel.QueueBindAsync(mainQueue, mainExchange, string.Empty);


    // Declare Dead Letter Exchange and Queue

    await channel.ExchangeDeclareAsync(deadLetterExchange, ExchangeType.Fanout, true, false);

    await channel.QueueDeclareAsync(deadLetterQueue, true, false, false);

    await channel.QueueBindAsync(deadLetterQueue, deadLetterExchange, string.Empty);


    // send message to main exchange
    var messageBody = "Hello, World!";


    var properties = new BasicProperties();

    properties.Persistent = true;
    //properties.Expiration = "10000"; // 10 saniye (milisaniye cinsinden)


    var body = Encoding.UTF8.GetBytes(messageBody);
    await channel.BasicPublishAsync(mainExchange, string.Empty, true, body);


    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += async (sender, eventArgs) =>
    {
        Console.WriteLine($"Message Processing-{eventArgs.DeliveryTag}");

        try
        {
            var receivedMessage = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            // Acknowledge the message
            await channel.BasicAckAsync(eventArgs.DeliveryTag, false);
            Console.WriteLine($"Message Acknowledged-{eventArgs.DeliveryTag}");
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            await channel!.BasicNackAsync(eventArgs.DeliveryTag, false, true);
        }
    };

    await channel.BasicConsumeAsync("main.queue", false, consumer);

    Console.ReadLine();
}


async Task DeadLetterExchangeExampleMethod()
{
    Console.WriteLine("Dead Letter Exchange");


    const string mainExchange = "main.exchange";
    const string mainQueue = "main.queue";

    const string deadLetterExchange = "dead.letter.exchange";
    const string deadLetterQueue = "dead.letter.queue";

    var connectionFactory = new ConnectionFactory
    {
        Uri = new Uri("amqps://enudhixi:gaGLgyyYazzQvgD8ohyn2ayfg8AzqQp5@gorilla.lmq.cloudamqp.com/enudhixi")
    };


    using var connection = await connectionFactory.CreateConnectionAsync();

    using var channel = await connection.CreateChannelAsync();


    await channel.ExchangeDeclareAsync(mainExchange, ExchangeType.Fanout, true, false);


    var arguments = new Dictionary<string, object>
    {
        { "x-dead-letter-exchange", deadLetterExchange },
        { "x-message-ttl", 10000 }
    };

    await channel.QueueDeclareAsync(mainQueue, true, false, false, arguments);


    await channel.QueueBindAsync(mainQueue, mainExchange, string.Empty);


    // Declare Dead Letter Exchange and Queue

    await channel.ExchangeDeclareAsync(deadLetterExchange, ExchangeType.Fanout, true, false);

    await channel.QueueDeclareAsync(deadLetterQueue, true, false, false);

    await channel.QueueBindAsync(deadLetterQueue, deadLetterExchange, string.Empty);


    // send message to main exchange
    var messageBody = "Hello, World!";


    var properties = new BasicProperties();

    properties.Persistent = true;
    //properties.Expiration = "10000"; // 10 saniye (milisaniye cinsinden)


    var body = Encoding.UTF8.GetBytes(messageBody);
    await channel.BasicPublishAsync(mainExchange, string.Empty, true, body);


    Console.WriteLine("Message sent to main exchange. It will be dead-lettered after TTL expires if not consumed.");

    var consumer = new AsyncEventingBasicConsumer(channel);

    consumer.ReceivedAsync += async (sender, eventArgs) =>
    {
        try
        {
            var receivedMessage = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
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
    Console.ReadLine();
}