using System.Text;
using Confluent.Kafka;
using KafkaApp.API.ConsumerSerializer;
using KafkaApp.API.Events;
using static Confluent.Kafka.ConfigPropertyNames;

namespace KafkaApp.API.Consumers
{
    public class MessageConsumerWithComplexType(IConfiguration configuration) : BackgroundService
    {
        private IConsumer<Guid, UserCreatedEvent> consumer;

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                GroupId = "c",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            consumer = new ConsumerBuilder<Guid, UserCreatedEvent>(consumerConfig)
                .SetKeyDeserializer(new ConsumerDeserializer<Guid>())
                .SetValueDeserializer(new ConsumerDeserializer<UserCreatedEvent>()).Build();
            consumer.Subscribe("topic-with-complex-type");
            return base.StartAsync(cancellationToken);
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            consumer.Dispose();
            return base.StopAsync(cancellationToken);
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10));

                    if (consumeResult is null || consumeResult.Message.Value is null)
                    {
                        continue;
                    }


                    // check header version

                    if (consumeResult.Message.Headers.TryGetLastBytes("version", out byte[] versionHeader))
                    {
                        var version = Encoding.UTF8.GetString(versionHeader);
                        Console.WriteLine($"Message version: {version}");
                    }

                    Console.WriteLine(
                        $"Consumed message ' Email={consumeResult.Message.Value.Email}' at: '{consumeResult.TopicPartitionOffset}'.");
                }
                catch (Exception e)
                {
                    //Logging
                    // send message to error-topic
                    Console.WriteLine(e);
                }
            }

            return Task.CompletedTask;
        }
    }
}