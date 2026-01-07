using Confluent.Kafka;
using static Confluent.Kafka.ConfigPropertyNames;

namespace KafkaApp.API.Consumers
{
    public class MessageConsumerWithNoAck(IConfiguration configuration) : BackgroundService
    {
        private IConsumer<Null, string> consumer;

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                GroupId = "a",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
            consumer.Subscribe("topic2");
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


                    Console.WriteLine(
                        $"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.");
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