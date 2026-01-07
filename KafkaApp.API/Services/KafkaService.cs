using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaApp.API.Services
{
    public class KafkaService(IConfiguration configuration)
    {
        public async Task CreateTopic(string topicName)
        {
            var config = new Confluent.Kafka.AdminClientConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"]
            };

            using var adminClient = new Confluent.Kafka.AdminClientBuilder(config).Build();

            // Create topic with 1 partition and replication factor of 1
            var topicSpecification = new TopicSpecification()
            {
                Name = topicName,
                NumPartitions = 3,
                ReplicationFactor = 1 // 3 => 1 Leader +2 Replica
            };


            await adminClient.CreateTopicsAsync([topicSpecification]);
        }

        //Ack=no,retry=no
        public async Task SendMessageWithAtMostOnce(string topic, string message)
        {
            // Avro
            // Json (int,string)
            // Protobuf

            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                Acks = Acks.None,
                MessageSendMaxRetries = 0
            };

            using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

            var kafkaMessage = new Message<Null, string>
            {
                Value = message
            };

            var deliveryResult = await producer.ProduceAsync(topic, kafkaMessage);

            Console.WriteLine(
                $"Message sent to topic {deliveryResult.Topic}, partition {deliveryResult.Partition}, offset {deliveryResult.Offset}");
        }


        public async Task ConsumeWithMessage(string topic)
        {
            //consumer config
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                GroupId = "a",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
            consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10));

                    if (consumeResult is null || consumeResult.Message.Value is null)
                    {
                        continue;
                    }


                    Console.WriteLine(
                        $"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.");
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumer.Close();
            }
        }
    }
}