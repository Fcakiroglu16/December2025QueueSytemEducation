using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaApp.API.Events;
using KafkaApp.API.ProducerSerializer;

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
                // in-sync-replicate
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


        public async Task SendMessageWithAtLeastOnce(string topic, string message)
        {
            // Avro
            // Json (int,string)
            // Protobuf

            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                EnableIdempotence = false,
                RetryBackoffMs = 2000
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


        public async Task SendMessageWithAtExactlyOnce(string topic, string message)
        {
            // Avro
            // Json (int,string)
            // Protobuf

            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                EnableIdempotence = true,
                RetryBackoffMs = 2000
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


        public async Task SendMessageWithAtExactlyOnceWithTransaction(string message)
        {
            // Avro
            // Json (int,string)
            // Protobuf

            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                EnableIdempotence = true,
                TransactionalId = "transactional-id-1"
            };

            using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

            var kafkaMessage = new Message<Null, string>
            {
                Value = message
            };


            producer.InitTransactions(TimeSpan.FromSeconds(10));
            producer.BeginTransaction();
            try
            {
                var deliveryResult = await producer.ProduceAsync("topic-at-least-once", kafkaMessage);


                //throw new Exception("db hatası");
                var deliveryResult2 = await producer.ProduceAsync("topic-at-least-once-2", kafkaMessage);


                Console.WriteLine(
                    $"Message sent to topic {deliveryResult.Topic}, partition {deliveryResult.Partition}, offset {deliveryResult.Offset}");
                Console.WriteLine(
                    $"Message sent to topic {deliveryResult2.Topic}, partition {deliveryResult2.Partition}, offset {deliveryResult2.Offset}");


                producer.CommitTransaction();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                producer.AbortTransaction();
            }
        }


        public async Task SendMessageWithAtLeastOnceWithKey(string topic, string message)
        {
            // Avro
            // Json (int,string)
            // Protobuf

            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                EnableIdempotence = false,
                RetryBackoffMs = 2000
            };

            using var producer = new ProducerBuilder<int, string>(producerConfig).Build();

            var kafkaMessage = new Message<int, string>
            {
                Value = message,
                Key = 10
            };

            var deliveryResult = await producer.ProduceAsync(topic, kafkaMessage);

            Console.WriteLine(
                $"Message sent to topic {deliveryResult.Topic}, partition {deliveryResult.Partition}, offset {deliveryResult.Offset}");
        }


        public async Task SendMessageWithAtLeastOnceWithComplexType(string topic)
        {
            // Avro
            // Json (int,string)
            // Protobuf

            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                EnableIdempotence = false,
                RetryBackoffMs = 2000
            };

            using var producer = new ProducerBuilder<Guid, UserCreatedEvent>(producerConfig)
                .SetValueSerializer(new ProducerSerializer<UserCreatedEvent>())
                .SetKeySerializer(new ProducerSerializer<Guid>()).Build();


            var userId = Guid.NewGuid();

            var header = new Headers();
            header.Add("version", Encoding.UTF8.GetBytes("v1"));
            header.Add("content-type", Encoding.UTF8.GetBytes("json"));
            var kafkaMessage = new Message<Guid, UserCreatedEvent>
            {
                Value = new UserCreatedEvent(userId, "ahmet16", "ahmet16@outlook.com"),
                Key = userId,
                Headers = header
            };

            var deliveryResult = await producer.ProduceAsync(topic, kafkaMessage);

            Console.WriteLine(
                $"Message sent to topic {deliveryResult.Topic}, partition {deliveryResult.Partition}, offset {deliveryResult.Offset}");
        }
    }
}