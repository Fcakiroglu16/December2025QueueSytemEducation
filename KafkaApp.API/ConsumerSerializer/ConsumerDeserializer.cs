using System.Text.Json;
using Confluent.Kafka;

namespace KafkaApp.API.ConsumerSerializer
{
    public class ConsumerDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return JsonSerializer.Deserialize<T>(data)!;
        }
    }
}
