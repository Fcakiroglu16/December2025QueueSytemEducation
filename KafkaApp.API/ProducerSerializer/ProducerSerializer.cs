using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace KafkaApp.API.ProducerSerializer
{
    public class ProducerSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
        }
    }
}
