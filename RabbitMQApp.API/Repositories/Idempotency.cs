using Bus.Shared.Events;

namespace RabbitMQApp.API.Repositories
{
    public class Idempotency
    {
        public Guid Key { get; set; }

        public EventType EventType { get; set; }
        public DateTime Created { get; set; }
    }
}
