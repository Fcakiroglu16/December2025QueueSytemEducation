using Bus.Shared.Events;

namespace RabbitMQApp.API.Repositories;

public class OutBox
{
    public int Id { get; set; }
    public DateTime Created { get; set; }

    public EventType EventType { get; set; }

    public string EventData { get; set; } = string.Empty;

    public Guid IdempotencyKey { get; set; }
    public bool IsSent { get; set; }
}