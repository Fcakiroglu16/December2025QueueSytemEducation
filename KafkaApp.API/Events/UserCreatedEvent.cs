namespace KafkaApp.API.Events
{
    public record UserCreatedEvent(Guid UserId, string UserName, string Email);
}
