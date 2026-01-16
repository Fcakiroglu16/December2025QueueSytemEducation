namespace RedisApp.Events
{
    public record UserCreatedEvent(Guid UserId, string UserName);
}
