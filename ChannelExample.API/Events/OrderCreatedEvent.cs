namespace ChannelExample.API.Events
{
    public record OrderCreatedEvent(int OrderId, int UserId, decimal TotalAmount);
}
