using System.Threading.Channels;
using ChannelExample.API.Events;

namespace ChannelExample.API.Consumer
{
    public class OrderCreatedEventConsumer(Channel<OrderCreatedEvent> channel) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var reader = channel.Reader;

            while (await reader.WaitToReadAsync(stoppingToken))
            {
                while (reader.TryRead(out OrderCreatedEvent? item))
                {
                    // Process the OrderCreatedEvent
                    Console.WriteLine(
                        $"Processing OrderCreatedEvent: OrderId={item.OrderId}, UserId={item.UserId}, TotalAmount={item.TotalAmount}");
                }
            }
        }
    }
}
