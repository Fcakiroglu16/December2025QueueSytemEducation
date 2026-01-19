using System.Threading.Channels;
using ChannelExample.API.Events;

namespace ChannelExample.API.Consumer
{
    public class UserCreatedEventConsumer(Channel<UserCreatedEvent> channel) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var reader = channel.Reader;

            await foreach (var userCreatedEvent in reader.ReadAllAsync(stoppingToken))
            {
                // Process the UserCreatedEvent
                Console.WriteLine(
                    $"Processing UserCreatedEvent: UserId={userCreatedEvent.UserId}, UserName={userCreatedEvent.UserName}");
            }
        }
    }
}