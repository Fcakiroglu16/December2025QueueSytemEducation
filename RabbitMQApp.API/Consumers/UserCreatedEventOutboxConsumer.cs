using System.Text.Json;
using Bus.Shared;
using Bus.Shared.Events;
using RabbitMQApp.API.Repositories;

namespace RabbitMQApp.API.Consumers;

public class UserCreatedEventOutboxConsumer(IServiceProvider serviceProvider, IBusService busService)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            using var scope = serviceProvider.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();


            var outboxMessages = dbContext.OutBoxes
                .Where(ob => ob.EventType == EventType.UserCreated && !ob.IsSent).Take(100).ToList();


            foreach (var outboxMessage in outboxMessages)
            {
                var headers = new Dictionary<string, object?>
                {
                    { "idempotency-key", outboxMessage.IdempotencyKey },
                    { "event-type", outboxMessage.EventType }
                };

                var userCreatedEvent = JsonSerializer.Deserialize<UserCreatedEvent>(outboxMessage.EventData);


                await busService.PublishWithAck(userCreatedEvent, headers!);

                outboxMessage.IsSent = true;

                await dbContext.SaveChangesAsync(stoppingToken);
            }
        }
    }
}