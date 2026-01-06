using System.Text.Json;
using Bus.Shared;
using Bus.Shared.Events;
using RabbitMQApp.API.Repositories;

namespace RabbitMQApp.API.Consumers;

public class UserCreatedEventInboxConsumer(IServiceProvider serviceProvider, IBusService busService) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            using var scope = serviceProvider.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();


            var inboxMessages = dbContext.Inboxes
                .Where(x => x.EventType == EventType.UserCreated && x.IsProcess == false).Take(100).ToList();


            foreach (var inboxMessage in inboxMessages)
            {
                var userCreatedEvent = JsonSerializer.Deserialize<UserCreatedEvent>(inboxMessage.EventData);


                var discount = new Discount
                {
                    IsUsed = false,
                    Rate = 0.1,
                    UserId = userCreatedEvent!.UserId
                };

                await dbContext.Discounts.AddAsync(discount, stoppingToken);
                inboxMessage.IsProcess = true;

                await dbContext.SaveChangesAsync(stoppingToken);
            }
        }
    }
}