using Bus.Shared;
using Bus.Shared.Events;
using RabbitMQApp.API.Repositories;
using System.Text.Json;

namespace RabbitMQApp.API.Consumers
{
    public class UserCreatedEventOutboxConsumer(IServiceProvider serviceProvider, IBusService busService) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {


            while (!stoppingToken.IsCancellationRequested)
            {
                using IServiceScope scope = serviceProvider.CreateScope();
                AppDbContext dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();


                List<OutBox> outboxMessages = dbContext.OutBoxes
                    .Where(ob => ob.EventType == EventType.UserCreated && !ob.IsSent).Take(100).ToList();



                foreach (OutBox outboxMessage in outboxMessages)
                {

                    Dictionary<string, object?> headers = new Dictionary<string, object?>()
                    {
                        { "idempotency-key",outboxMessage.IdempotencyKey },
                        { "event-type", outboxMessage.EventType }
                    };

                    UserCreatedEvent? userCreatedEvent = JsonSerializer.Deserialize<UserCreatedEvent>(outboxMessage.EventData);


                    await busService.PublishWithAck(userCreatedEvent, headers!);

                    outboxMessage.IsSent = true;

                    await dbContext.SaveChangesAsync(stoppingToken);



                }

            }


        }
    }
}
