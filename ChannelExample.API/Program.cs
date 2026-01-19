using ChannelExample.API.Consumer;
using ChannelExample.API.Events;
using System.Threading.Channels;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();


builder.Services.AddSingleton(Channel.CreateBounded<OrderCreatedEvent>(new BoundedChannelOptions(100)
{
    SingleReader = true,
    SingleWriter = true,
    FullMode = BoundedChannelFullMode.Wait,
    AllowSynchronousContinuations = false
}));


builder.Services.AddSingleton(Channel.CreateUnbounded<UserCreatedEvent>(new UnboundedChannelOptions()
{
    SingleReader = true,
    SingleWriter = true,
    AllowSynchronousContinuations = false
}));

builder.Services.AddHostedService<OrderCreatedEventConsumer>();

builder.Services.AddHostedService<UserCreatedEventConsumer>();
var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();


app.MapGet("order-created-event-write",
    async (Channel<OrderCreatedEvent> channel, ILogger<Channel<OrderCreatedEvent>> logging) =>
    {
        var writer = channel.Writer;


        if (writer.TryWrite(new OrderCreatedEvent(1, 10, 90)))
        {
            logging.LogInformation("OrderCreatedEvent written to channel.");
        }
        else
        {
            logging.LogWarning("Failed to write OrderCreatedEvent to channel.");
        }


        await writer.WriteAsync(new OrderCreatedEvent(2, 20, 80));
    });


app.MapGet("user-created-event-write", async (Channel<UserCreatedEvent> channel, ILoggerFactory loggerFactory) =>
{
    var logger = loggerFactory.CreateLogger("UserCreatedEventChannel");
    var writer = channel.Writer;
    if (writer.TryWrite(new UserCreatedEvent(1, "ahmet16")))
    {
        logger.LogInformation("UserCreatedEvent written to channel.");
    }
    else
    {
        logger.LogWarning("Failed to write UserCreatedEvent to channel.");
    }

    var isWriter = await writer.WaitToWriteAsync();
    await writer.WriteAsync(new UserCreatedEvent(2, "mehmet34"));
});
app.Run();


