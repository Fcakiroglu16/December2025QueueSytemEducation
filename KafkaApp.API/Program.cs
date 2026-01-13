using KafkaApp.API.Consumers;
using KafkaApp.API.Services;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.AddSingleton<KafkaService>();
builder.Services.AddHostedService<MessageConsumerWithNoAck>();
builder.Services.AddHostedService<MessageConsumerWithComplexType>();
var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.MapScalarApiReference();

app.MapGet("/create-topic",
        async (KafkaService kafkaService) => { await kafkaService.CreateTopic("topic-with-complex-type"); })
    .WithName("create-topic");
app.MapGet("/send-message-at-most-once", async (KafkaService kafkaService) =>
    {
        await kafkaService.SendMessageWithAtMostOnce("topic2", "merhaba");

        return Results.Ok();
    })
    .WithName("send-message-at-most-oncee");

app.MapGet("/send-message-at-least-once", async (KafkaService kafkaService) =>
    {
        await kafkaService.SendMessageWithAtMostOnce("topic-at-least-once", "merhaba");

        return Results.Ok();
    })
    .WithName("send-message-at-least-once");


app.MapGet("/send-message-at-least-once-with-key", async (KafkaService kafkaService) =>
    {
        await kafkaService.SendMessageWithAtLeastOnceWithKey("topic-with-key", "merhaba");

        return Results.Ok();
    })
    .WithName("send-message-at-least-once-with-key");

app.MapGet("/send-message-at-least-once-transaction", async (KafkaService kafkaService) =>
    {
        await kafkaService.SendMessageWithAtExactlyOnceWithTransaction("merhaba-100");

        return Results.Ok();
    })
    .WithName("send-message-at-least-once-transaction");


app.MapGet("/send-message-at-least-once-with-complex-type", async (KafkaService kafkaService) =>
    {
        await kafkaService.SendMessageWithAtLeastOnceWithComplexType("topic-with-complex-type");

        return Results.Ok();
    })
    .WithName("send-message-at-least-once-with-complex-type");
app.Run();


