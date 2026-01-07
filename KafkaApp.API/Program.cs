using KafkaApp.API.Consumers;
using KafkaApp.API.Services;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.AddSingleton<KafkaService>();
builder.Services.AddHostedService<MessageConsumerWithNoAck>();
var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.MapScalarApiReference();

app.MapGet("/create-topic", async (KafkaService kafkaService) =>
    {
        await kafkaService.CreateTopic("topic2");

        return Results.Ok("topic created");
    })
    .WithName("create-topic");
app.MapGet("/send-message", async (KafkaService kafkaService) =>
    {
        await kafkaService.SendMessageWithAtMostOnce("topic2", "merhaba");

        return Results.Ok();
    })
    .WithName("send-message");
app.Run();


