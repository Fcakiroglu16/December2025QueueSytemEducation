using System.Globalization;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using RedisApp.Events;
using RedisApp.PubSubConsumers;
using RedisApp.Servives;
using RedisApp.StreamConsumers;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.AddSingleton<RedisService>(sp =>
{
    var redisHost = builder.Configuration["RedisOption:Host"];
    var redisPort = builder.Configuration["RedisOption:Port"];
    return new RedisService(redisHost!, redisPort!);
});

builder.Services.AddHostedService<PubSubMessageConsumer>();
builder.Services.AddHostedService<PubSubMessageConsumer2>();
builder.Services.AddHostedService<PubSubMessageConsumerWithPattern>();
builder.Services.AddHostedService<PubSubMessageConsumerWithPattern2>();
builder.Services.AddHostedService<MyStreamConsumer>();
builder.Services.AddHostedService<MyStreamConsumer2>();
builder.Services.AddHostedService<MyStreamConsumerComplexType>();
var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment()) app.MapOpenApi();

app.UseHttpsRedirection();


app.MapGet("redis-stream-with-at-least-once", async ([FromServices] RedisService redisService) =>
{
    var db = redisService.GetDatabase();


    var message = new NameValueEntry[]
    {
        new("user", JsonSerializer.Serialize(new UserCreatedEvent(Guid.NewGuid(), "ahmet16"))),
        new("version", "v1"),
        new("created_date", DateTime.UtcNow.ToString(CultureInfo.InvariantCulture))
    };


    var maxRetries = 3;
    var retryCount = 0;
    RedisValue messageId = RedisValue.Null;

    while (retryCount < maxRetries)
    {
        try
        {
            messageId = await db.StreamAddAsync("my-stream-complex-type", message, null, null, false);

            if (messageId.HasValue)
            {
                break;
            }

            retryCount++;
            if (retryCount >= maxRetries)
            {
                throw new Exception("message can not send redis stream");
            }
        }
        catch (Exception)
        {
            retryCount++;
            if (retryCount >= maxRetries)
            {
                throw;
            }

            await Task.Delay(100 * retryCount);
        }
    }


    //await db.StreamAddAsync("my-stream2", message, null, 1000, true);
    return Results.Ok();
});


app.MapGet("redis-stream-with-at-most-once", async ([FromServices] RedisService redisService) =>
{
    var db = redisService.GetDatabase();


    var message = new NameValueEntry[]
    {
        new("user", JsonSerializer.Serialize(new UserCreatedEvent(Guid.NewGuid(), "ahmet16"))),
        new("version", "v1"),
        new("created_date", DateTime.UtcNow.ToString(CultureInfo.InvariantCulture))
    };
    var messageId = await db.StreamAddAsync("my-stream-complex-type", message, null, null, false);


    //await db.StreamAddAsync("my-stream2", message, null, 1000, true);
    return Results.Ok();
});


app.MapGet("redis-stream-with-complex-type", async ([FromServices] RedisService redisService) =>
{
    var db = redisService.GetDatabase();


    var message = new NameValueEntry[]
        {
            new("user", JsonSerializer.Serialize(new UserCreatedEvent(Guid.NewGuid(), "ahmet16"))),
            new("version", "v1"),
            new("created_date", DateTime.UtcNow.ToString(CultureInfo.InvariantCulture))
        };
        await db.StreamAddAsync("my-stream-complex-type", message, null, null, false);


        //await db.StreamAddAsync("my-stream2", message, null, 1000, true);
        return Results.Ok();
});

app.MapGet("redis-stream", async ([FromServices] RedisService redisService) =>
{
    var db = redisService.GetDatabase();


    foreach (var i in Enumerable.Range(1, 4).ToList())
    {
        var message = new NameValueEntry[]
        {
            new("user_id", i),
            new("user_name", "ahmet16")
        };


        await db.StreamAddAsync("my-stream", message, null, null, false);
    }


    //await db.StreamAddAsync("my-stream2", message, null, 1000, true);
    return Results.Ok();
});


app.MapGet("pub-sub", async ([FromServices] RedisService redisService) =>
{
    var subscriber = redisService.GetSubscriber();

    var result = await subscriber.PublishAsync("mychannel", "hello world");

    return Results.Ok();
});

app.MapGet("pub-sub-with-pattern", async ([FromServices] RedisService redisService) =>
{
    var subscriber = redisService.GetSubscriber();

    var result = await subscriber.PublishAsync("user.login", "user login");
    var result2 = await subscriber.PublishAsync("user.logout", "user login");
    var result3 = await subscriber.PublishAsync("sensor:1", "sensor 1");
    var result4 = await subscriber.PublishAsync("sensor:2", "sensor 2");
    return Results.Ok();
});


app.Run();