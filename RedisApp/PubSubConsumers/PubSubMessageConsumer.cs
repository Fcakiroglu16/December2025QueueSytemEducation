using RedisApp.Servives;
using StackExchange.Redis;

namespace RedisApp.PubSubConsumers;

public class PubSubMessageConsumer(RedisService redisService, ILogger<PubSubMessageConsumer> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var subscriber = redisService.GetSubscriber();
        await subscriber.SubscribeAsync(new RedisChannel("mychannel", RedisChannel.PatternMode.Literal),
            (channel, message) =>
            {
                logger.LogInformation("1. Consumer => Received message: {Message} on channel: {Channel}", message,
                    channel);
            });
    }
}