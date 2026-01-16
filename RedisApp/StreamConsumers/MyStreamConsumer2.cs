using RedisApp.Servives;
using StackExchange.Redis;

namespace RedisApp.StreamConsumers
{
    public class MyStreamConsumer2(RedisService redisService, ILogger<MyStreamConsumer> logger) : BackgroundService
    {
        private IDatabase db;

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            db = redisService.GetDatabase();


            return base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var streamName = "my-stream";
            var consumerGroupName = "myconsumergroup-1";
            var consumerName = "consumer-2";


            var streamExists = await db.KeyExistsAsync(streamName);
            if (!streamExists)
            {
                await db.StreamAddAsync(streamName, "init", "stream-created");
                logger.LogInformation("Stream {StreamName} created", streamName);
            }

            var groups = await db.StreamGroupInfoAsync(streamName);

            if (groups.All(g => g.Name != consumerGroupName))
            {
                await db.StreamCreateConsumerGroupAsync(streamName, consumerGroupName, StreamPosition.Beginning);
                logger.LogInformation("Consumer group {ConsumerGroup} created on stream {StreamName}",
                    consumerGroupName, streamName);
            }
            else
            {
                logger.LogInformation("Consumer group {ConsumerGroup} already exists on stream {StreamName}",
                    consumerGroupName, streamName);
            }


            while (!stoppingToken.IsCancellationRequested)
            {
                var entries = await db.StreamReadGroupAsync(streamName, consumerGroupName, consumerName, ">", 2,
                    CommandFlags.DemandMaster);
                foreach (var entry in entries)
                {
                    try
                    {
                        logger.LogInformation("2.Consumer => Processing message {MessageId}", entry.Id);
                        foreach (var item in entry.Values)
                        {
                            logger.LogInformation("Key: {Key}, Value: {Value}", item.Name, item.Value);
                        }

                        await db.StreamAcknowledgeAsync(streamName, consumerGroupName, entry.Id);
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e, e.Message);
                        throw;
                    }
                }

                await Task.Delay(1000, stoppingToken);
            }
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            return base.StopAsync(cancellationToken);
        }
    }
}