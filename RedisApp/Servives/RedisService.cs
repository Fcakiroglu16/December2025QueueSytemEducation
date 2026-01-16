using StackExchange.Redis;

namespace RedisApp.Servives;

public class RedisService
{
    private readonly ConnectionMultiplexer _connectionMultiplexer;

    public RedisService(string host, string port)
    {
        var connectionString = $"{host}:{port}";


        _connectionMultiplexer = ConnectionMultiplexer.Connect(connectionString);
    }

    public IDatabase GetDatabase(int db = 0)
    {
        return _connectionMultiplexer.GetDatabase(db);
    }

    public ISubscriber GetSubscriber()
    {
        return _connectionMultiplexer.GetSubscriber();
    }
}