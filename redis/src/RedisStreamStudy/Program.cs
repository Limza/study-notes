using RedisStreamStudy.Infrastructure;
using StackExchange.Redis;

public class Program
{
    public static async Task Main(string[] args)
    {
        await using var redisContainer = RedisContainerFactory.Create();

        await redisContainer.StartAsync();

        var redisPort = redisContainer.GetMappedPublicPort(6379);
        var connectionString = $"localhost:{redisPort}";

        await using var connection = await ConnectionMultiplexer.ConnectAsync(connectionString);

        var database = connection.GetDatabase();
        var pong = await database.PingAsync();

        Console.WriteLine($"Redis PING: {pong.TotalMilliseconds} ms");
    }
}
