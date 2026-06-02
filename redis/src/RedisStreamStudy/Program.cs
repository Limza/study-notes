using RedisStreamStudy.Infrastructure;
using RedisStreamStudy.Scenarios;
using StackExchange.Redis;

namespace RedisStreamStudy;

public class Program
{
    public static async Task Main(string[] args)
    {
        await using var redisContainer = RedisContainerFactory.Create();

        await redisContainer.StartAsync();

        // 컨테이너 내부의 redis 포트가 6379이므로, 이 포트가 내 PC의 어떤 포트로 매핑되었는지 가져온다.
        var redisPort = redisContainer.GetMappedPublicPort(6379);
        var connectionString = $"localhost:{redisPort}";

        await using var connection =
            await ConnectionMultiplexer.ConnectAsync(connectionString);

        var database = connection.GetDatabase();

        // await BasicStreamScenario.RunAsync(database); 
        await ConsumerGroupScenario.RunAsync(database);
    }
}
