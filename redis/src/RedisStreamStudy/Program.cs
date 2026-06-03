using RedisStreamStudy.Infrastructure;
using RedisStreamStudy.Scenarios;
using StackExchange.Redis;

namespace RedisStreamStudy;

public class Program
{
    public static async Task Main(string[] args)
    {
        // Redis GUI에서 접속하기 쉽게 내 PC의 6379 포트에 Redis 컨테이너를 고정한다.
        // 이미 로컬 Redis가 6379 포트를 사용 중이면 컨테이너 시작이 실패할 수 있다.
        const int redisPort = 6379;

        // Testcontainers로 Redis 컨테이너 설정을 만든다.
        // redisPort를 넘겨서 host port와 container port를 같은 번호로 바인딩한다.
        await using var redisContainer = RedisContainerFactory.Create(redisPort);

        // 실제 Docker Redis 컨테이너를 시작한다.
        await redisContainer.StartAsync();

        // Redis GUI에서 localhost:6379로 접속할 수 있도록 고정 포트로 연결한다.
        var connectionString = $"localhost:{redisPort}";

        // StackExchange.Redis의 Redis 연결 관리자를 만든다.
        // 이 connection을 통해 Redis 명령을 보낼 수 있다.
        await using var connection =
            await ConnectionMultiplexer.ConnectAsync(connectionString);

        // Redis 명령을 실행할 IDatabase 객체를 꺼낸다.
        // 각 Scenario는 이 database를 받아서 Stream 명령을 실행한다.
        var database = connection.GetDatabase();

        // await BasicStreamScenario.RunAsync(database);
        // await ConsumerGroupScenario.RunAsync(database);

        // 먼저 Consumer가 메시지를 읽고 ACK하지 않은 장애 상황을 만든다.
        await FailureSimulationScenario.RunAsync(database);

        // 그 다음 Redis Stream 상태를 조회해서 Pending 메시지를 추적한다.
        await TroubleshootingScenario.RunAsync(database);

        Console.WriteLine("Redis GUI에서 localhost:6379로 확인하세요.");
        Console.WriteLine("컨테이너를 종료하려면 Enter를 누르세요.");
        Console.ReadLine();
    }
}
