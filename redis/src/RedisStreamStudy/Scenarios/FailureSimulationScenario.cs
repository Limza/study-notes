using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class FailureSimulationScenario
{
    public static async Task RunAsync(IDatabase database)
    {
        // 장애를 재현할 Stream, Consumer Group, Consumer 이름을 정한다.
        var streamKey = "game:events";
        var groupName = "game-workers";
        var consumerName = "consumer-a";
        var messageCount = 10;
        var readCount = 5;

        try
        {
            // Consumer Group이 없으면 만들고, 이미 있으면 아래 catch에서 넘어간다.
            await database.StreamCreateConsumerGroupAsync(
                streamKey,
                groupName,
                position: "0-0",
                createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            Console.WriteLine("Consumer group already exists.");
        }

        // Producer가 테스트 메시지 10개를 Stream에 추가한다.
        for (var i = 1; i <= messageCount; i++)
        {
            await database.StreamAddAsync(
                streamKey,
                new NameValueEntry[]
                {
                    new("eventType", "match.completed"),
                    new("matchId", $"match-{i:000}")
                });
        }

        // consumer-a가 새 메시지 5개를 읽는다.
        // 이 순간 Redis에는 consumer-a가 메시지를 가져갔다는 Pending 기록이 생긴다.
        var entries = await database.StreamReadGroupAsync(
            key: streamKey,
            groupName: groupName,
            consumerName: consumerName,
            position: ">",
            count: readCount);

        // 처리 도중 Consumer가 종료된 상황을 흉내 낸다.
        // 일부러 XACK를 호출하지 않기 때문에 아래 메시지들은 Pending 상태로 남는다.
        foreach (var entry in entries)
        {
            Console.WriteLine($"Processing before crash: {entry.Id}");
            await Task.Delay(TimeSpan.FromMilliseconds(300));
        }

        // 실제 프로세스 종료 대신, ACK 전에 죽었다는 상황을 출력으로 표시한다.
        Console.WriteLine("Simulated crash before XACK.");

        await PrintPendingStatusAsync(database, streamKey, groupName);
    }

    private static async Task PrintPendingStatusAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        var pending = await database.StreamPendingAsync(streamKey, groupName);
        var pendingMessages = await database.StreamPendingMessagesAsync(
            streamKey,
            groupName,
            count: 10,
            consumerName: RedisValue.Null);

        Console.WriteLine();
        Console.WriteLine("=== Pending Status ===");
        Console.WriteLine(
            $"pending={pending.PendingMessageCount}, lowest={pending.LowestPendingMessageId}, highest={pending.HighestPendingMessageId}");

        foreach (var message in pendingMessages)
        {
            Console.WriteLine(
                $"message={message.MessageId}, consumer={message.ConsumerName}, delivery-count={message.DeliveryCount}");
        }
    }
}
