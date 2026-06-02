using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class ConsumerGroupScenario
{
    public static async Task RunAsync(IDatabase database)
    {
        // Consumer Group을 붙일 Stream 이름과 Group 이름을 정한다.
        var streamKey = "game:events";
        var groupName = "game-workers";

        try
        {
            // StreamCreateConsumerGroupAsync는 Redis의 XGROUP CREATE 명령에 해당한다.
            await database.StreamCreateConsumerGroupAsync(
                streamKey,
                groupName,
                position: "0-0", // 가장 처음 위치부터 읽음
                createStream: true); // true는 Stream이 없어도 함께 만들겠다
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            Console.WriteLine("Consumer group already exists.");
        }

        // Consumer들이 나눠 처리할 테스트 메시지 10개를 Stream에 추가한다
        for (int i = 0; i < 10; i++)
        {
            await database.StreamAddAsync(
                streamKey,
                new NameValueEntry[]
                {
                    new ("eventType", "match.completed"),
                    new ("matchId", $"match-{i:000}")
                });
        }

        // consumer-a와 consumer-b를 동시에 실행한다.
        // Redis는 메시지를 나눠 주고, 오래 걸리는 실제 처리는 각 Consumer가 병렬로 수행한다.
        await Task.WhenAll(
            ReadAndAckAsync(database, streamKey, groupName, "consumer-a"),
            ReadAndAckAsync(database, streamKey, groupName, "consumer-b"));

        // 처리 후 Consumer Group과 Pending 상태를 확인한다.
        await PrintConsumerGroupStatusAsync(database, streamKey, groupName);
    }

    // Consumer Group을 이용해 Stream에서 메시지를 읽고 처리 완료(ACK)하는 예시 메서드
    private static async Task ReadAndAckAsync(
        IDatabase database,
        string streamKey,
        string groupName,
        string consumerName)
    {
        // StreamReadGroupAsync는 Redis의 XREADGROUP 명령에 해당한다.
        var entries = await database.StreamReadGroupAsync(
            key: streamKey,
            groupName: groupName,
            consumerName: consumerName,
            position: ">", // >는 아직 어떤 Consumer도 처리하지 않은 메시지를 읽겠다는 의미
            count: 5); // 최대 5개 메시지 읽기

        // 실제 서비스에서는 메시지를 읽은 뒤 오래 걸리는 처리를 수행할 수 있다.
        // ACK는 처리가 성공한 뒤에만 보낸다.
        foreach (var entry in entries)
        {
            Console.WriteLine($"Consumer {consumerName} processing {entry.Id}");

            // 시간이 걸리는 비즈니스 처리를 흉내 낸다.
            await Task.Delay(TimeSpan.FromSeconds(1));

            // StreamAcknowledgeAsync는 Redis의 XACK 명령에 해당한다.
            // ACK를 보내면 Redis는 해당 메시지를 Consumer Group의 Pending 상태에서 제거한다
            await database.StreamAcknowledgeAsync(streamKey, groupName, entry.Id);
        }
    }

    private static async Task PrintConsumerGroupStatusAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        // XINFO GROUPS game:events에 해당한다.
        var groups = await database.StreamGroupInfoAsync(streamKey);

        Console.WriteLine();
        Console.WriteLine("=== XINFO GROUPS ===");
        foreach (var group in groups)
        {
            Console.WriteLine(
                $"group={group.Name}, consumers={group.ConsumerCount}, pending={group.PendingMessageCount}, last-delivered-id={group.LastDeliveredId}");
        }

        // XINFO CONSUMERS game:events game-workers에 해당한다.
        var consumers = await database.StreamConsumerInfoAsync(streamKey, groupName);

        Console.WriteLine();
        Console.WriteLine("=== XINFO CONSUMERS ===");
        foreach (var consumer in consumers)
        {
            Console.WriteLine(
                $"consumer={consumer.Name}, pending={consumer.PendingMessageCount}, idle-ms={consumer.IdleTimeInMilliseconds}");
        }

        // XPENDING game:events game-workers에 해당한다.
        var pending = await database.StreamPendingAsync(streamKey, groupName);

        Console.WriteLine();
        Console.WriteLine("=== XPENDING ===");
        Console.WriteLine(
            $"pending={pending.PendingMessageCount}, lowest={pending.LowestPendingMessageId}, highest={pending.HighestPendingMessageId}");
    }
}
