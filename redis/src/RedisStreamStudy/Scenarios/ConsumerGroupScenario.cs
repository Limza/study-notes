using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class ConsumerGroupScenario
{
    public static async Task RunAsync(IDatabase database)
    {
        // Consumer Group을 붙일 Redis Stream key를 정한다.
        // Producer가 추가하는 메시지는 모두 이 Stream에 쌓인다.
        var streamKey = "game:events";

        // Consumer Group 이름을 정한다.
        // 같은 Group 안의 Consumer들은 메시지를 나눠서 처리한다.
        var groupName = "game-workers";

        try
        {
            // StreamCreateConsumerGroupAsync는 Redis의 XGROUP CREATE 명령에 해당한다.
            // position: "0-0"은 Stream의 처음부터 읽을 수 있게 Group 시작 위치를 잡는다는 뜻이다.
            // createStream: true는 Stream key가 아직 없어도 함께 만들겠다는 뜻이다.
            await database.StreamCreateConsumerGroupAsync(
                streamKey,
                groupName,
                position: "0-0",
                createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            // 같은 Stream에 같은 Consumer Group을 다시 만들면 Redis가 BUSYGROUP 에러를 낸다.
            // 실습을 반복 실행할 수 있도록 이미 있으면 그대로 진행한다.
            Console.WriteLine("Consumer group already exists.");
        }

        // Consumer들이 나눠 처리할 테스트 메시지 10개를 Stream에 추가한다.
        // 반복문 한 바퀴마다 XADD가 한 번 실행되고 message id가 하나 생긴다.
        for (var i = 1; i <= 10; i++)
        {
            await database.StreamAddAsync(
                streamKey,
                new NameValueEntry[]
                {
                    new("eventType", "match.completed"),
                    new("matchId", $"match-{i:000}")
                });
        }

        // consumer-a와 consumer-b를 동시에 실행한다.
        // Redis는 새 메시지를 Consumer들에게 나눠 주고, 오래 걸리는 실제 처리는 각 Consumer가 병렬로 수행한다.
        await Task.WhenAll(
            ReadAndAckAsync(database, streamKey, groupName, "consumer-a"),
            ReadAndAckAsync(database, streamKey, groupName, "consumer-b"));

        // 모든 Consumer가 ACK를 보낸 뒤 Group과 Pending 상태를 확인한다.
        await PrintConsumerGroupStatusAsync(database, streamKey, groupName);
    }

    // Consumer Group을 이용해 Stream에서 메시지를 읽고 처리 완료 ACK를 보내는 예시 메서드다.
    private static async Task ReadAndAckAsync(
        IDatabase database,
        string streamKey,
        string groupName,
        string consumerName)
    {
        // StreamReadGroupAsync는 Redis의 XREADGROUP 명령에 해당한다.
        // consumerName은 이 메시지를 어떤 Consumer가 가져갔는지 Redis에 기록할 이름이다.
        // position: ">"는 아직 이 Group 안의 어떤 Consumer에게도 전달되지 않은 새 메시지를 읽겠다는 뜻이다.
        // count: 5는 이 Consumer가 최대 5개까지만 가져가겠다는 뜻이다.
        var entries = await database.StreamReadGroupAsync(
            key: streamKey,
            groupName: groupName,
            consumerName: consumerName,
            position: ">",
            count: 5);

        // 실제 서비스에서는 메시지를 읽은 뒤 오래 걸리는 처리를 수행할 수 있다.
        // 메시지를 읽는 순간 Pending 상태가 되고, ACK는 처리가 성공한 뒤에만 보낸다.
        foreach (var entry in entries)
        {
            Console.WriteLine($"Consumer {consumerName} processing {entry.Id}");

            // 시간이 걸리는 비즈니스 처리를 흉내 낸다.
            await Task.Delay(TimeSpan.FromSeconds(1));

            // StreamAcknowledgeAsync는 Redis의 XACK 명령에 해당한다.
            // ACK를 보내면 Redis는 해당 메시지를 Consumer Group의 Pending 상태에서 제거한다.
            await database.StreamAcknowledgeAsync(streamKey, groupName, entry.Id);
        }
    }

    private static async Task PrintConsumerGroupStatusAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        // XINFO GROUPS game:events에 해당한다.
        // Group별 Consumer 수, Pending 수, 마지막으로 전달된 ID를 확인한다.
        var groups = await database.StreamGroupInfoAsync(streamKey);

        Console.WriteLine();
        Console.WriteLine("=== XINFO GROUPS ===");
        foreach (var group in groups)
        {
            Console.WriteLine(
                $"group={group.Name}, consumers={group.ConsumerCount}, pending={group.PendingMessageCount}, last-delivered-id={group.LastDeliveredId}");
        }

        // XINFO CONSUMERS game:events game-workers에 해당한다.
        // Consumer별 Pending 수와 idle 시간을 확인한다.
        var consumers = await database.StreamConsumerInfoAsync(streamKey, groupName);

        Console.WriteLine();
        Console.WriteLine("=== XINFO CONSUMERS ===");
        foreach (var consumer in consumers)
        {
            Console.WriteLine(
                $"consumer={consumer.Name}, pending={consumer.PendingMessageCount}, idle-ms={consumer.IdleTimeInMilliseconds}");
        }

        // XPENDING game:events game-workers에 해당한다.
        // Group 전체에서 ACK되지 않은 메시지 개수와 ID 범위를 확인한다.
        var pending = await database.StreamPendingAsync(streamKey, groupName);

        Console.WriteLine();
        Console.WriteLine("=== XPENDING ===");
        Console.WriteLine(
            $"pending={pending.PendingMessageCount}, lowest={pending.LowestPendingMessageId}, highest={pending.HighestPendingMessageId}");
    }
}
