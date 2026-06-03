using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class FailureSimulationScenario
{
    public static async Task RunAsync(IDatabase database)
    {
        // 장애 재현에 사용할 Redis Stream key를 정한다.
        // 모든 테스트 메시지는 이 Stream 안에 message id 단위로 쌓인다.
        var streamKey = "game:events";

        // Consumer Group 이름을 정한다.
        // 같은 Group 안의 Consumer들은 메시지를 나눠서 처리한다.
        var groupName = "game-workers";

        // 메시지를 읽을 Consumer 이름을 정한다.
        // 이 Consumer가 메시지를 가져가고 ACK하지 않는 상황을 만들 것이다.
        var consumerName = "consumer-a";

        // Stream에 넣을 테스트 메시지 개수다.
        var messageCount = 10;

        // Consumer가 읽어 갈 메시지 개수다.
        // 10개 중 5개만 읽고 ACK하지 않아서 Pending 상태를 만든다.
        var readCount = 5;

        try
        {
            // Redis의 XGROUP CREATE에 해당한다.
            // streamKey에 groupName이라는 Consumer Group을 만든다.
            // position: "0-0"은 Stream의 처음부터 메시지를 읽을 수 있게 시작 위치를 잡는다는 뜻이다.
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
            // 실습을 여러 번 실행할 수 있도록, 이미 있으면 실패로 보지 않고 그대로 진행한다.
            Console.WriteLine("Consumer group already exists.");
        }

        // Producer 역할로 테스트 메시지를 Stream에 추가한다.
        // 반복문 한 바퀴마다 Redis XADD가 한 번 실행되고, 메시지 ID가 하나 생긴다.
        for (var i = 1; i <= messageCount; i++)
        {
            // StreamAddAsync는 Redis의 XADD 명령에 해당한다.
            // 첫 번째 인자는 메시지를 넣을 Stream key다.
            // 두 번째 인자는 하나의 메시지 ID에 함께 저장될 field-value 목록이다.
            await database.StreamAddAsync(
                streamKey,
                new NameValueEntry[]
                {
                    // eventType field에는 이벤트 종류를 저장한다.
                    new("eventType", "match.completed"),

                    // matchId field에는 반복문 번호를 이용해 match-001 같은 값을 저장한다.
                    new("matchId", $"match-{i:000}")
                });
        }

        // consumer-a가 Consumer Group을 통해 새 메시지 5개를 읽는다.
        // StreamReadGroupAsync는 Redis의 XREADGROUP 명령에 해당한다.
        // position: ">"는 이 Group에서 아직 어떤 Consumer에게도 전달되지 않은 새 메시지만 읽겠다는 뜻이다.
        // 메시지를 읽는 순간 Redis는 "consumer-a에게 전달했지만 아직 ACK되지 않았다"는 Pending 기록을 만든다.
        var entries = await database.StreamReadGroupAsync(
            key: streamKey,
            groupName: groupName,
            consumerName: consumerName,
            position: ">",
            count: readCount);

        // 처리 도중 Consumer가 죽은 상황을 흉내 낸다.
        // 실제 업무 코드라면 처리 성공 후 StreamAcknowledgeAsync, 즉 XACK를 호출해야 한다.
        // 여기서는 일부러 ACK하지 않아서 entries에 들어 있는 메시지들이 Pending 상태로 남게 한다.
        foreach (var entry in entries)
        {
            // entry.Id는 Redis가 각 Stream 메시지에 붙인 message id다.
            Console.WriteLine($"Processing before crash: {entry.Id}");

            // 처리 시간이 조금 걸리는 것처럼 보이게 하는 지연이다.
            await Task.Delay(TimeSpan.FromMilliseconds(300));
        }

        // 실제 프로세스를 종료하지는 않고, ACK 전에 죽었다는 상황만 출력으로 표시한다.
        Console.WriteLine("Simulated crash before XACK.");

        // 방금 만든 Pending 상태를 StackExchange.Redis API로 확인한다.
        await PrintPendingStatusAsync(database, streamKey, groupName);
    }

    private static async Task PrintPendingStatusAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        // StreamPendingAsync는 Redis의 XPENDING 요약 조회에 해당한다.
        // Group 전체의 Pending 개수, 가장 낮은 message id, 가장 높은 message id를 확인한다.
        var pending = await database.StreamPendingAsync(streamKey, groupName);

        // StreamPendingMessagesAsync는 XPENDING 상세 조회에 가깝다.
        // Pending 메시지마다 message id, owner consumer, delivery count 등을 확인한다.
        // consumerName: RedisValue.Null은 특정 Consumer로 제한하지 않고 전체 Pending을 보겠다는 뜻이다.
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
            // delivery-count는 이 메시지가 Consumer에게 몇 번 전달됐는지를 나타낸다.
            // 값이 높으면 같은 메시지를 여러 번 재처리하다 실패했을 가능성을 의심할 수 있다.
            Console.WriteLine(
                $"message={message.MessageId}, consumer={message.ConsumerName}, delivery-count={message.DeliveryCount}");
        }
    }
}
