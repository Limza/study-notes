using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class RecoveryScenario
{
    public static async Task RunAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        // 멈춘 Consumer 대신 메시지를 가져와 재처리할 Consumer 이름을 recovery-consumer로 정한다.
        // XAUTOCLAIM 이후 Pending 메시지의 owner가 recovery-consumer 로 바뀐다.
        var recoveryConsumerName = "recovery-consumer";

        // XAUTOCLAIM으로 idle time이 min-idle-time 이상인 Pending 메시지를 가져온다.
        // 여기서 2000은 min-idle-time이며, 2초 이상 ACK되지 않은 메시지만 가져오겠다는 뜻이다.
        // "0-0"은 Pending 목록을 처음부터 스캔하겠다는 시작 ID다.
        // COUNT 10은 한 번에 최대 10개까지만 가져오겠다는 뜻이다.
        var autoClaimResult = await database.ExecuteAsync(
            "XAUTOCLAIM",
            streamKey,
            groupName,
            recoveryConsumerName,
            "2000", // 2초
            "0-0", // Pending 목록 처음부터
            "COUNT", // COUNT 10 == 한번에 최대 10개
            "10"
        );
        var claimedMessageIds = PrintAutoClaimResult(autoClaimResult);

        // XAUTOCLAIM 실행 후 Pending 상태가 어떻게 바뀌었는지 다시 확인한다.
        // owner consumer가 recovery-consumer로 바뀌었는지, delivery count가 증가했는지 볼 수 있다.
        var pendingDetails = await database.ExecuteAsync(
            "XPENDING", streamKey, groupName, "-", "+", "10");

        PrintPendingDetails("=== XPENDING AFTER XAUTOCLAIM ===", pendingDetails);

        // 실제 업무 코드라면 여기에서 메시지를 다시 처리한다.
        // 이 실습에서는 재처리에 성공했다고 가정하고, 가져온 메시지를 XACK로 완료 처리한다.
        var acknowledgedCount = 0L;

        foreach (var messageId in claimedMessageIds)
        {
            acknowledgedCount += await database.StreamAcknowledgeAsync(
                streamKey,
                groupName,
                messageId);
        }

        Console.WriteLine();
        Console.WriteLine("=== XACK ===");
        Console.WriteLine($"acknowledged-count={acknowledgedCount}");

        // XACK 후에는 ACK된 메시지가 Consumer Group의 Pending 목록에서 제거된다.
        var pendingDetailsAfterAck = await database.ExecuteAsync(
            "XPENDING", streamKey, groupName, "-", "+", "10");

        PrintPendingDetails("=== XPENDING AFTER XACK ===", pendingDetailsAfterAck);
    }

    private static RedisValue[] PrintAutoClaimResult(RedisResult result)
    {
        var resultParts = ToArray(result);

        var nextStartId = (RedisValue)resultParts[0];
        var claimedMessages = ToArray(resultParts[1]);
        var claimedMessageIds = new List<RedisValue>();

        Console.WriteLine();
        Console.WriteLine("=== XAUTOCLAIM ===");
        Console.WriteLine($"next-start-id={nextStartId}");
        Console.WriteLine($"claimed-count={claimedMessages.Length}");

        foreach (var claimedMessage in claimedMessages)
        {
            var messageParts = ToArray(claimedMessage);
            var messageId = (RedisValue)messageParts[0];
            var fields = ToArray(messageParts[1]);
            claimedMessageIds.Add(messageId);

            Console.WriteLine($"message={messageId}");

            for (var i = 0; i < fields.Length; i += 2)
            {
                var fieldName = (RedisValue)fields[i];
                var fieldValue = (RedisValue)fields[i + 1];

                Console.WriteLine($"  {fieldName}={fieldValue}");
            }
        }

        return claimedMessageIds.ToArray();
    }

    private static void PrintPendingDetails(string title, RedisResult result)
    {
        var pendingMessages = ToArray(result);

        Console.WriteLine();
        Console.WriteLine(title);

        if (pendingMessages.Length == 0)
        {
            Console.WriteLine("pending-details=empty");
            return;
        }

        foreach (var pendingMessage in pendingMessages)
        {
            var messageParts = ToArray(pendingMessage);

            var messageId = (RedisValue)messageParts[0];
            var ownerConsumer = (RedisValue)messageParts[1];
            var idleTimeMs = (long)(RedisValue)messageParts[2];
            var deliveryCount = (long)(RedisValue)messageParts[3];

            Console.WriteLine(
                $"message={messageId}, owner={ownerConsumer}, idle-ms={idleTimeMs}, delivery-count={deliveryCount}");
        }
    }

    private static RedisResult[] ToArray(RedisResult result)
    {
        return (RedisResult[]?)result ?? Array.Empty<RedisResult>();
    }
}
