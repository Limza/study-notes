using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class DeadLetterScenario
{
    private const long DeadLetterDeliveryCount = 3;
    private const int PendingScanCount = 10;

    public static async Task RunAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        var consumerName = "consumer-a";
        var deadLetterStreamKey = $"{streamKey}:dead-letter";

        // 실습에서는 같은 Pending 메시지를 여러 번 다시 읽어서 delivery count를 기준 이상으로 만든다.
        // 실제 운영 코드라면 이 준비 단계 없이 이미 누적된 delivery count를 그대로 판단하면 된다.
        await PrepareRepeatedFailureForDemoAsync(
            database,
            streamKey,
            groupName,
            consumerName);

        // XPENDING stream group - + count 와 같은 상세 조회다.
        // 각 Pending 메시지의 id, owner consumer, idle time, delivery count를 확인한다.
        var pendingMessages = await database.StreamPendingMessagesAsync(
            streamKey,
            groupName,
            count: PendingScanCount,
            consumerName: RedisValue.Null);

        var deadLetterCandidates = pendingMessages
            .Where(message => message.DeliveryCount >= DeadLetterDeliveryCount)
            .ToArray();

        Console.WriteLine();
        Console.WriteLine("=== DEAD LETTER CANDIDATES ===");
        Console.WriteLine($"candidate-count={deadLetterCandidates.Length}");

        foreach (var candidate in deadLetterCandidates)
        {
            // Pending 상세 정보에는 원본 field-value가 들어 있지 않다.
            // 그래서 message id로 원본 Stream을 다시 조회해 Dead Letter Stream에 복사할 payload를 만든다.
            var originalEntries = await database.StreamRangeAsync(
                streamKey,
                candidate.MessageId,
                candidate.MessageId,
                count: 1);

            if (originalEntries.Length == 0)
            {
                Console.WriteLine($"message={candidate.MessageId}, skipped=original-message-not-found");
                continue;
            }

            var originalEntry = originalEntries[0];
            var deadLetterFields = BuildDeadLetterFields(streamKey, candidate, originalEntry);

            // XADD game:events:dead-letter * ... 와 같은 역할이다.
            // 원본 메시지와 실패 판단에 필요한 메타데이터를 별도 Stream에 남긴다.
            var deadLetterId = await database.StreamAddAsync(
                deadLetterStreamKey,
                deadLetterFields);

            // Dead Letter Stream으로 옮긴 뒤에는 원본 Consumer Group의 Pending 목록에서 제거한다.
            // XACK game:events game-workers message-id 와 같은 역할이다.
            var acknowledgedCount = await database.StreamAcknowledgeAsync(
                streamKey,
                groupName,
                candidate.MessageId);

            Console.WriteLine(
                $"message={candidate.MessageId}, dead-letter-id={deadLetterId}, acknowledged={acknowledgedCount}");
        }

        await PrintDeadLetterStreamAsync(database, deadLetterStreamKey);
    }

    private static async Task PrepareRepeatedFailureForDemoAsync(
        IDatabase database,
        string streamKey,
        string groupName,
        string consumerName)
    {
        // FailureSimulationScenario에서 consumer-a가 ACK하지 않은 메시지를 이미 Pending으로 만들어 둔다.
        // XREADGROUP ... 0 으로 자기 Pending 메시지를 다시 읽으면 delivery count가 증가한다.
        for (var attempt = 1; attempt <= 2; attempt++)
        {
            var entries = await database.StreamReadGroupAsync(
                key: streamKey,
                groupName: groupName,
                consumerName: consumerName,
                position: "0",
                count: PendingScanCount);

            Console.WriteLine();
            Console.WriteLine($"=== REDELIVERY ATTEMPT {attempt} ===");
            Console.WriteLine($"read-count={entries.Length}");
        }
    }

    private static NameValueEntry[] BuildDeadLetterFields(
        string streamKey,
        StreamPendingMessageInfo candidate,
        StreamEntry originalEntry)
    {
        var fields = new List<NameValueEntry>
        {
            new("originalStream", streamKey),
            new("originalMessageId", candidate.MessageId),
            new("originalConsumer", candidate.ConsumerName),
            new("idleTimeMs", candidate.IdleTimeInMilliseconds),
            new("deliveryCount", candidate.DeliveryCount),
            new("reason", "max-delivery-count")
        };

        foreach (var value in originalEntry.Values)
        {
            fields.Add(new NameValueEntry(
                $"original.{value.Name}",
                value.Value));
        }

        return fields.ToArray();
    }

    private static async Task PrintDeadLetterStreamAsync(
        IDatabase database,
        string deadLetterStreamKey)
    {
        var entries = await database.StreamRangeAsync(deadLetterStreamKey);

        Console.WriteLine();
        Console.WriteLine("=== DEAD LETTER STREAM ===");
        Console.WriteLine($"stream={deadLetterStreamKey}, count={entries.Length}");

        foreach (var entry in entries)
        {
            Console.WriteLine($"message={entry.Id}");

            foreach (var value in entry.Values)
            {
                Console.WriteLine($"  {value.Name}={value.Value}");
            }
        }
    }
}
