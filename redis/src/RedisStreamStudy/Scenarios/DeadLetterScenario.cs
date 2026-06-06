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
        var deadLetterConsumerName = "dead-letter-consumer";
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

        if (deadLetterCandidates.Length == 0)
        {
            await PrintDeadLetterStreamAsync(database, deadLetterStreamKey);
            return;
        }

        var candidatesById = deadLetterCandidates.ToDictionary(
            message => message.MessageId,
            message => message);

        // XCLAIM stream group consumer min-idle-time message-id ... 와 같은 역할이다.
        // Dead Letter 전용 Consumer가 소유권을 가져오면서 원본 field-value도 함께 받는다.
        var claimedEntries = await database.StreamClaimAsync(
            streamKey,
            groupName,
            deadLetterConsumerName,
            minIdleTimeInMs: 0,
            messageIds: candidatesById.Keys.ToArray());

        foreach (var originalEntry in claimedEntries)
        {
            if (!candidatesById.TryGetValue(originalEntry.Id, out var candidate))
            {
                continue;
            }

            var envelope = new DeadLetterEnvelope(
                OriginalStream: streamKey,
                OriginalMessageId: candidate.MessageId,
                OriginalConsumer: candidate.ConsumerName,
                DeadLetterConsumer: deadLetterConsumerName,
                IdleTimeMs: candidate.IdleTimeInMilliseconds,
                DeliveryCount: candidate.DeliveryCount,
                Reason: "max-delivery-count",
                OriginalValues: originalEntry.Values);

            // XADD game:events:dead-letter * ... 와 같은 역할이다.
            // 원본 메시지와 실패 판단에 필요한 메타데이터를 별도 Stream에 남긴다.
            var deadLetterId = await database.StreamAddAsync(
                deadLetterStreamKey,
                envelope.ToStreamFields());

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

    private sealed record DeadLetterEnvelope(
        RedisValue OriginalStream,
        RedisValue OriginalMessageId,
        RedisValue OriginalConsumer,
        RedisValue DeadLetterConsumer,
        long IdleTimeMs,
        long DeliveryCount,
        RedisValue Reason,
        NameValueEntry[] OriginalValues)
    {
        public NameValueEntry[] ToStreamFields()
        {
            var fields = new List<NameValueEntry>
            {
                new("schema", "redis-stream-dead-letter/v1"),
                new("originalStream", OriginalStream),
                new("originalMessageId", OriginalMessageId),
                new("originalConsumer", OriginalConsumer),
                new("deadLetterConsumer", DeadLetterConsumer),
                new("idleTimeMs", IdleTimeMs),
                new("deliveryCount", DeliveryCount),
                new("reason", Reason)
            };

            foreach (var value in OriginalValues)
            {
                fields.Add(new NameValueEntry(
                    $"original.{value.Name}",
                    value.Value));
            }

            return fields.ToArray();
        }
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
