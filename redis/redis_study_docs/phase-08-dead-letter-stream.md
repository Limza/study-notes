---
tags:
  - redis
  - redis-stream
  - dead-letter
  - csharp
---

# Phase 08. Dead Letter Stream 처리

> [!NOTE] 목표
> 반복해서 재처리해도 실패하는 Pending 메시지를  
> 원본 Consumer Group에 계속 남겨 두지 않고, 별도 Dead Letter Stream으로 분리한다.

---

## 이번 Phase에서 할 일

- Pending 메시지의 `delivery count`를 확인한다.
- `delivery count`가 기준 이상인 메시지를 Dead Letter 후보로 판단한다.
- Dead Letter 전용 Consumer가 후보 메시지의 소유권을 가져온다.
- 소유권을 가져오면서 원본 메시지 본문도 함께 받는다.
- 공통 Dead Letter 양식으로 변환해 Dead Letter Stream에 `XADD`한다.
- 원본 Consumer Group에서는 `XACK`로 Pending 상태를 정리한다.

---

## 이번 Phase에서 만들 파일

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/DeadLetterScenario.cs
```

클래스 / 메서드:

```text
DeadLetterScenario.RunAsync
```

역할:

```text
delivery count가 기준 이상인 Pending 메시지를 Dead Letter Stream으로 복사하고,
원본 Consumer Group에서는 XACK로 Pending 상태를 정리한다.
```

---

## `DeadLetterScenario.cs` 작성

먼저 전체 파일 예시를 만든다.

```csharp
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
```

---

## 왜 `XRANGE`를 따로 쓰지 않았나

`XPENDING` 상세 조회는 Pending 메시지의 메타데이터만 보여준다.

```text
message id
owner consumer
idle time
delivery count
```

즉 `XPENDING` 결과만으로는 원본 `eventType`, `matchId` 같은 field-value를 알 수 없다.

원본 본문을 얻는 방법은 두 가지다.

```redis
XRANGE game:events <message-id> <message-id> COUNT 1
```

또는

```redis
XCLAIM game:events game-workers dead-letter-consumer 0 <message-id>
```

처음 예제에서는 `XRANGE`로 본문을 다시 조회했다.
하지만 Dead Letter 처리에서는 어차피 메시지를 정리할 주체가 필요하므로,
`XCLAIM`으로 Dead Letter 전용 Consumer가 소유권을 가져오면서 본문도 같이 받는 쪽이 더 자연스럽다.

---

## Redis 명령어와 C# API 연결

Pending 상세 조회:

```redis
XPENDING game:events game-workers - + 10
```

C# 코드:

```csharp
var pendingMessages = await database.StreamPendingMessagesAsync(
    streamKey,
    groupName,
    count: PendingScanCount,
    consumerName: RedisValue.Null);
```

Dead Letter Consumer가 소유권과 본문을 함께 가져오기:

```redis
XCLAIM game:events game-workers dead-letter-consumer 0 <message-id>
```

C# 코드:

```csharp
var claimedEntries = await database.StreamClaimAsync(
    streamKey,
    groupName,
    deadLetterConsumerName,
    minIdleTimeInMs: 0,
    messageIds: candidatesById.Keys.ToArray());
```

Dead Letter Stream에 복사:

```redis
XADD game:events:dead-letter * schema redis-stream-dead-letter/v1 originalMessageId <message-id> ...
```

C# 코드:

```csharp
var deadLetterId = await database.StreamAddAsync(
    deadLetterStreamKey,
    envelope.ToStreamFields());
```

원본 Consumer Group의 Pending 정리:

```redis
XACK game:events game-workers <message-id>
```

C# 코드:

```csharp
var acknowledgedCount = await database.StreamAcknowledgeAsync(
    streamKey,
    groupName,
    candidate.MessageId);
```

---

## Dead Letter 공통 양식

이 예제에서는 `DeadLetterEnvelope`를 공통 양식으로 둔다.

```text
schema=redis-stream-dead-letter/v1
originalStream=game:events
originalMessageId=<원본 메시지 ID>
originalConsumer=<마지막 소유 Consumer>
deadLetterConsumer=dead-letter-consumer
idleTimeMs=<Pending으로 머문 시간>
deliveryCount=<전달 횟수>
reason=max-delivery-count
original.eventType=<원본 eventType>
original.matchId=<원본 matchId>
```

`schema`를 넣어 두면 나중에 Dead Letter 양식을 바꾸더라도 버전별로 해석할 수 있다.

---

## 소유권과 `XACK`

Pending 메시지는 Consumer Group 안에서 특정 Consumer에게 소유되어 있다.

`XAUTOCLAIM`이나 `XCLAIM`은 이 소유권을 다른 Consumer로 옮긴다.
이 예제에서는 `dead-letter-consumer`가 소유권을 가져온 뒤 Dead Letter Stream에 기록하고 `XACK`한다.

하지만 Redis는 `XACK`를 호출할 때 호출자가 메시지 owner인지 검사하지 않는다.

```text
XACK stream group message-id
```

이 명령은 해당 메시지 ID가 그 Consumer Group의 Pending Entries List에 있으면 제거하고 `1`을 반환한다.
Pending 목록에 없으면 `0`을 반환한다.

따라서 소유권 없이도 `XACK`는 가능하다.
다만 운영 코드에서는 위험하다.

소유권 없이 `XACK`하면 다른 Consumer가 아직 처리 중인 메시지를 Pending 목록에서 지워버릴 수 있다.
Redis 입장에서는 “처리 완료”로 보이지만, 실제 업무 처리는 끝나지 않았을 수 있다.

그래서 안전한 흐름은 아래처럼 잡는다.

```text
1. XPENDING으로 반복 실패 후보를 찾는다.
2. XCLAIM 또는 XAUTOCLAIM으로 Dead Letter 처리 Consumer가 소유권을 가져온다.
3. Dead Letter Stream에 원본 메시지와 실패 메타데이터를 기록한다.
4. 기록이 성공하면 XACK로 원본 Pending을 정리한다.
```

이번 Phase의 핵심은 Dead Letter가 단순 삭제가 아니라는 점이다.
반복 실패 메시지를 별도 Stream에 보존한 뒤, 원본 Consumer Group에서는 더 이상 재처리 대상이 아니도록 정리하는 것이다.
