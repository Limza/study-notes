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
- 원본 Stream에서 메시지 본문을 다시 조회한다.
- Dead Letter Stream에 원본 메시지와 실패 메타데이터를 복사한다.
- 원본 Consumer Group에서는 `XACK`로 Pending 상태를 정리한다.

---

## 왜 Dead Letter Stream이 필요한가

`XAUTOCLAIM`으로 Pending 메시지를 다른 Consumer가 가져와 재처리할 수 있다.

하지만 같은 메시지가 계속 실패한다면 다시 가져와도 같은 실패가 반복될 가능성이 높다.
이런 메시지를 계속 Pending 목록에 남겨 두면 운영자는 정상 복구 대상과 문제 메시지를 구분하기 어려워진다.

그래서 일정 기준 이상 실패한 메시지는 별도 Stream으로 옮긴다.
이 별도 Stream을 여기서는 Dead Letter Stream이라고 부른다.

```text
game:events
  -> 원본 업무 메시지 Stream

game:events:dead-letter
  -> 반복 실패 메시지를 격리해 두는 Stream
```

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

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/DeadLetterScenario.cs
```

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
```

---

## `Program.cs` 호출 추가

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Program.cs
```

`FailureSimulationScenario`가 Pending 메시지를 만들고, `PendingAlertScenario`가 알림 payload를 출력한 다음,
`DeadLetterScenario`가 반복 실패 메시지를 격리하도록 호출한다.

```csharp
// 먼저 Consumer가 메시지를 읽고 ACK하지 않은 장애 상황을 만든다.
await FailureSimulationScenario.RunAsync(database, streamKey, groupName);

// Pending 상태를 알림 형식으로 만든 뒤 콘솔에 출력한다.
await PendingAlertScenario.RunAsync(database, streamKey, groupName);

// delivery count가 기준 이상으로 반복 실패한 Pending 메시지를
// Dead Letter Stream으로 복사하고 원본 Consumer Group에서는 XACK로 정리한다.
await DeadLetterScenario.RunAsync(database, streamKey, groupName);
```

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

원본 메시지 본문 조회:

```redis
XRANGE game:events <message-id> <message-id> COUNT 1
```

C# 코드:

```csharp
var originalEntries = await database.StreamRangeAsync(
    streamKey,
    candidate.MessageId,
    candidate.MessageId,
    count: 1);
```

Dead Letter Stream에 복사:

```redis
XADD game:events:dead-letter * originalMessageId <message-id> reason max-delivery-count ...
```

C# 코드:

```csharp
var deadLetterId = await database.StreamAddAsync(
    deadLetterStreamKey,
    deadLetterFields);
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

## 판단 기준

```text
deliveryCount < 3
=> 아직 재처리 후보

deliveryCount >= 3
=> Dead Letter Stream으로 분리
```

여기서 `3`은 실습용 기준이다.
운영에서는 메시지 처리 비용, 외부 API 실패 가능성, 재시도 정책을 보고 정해야 한다.

중요한 점은 `pendingCount`가 아니라 `deliveryCount`를 본다는 것이다.

`pendingCount`는 ACK되지 않은 메시지가 몇 개인지만 알려준다.
하지만 Dead Letter 판단에는 같은 메시지가 몇 번이나 Consumer에게 전달되었는지가 더 중요하다.

---

## Dead Letter Stream에 남기는 값

| 필드 | 의미 |
| --- | --- |
| `originalStream` | 원본 Stream 이름 |
| `originalMessageId` | 원본 메시지 ID |
| `originalConsumer` | 마지막으로 메시지를 소유한 Consumer |
| `idleTimeMs` | ACK 없이 Pending으로 머문 시간 |
| `deliveryCount` | Consumer에게 전달된 횟수 |
| `reason` | Dead Letter로 보낸 이유 |
| `original.eventType` | 원본 메시지의 `eventType` 필드 |
| `original.matchId` | 원본 메시지의 `matchId` 필드 |

Dead Letter Stream은 단순히 실패 메시지를 버리는 곳이 아니다.
나중에 원인을 분석하거나 수동 재처리할 수 있도록 원본 내용과 실패 메타데이터를 같이 남기는 곳이다.

---

## 실행 결과 예시

콘솔에는 반복 전달 준비, 후보 수, Dead Letter Stream 내용이 출력된다.

```text
=== REDELIVERY ATTEMPT 1 ===
read-count=5

=== REDELIVERY ATTEMPT 2 ===
read-count=5

=== DEAD LETTER CANDIDATES ===
candidate-count=5
message=1710000000000-0, dead-letter-id=1710000001000-0, acknowledged=1

=== DEAD LETTER STREAM ===
stream=game:events:dead-letter, count=5
message=1710000001000-0
  originalStream=game:events
  originalMessageId=1710000000000-0
  originalConsumer=consumer-a
  idleTimeMs=1200
  deliveryCount=3
  reason=max-delivery-count
  original.eventType=match.completed
  original.matchId=match-001
```

---

## 실행 흐름

```text
1. FailureSimulationScenario가 메시지를 읽고 ACK하지 않아 Pending을 만든다.
2. PendingAlertScenario가 Pending 상태를 Slack payload로 출력한다.
3. DeadLetterScenario가 Pending 메시지를 다시 읽어 delivery count를 증가시킨다.
4. delivery count가 기준 이상인 메시지를 후보로 고른다.
5. 원본 Stream에서 메시지 본문을 조회한다.
6. Dead Letter Stream에 원본 본문과 실패 메타데이터를 XADD한다.
7. 원본 Consumer Group에서는 XACK로 Pending을 정리한다.
```

이번 Phase의 핵심은 복구가 항상 재처리를 뜻하지 않는다는 점이다.
반복 실패 메시지는 정상 메시지 흐름에서 분리하고, 별도 Stream에 남겨서 분석하거나 수동 처리하는 편이 더 안전하다.
