---
tags:
  - redis
  - redis-stream
  - recovery
  - csharp
---

# Phase 06. Pending 메시지 복구

> [!NOTE] 목표
> Pending 메시지 중 idle time이 `min-idle-time` 이상인 메시지를 다른 Consumer가 가져와 재처리한다.  
> 핵심은 `XAUTOCLAIM`으로 소유권을 가져오고, 재처리 성공 후 `XACK`로 Pending 상태를 정리하는 것이다.

---

## 이번 Phase에서 할 일

- Pending 메시지의 idle time이 복구 기준을 넘었는지 확인한다.
- `XAUTOCLAIM`으로 메시지 소유권을 가져온다.
- `XAUTOCLAIM` 응답에서 실제 메시지 ID와 field-value를 꺼내 출력한다.
- `XPENDING`으로 owner consumer와 delivery count가 어떻게 바뀌었는지 확인한다.
- 재처리에 성공했다고 가정하고 `XACK`로 Pending 상태에서 제거한다.
- `XACK` 후 `XPENDING`을 다시 조회해서 Pending 목록이 비었는지 확인한다.

---

## 필요한 지식

Pending 메시지는 이미 어떤 Consumer에게 전달되었지만 ACK되지 않은 메시지다.

다른 Consumer가 이 메시지를 처리하려면 소유권을 가져와야 한다.

Redis Stream에서는 `XCLAIM` 또는 `XAUTOCLAIM`을 사용할 수 있다.

이 학습에서는 `XAUTOCLAIM`을 우선 사용한다.

`XAUTOCLAIM`의 시간 기준은 `min-idle-time`이다.

`min-idle-time`은 Pending 메시지가 마지막으로 Consumer에게 전달된 뒤 ACK 없이 유지된 시간을 밀리초 단위로 비교하는 값이다.

예를 들어 `min-idle-time`이 `5000`이면 idle time이 5초 이상인 Pending 메시지만 복구 대상으로 가져온다.

---

## 핵심 명령어

```redis
XAUTOCLAIM game:events game-workers recovery-consumer 2000 0-0 COUNT 10
XPENDING game:events game-workers - + 10
XACK game:events game-workers 1717050000000-0
```

---

## 이번 Phase에서 만들 파일

Pending 메시지 복구는 장애 추적 코드와 분리해서 복구 전용 시나리오로 만든다.

```text
study-notes/
  redis/
    src/
      RedisStreamStudy/
        Program.cs
        Scenarios/
          FailureSimulationScenario.cs
          TroubleshootingScenario.cs
          RecoveryScenario.cs
```

`RecoveryScenario.cs`에는 `XAUTOCLAIM`, `XPENDING`, `XACK` 결과를 사람이 읽을 수 있게 풀어 출력하는 코드를 넣는다.

실습 흐름은 Pending 상태 생성, 복구 실행, `XAUTOCLAIM` 후 Pending 확인, `XACK` 완료 처리, `XACK` 후 Pending 재확인 순서로 진행한다.

---

## `RecoveryScenario.cs` 작성

먼저 아래 전체 파일 예시를 만든다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/RecoveryScenario.cs
```

클래스 / 메서드:

```text
RecoveryScenario.RunAsync
```

역할:

```text
idle time이 min-idle-time 이상인 Pending 메시지를 recovery-consumer가 가져오고, 재처리 성공 후 XACK로 Pending 상태를 정리한다.
```

```csharp
using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class RecoveryScenario
{
    public static async Task RunAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        // 복구 대상 Redis Stream key는 Program.cs에서 넘겨받는다.
        // Pending 메시지가 남아 있는 원본 Stream이다.

        // 복구할 Pending 메시지가 속한 Consumer Group 이름도 Program.cs에서 넘겨받는다.
        // Pending은 Stream 전체가 아니라 Group 단위로 관리된다.

        // 멈춘 Consumer 대신 메시지를 가져와 재처리할 Consumer 이름을 recovery-consumer로 정한다.
        // XAUTOCLAIM 이후 Pending 메시지의 owner가 recovery-consumer로 바뀐다.
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
            "2000",
            "0-0",
            "COUNT",
            "10");

        var claimedMessageIds = PrintAutoClaimResult(autoClaimResult);

        // XAUTOCLAIM 실행 후 Pending 상태가 어떻게 바뀌었는지 다시 확인한다.
        // owner consumer가 recovery-consumer로 바뀌었는지, delivery count가 증가했는지 볼 수 있다.
        var pendingDetails = await database.ExecuteAsync(
            "XPENDING",
            streamKey,
            groupName,
            "-",
            "+",
            "10");

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
```

`XAUTOCLAIM`과 `XPENDING ... - + 10`은 배열 응답을 돌려준다.  
그대로 `Console.WriteLine(result)`를 호출하면 `3 element(s)`, `5 element(s)`처럼 요약만 보이므로, 위 코드처럼 `RedisResult[]`로 펼쳐서 확인한다.

`StreamAcknowledgeAsync`는 Redis의 `XACK` 명령에 해당한다.  
재처리에 성공한 메시지를 ACK하면 Consumer Group의 Pending 목록에서 제거된다.

`database`, `streamKey`, `groupName`은 `Program.cs`에서 만들어서 `RecoveryScenario.RunAsync(database, streamKey, groupName)`에 넘긴다.

`Program.cs`의 호출 부분:

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Program.cs
```

```csharp
// ...

const string streamKey = "game:events";
const string groupName = "game-workers";

// 먼저 ACK하지 않은 Pending 메시지를 만든다.
await FailureSimulationScenario.RunAsync(database, streamKey, groupName);

// 그 다음 recovery-consumer가 idle time이 복구 기준을 넘은 Pending 메시지를 가져오게 한다.
await RecoveryScenario.RunAsync(database, streamKey, groupName);

// ...
```

---

## 복구 흐름

```text
1. Pending 메시지 중 idle time이 min-idle-time 이상인 메시지를 찾는다.
2. recovery-consumer가 XAUTOCLAIM으로 메시지를 가져온다.
3. XAUTOCLAIM 응답에서 가져온 메시지 ID와 field-value를 출력한다.
4. XPENDING으로 owner consumer와 delivery count를 다시 확인한다.
5. 재처리에 성공했다고 가정하고 XACK를 호출한다.
6. XACK 후 XPENDING으로 Pending 목록에서 제거됐는지 확인한다.
```

---

## C#에서 XAUTOCLAIM 호출

`StackExchange.Redis` 버전에 따라 고수준 API가 부족할 수 있다.

그럴 때는 `ExecuteAsync`를 사용한다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/RecoveryScenario.cs
```

클래스 / 메서드:

```text
RecoveryScenario.RunAsync
```

역할:

```text
idle time이 min-idle-time 이상인 Pending 메시지를 recovery-consumer로 가져온다.
```

```csharp
// 2000ms 이상 ACK되지 않은 Pending 메시지를 recovery-consumer에게 옮긴다.
// Redis CLI 기준으로는 아래 명령과 같다.
// XAUTOCLAIM game:events game-workers recovery-consumer 2000 0-0 COUNT 10
var autoClaimResult = await database.ExecuteAsync(
    "XAUTOCLAIM",
    streamKey,
    groupName,
    "recovery-consumer",
    "2000",
    "0-0",
    "COUNT",
    "10");

// XAUTOCLAIM 응답은 배열이다.
// [0] 다음 스캔 시작 ID
// [1] claim된 메시지 목록
// [2] Redis 7 기준으로 더 이상 존재하지 않는 deleted message id 목록
var resultParts = (RedisResult[])autoClaimResult;
var nextStartId = (RedisValue)resultParts[0];
var claimedMessages = (RedisResult[])resultParts[1];

Console.WriteLine($"next-start-id={nextStartId}");
Console.WriteLine($"claimed-count={claimedMessages.Length}");

foreach (var claimedMessage in claimedMessages)
{
    var messageParts = (RedisResult[])claimedMessage;
    var messageId = (RedisValue)messageParts[0];
    var fields = (RedisResult[])messageParts[1];

    Console.WriteLine($"message={messageId}");

    for (var i = 0; i < fields.Length; i += 2)
    {
        var fieldName = (RedisValue)fields[i];
        var fieldValue = (RedisValue)fields[i + 1];

        Console.WriteLine($"  {fieldName}={fieldValue}");
    }
}
```

---

## 처리 성공 후 XACK

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/RecoveryScenario.cs
```

클래스 / 메서드:

```text
RecoveryScenario.RunAsync
```

역할:

```text
재처리에 성공한 Pending 메시지를 Consumer Group의 Pending 목록에서 제거한다.
```

```csharp
// XAUTOCLAIM 결과에서 꺼낸 message id들을 XACK로 완료 처리한다.
// XACK가 성공하면 해당 메시지는 더 이상 Pending으로 잡히지 않는다.
var acknowledgedCount = 0L;

foreach (var messageId in claimedMessageIds)
{
    acknowledgedCount += await database.StreamAcknowledgeAsync(
        streamKey,
        groupName,
        messageId);
}

Console.WriteLine($"acknowledged-count={acknowledgedCount}");
```

## 멱등성 정리

재처리 구조에서는 같은 메시지가 두 번 처리될 수 있다고 가정해야 한다.

두 번 이상 처리될때 문제가 발생하는 컨텐츠는, 
중복 처리를 막아야 한다. 
메세지 id를 따로 저장하거나, orderId, 등.. 

---

## 복구 판단 기준

| 조건 | 판단 |
| --- | --- |
| idle time이 짧음 | 아직 처리 중일 수 있으므로 대기 |
| idle time이 min-idle-time 이상 | 복구 후보 |
| delivery count가 낮음 | 재처리 시도 |
| delivery count가 높음 | 자동 재처리 대신 격리 후보 |
