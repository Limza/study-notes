---
tags:
  - redis
  - redis-stream
  - recovery
  - csharp
---

# Phase 06. Pending 메시지 복구

> [!NOTE] 목표
> 오래된 Pending 메시지를 다른 Consumer가 가져와 재처리한다.  
> 핵심은 `XAUTOCLAIM`으로 소유권을 가져오고, 성공 후 `XACK`하는 것이다.

---

## 이번 Phase에서 할 일

- 오래된 Pending 메시지를 찾는다.
- `XAUTOCLAIM`으로 메시지 소유권을 가져온다.
- 메시지를 재처리한다.
- 성공하면 `XACK`를 호출한다.
- 반복 실패 메시지는 Dead Letter Stream으로 보낸다.

---

## 필요한 지식

Pending 메시지는 이미 어떤 Consumer에게 전달되었지만 ACK되지 않은 메시지다.

다른 Consumer가 이 메시지를 처리하려면 소유권을 가져와야 한다.

Redis Stream에서는 `XCLAIM` 또는 `XAUTOCLAIM`을 사용할 수 있다.

이 학습에서는 `XAUTOCLAIM`을 우선 사용한다.

---

## 핵심 명령어

```redis
XAUTOCLAIM game:events game-workers recovery-consumer 5000 0-0 COUNT 10
XACK game:events game-workers 1717050000000-0
XADD game:events:dead-letter * originalId 1717050000000-0 reason processing_failed
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

`RecoveryScenario.cs`에는 `XAUTOCLAIM`, 재처리, `XACK`, Dead Letter Stream 기록 코드를 넣는다.

실습 흐름은 Pending 상태 생성, 상태 조회, 복구 실행, 복구 후 `XPENDING` 재확인 순서로 진행한다.

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
오래된 Pending 메시지를 recovery-consumer가 가져오고, 성공한 메시지를 ACK한다.
```

```csharp
using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class RecoveryScenario
{
    public static async Task RunAsync(IDatabase database)
    {
        // 복구 대상 Stream, Consumer Group, 복구 담당 Consumer 이름을 정한다.
        var streamKey = "game:events";
        var groupName = "game-workers";
        var recoveryConsumer = "recovery-consumer";

        // XAUTOCLAIM으로 오래된 Pending 메시지의 소유권을 recovery-consumer로 가져온다.
        // 5000은 idle time이 5초 이상인 메시지만 가져오겠다는 기준이다.
        var result = await database.ExecuteAsync(
            "XAUTOCLAIM",
            streamKey,
            groupName,
            recoveryConsumer,
            "5000",
            "0-0",
            "COUNT",
            "10");

        Console.WriteLine(result);

        // 복구 명령 실행 후 Pending 상태가 어떻게 바뀌었는지 다시 확인한다.
        var pendingDetails = await database.ExecuteAsync(
            "XPENDING",
            streamKey,
            groupName,
            "-",
            "+",
            "10");

        Console.WriteLine(pendingDetails);
    }
}
```

`XAUTOCLAIM` 결과를 실제 재처리 코드로 풀어 쓰는 부분은 Redis 응답 파싱이 필요하다.  
이 Phase에서는 먼저 명령 호출과 결과 확인 흐름을 만들고, 다음 단계에서 메시지별 재처리와 `XACK`를 붙인다.

`database`는 `Program.cs`에서 만들어서 `RecoveryScenario.RunAsync(database)`에 넘긴다.

`Program.cs`의 호출 부분:

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Program.cs
```

```csharp
// ...

// 먼저 ACK하지 않은 Pending 메시지를 만든다.
await FailureSimulationScenario.RunAsync(database);

// 그 다음 recovery-consumer가 오래된 Pending 메시지를 가져오게 한다.
await RecoveryScenario.RunAsync(database);

// ...
```

---

## 복구 흐름

```text
1. Pending 메시지 중 idle time이 기준 이상인 메시지를 찾는다.
2. recovery-consumer가 XAUTOCLAIM으로 메시지를 가져온다.
3. 메시지를 다시 처리한다.
4. 성공하면 XACK를 호출한다.
5. 실패가 반복되면 Dead Letter Stream으로 보낸다.
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
오래 idle 상태인 Pending 메시지를 recovery-consumer로 가져온다.
```

```csharp
// XAUTOCLAIM으로 오래 idle 상태인 Pending 메시지를 recovery-consumer에게 옮긴다.
var result = await database.ExecuteAsync(
    "XAUTOCLAIM",
    "game:events",
    "game-workers",
    "recovery-consumer",
    "5000",
    "0-0",
    "COUNT",
    "10");

Console.WriteLine(result);
```

---

## 처리 성공 후 ACK

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
재처리에 성공한 Pending 메시지를 처리 완료 상태로 만든다.
```

```csharp
// 재처리에 성공한 메시지는 XACK로 Pending 상태에서 제거한다.
await database.StreamAcknowledgeAsync(
    "game:events",
    "game-workers",
    messageId);
```

---

## Dead Letter Stream 예시

처리 실패가 반복되는 메시지를 원본 Stream에 계속 두면 복구 루프가 생길 수 있다.

이런 메시지는 별도 Stream에 기록해서 나중에 수동 분석한다.

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
반복 실패 메시지를 game:events:dead-letter Stream에 따로 기록한다.
```

```csharp
// 반복 실패 메시지는 원본 Stream 대신 Dead Letter Stream에 따로 기록한다.
await database.StreamAddAsync(
    "game:events:dead-letter",
    new NameValueEntry[]
    {
        new("originalId", messageId),
        new("reason", "processing_failed"),
        new("failedAt", DateTimeOffset.UtcNow.ToString("O"))
    });
```

---

## 멱등성 정리

재처리 구조에서는 같은 메시지가 두 번 처리될 수 있다고 가정해야 한다.

예를 들어 보상 지급 이벤트가 두 번 처리되면 보상이 중복 지급될 수 있다.

방지 방법:

- 처리 완료한 message id를 별도 저장소에 기록한다.
- 비즈니스 키 기준으로 중복 처리를 막는다.
- DB update를 idempotent하게 설계한다.
- 외부 부작용이 있는 작업은 재시도 정책을 조심해서 설계한다.

---

## 복구 판단 기준

| 조건 | 판단 |
| --- | --- |
| idle time이 짧음 | 아직 처리 중일 수 있으므로 대기 |
| idle time이 기준 이상 | 복구 후보 |
| delivery count가 낮음 | 재처리 시도 |
| delivery count가 높음 | Dead Letter 후보 |
