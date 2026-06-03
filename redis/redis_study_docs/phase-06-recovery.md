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
> 핵심은 `XAUTOCLAIM`으로 소유권을 가져오고, 성공 후 `XACK`하는 것이다.

---

## 이번 Phase에서 할 일

- Pending 메시지의 idle time이 복구 기준을 넘었는지 확인한다.
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

`XAUTOCLAIM`의 시간 기준은 `min-idle-time`이다.

`min-idle-time`은 Pending 메시지가 마지막으로 Consumer에게 전달된 뒤 ACK 없이 유지된 시간을 밀리초 단위로 비교하는 값이다.

예를 들어 `min-idle-time`이 `5000`이면 idle time이 5초 이상인 Pending 메시지만 복구 대상으로 가져온다.

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
idle time이 min-idle-time 이상인 Pending 메시지를 recovery-consumer가 가져오고, 성공한 메시지를 ACK한다.
```

```csharp
using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class RecoveryScenario
{
    public static async Task RunAsync(IDatabase database)
    {
        // 복구 대상 Redis Stream key를 정한다.
        // Pending 메시지가 남아 있는 원본 Stream이다.
        var streamKey = "game:events";

        // 복구할 Pending 메시지가 속한 Consumer Group 이름을 정한다.
        // Pending은 Stream 전체가 아니라 Group 단위로 관리된다.
        var groupName = "game-workers";

        // 멈춘 Consumer 대신 메시지를 가져와 재처리할 Consumer 이름을 정한다.
        // XAUTOCLAIM 이후 Pending 메시지의 owner가 이 Consumer로 바뀐다.
        var recoveryConsumer = "recovery-consumer";

        // XAUTOCLAIM으로 idle time이 min-idle-time 이상인 Pending 메시지를 가져온다.
        // 여기서 5000은 min-idle-time이며, 5초 이상 ACK되지 않은 메시지만 가져오겠다는 뜻이다.
        // "0-0"은 Pending 목록을 처음부터 스캔하겠다는 시작 ID다.
        // COUNT 10은 한 번에 최대 10개까지만 가져오겠다는 뜻이다.
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
        // owner consumer가 recovery-consumer로 바뀌었는지, delivery count가 증가했는지 볼 수 있다.
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

// 그 다음 recovery-consumer가 idle time이 복구 기준을 넘은 Pending 메시지를 가져오게 한다.
await RecoveryScenario.RunAsync(database);

// ...
```

---

## 복구 흐름

```text
1. Pending 메시지 중 idle time이 min-idle-time 이상인 메시지를 찾는다.
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
idle time이 min-idle-time 이상인 Pending 메시지를 recovery-consumer로 가져온다.
```

```csharp
// 5000ms 이상 ACK되지 않은 Pending 메시지를 recovery-consumer에게 옮긴다.
// Redis CLI 기준으로는 아래 명령과 같다.
// XAUTOCLAIM game:events game-workers recovery-consumer 5000 0-0 COUNT 10
var result = await database.ExecuteAsync(
    "XAUTOCLAIM",
    "game:events",
    "game-workers",
    "recovery-consumer",
    "5000",
    "0-0",
    "COUNT",
    "10");

// result에는 다음 스캔 시작 ID와 claim된 메시지 목록이 들어온다.
// 실제 재처리를 하려면 이 응답을 파싱해서 message id와 field-value를 꺼내야 한다.
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
// messageId는 XAUTOCLAIM 결과에서 꺼낸 원본 Stream 메시지 ID다.
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
// originalId에는 원본 message id를 남겨 나중에 추적할 수 있게 한다.
// reason에는 왜 자동 복구 대신 격리했는지 기록한다.
// failedAt은 장애 분석을 위해 UTC 시각으로 남긴다.
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
| idle time이 min-idle-time 이상 | 복구 후보 |
| delivery count가 낮음 | 재처리 시도 |
| delivery count가 높음 | Dead Letter 후보 |
