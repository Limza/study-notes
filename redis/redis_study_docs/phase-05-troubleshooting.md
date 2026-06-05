---
tags:
  - redis
  - redis-stream
  - troubleshooting
---

# Phase 05. 장애 추적 절차 만들기

> [!NOTE] 목표
> Redis Stream 장애가 발생했을 때  
> 어떤 순서로 상태를 확인할지 체크리스트를 만든다.

---

## 이번 Phase에서 할 일

- Stream 길이를 확인한다.
- Consumer Group 상태를 확인한다.
- Consumer별 Pending 상태를 확인한다.
- idle time과 delivery count를 해석한다.
- 재처리 후보를 판단한다.

---

## 장애 추적 순서

장애를 볼 때는 아래 순서로 상태를 확인한다.

```text
1. Stream에 메시지가 쌓이는지 확인
2. Consumer Group 존재 여부 확인
3. Consumer 활성 상태 확인
4. Pending 메시지 존재 여부 확인
5. Pending 메시지가 묶인 Consumer 확인
6. Pending 메시지의 idle time 확인
7. Pending 메시지의 delivery count 확인
8. 재처리 가능 여부 판단
```

---

## 핵심 명령어

```redis
XLEN game:events
XINFO STREAM game:events
XINFO GROUPS game:events
XINFO CONSUMERS game:events game-workers
XPENDING game:events game-workers
XPENDING game:events game-workers - + 10
```

---

## 이번 Phase에서 만들 파일

장애 추적 명령어는 반복해서 사용할 수 있으므로 조회 전용 시나리오로 분리한다.

```text
study-notes/
  redis/
    src/
      RedisStreamStudy/
        Program.cs
        Scenarios/
          FailureSimulationScenario.cs
          TroubleshootingScenario.cs
```

`TroubleshootingScenario.cs`에는 `XLEN`, `XINFO`, `XPENDING` 조회 코드를 넣는다.

실습 흐름은 `FailureSimulationScenario`로 Pending 상태를 만든 뒤, `TroubleshootingScenario`로 Redis 상태를 읽는 방식으로 진행한다.

---

## `TroubleshootingScenario.cs` 작성

먼저 아래 전체 파일 예시를 만든다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/TroubleshootingScenario.cs
```

클래스 / 메서드:

```text
TroubleshootingScenario.RunAsync
```

역할:

```text
Stream 길이, Consumer Group 상태, Pending 메시지 상세를 조회한다.
```

```csharp
using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class TroubleshootingScenario
{
    public static async Task RunAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        // 장애 상태를 조회할 Redis Stream key는 Program.cs에서 넘겨받는다.
        // FailureSimulationScenario에서 메시지를 넣고 Pending을 만든 Stream과 같은 이름이어야 한다.

        // 조회할 Consumer Group 이름도 Program.cs에서 넘겨받는다.
        // Pending 메시지는 Stream 전체가 아니라 Consumer Group 단위로 관리된다.

        // 1단계: Stream에 메시지가 실제로 쌓여 있는지 확인한다.
        // StreamLengthAsync는 Redis의 XLEN 명령에 해당한다.
        // 값이 0이면 Producer가 메시지를 넣지 못했거나 다른 Stream key를 보고 있을 수 있다.
        var length = await database.StreamLengthAsync(streamKey);
        Console.WriteLine($"XLEN {streamKey}: {length}");

        // 2단계: Stream에 어떤 Consumer Group이 붙어 있는지 확인한다.
        // StackExchange.Redis에 전용 고수준 API가 애매한 명령은 ExecuteAsync로 직접 보낼 수 있다.
        // XINFO GROUPS는 Group 이름, Consumer 수, Pending 수, 마지막 전달 ID 등을 보여준다.
        // 여기서 groupName이 안 보이면 Consumer Group 생성이 안 된 것이다.
        var groups = await database.ExecuteAsync("XINFO", "GROUPS", streamKey);
        Console.WriteLine(groups);

        // 3단계: Group 안의 Consumer별 상태를 확인한다.
        // XINFO CONSUMERS는 각 Consumer가 가진 Pending 수와 idle 시간을 보여준다.
        // idle 시간이 길고 Pending이 많으면 해당 Consumer가 멈췄을 가능성을 의심한다.
        var consumers = await database.ExecuteAsync("XINFO", "CONSUMERS", streamKey, groupName);
        Console.WriteLine(consumers);

        // 4단계: Group 전체 Pending 상태를 요약해서 확인한다.
        // XPENDING stream group 형태는 Pending 개수와 Pending message id 범위를 보여준다.
        // Pending이 0이면 현재 ACK되지 않은 메시지가 없다는 뜻이다.
        var pending = await database.ExecuteAsync("XPENDING", streamKey, groupName);
        Console.WriteLine(pending);

        // 5단계: Pending 메시지를 하나씩 상세 조회한다.
        // "-"는 가장 오래된 Pending message id부터 보겠다는 뜻이다.
        // "+"는 가장 최신 Pending message id까지 보겠다는 뜻이다.
        // "10"은 그 범위 안에서 오래된 순서로 최대 10개만 가져오겠다는 뜻이다.
        // 결과에서는 message id, owner consumer, idle time, delivery count를 확인한다.
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

`database`, `streamKey`, `groupName`은 `Program.cs`에서 만들어서 `TroubleshootingScenario.RunAsync(database, streamKey, groupName)`에 넘긴다.

`Program.cs`의 호출 부분:

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Program.cs
```

```csharp
// ...

const string streamKey = "game:events";
const string groupName = "game-workers";

// 먼저 Phase 04 시나리오로 Pending 메시지를 만든다.
await FailureSimulationScenario.RunAsync(database, streamKey, groupName);

// 그 다음 Redis 상태를 조회해서 장애 추적 정보를 확인한다.
await TroubleshootingScenario.RunAsync(database, streamKey, groupName);

// ...
```

---

## `XLEN`

Stream에 쌓인 전체 메시지 수를 본다.

```redis
XLEN game:events
```

메시지가 계속 늘어나는데 Consumer 처리량이 따라가지 못하면 적체 가능성이 있다.

---

## `XINFO GROUPS`

Consumer Group의 상태를 확인한다.

중요하게 볼 항목:

- `name`: 그룹 이름
- `consumers`: Consumer 수
- `pending`: ACK되지 않은 메시지 수
- `last-delivered-id`: 마지막으로 전달된 메시지 ID

---

## `XINFO CONSUMERS`

Consumer별 상태를 확인한다.

중요하게 볼 항목:

- `name`: Consumer 이름
- `pending`: 해당 Consumer가 잡고 있는 Pending 수
- `idle`: 마지막 활동 이후 경과 시간

---

## `XPENDING`

Pending 메시지 요약과 상세를 확인한다.

요약:

```redis
XPENDING game:events game-workers
```

상세:

```redis
XPENDING game:events game-workers - + 10
```

상세 결과에서 볼 것:

- message id
- owner consumer
- idle time
- delivery count

---

## C#에서 직접 명령어 실행

`StackExchange.Redis`에 고수준 API가 부족한 명령은 `ExecuteAsync`를 사용해도 된다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/TroubleshootingScenario.cs
```

클래스 / 메서드:

```text
TroubleshootingScenario.RunAsync
```

역할:

```text
XPENDING 상세 조회를 직접 실행해서 Pending 메시지의 owner, idle time, delivery count를 확인한다.
```

```csharp
// StackExchange.Redis에 전용 메서드가 없을 때는 ExecuteAsync로 Redis 명령을 직접 보낸다.
// 아래 인자들은 Redis CLI의 `XPENDING game:events game-workers - + 10`과 같은 순서다.
var result = await database.ExecuteAsync(
    "XPENDING",
    streamKey,
    groupName,
    "-",
    "+",
    "10");

// result에는 Pending 메시지의 message id, owner consumer, idle time, delivery count가 들어 있다.
// owner와 idle time을 보고 어느 Consumer의 메시지를 복구할지 판단한다.
Console.WriteLine(result);
```

---

## 장애 판단 기준 예시

| 관찰 | 의미 | 판단 |
| --- | --- | --- |
| pending이 0 | 처리 완료 상태 | 장애 가능성 낮음 |
| pending이 많고 idle이 짧음 | 처리 중일 수 있음 | 조금 더 관찰 |
| pending이 많고 idle이 김 | Consumer 장애 가능성 | 복구 후보 |
| delivery count가 높음 | 반복 실패 가능성 | Dead Letter 검토 |
