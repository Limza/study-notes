---
tags:
  - redis
  - redis-stream
  - failure-simulation
  - csharp
---

# Phase 04. 장애 상황 시뮬레이션

> [!NOTE] 목표
> Consumer가 메시지를 읽은 뒤 ACK 전에 종료되는 상황을 재현한다.  
> Redis에 어떤 상태가 남는지 직접 확인한다.

---

## 이번 Phase에서 할 일

- ACK하지 않는 Consumer를 만든다.
- 처리 중 예외로 종료되는 상황을 만든다.
- Pending 메시지가 생기는지 확인한다.
- Consumer 장애와 Redis 장애를 구분한다.

---

## 필요한 지식

Consumer Group에서 메시지를 읽으면 Redis는 메시지를 특정 Consumer에게 전달한 것으로 기록한다.

하지만 `XACK` 전에는 처리 완료가 아니다.

Consumer가 이 시점에 죽으면 메시지는 Pending 상태로 남는다.

---

## 이번 Phase에서 만들 파일

ACK 전 종료 상황은 정상 Consumer 코드와 섞지 않고 장애 재현 시나리오로 분리한다.

```text
study-notes/
  redis/
    src/
      RedisStreamStudy/
        Program.cs
        Scenarios/
          FailureSimulationScenario.cs
```

`FailureSimulationScenario.cs`에는 메시지를 읽고 ACK하지 않는 Consumer, 예외 종료, 처리 지연 실험 코드를 넣는다.

`Program.cs`에서는 `FailureSimulationScenario`를 실행해서 Pending 메시지가 남는 상태를 만든다.

---

## `FailureSimulationScenario.cs` 작성

먼저 아래 전체 파일 예시를 만든다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/FailureSimulationScenario.cs
```

클래스 / 메서드:

```text
FailureSimulationScenario.RunAsync
```

역할:

```text
Consumer가 메시지를 읽고 ACK하지 않아 Pending 메시지가 남는 상황을 만든다.
```

```csharp
using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class FailureSimulationScenario
{
    public static async Task RunAsync(IDatabase database)
    {
        // 장애를 재현할 Stream, Consumer Group, Consumer 이름을 정한다.
        var streamKey = "game:events";
        var groupName = "game-workers";
        var consumerName = "consumer-a";

        try
        {
            // Consumer Group이 없으면 만들고, 이미 있으면 아래 catch에서 넘어간다.
            await database.StreamCreateConsumerGroupAsync(
                streamKey,
                groupName,
                position: "0-0",
                createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            Console.WriteLine("Consumer group already exists.");
        }

        // consumer-a가 읽을 테스트 메시지 5개를 Stream에 추가한다.
        for (var i = 1; i <= 5; i++)
        {
            await database.StreamAddAsync(
                streamKey,
                new NameValueEntry[]
                {
                    new("eventType", "match.completed"),
                    new("matchId", $"match-{i:000}")
                });
        }

        // consumer-a가 새 메시지를 읽는다.
        // 이 순간 Redis에는 consumer-a가 메시지를 가져갔다는 Pending 기록이 생긴다.
        var entries = await database.StreamReadGroupAsync(
            key: streamKey,
            groupName: groupName,
            consumerName: consumerName,
            position: ">",
            count: 5);

        // 일부러 XACK를 호출하지 않는다.
        // 그래서 아래 메시지들은 처리 완료가 아니라 Pending 상태로 남는다.
        foreach (var entry in entries)
        {
            Console.WriteLine($"Read but not ack: {entry.Id}");
        }

        // 실제 프로세스 종료 대신, ACK 전에 죽었다는 상황을 출력으로 표시한다.
        Console.WriteLine("Simulated crash before XACK.");
    }
}
```

`database`는 `Program.cs`에서 만들어서 `FailureSimulationScenario.RunAsync(database)`에 넘긴다.

`Program.cs`의 호출 부분:

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Program.cs
```

```csharp
// ...

// Phase 04에서는 ACK하지 않는 장애 재현 시나리오를 실행한다.
await FailureSimulationScenario.RunAsync(database);

// ...
```

---

## 기본 장애 시나리오

```text
1. Producer가 메시지 10개 발행
2. Consumer A가 메시지 5개 읽기
3. Consumer A가 처리 도중 종료
4. XACK가 호출되지 않음
5. 메시지 5개가 Pending 상태로 남음
```

---

## ACK를 하지 않는 Consumer 예시

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/FailureSimulationScenario.cs
```

클래스 / 메서드:

```text
FailureSimulationScenario.RunAsync
```

역할:

```text
consumer-a가 메시지를 읽지만 ACK하지 않아 Pending 상태를 만든다.
```

```csharp
// consumer-a가 새 메시지를 읽어서 Pending 상태를 만든다.
var entries = await database.StreamReadGroupAsync(
    key: "game:events",
    groupName: "game-workers",
    consumerName: "consumer-a",
    position: ">",
    count: 5);

foreach (var entry in entries)
{
    // 일부러 ACK하지 않고 읽었다는 사실만 출력한다.
    Console.WriteLine($"Read but not ack: {entry.Id}");
}

// ACK 전에 Consumer가 죽은 상황을 흉내 낸다.
Console.WriteLine("Simulated crash before XACK.");
return;
```

---

## 예외로 종료하는 방식

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/FailureSimulationScenario.cs
```

클래스 / 메서드:

```text
FailureSimulationScenario.RunAsync
```

역할:

```text
처리 중 예외가 발생해 XACK 전에 Consumer가 종료되는 상황을 만든다.
```

```csharp
foreach (var entry in entries)
{
    Console.WriteLine($"Processing: {entry.Id}");

    // 처리 중 예외가 나면 아래 XACK 단계까지 가지 못한다.
    throw new InvalidOperationException("Consumer crashed before ACK.");
}
```

---

## 처리 지연 시뮬레이션

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/FailureSimulationScenario.cs
```

클래스 / 메서드:

```text
FailureSimulationScenario.RunAsync
```

역할:

```text
메시지 처리 시간이 길어질 때 idle time이 증가하는 상황을 관찰한다.
```

```csharp
foreach (var entry in entries)
{
    Console.WriteLine($"Long processing: {entry.Id}");

    // 처리 시간이 길어지면 Pending 메시지의 idle time이 증가한다.
    await Task.Delay(TimeSpan.FromSeconds(30));
}
```

---

## 확인 명령어

```redis
XPENDING game:events game-workers
XPENDING game:events game-workers - + 10
XINFO CONSUMERS game:events game-workers
```

---

## 예상 관찰 결과

```text
pending count > 0
consumer-a에 pending 메시지가 묶여 있음
idle time이 시간이 지날수록 증가함
delivery count는 최초 전달이면 1
```

---

## Consumer 장애와 Redis 장애

| 구분 | 상태 | 확인 포인트 |
| --- | --- | --- |
| Consumer 장애 | Redis는 살아 있음 | Pending 메시지를 조회할 수 있음 |
| Redis 장애 | Redis 연결이 끊김 | 명령어 실행 자체가 실패할 수 있음 |

학습 초반에는 Consumer 장애를 먼저 다룬다.

Redis 컨테이너 중지 실험은 추가 실험으로 둔다.
