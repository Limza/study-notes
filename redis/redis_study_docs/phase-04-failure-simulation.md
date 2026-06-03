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
        // 장애 재현에 사용할 Redis Stream key를 정한다.
        // 모든 테스트 메시지는 이 Stream 안에 message id 단위로 쌓인다.
        var streamKey = "game:events";

        // Consumer Group 이름을 정한다.
        // 같은 Group 안의 Consumer들은 메시지를 나눠서 처리한다.
        var groupName = "game-workers";

        // 메시지를 읽을 Consumer 이름을 정한다.
        // 이 Consumer가 메시지를 가져가고 ACK하지 않는 상황을 만들 것이다.
        var consumerName = "consumer-a";

        // Stream에 넣을 테스트 메시지 개수다.
        var messageCount = 10;

        // Consumer가 읽어 갈 메시지 개수다.
        // 10개 중 5개만 읽고 ACK하지 않아서 Pending 상태를 만든다.
        var readCount = 5;

        try
        {
            // Redis의 XGROUP CREATE에 해당한다.
            // streamKey에 groupName이라는 Consumer Group을 만든다.
            // position: "0-0"은 Stream의 처음부터 메시지를 읽을 수 있게 시작 위치를 잡는다는 뜻이다.
            // createStream: true는 Stream key가 아직 없어도 함께 만들겠다는 뜻이다.
            await database.StreamCreateConsumerGroupAsync(
                streamKey,
                groupName,
                position: "0-0",
                createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            // 같은 Stream에 같은 Consumer Group을 다시 만들면 Redis가 BUSYGROUP 에러를 낸다.
            // 실습을 여러 번 실행할 수 있도록, 이미 있으면 실패로 보지 않고 그대로 진행한다.
            Console.WriteLine("Consumer group already exists.");
        }

        // Producer 역할로 테스트 메시지를 Stream에 추가한다.
        // 반복문 한 바퀴마다 Redis XADD가 한 번 실행되고, 메시지 ID가 하나 생긴다.
        for (var i = 1; i <= messageCount; i++)
        {
            // StreamAddAsync는 Redis의 XADD 명령에 해당한다.
            // 첫 번째 인자는 메시지를 넣을 Stream key다.
            // 두 번째 인자는 하나의 메시지 ID에 함께 저장될 field-value 목록이다.
            await database.StreamAddAsync(
                streamKey,
                new NameValueEntry[]
                {
                    // eventType field에는 이벤트 종류를 저장한다.
                    new("eventType", "match.completed"),

                    // matchId field에는 반복문 번호를 이용해 match-001 같은 값을 저장한다.
                    new("matchId", $"match-{i:000}")
                });
        }

        // consumer-a가 Consumer Group을 통해 새 메시지 5개를 읽는다.
        // StreamReadGroupAsync는 Redis의 XREADGROUP 명령에 해당한다.
        // position: ">"는 이 Group에서 아직 어떤 Consumer에게도 전달되지 않은 새 메시지만 읽겠다는 뜻이다.
        // 메시지를 읽는 순간 Redis는 "consumer-a에게 전달했지만 아직 ACK되지 않았다"는 Pending 기록을 만든다.
        var entries = await database.StreamReadGroupAsync(
            key: streamKey,
            groupName: groupName,
            consumerName: consumerName,
            position: ">",
            count: readCount);

        // 처리 도중 Consumer가 죽은 상황을 흉내 낸다.
        // 실제 업무 코드라면 처리 성공 후 StreamAcknowledgeAsync, 즉 XACK를 호출해야 한다.
        // 여기서는 일부러 ACK하지 않아서 entries에 들어 있는 메시지들이 Pending 상태로 남게 한다.
        foreach (var entry in entries)
        {
            // entry.Id는 Redis가 각 Stream 메시지에 붙인 message id다.
            Console.WriteLine($"Processing before crash: {entry.Id}");

            // 처리 시간이 조금 걸리는 것처럼 보이게 하는 지연이다.
            await Task.Delay(TimeSpan.FromMilliseconds(300));
        }

        // 실제 프로세스를 종료하지는 않고, ACK 전에 죽었다는 상황만 출력으로 표시한다.
        Console.WriteLine("Simulated crash before XACK.");

        // 방금 만든 Pending 상태를 StackExchange.Redis API로 확인한다.
        await PrintPendingStatusAsync(database, streamKey, groupName);
    }

    private static async Task PrintPendingStatusAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        // StreamPendingAsync는 Redis의 XPENDING 요약 조회에 해당한다.
        // Group 전체의 Pending 개수, 가장 낮은 message id, 가장 높은 message id를 확인한다.
        var pending = await database.StreamPendingAsync(streamKey, groupName);

        // StreamPendingMessagesAsync는 XPENDING 상세 조회에 가깝다.
        // Pending 메시지마다 message id, owner consumer, delivery count 등을 확인한다.
        // consumerName: RedisValue.Null은 특정 Consumer로 제한하지 않고 전체 Pending을 보겠다는 뜻이다.
        var pendingMessages = await database.StreamPendingMessagesAsync(
            streamKey,
            groupName,
            count: 10,
            consumerName: RedisValue.Null);

        Console.WriteLine();
        Console.WriteLine("=== Pending Status ===");
        Console.WriteLine(
            $"pending={pending.PendingMessageCount}, lowest={pending.LowestPendingMessageId}, highest={pending.HighestPendingMessageId}");

        foreach (var message in pendingMessages)
        {
            // delivery-count는 이 메시지가 Consumer에게 몇 번 전달됐는지를 나타낸다.
            // 값이 높으면 같은 메시지를 여러 번 재처리하다 실패했을 가능성을 의심할 수 있다.
            Console.WriteLine(
                $"message={message.MessageId}, consumer={message.ConsumerName}, delivery-count={message.DeliveryCount}");
        }
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
// Producer가 메시지 10개를 발행한다.
// 반복문 한 바퀴가 Stream 메시지 한 건이고, message id도 하나씩 생긴다.
for (var i = 1; i <= 10; i++)
{
    await database.StreamAddAsync(
        "game:events",
        new NameValueEntry[]
        {
            // 이 값들은 game:events Stream 안의 메시지 field-value다.
            new("eventType", "match.completed"),
            new("matchId", $"match-{i:000}")
        });
}

// consumer-a가 새 메시지를 읽어서 Pending 상태를 만든다.
// position: ">"는 아직 Group에 전달되지 않은 새 메시지만 읽겠다는 뜻이다.
var entries = await database.StreamReadGroupAsync(
    key: "game:events",
    groupName: "game-workers",
    consumerName: "consumer-a",
    position: ">",
    count: 5);

foreach (var entry in entries)
{
    // 처리 도중 종료되는 상황을 흉내 내고, 일부러 ACK하지 않는다.
    // ACK하지 않으면 이 message id는 consumer-a의 Pending으로 남는다.
    Console.WriteLine($"Processing before crash: {entry.Id}");
    await Task.Delay(TimeSpan.FromMilliseconds(300));
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
    // 따라서 Redis 입장에서는 메시지를 전달했지만 완료 확인을 받지 못한 상태가 된다.
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
    // idle time이 길면 복구 Consumer가 가져가야 할 후보로 볼 수 있다.
    await Task.Delay(TimeSpan.FromSeconds(30));
}
```

---

## C#에서 Pending 확인

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/FailureSimulationScenario.cs
```

클래스 / 메서드:

```text
FailureSimulationScenario.PrintPendingStatusAsync
```

역할:

```text
ACK되지 않은 메시지가 몇 개이고, 어떤 Consumer에게 묶여 있는지 확인한다.
```

```csharp
// StreamPendingAsync는 Group 전체 Pending 요약을 가져온다.
var pending = await database.StreamPendingAsync(streamKey, groupName);

// StreamPendingMessagesAsync는 Pending 메시지별 상세 정보를 가져온다.
// RedisValue.Null은 특정 Consumer 하나가 아니라 전체 Consumer를 대상으로 본다는 뜻이다.
var pendingMessages = await database.StreamPendingMessagesAsync(
    streamKey,
    groupName,
    count: 10,
    consumerName: RedisValue.Null);

Console.WriteLine(
    $"pending={pending.PendingMessageCount}, lowest={pending.LowestPendingMessageId}, highest={pending.HighestPendingMessageId}");

foreach (var message in pendingMessages)
{
    // MessageId는 ACK되지 않은 Stream 메시지 ID다.
    // ConsumerName은 그 메시지를 가져간 Consumer 이름이다.
    // DeliveryCount는 메시지가 전달된 횟수다.
    Console.WriteLine(
        $"message={message.MessageId}, consumer={message.ConsumerName}, delivery-count={message.DeliveryCount}");
}
```

---

## 예상 관찰 결과

```text
pending=5
읽은 메시지 5개가 consumer-a에 묶여 있음
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
