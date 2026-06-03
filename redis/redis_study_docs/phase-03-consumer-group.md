---
tags:
  - redis
  - redis-stream
  - consumer-group
  - csharp
---

# Phase 03. Consumer Group 이해

> [!NOTE] 목표
> 여러 Consumer가 하나의 Stream을 나눠 처리하는 구조를 만든다.  
> 핵심은 **메시지를 읽는 것**과 **처리 완료 ACK**가 다르다는 점이다.

---

## 이번 Phase에서 할 일

- Consumer Group을 생성한다.
- Consumer 2개가 메시지를 나눠 처리하게 만든다.
- 처리 성공 후 `XACK`를 호출한다.
- `XPENDING`으로 ACK되지 않은 메시지가 없는지 확인한다.

---

## 필요한 지식

### Consumer Group

Consumer Group은 하나의 Stream을 여러 Consumer가 나눠 처리하기 위한 기능이다.

각 메시지는 그룹 안의 특정 Consumer에게 전달된다.

Redis 서버는 명령을 빠르게 처리하고 메시지를 나눠 주는 역할을 한다.

실제로 오래 걸리는 작업은 Redis 밖의 Consumer 애플리케이션에서 수행된다.

그래서 Consumer를 여러 개 실행하면 메시지를 받은 뒤의 처리 작업을 병렬로 나눌 수 있다.

---

### Pending Entries List

Consumer가 메시지를 읽으면 Redis는 해당 메시지를 Pending 상태로 기록한다.

처리 성공 후 `XACK`를 호출해야 Pending 상태에서 제거된다.

즉, 메시지를 읽었다고 처리 완료가 아니다.

---

## 핵심 명령어

```redis
XGROUP CREATE game:events game-workers 0 MKSTREAM
XREADGROUP GROUP game-workers consumer-a COUNT 5 STREAMS game:events >
XACK game:events game-workers 1717050000000-0
XINFO GROUPS game:events
XINFO CONSUMERS game:events game-workers
XPENDING game:events game-workers
```

---

## 이번 Phase에서 만들 파일

Consumer Group 생성과 Consumer별 처리 흐름은 별도 시나리오 파일로 분리한다.

```text
study-notes/
  redis/
    src/
      RedisStreamStudy/
        Program.cs
        Scenarios/
          BasicStreamScenario.cs
          ConsumerGroupScenario.cs
```

`ConsumerGroupScenario.cs`에는 Consumer Group 생성, 메시지 발행, Consumer A/B 처리, ACK 확인 코드를 넣는다.

`Program.cs`에서는 Phase 02 시나리오 대신 `ConsumerGroupScenario`를 실행하도록 바꾼다.

---

## `ConsumerGroupScenario.cs` 작성

먼저 아래 전체 파일 예시를 만든다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/ConsumerGroupScenario.cs
```

클래스 / 메서드:

```text
ConsumerGroupScenario.RunAsync
```

역할:

```text
Consumer Group을 만들고, consumer-a와 consumer-b가 메시지를 나눠 처리한 뒤 ACK한다.
```

```csharp
using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class ConsumerGroupScenario
{
    public static async Task RunAsync(IDatabase database)
    {
        // Consumer Group을 붙일 Redis Stream key를 정한다.
        // Producer가 추가하는 메시지는 모두 이 Stream에 쌓인다.
        var streamKey = "game:events";

        // Consumer Group 이름을 정한다.
        // 같은 Group 안의 Consumer들은 메시지를 나눠서 처리한다.
        var groupName = "game-workers";

        try
        {
            // StreamCreateConsumerGroupAsync는 Redis의 XGROUP CREATE 명령에 해당한다.
            // position: "0-0"은 Stream의 처음부터 읽을 수 있게 Group 시작 위치를 잡는다는 뜻이다.
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
            // 실습을 반복 실행할 수 있도록 이미 있으면 그대로 진행한다.
            Console.WriteLine("Consumer group already exists.");
        }

        // Consumer들이 나눠 처리할 테스트 메시지 10개를 Stream에 추가한다.
        // 반복문 한 바퀴마다 XADD가 한 번 실행되고 message id가 하나 생긴다.
        for (var i = 1; i <= 10; i++)
        {
            await database.StreamAddAsync(
                streamKey,
                new NameValueEntry[]
                {
                    new("eventType", "match.completed"),
                    new("matchId", $"match-{i:000}")
                });
        }

        // consumer-a와 consumer-b를 동시에 실행한다.
        // Redis는 새 메시지를 Consumer들에게 나눠 주고, 오래 걸리는 실제 처리는 각 Consumer가 병렬로 수행한다.
        await Task.WhenAll(
            ReadAndAckAsync(database, streamKey, groupName, "consumer-a"),
            ReadAndAckAsync(database, streamKey, groupName, "consumer-b"));

        // 모든 Consumer가 ACK를 보낸 뒤 Group과 Pending 상태를 확인한다.
        await PrintConsumerGroupStatusAsync(database, streamKey, groupName);
    }

    private static async Task ReadAndAckAsync(
        IDatabase database,
        string streamKey,
        string groupName,
        string consumerName)
    {
        // StreamReadGroupAsync는 Redis의 XREADGROUP 명령에 해당한다.
        // consumerName은 이 메시지를 어떤 Consumer가 가져갔는지 Redis에 기록할 이름이다.
        // position: ">"는 아직 이 Group 안의 어떤 Consumer에게도 전달되지 않은 새 메시지를 읽겠다는 뜻이다.
        // count: 5는 이 Consumer가 최대 5개까지만 가져가겠다는 뜻이다.
        var entries = await database.StreamReadGroupAsync(
            key: streamKey,
            groupName: groupName,
            consumerName: consumerName,
            position: ">",
            count: 5);

        // 실제 서비스에서는 메시지를 읽은 뒤 오래 걸리는 처리를 수행할 수 있다.
        // 메시지를 읽는 순간 Pending 상태가 되고, ACK는 처리가 성공한 뒤에만 보낸다.
        foreach (var entry in entries)
        {
            Console.WriteLine($"{consumerName} processing: {entry.Id}");

            // 시간이 걸리는 비즈니스 처리를 흉내 낸다.
            await Task.Delay(TimeSpan.FromSeconds(1));

            // StreamAcknowledgeAsync는 Redis의 XACK 명령에 해당한다.
            // ACK를 보내면 Redis는 해당 메시지를 Consumer Group의 Pending 상태에서 제거한다.
            await database.StreamAcknowledgeAsync(
                streamKey,
                groupName,
                entry.Id);
        }
    }

    private static async Task PrintConsumerGroupStatusAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        // XINFO GROUPS game:events에 해당한다.
        // Group별 Consumer 수, Pending 수, 마지막으로 전달된 ID를 확인한다.
        var groups = await database.StreamGroupInfoAsync(streamKey);

        Console.WriteLine();
        Console.WriteLine("=== XINFO GROUPS ===");
        foreach (var group in groups)
        {
            Console.WriteLine(
                $"group={group.Name}, consumers={group.ConsumerCount}, pending={group.PendingMessageCount}, last-delivered-id={group.LastDeliveredId}");
        }

        // XINFO CONSUMERS game:events game-workers에 해당한다.
        // Consumer별 Pending 수와 idle 시간을 확인한다.
        var consumers = await database.StreamConsumerInfoAsync(streamKey, groupName);

        Console.WriteLine();
        Console.WriteLine("=== XINFO CONSUMERS ===");
        foreach (var consumer in consumers)
        {
            Console.WriteLine(
                $"consumer={consumer.Name}, pending={consumer.PendingMessageCount}, idle-ms={consumer.IdleTimeInMilliseconds}");
        }

        // XPENDING game:events game-workers에 해당한다.
        // Group 전체에서 ACK되지 않은 메시지 개수와 ID 범위를 확인한다.
        var pending = await database.StreamPendingAsync(streamKey, groupName);

        Console.WriteLine();
        Console.WriteLine("=== XPENDING ===");
        Console.WriteLine(
            $"pending={pending.PendingMessageCount}, lowest={pending.LowestPendingMessageId}, highest={pending.HighestPendingMessageId}");
    }
}
```

`database`는 `Program.cs`에서 `connection.GetDatabase()`로 만든 뒤 `ConsumerGroupScenario.RunAsync(database)`에 넘긴다.

`Program.cs`의 호출 부분만 Phase 02와 다르게 바꾼다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Program.cs
```

```csharp
// ...

// Phase 03에서는 Consumer Group 실습 시나리오를 실행한다.
await ConsumerGroupScenario.RunAsync(database);

// ...
```

---

## C#에서 Consumer Group 생성

이미 그룹이 있으면 `BUSYGROUP` 예외가 날 수 있다.

학습 코드에서는 이미 있으면 넘어가도 된다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/ConsumerGroupScenario.cs
```

클래스 / 메서드:

```text
ConsumerGroupScenario.RunAsync
```

역할:

```text
game:events Stream에 game-workers Consumer Group을 만든다.
```

```csharp
// Consumer Group을 붙일 Stream과 Group 이름을 정한다.
// streamKey는 메시지가 쌓이는 Redis key이고, groupName은 메시지를 나눠 처리할 그룹 이름이다.
var streamKey = "game:events";
var groupName = "game-workers";

try
{
    // Consumer Group이 없으면 만들고, 이미 있으면 catch에서 넘어간다.
    // position: "0-0"은 Stream의 처음부터 읽을 수 있게 시작 위치를 잡는다는 뜻이다.
    // createStream: true는 Stream이 아직 없어도 함께 만들겠다는 뜻이다.
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
```

---

## C#에서 Consumer Group으로 읽기

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/ConsumerGroupScenario.cs
```

클래스 / 메서드:

```text
ConsumerGroupScenario.RunAsync
```

역할:

```text
consumer-a가 새 메시지를 읽고 처리 성공 후 XACK한다.
```

```csharp
// consumer-a가 아직 Group에 전달되지 않은 새 메시지를 최대 5개 읽는다.
// 읽은 메시지는 consumer-a의 Pending 상태로 기록된다.
var entries = await database.StreamReadGroupAsync(
    key: "game:events",
    groupName: "game-workers",
    consumerName: "consumer-a",
    position: ">",
    count: 5);

foreach (var entry in entries)
{
    Console.WriteLine($"Processing: {entry.Id}");

    // 시간이 걸리는 비즈니스 처리를 흉내 낸다.
    await Task.Delay(TimeSpan.FromSeconds(1));

    // 처리에 성공한 뒤 XACK로 Pending 상태에서 제거한다.
    // 이 호출이 없으면 Redis는 아직 처리 완료되지 않은 메시지로 본다.
    await database.StreamAcknowledgeAsync(
        "game:events",
        "game-workers",
        entry.Id);
}
```

---

## C#에서 여러 Consumer 병렬 실행

Redis 자체가 모든 비즈니스 처리를 병렬로 해 주는 것은 아니다.

Redis는 Stream 메시지를 Consumer들에게 나눠 주고, Consumer 애플리케이션이 각자 오래 걸리는 작업을 처리한다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/ConsumerGroupScenario.cs
```

클래스 / 메서드:

```text
ConsumerGroupScenario.RunAsync
```

역할:

```text
consumer-a와 consumer-b를 동시에 실행해서 메시지 처리 작업을 나눠 수행한다.
```

```csharp
// consumer-a와 consumer-b를 동시에 실행한다.
// Redis는 메시지를 나눠 주고, 오래 걸리는 실제 처리는 각 Consumer가 병렬로 수행한다.
// 각 Consumer는 ReadAndAckAsync 안에서 XREADGROUP으로 읽고 XACK로 완료 처리한다.
await Task.WhenAll(
    ReadAndAckAsync(database, streamKey, groupName, "consumer-a"),
    ReadAndAckAsync(database, streamKey, groupName, "consumer-b"));
```

Consumer Group의 병렬 처리 이점은 Redis 명령 처리 자체가 아니라,
메시지를 읽은 뒤의 작업을 여러 Consumer가 나눠 수행할 수 있다는 점에 있다.

---

## C#에서 처리 결과 확인

Consumer Group 상태는 `StackExchange.Redis`의 Stream 정보 API로 확인할 수 있다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/ConsumerGroupScenario.cs
```

클래스 / 메서드:

```text
ConsumerGroupScenario.PrintConsumerGroupStatusAsync
```

역할:

```text
Consumer 이름, Consumer별 Pending 개수, Group 전체 Pending 개수를 확인한다.
```

```csharp
// XINFO GROUPS game:events에 해당한다.
// Group 전체의 Consumer 수와 Pending 수를 확인한다.
var groups = await database.StreamGroupInfoAsync(streamKey);

foreach (var group in groups)
{
    Console.WriteLine(
        $"group={group.Name}, consumers={group.ConsumerCount}, pending={group.PendingMessageCount}");
}

// XINFO CONSUMERS game:events game-workers에 해당한다.
// Consumer별 Pending 개수를 확인해서 특정 Consumer에 메시지가 묶였는지 본다.
var consumers = await database.StreamConsumerInfoAsync(streamKey, groupName);

foreach (var consumer in consumers)
{
    Console.WriteLine(
        $"consumer={consumer.Name}, pending={consumer.PendingMessageCount}");
}

// XPENDING game:events game-workers에 해당한다.
// Group 전체에서 ACK되지 않은 메시지가 몇 개인지 확인한다.
var pending = await database.StreamPendingAsync(streamKey, groupName);

Console.WriteLine($"pending={pending.PendingMessageCount}");
```

모든 메시지를 `XACK`했다면 `pending=0`이 나와야 한다.

`XINFO CONSUMERS` 결과에 `consumer-a`, `consumer-b`가 보이면
두 Consumer가 실제로 Group 안에 등록된 것이다.

ACK하지 않은 메시지를 확인하고 싶다면 `StreamAcknowledgeAsync` 호출을 잠시 주석 처리하고 다시 실행한다.

```csharp
// await database.StreamAcknowledgeAsync(streamKey, groupName, entry.Id);
```

그 상태에서 다시 실행하면 출력의 Pending 개수가 증가하고,
어떤 Consumer가 메시지를 가져갔는지도 확인할 수 있다.

---

## `position: ">"` 의미

`>`는 아직 Consumer Group에 전달되지 않은 새 메시지를 읽겠다는 의미다.

이미 다른 Consumer에게 전달되어 Pending 상태인 메시지는 자동으로 가져오지 않는다.

이 점이 장애 복구에서 중요하다.

---

## 실습 시나리오

```text
1. game:events Stream 생성
2. game-workers Consumer Group 생성
3. 메시지 10개 발행
4. consumer-a로 5개 처리
5. consumer-b로 5개 처리
6. XINFO GROUPS로 pending이 0인지 확인
```

---

## 확인할 점

- Consumer 이름이 `XINFO CONSUMERS` 결과에 남는가?
- ACK한 메시지는 `XINFO GROUPS`나 `XPENDING`에서 Pending 개수가 `0`으로 보이는가?
- ACK하지 않은 메시지는 `XPENDING`에서 Pending으로 잡히고, `XINFO CONSUMERS`에서 특정 Consumer의 Pending 개수로 보이는가?
