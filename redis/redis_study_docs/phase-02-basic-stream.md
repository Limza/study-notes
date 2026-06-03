---
tags:
  - redis
  - redis-stream
  - csharp
---

# Phase 02. Redis Stream 기본기

> [!NOTE] 목표
> Consumer Group을 사용하기 전에  
> Redis Stream의 기본 쓰기와 읽기를 C#으로 구현한다.

---

## 이번 Phase에서 할 일

- Redis Stream에 메시지를 추가한다.
- Stream 길이를 확인한다.
- Stream 메시지를 읽는다.
- 메시지 ID와 field-value 구조를 이해한다.

---

## 필요한 지식

Redis Stream은 key 하나에 여러 메시지가 시간 순서대로 쌓이는 자료구조다.

각 메시지는 아래 두 가지로 구성된다.

- Message ID
- field-value 목록

`XADD`는 Stream에 메시지 한 건을 추가하는 Redis 명령이다.

```redis
XADD game:events * eventType match.completed matchId match-001
```

위 명령은 `game:events`라는 Redis key 안에 메시지 하나를 추가한다.

저장 구조는 대략 아래처럼 보면 된다.

```text
key: game:events

message id: Redis가 자동 생성
  eventType = match.completed
  matchId   = match-001
```

C#의 `NameValueEntry[]`는 이 메시지 안에 들어가는 field-value 목록이다.

```csharp
new NameValueEntry[]
{
    new("eventType", "match.completed"),
    new("matchId", "match-001"),
    new("playerId", "player-001"),
    new("score", "1200")
}
```

즉 `eventType`, `matchId`, `playerId`, `score`가 각각 Redis key가 되는 것이 아니다.  
`game:events` key 안에 쌓이는 메시지 한 건의 필드로 저장된다.

---

## 핵심 명령어

```redis
XADD game:events * eventType match.completed matchId match-001 playerId player-001 score 1200
XLEN game:events
XRANGE game:events - +
XREAD COUNT 10 STREAMS game:events 0-0
```

---

## 이번 Phase에서 만들 파일

Phase 01에서 만든 `RedisStreamStudy` 콘솔 프로젝트 안에 Stream 기본 실습 파일을 추가한다.

```text
study-notes/
  redis/
    src/
      RedisStreamStudy/
        Program.cs
        Scenarios/
          BasicStreamScenario.cs
```

`BasicStreamScenario.cs`에는 Stream에 메시지를 쓰고 읽는 코드를 넣는다.

`Program.cs`에서는 Redis 컨테이너와 연결을 만든 뒤 `BasicStreamScenario`를 호출한다.

---

## `BasicStreamScenario.cs` 작성

먼저 새 파일을 만들고 아래 코드를 그대로 넣는다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/BasicStreamScenario.cs
```

클래스 / 메서드:

```text
BasicStreamScenario.RunAsync
```

역할:

```text
game:events Stream에 메시지를 추가하고, 길이를 확인하고, 처음부터 다시 읽는다.
```

```csharp
using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class BasicStreamScenario
{
    public static async Task RunAsync(IDatabase database)
    {
        // 메시지를 저장할 Redis Stream key를 정한다.
        // 이 key 하나 안에 여러 message id가 시간 순서대로 쌓인다.
        var streamKey = "game:events";

        // 같은 Stream에 테스트 메시지 3개를 차례대로 추가한다.
        // 반복문 한 바퀴가 Redis Stream 메시지 한 건에 해당한다.
        for (var i = 1; i <= 3; i++)
        {
            // StreamAddAsync는 Redis의 XADD 명령에 해당한다.
            // 첫 번째 인자는 메시지를 넣을 Stream key다.
            // 두 번째 인자는 하나의 message id에 함께 저장할 field-value 목록이다.
            var messageId = await database.StreamAddAsync(
                streamKey,
                new NameValueEntry[]
                {
                    // eventType, matchId, playerId, score는 각각 Redis key가 아니다.
                    // game:events Stream 안에 들어가는 메시지 한 건의 field 이름이다.
                    new("eventType", "match.completed"),
                    new("matchId", $"match-{i:000}"),
                    new("playerId", $"player-{i:000}"),
                    new("score", (1000 + i * 100).ToString())
                });

            Console.WriteLine($"Added message: {messageId}");
        }

        // StreamLengthAsync는 Redis의 XLEN 명령에 해당한다.
        // Stream key 안에 쌓인 전체 메시지 개수를 확인한다.
        var streamLength = await database.StreamLengthAsync(streamKey);
        Console.WriteLine($"Stream length: {streamLength}");

        // StreamReadAsync는 Redis의 XREAD 명령에 해당한다.
        // "0-0"은 Stream의 가장 처음 message id부터 읽겠다는 뜻이다.
        // count: 10은 최대 10개까지만 읽겠다는 뜻이다.
        var entries = await database.StreamReadAsync(
            streamKey,
            "0-0",
            count: 10);

        // 읽어 온 각 Stream 메시지를 출력한다.
        // entry.Id는 Redis가 붙인 message id이고, entry.Values는 field-value 목록이다.
        foreach (var entry in entries)
        {
            Console.WriteLine($"MessageId: {entry.Id}");

            foreach (var value in entry.Values)
            {
                // value.Name은 field 이름이고, value.Value는 field 값이다.
                Console.WriteLine($"{value.Name}: {value.Value}");
            }
        }
    }
}
```

`database`는 이 파일 안에서 새로 만드는 값이 아니다.  
`Program.cs`에서 Redis에 연결한 뒤 `connection.GetDatabase()`로 만들고, `RunAsync(database)`에 넘겨준다.

---

## `Program.cs` 수정

Phase 01에서 만든 `Program.cs`를 아래처럼 수정한다.  
바뀌지 않는 기존 코드는 `// ...`로 생략한다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Program.cs
```

클래스 / 메서드:

```text
Program.Main
```

역할:

```text
Redis 컨테이너를 시작하고 database를 만든 뒤 BasicStreamScenario.RunAsync에 넘긴다.
```

```csharp
using RedisStreamStudy.Infrastructure;
using RedisStreamStudy.Scenarios;
using StackExchange.Redis;

namespace RedisStreamStudy;

public class Program
{
    public static async Task Main(string[] args)
    {
        // Redis GUI에서 접속하기 쉽게 내 PC의 6379 포트에 Redis 컨테이너를 고정한다.
        const int redisPort = 6379;

        // RedisContainerFactory가 host port와 container port를 연결한 컨테이너 설정을 만든다.
        await using var redisContainer = RedisContainerFactory.Create(redisPort);

        // 실제 Docker Redis 컨테이너를 시작한다.
        await redisContainer.StartAsync();

        // Redis GUI와 C# 코드가 모두 localhost:6379로 접속하게 한다.
        var connectionString = $"localhost:{redisPort}";

        // StackExchange.Redis의 Redis 연결 관리자를 만든다.
        await using var connection =
            await ConnectionMultiplexer.ConnectAsync(connectionString);

        // Redis 명령을 보낼 database 객체를 꺼낸다.
        var database = connection.GetDatabase();

        // Phase 01의 Ping 확인 코드 대신 Stream 기본 실습 코드를 실행한다.
        await BasicStreamScenario.RunAsync(database);
    }
}
```

여기서 `database`는 Redis 명령을 보내는 창구다.  
`BasicStreamScenario`는 이 창구를 받아서 `StreamAddAsync`, `StreamLengthAsync`, `StreamReadAsync`를 실행한다.

---

## C#에서 메시지 쓰기

`StackExchange.Redis`에서는 `StreamAddAsync`를 사용한다.

이 코드는 위의 `BasicStreamScenario.RunAsync` 안에 들어 있다.

```csharp
// StreamAddAsync는 Redis의 XADD 명령에 해당한다.
// streamKey에 메시지 한 건을 추가하고, Redis가 만든 message id를 돌려받는다.
var messageId = await database.StreamAddAsync(
    streamKey,
    new NameValueEntry[]
    {
        // 이 값들은 Redis key가 아니라 하나의 메시지 안에 들어가는 field-value다.
        new("eventType", "match.completed"),
        new("matchId", $"match-{i:000}"),
        new("playerId", $"player-{i:000}"),
        new("score", (1000 + i * 100).ToString())
    });
```

`database`는 `Program.cs`에서 넘겨받은 Redis 명령 창구다.

---

## C#에서 메시지 읽기

이 코드도 `BasicStreamScenario.RunAsync` 안에 들어 있다.

```csharp
// "0-0"부터 읽어서 Stream의 처음 메시지부터 확인한다.
// count: 10은 최대 10개의 메시지만 가져오겠다는 뜻이다.
var entries = await database.StreamReadAsync(
    streamKey,
    "0-0",
    count: 10);
```

`"0-0"`은 Stream의 처음부터 읽겠다는 뜻이다.

---

## 실습 시나리오

```text
1. Redis 컨테이너 실행
2. game:events Stream에 match.completed 이벤트 3개 발행
3. Stream 길이 확인
4. Stream 전체 메시지 읽기
5. 메시지 ID와 필드 구조 확인
```

---

## 메시지 ID 이해

Redis Stream에 저장된 각 메시지는 ID를 가진다.

`StreamAddAsync`는 메시지를 추가한 뒤 이 메시지 ID를 반환한다.

```csharp
var messageId = await database.StreamAddAsync(...);
```

메시지 ID는 보통 아래 형태다.

```text
milliseconds-sequence
```

예시:

```text
1717050000000-0
1717050000000-1
```

`XADD`에서 `*`를 사용하거나 C#의 `StreamAddAsync`에서 ID를 따로 지정하지 않으면 Redis가 메시지 ID를 자동으로 만든다.

일반적인 발행 흐름에서는 자동 ID를 사용해도 충분하다.

---

## 중요한 관찰 포인트

- 메시지를 읽어도 Stream에서 삭제되지 않는다.
- `XREAD`는 읽기일 뿐 처리 완료를 의미하지 않는다.
- Stream이 계속 커질 수 있으므로 보관 정책을 나중에 고민해야 한다.
