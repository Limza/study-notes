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
- Stream ID와 field-value 구조를 이해한다.

---

## 필요한 지식

Redis Stream은 key 하나에 여러 메시지가 시간 순서대로 쌓이는 자료구조다.

각 메시지는 아래 두 가지로 구성된다.

- Message ID
- field-value 목록

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

## C#에서 메시지 쓰기

`StackExchange.Redis`에서는 `StreamAddAsync`를 사용한다.

```csharp
var streamKey = "game:events";

var messageId = await database.StreamAddAsync(
    streamKey,
    new NameValueEntry[]
    {
        new("eventType", "match.completed"),
        new("matchId", "match-001"),
        new("playerId", "player-001"),
        new("score", "1200")
    });

Console.WriteLine($"Added message: {messageId}");
```

---

## C#에서 메시지 읽기

```csharp
var entries = await database.StreamReadAsync(
    "game:events",
    "0-0",
    count: 10);

foreach (var entry in entries)
{
    Console.WriteLine($"MessageId: {entry.Id}");

    foreach (var value in entry.Values)
    {
        Console.WriteLine($"{value.Name}: {value.Value}");
    }
}
```

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

## Stream ID 이해

Stream ID는 보통 아래 형태다.

```text
milliseconds-sequence
```

예시:

```text
1717050000000-0
1717050000000-1
```

`*`를 사용하면 Redis가 자동으로 ID를 만든다.

일반적인 발행 흐름에서는 자동 ID를 사용해도 충분하다.

---

## 중요한 관찰 포인트

- 메시지를 읽어도 Stream에서 삭제되지 않는다.
- `XREAD`는 읽기일 뿐 처리 완료를 의미하지 않는다.
- Stream이 계속 커질 수 있으므로 보관 정책을 나중에 고민해야 한다.
