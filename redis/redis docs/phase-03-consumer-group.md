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

## C#에서 Consumer Group 생성

이미 그룹이 있으면 `BUSYGROUP` 예외가 날 수 있다.

학습 코드에서는 이미 있으면 넘어가도 된다.

```csharp
var streamKey = "game:events";
var groupName = "game-workers";

try
{
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

```csharp
var entries = await database.StreamReadGroupAsync(
    key: "game:events",
    groupName: "game-workers",
    consumerName: "consumer-a",
    position: ">",
    count: 5);

foreach (var entry in entries)
{
    Console.WriteLine($"Processing: {entry.Id}");

    await database.StreamAcknowledgeAsync(
        "game:events",
        "game-workers",
        entry.Id);
}
```

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

- Consumer 이름이 `XINFO CONSUMERS`에 남는가?
- ACK한 메시지는 Pending에서 사라지는가?
- ACK하지 않은 메시지는 Consumer에 묶여 Pending 상태로 남는가?

