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

```csharp
var entries = await database.StreamReadGroupAsync(
    key: "game:events",
    groupName: "game-workers",
    consumerName: "consumer-a",
    position: ">",
    count: 5);

foreach (var entry in entries)
{
    Console.WriteLine($"Read but not ack: {entry.Id}");
}

Console.WriteLine("Simulated crash before XACK.");
return;
```

---

## 예외로 종료하는 방식

```csharp
foreach (var entry in entries)
{
    Console.WriteLine($"Processing: {entry.Id}");

    throw new InvalidOperationException("Consumer crashed before ACK.");
}
```

---

## 처리 지연 시뮬레이션

```csharp
foreach (var entry in entries)
{
    Console.WriteLine($"Long processing: {entry.Id}");

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
