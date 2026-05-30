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

```csharp
var result = await database.ExecuteAsync(
    "XPENDING",
    "game:events",
    "game-workers",
    "-",
    "+",
    "10");

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
