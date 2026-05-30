---
tags:
  - redis
  - redis-stream
---

# Phase 00. 학습 기준 잡기

> [!NOTE] 목표
> Redis Stream을 왜 공부하는지 정리한다.  
> 이 주제가 백엔드 운영 학습에서 어떤 의미를 가지는지 함께 정리한다.

---

## 이번 Phase에서 할 일

- Redis Stream이 필요한 상황을 이해한다.
- Pub/Sub, List, Stream의 차이를 비교한다.
- 장애 추적 관점에서 Redis Stream이 왜 좋은 학습 주제인지 정리한다.
- 실습을 진행할 기준과 기록 방식을 정한다.

---

## 핵심 개념

### Redis Stream

Redis Stream은 Redis 안에 저장되는 메시지 로그다.

Producer가 메시지를 추가하면 Stream에 기록된다.  
Consumer는 필요한 위치부터 메시지를 읽을 수 있다.

읽은 메시지가 바로 삭제되는 구조가 아니기 때문에, 장애가 발생했을 때 추적하기 좋다.

---

### Pub/Sub과의 차이

Pub/Sub은 실시간 전달 중심이다.

구독자가 살아 있을 때는 메시지를 받을 수 있지만, 구독자가 죽어 있는 동안 발행된 메시지를 나중에 다시 읽기 어렵다.

Redis Stream은 메시지가 Stream에 남는다.

따라서 Consumer가 장애로 멈춰도, 나중에 어떤 메시지가 처리되지 않았는지 확인할 수 있다.

---

## 비교 표

| 방식 | 특징 | 장애 상황에서의 차이 |
| --- | --- | --- |
| Pub/Sub | 실시간 전달 중심 | 구독자가 죽어 있으면 메시지 유실 가능 |
| List | 간단한 큐 구현 가능 | Consumer Group, Pending 추적 기능은 부족 |
| Stream | 메시지 로그 + Consumer Group | ACK 누락과 Pending 메시지 추적 가능 |

---

## 예시 서비스 시나리오

```text
게임 서버에서 매치 종료 이벤트가 발생한다.

Producer는 match.completed 이벤트를 Redis Stream에 기록한다.
Consumer는 이 이벤트를 읽어서 보상 지급, 랭킹 반영, 로그 저장을 처리한다.

Consumer가 처리 중 죽으면 메시지가 사라지면 안 된다.

메시지는 Pending 상태로 남아야 하고,
나중에 다른 Consumer가 가져와 복구할 수 있어야 한다.
```

---

## 학습 이유

- Redis를 단순 캐시가 아니라 이벤트 처리 도구로 이해하게 된다.
- Consumer 장애, ACK 누락, 메시지 재처리를 설명할 수 있다.
- 운영 중 문제가 생겼을 때 상태를 확인하는 순서를 만들 수 있다.
- 장애를 재현하고 관찰하고 복구하는 흐름을 작은 예제로 반복할 수 있다.
