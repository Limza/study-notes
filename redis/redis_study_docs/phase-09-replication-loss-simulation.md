---
tags:
  - redis
  - redis-stream
  - redis-cluster
  - kafka
  - event-system
  - operation
---

# Phase 09. Redis Cluster 장애와 이벤트 통지 시스템 선택

> [!NOTE] 목표
> Redis Cluster 구조에서 머신에 문제가 생겼을 때
> Redis Stream 메시지가 유실될 수 있는 지점을 이해하고,
> 유실이 문제가 되는 시스템에서는 Kafka 같은 durable event log를 검토해야 하는 이유를 정리한다.

---

## 이 Phase를 다시 정리하는 이유

앞 단계까지는 Redis Stream Consumer 장애를 다뤘다.

Consumer가 메시지를 읽고 `XACK` 전에 죽으면 메시지는 바로 사라지지 않는다.

이 경우 Redis 안에 Stream 메시지가 남아 있고,
Consumer Group의 Pending Entries List에 처리 중 상태가 남는다.

그래서 아래 명령으로 추적하고 복구할 수 있다.

```redis
XPENDING game:events game-workers
XAUTOCLAIM game:events game-workers recovery-consumer 60000 0-0
XACK game:events game-workers <message-id>
```

하지만 이것은 Redis 안에 메시지가 남아 있다는 전제가 있을 때만 가능하다.

Redis Cluster에서 master 머신 자체가 죽고,
그 master에만 있던 최신 Stream 메시지가 replica로 복제되지 않았다면
Consumer Group 복구 명령으로는 그 메시지를 살릴 수 없다.

이 Phase의 핵심 질문은 이것이다.

```text
Redis Stream은 Consumer 장애에는 강하지만,
Redis 노드 장애까지 메시지 유실 없이 보장할 수 있는가?
```

결론부터 말하면, Redis만으로는 모든 장애 상황에서 메시지 유실을 막기 어렵다.

특히 메시지가 절대 유실되면 안 되는 이벤트 통지 시스템이라면
Redis Stream을 최종 이벤트 로그로 볼 것이 아니라,
Kafka 같은 durable event log 또는 별도의 outbox를 같이 검토해야 한다.

---

## Redis Cluster에서 메시지가 유실될 수 있는 지점

예를 들어 Redis Cluster가 아래처럼 구성되어 있다고 가정한다.

```text
master-1   master-2   master-3
replica-1  replica-2  replica-3
```

`game:events` Stream key는 특정 hash slot에 속하고,
그 slot을 담당하는 master에 저장된다.

문제 상황은 아래와 같다.

```text
1. Producer가 master-1에 XADD를 보낸다.
2. master-1은 XADD 성공 응답을 반환한다.
3. 그런데 이 메시지가 replica-1에 복제되기 전에 master-1 머신이 죽는다.
4. Cluster는 replica-1을 새 master로 승격한다.
5. 새 master에는 방금 성공 응답을 받은 메시지가 없다.
```

이 경우 Producer 입장에서는 성공한 메시지처럼 보였지만,
failover 이후 Redis Cluster에서 조회하면 메시지가 없을 수 있다.

확인 명령:

```redis
CLUSTER INFO
CLUSTER NODES
CLUSTER SLOTS
XINFO STREAM game:events
XREVRANGE game:events + - COUNT 10
```

중요한 점은 `XPENDING`으로도 복구할 수 없다는 것이다.

`XPENDING`은 Consumer가 읽었지만 ACK하지 않은 메시지를 추적한다.

애초에 새 master의 Stream에 메시지가 없다면 Pending 상태도 남을 수 없다.

---

## Redis에서 유실 가능성을 줄이는 방법

Redis에도 유실 가능성을 줄이는 장치는 있다.

### `WAIT`

`WAIT`는 write 후 replica 반영을 기다리는 명령이다.

```redis
XADD game:events * type match.completed matchId 1001
WAIT 1 1000
```

`WAIT 1 1000`은 최소 1개 replica가 1000ms 안에 write를 확인하기를 기다린다는 뜻이다.

장점:

```text
replica에 복제되기 전에 master가 죽는 위험을 줄인다.
```

한계:

```text
latency가 증가한다.
timeout이 발생할 수 있다.
모든 장애를 완전히 제거하지는 못한다.
```

### `min-replicas-to-write`

master가 충분한 replica를 갖지 못하면 write를 거부하게 할 수 있다.

```conf
min-replicas-to-write 1
min-replicas-max-lag 1
```

장점:

```text
복제본이 없는 상태에서 master가 계속 write를 받는 위험을 줄인다.
```

한계:

```text
가용성이 낮아질 수 있다.
replica lag 조건에 따라 write가 막힐 수 있다.
```

### AOF

AOF는 Redis 재시작 시 write를 복구할 가능성을 높인다.

```conf
appendonly yes
appendfsync everysec
```

또는 더 강하게:

```conf
appendfsync always
```

장점:

```text
master 머신이 재시작 가능한 장애라면 최근 write를 복구할 가능성이 있다.
```

한계:

```text
머신 자체가 유실되거나 새 master로 failover된 뒤에는
old master 로컬 디스크에만 있던 데이터가 즉시 서비스 데이터가 되지 않는다.
```

---

## Redis만으로 부족한 이유

Redis Stream은 빠르고 단순하다.

하지만 Redis Stream을 중요한 이벤트의 최종 저장소로 쓰려면 아래 질문에 답해야 한다.

```text
Producer가 성공 응답을 받은 이벤트를 나중에 반드시 다시 찾을 수 있는가?
장애 후 특정 시점부터 다시 replay할 수 있는가?
여러 Consumer Group이 독립적인 offset으로 오래 소비할 수 있는가?
오래된 이벤트를 보관하고 필요할 때 재처리할 수 있는가?
```

Redis Stream도 Consumer Group과 Pending 추적을 제공하지만,
Kafka처럼 이벤트 로그 보존과 replay를 중심으로 설계된 시스템은 아니다.

따라서 아래 요구사항이 강하면 Redis Stream만으로는 불안하다.

```text
절대 유실되면 안 되는 업무 이벤트
장애 후 장기간 replay
여러 서비스가 같은 이벤트를 각자 소비
이벤트 로그 자체가 시스템의 원천 기록
감사, 정산, 결제, 주문 같은 복구 가능한 기록
```

이런 경우 Kafka를 쓰거나,
적어도 DB outbox 같은 외부 원천 저장소를 두고 Redis Stream은 delivery channel로만 보는 편이 안전하다.

---

## Kafka를 검토해야 하는 경우

Kafka는 Redis보다 무겁고 운영 복잡도가 있다.

하지만 이벤트 통지 시스템에서 아래 요구사항이 중요하면 Kafka가 더 자연스럽다.

```text
메시지 유실을 강하게 막아야 한다.
이벤트를 일정 기간 이상 보관해야 한다.
Consumer가 나중에 offset부터 다시 읽어야 한다.
여러 서비스가 같은 이벤트를 독립적으로 소비해야 한다.
처리 장애 후 replay가 중요하다.
이벤트 로그가 시스템 간 통합의 기준이 된다.
```

Kafka에서는 topic partition에 메시지가 append되고,
Consumer Group은 offset으로 읽은 위치를 관리한다.

Redis Stream의 Pending은 "읽었지만 ACK하지 않은 메시지"를 보는 데 좋다.

Kafka의 offset은 "Consumer Group이 어디까지 읽었는지"를 오래 유지하고 replay하는 데 좋다.

---

## Redis Stream을 써야 하는 경우

Redis Stream이 의미 없는 것은 아니다.

오히려 아래 상황에서는 Redis Stream이 Kafka보다 단순하고 빠르다.

```text
이미 Redis를 운영 중이다.
이벤트 보관 기간이 짧다.
낮은 latency가 중요하다.
메시지가 유실되어도 원천 DB나 로그에서 다시 만들 수 있다.
Consumer 장애 복구 정도면 충분하다.
운영 복잡도를 크게 늘리고 싶지 않다.
```

예시:

```text
실시간 알림
게임 매치 상태 fan-out
캐시 갱신 이벤트
짧게 보관하는 내부 작업 큐
실패 시 DB 기준으로 다시 만들 수 있는 비동기 작업
```

이때도 중요한 이벤트라면 메시지에 고유 id를 넣고,
Consumer를 idempotent하게 만들어야 한다.

```text
eventId=match-1001-completed
type=match.completed
matchId=1001
```

같은 `eventId`가 다시 들어와도 Consumer가 중복 처리하지 않도록 해야
장애 후 재발행 전략을 쓸 수 있다.

---

## Redis Stream과 Kafka 비교

| 기준 | Redis Stream | Kafka |
| --- | --- | --- |
| 기본 성격 | Redis 안의 Stream 자료구조 | 분산 append-only event log |
| 강점 | 낮은 latency, 단순한 운영, Redis와 통합 쉬움 | 내구성, replay, retention, Consumer Group offset |
| Consumer 장애 대응 | Pending, `XPENDING`, `XAUTOCLAIM`, `XACK` | offset commit, replay |
| 장기 보관 | 가능은 하지만 Redis 메모리/정책 영향을 크게 받음 | retention 중심 설계 |
| replay | Stream에 남은 범위에서 가능 | offset 기준 replay가 자연스러움 |
| 메시지 원장 역할 | 별도 outbox 없이 쓰기에는 위험 | 더 적합 |
| 운영 복잡도 | 낮음 | 높음 |
| latency | 보통 더 낮음 | 낮지만 Redis보다는 큰 편 |

짧게 정리하면:

```text
Redis Stream
빠른 delivery channel 또는 작업 큐에 가깝다.

Kafka
장애 후에도 다시 읽을 수 있는 durable event log에 가깝다.
```

---

## 벤치마크 수치로 보는 latency 차이

아래 수치는 서로 다른 환경에서 나온 공개 자료다.

따라서 정확한 1:1 비교가 아니라,
대략적인 latency 영역을 이해하기 위한 참고값으로 봐야 한다.

| 항목 | 공개 수치 | 의미 |
| --- | ---: | --- |
| Redis 일반 latency | 평균 1ms 미만, 마이크로초 단위도 흔함 | 적절히 provision된 Redis 기준 |
| Redis `LPUSH` benchmark p50 | 0.135ms | Redis 공식 benchmark 예시 |
| Redis `SET` benchmark p50 | 0.143ms | Redis 공식 benchmark 예시 |
| Redis pipelining `SET` p50 | 0.479ms | Redis 공식 benchmark 예시 |
| Kafka end-to-end p99 | 5ms | 1KB 메시지, 200k msg/s, 200MB/s load |

참고:

```text
Redis observability docs
https://redis.io/docs/latest/operate/rs/monitoring/observability/

Redis benchmark docs
https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/

Confluent Kafka performance benchmark
https://developer.confluent.io/learn/kafka-performance/
```

이 수치만 보면 Redis가 훨씬 빠르게 보인다.

대략적인 감각은 아래와 같다.

```text
Redis Stream: sub-ms ~ 1ms 근처를 기대하기 쉬움
Kafka: 잘 튜닝된 환경에서 end-to-end p99가 수 ms 수준 가능
```

하지만 latency만으로 선택하면 안 된다.

메시지가 중요한 시스템에서는 `0.x ms vs 5 ms`보다
장애 후 replay 가능성과 유실 복구 가능성이 더 중요할 수 있다.

---

## 선택 기준

### Redis Stream을 선택해도 좋은 경우

```text
1. 메시지가 짧은 시간 안에 처리되면 된다.
2. 장애 시 원천 DB에서 이벤트를 다시 만들 수 있다.
3. Consumer 장애 복구가 주된 관심사다.
4. Kafka 운영 복잡도를 감당하고 싶지 않다.
5. latency가 매우 중요하다.
```

### Kafka를 선택하는 것이 좋은 경우

```text
1. 메시지 유실이 비즈니스 장애가 된다.
2. 이벤트 로그가 시스템 간 통합의 기준이다.
3. 여러 Consumer Group이 같은 이벤트를 독립적으로 소비한다.
4. 장애 후 특정 offset부터 replay해야 한다.
5. 이벤트 보관 기간과 감사 가능성이 중요하다.
```

### Redis Stream을 쓰되 보완해야 하는 경우

```text
1. Producer outbox를 둔다.
2. Consumer를 idempotent하게 만든다.
3. 중요한 write에는 WAIT를 검토한다.
4. min-replicas-to-write를 검토한다.
5. Redis Stream을 원천 저장소가 아니라 delivery channel로 본다.
```

---

## 운영 관점 정리

Redis Stream은 Consumer 장애 복구에는 좋은 도구다.

`XPENDING`, `XAUTOCLAIM`, `XACK`, Dead Letter Stream으로
읽었지만 처리 완료되지 않은 메시지를 추적할 수 있다.

하지만 Redis Cluster의 master 머신이 죽고,
성공 응답을 받은 메시지가 replica로 복제되지 않았다면
Redis 내부 명령만으로는 그 메시지를 복구할 수 없다.

따라서 이벤트 통지 시스템을 설계할 때는 먼저 질문을 나눠야 한다.

```text
빠르고 단순한 내부 비동기 처리인가?
-> Redis Stream이 잘 맞을 수 있다.

유실되면 안 되는 이벤트 로그인가?
-> Kafka 또는 outbox 기반 구조를 검토한다.
```

이번 Phase의 결론은 아래와 같다.

```text
Redis Stream은 빠른 메시지 전달과 Consumer 장애 추적에 강하다.
하지만 Redis만으로 메시지 유실을 완전히 막기는 어렵다.
유실이 문제가 되는 이벤트 통지 시스템이라면 Kafka나 outbox를 함께 고려해야 한다.
```
