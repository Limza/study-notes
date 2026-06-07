---
tags:
  - redis
  - redis-stream
  - csharp
  - failure-tracing
---

# Redis Stream 장애 추적 학습 계획

> [!NOTE] 목표
> Redis Stream을 C#으로 실습하면서  
> **장애를 재현하고 → 상태를 추적하고 → 메시지를 복구하는 흐름**을 익힌다.

이 문서는 Obsidian에서 읽기 쉽도록 Phase별 문서로 나눈 학습 지도다.

각 Phase 문서에는 다음 내용을 넣었다.

- 필요한 개념
- Redis 명령어
- C# 코드 예시
- 실습 순서

---

## 전체 흐름

```text
학습 이유 정리
  -> C# Redis 컨테이너 실행
  -> Redis Stream 기본기
  -> Consumer Group
  -> 장애 재현
  -> 장애 추적
  -> 메시지 복구
  -> 장애 보고서
  -> 반복 실패 메시지 Dead Letter 분리
  -> Redis Cluster 노드 장애 대응
  -> 학습 회고와 운영 체크리스트
```

---

## Phase 목록

| Phase | 문서 | 목표 |
| --- | --- | --- |
| 00 | [[phase-00-learning-goal]] | 왜 Redis Stream 장애 추적을 공부하는지 정리 |
| 01 | [[phase-01-testcontainers-redis]] | C#에서 `IContainer`로 Redis 컨테이너 실행 |
| 02 | [[phase-02-basic-stream]] | Redis Stream 기본 읽기/쓰기 구현 |
| 03 | [[phase-03-consumer-group]] | Consumer Group과 ACK 흐름 이해 |
| 04 | [[phase-04-failure-simulation]] | ACK 전 Consumer 종료 장애 재현 |
| 05 | [[phase-05-troubleshooting]] | `XPENDING`, `XINFO`로 장애 추적 |
| 06 | [[phase-06-recovery]] | `XAUTOCLAIM`으로 Pending 메시지 복구 |
| 07 | [[phase-07-incident-report]] | Pending 상태를 Slack 알림 payload로 출력 |
| 08 | [[phase-08-dead-letter-stream]] | 반복 실패 메시지를 Dead Letter Stream으로 분리 |
| 09 | [[phase-09-replication-loss-simulation]] | Redis Cluster에서 노드 장애 대응 순서 정리 |
| 10 | [[phase-10-retrospective]] | 학습 회고와 운영 체크리스트 정리 |

---

## 최종적으로 설명할 수 있어야 하는 것

> [!SUMMARY] 핵심
> Redis Stream Consumer Group에서 Consumer가 ACK 전에 종료되면  
> 메시지는 사라지지 않고 **Pending 상태**로 남는다.
>
> 이 상태는 `XPENDING`, `XINFO GROUPS`, `XINFO CONSUMERS`로 추적할 수 있다.
>
> idle time이 `min-idle-time` 이상인 Pending 메시지는 `XAUTOCLAIM`으로 다른 Consumer가 가져와  
> 재처리한 뒤 `XACK`로 완료 처리할 수 있다.
>
> 반복해서 실패하는 메시지는 다시 재처리하기보다  
> Dead Letter Stream으로 분리하고 원본 Consumer Group에서는 `XACK`로 정리한다.
>
> 단, Redis Stream은 Redis Cluster 위에 저장되는 자료구조이므로  
> 머신 장애가 발생하면 먼저 master/replica 역할, slot failover, Cluster 상태를 확인해야 한다.

---

## 추천 학습 방식

1. Phase 문서를 하나 연다.
2. 개념을 먼저 읽는다.
3. Redis 명령어와 C# 코드를 직접 실행한다.
4. 결과를 `notes/`에 기록한다.
5. Anki로 따로 정리할 개념을 뽑는다.
