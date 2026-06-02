---
tags:
  - redis
  - retrospective
  - operation
---

# Phase 09. 학습 회고와 운영 체크리스트

> [!NOTE] 목표
> Redis Stream 장애 추적 실습을  
> 다시 재현할 수 있는 학습 기록과 운영 체크리스트로 정리한다.

---

## 이번 Phase에서 할 일

- 전체 실습 흐름을 다시 정리한다.
- 장애 추적에 사용한 명령어를 정리한다.
- 복구 판단 기준을 정리한다.
- 다음에 보완할 지점을 남긴다.

---

## 이번 Phase에서 정리할 파일

새 C# 코드를 만들기보다는 GitHub에서 바로 읽을 수 있는 최종 정리 문서를 보강한다.

```text
study-notes/
  redis/
    README.md
    reports/
      incident-report-ack-missing.md
    redis_study_docs/
      phase-09-retrospective.md
```

`README.md`에는 전체 학습 주제와 실행 방법을 짧게 정리한다.

`phase-09-retrospective.md`에는 배운 점, 운영 체크리스트, 다음 실험 후보를 남긴다.

---

## 전체 실습 흐름 요약

```text
1. C#에서 Testcontainers로 Redis 컨테이너를 실행한다.
2. Producer가 Redis Stream에 메시지를 발행한다.
3. Consumer Group을 만들고 Consumer가 메시지를 읽는다.
4. Consumer가 ACK 전에 종료되는 장애를 재현한다.
5. XPENDING, XINFO GROUPS, XINFO CONSUMERS로 상태를 확인한다.
6. XAUTOCLAIM으로 idle time이 min-idle-time 이상인 Pending 메시지를 다른 Consumer가 가져온다.
7. 재처리 성공 후 XACK로 Pending 상태를 정리한다.
8. 반복 실패 메시지는 Dead Letter Stream으로 분리하는 기준을 세운다.
```

---

## 장애 추적 명령어 정리

| 명령어 | 확인할 내용 |
| --- | --- |
| `XLEN stream` | Stream에 쌓인 메시지 수 |
| `XRANGE stream - + COUNT 10` | Stream에 실제로 남아 있는 메시지 |
| `XINFO STREAM stream` | Stream 길이, 마지막 ID, 그룹 수 |
| `XINFO GROUPS stream` | Consumer Group별 pending, lag |
| `XINFO CONSUMERS stream group` | Consumer별 pending, idle |
| `XPENDING stream group` | Pending 요약 |
| `XPENDING stream group - + 10` | Pending 메시지 상세 |
| `XAUTOCLAIM stream group consumer min-idle-time 0-0` | idle time이 min-idle-time 이상인 Pending 메시지 소유권 이전 |

---

## 운영 체크리스트

> [!SUMMARY] 장애를 볼 때 순서
> 먼저 Stream과 Group 상태를 보고,  
> Pending 메시지가 어느 Consumer에 묶여 있고 idle time이 얼마나 되었는지 확인한 뒤,  
> 복구 Consumer가 가져가도 되는 메시지인지 판단한다.

- [ ] Stream 이름과 Consumer Group 이름을 확인한다.
- [ ] Producer가 메시지를 정상 발행했는지 확인한다.
- [ ] Consumer가 메시지를 읽었는지 확인한다.
- [ ] `XACK`가 호출되지 않은 메시지가 있는지 확인한다.
- [ ] Pending 메시지의 idle 시간이 복구 기준을 넘었는지 확인한다.
- [ ] 재처리해도 안전한 메시지인지 확인한다.
- [ ] 재처리 성공 후 `XACK`를 호출한다.
- [ ] 반복 실패 메시지는 Dead Letter Stream으로 분리한다.
