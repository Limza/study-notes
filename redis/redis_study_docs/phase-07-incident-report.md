---
tags:
  - redis
  - incident-report
  - troubleshooting
---

# Phase 07. 운영 관점 장애 보고서

> [!NOTE] 목표
> 실습한 장애 상황을 실제 운영 장애 보고서처럼 정리한다.  
> 중요한 것은 “무엇을 봤고, 왜 그렇게 판단했는가”다.

---

## 이번 Phase에서 할 일

- 장애 상황을 보고서 형식으로 정리한다.
- 명령어 결과와 해석을 함께 남긴다.
- 원인과 복구 방법을 구분한다.
- 재발 방지 아이디어를 정리한다.

---

## 이번 Phase에서 만들 파일

장애 보고서는 코드가 아니라 실습 결과 문서로 남긴다.

```text
study-notes/
  redis/
    reports/
      incident-report-ack-missing.md
```

`incident-report-ack-missing.md`에는 Phase 04~06에서 만든 장애 재현, 추적, 복구 결과를 한 번에 정리한다.

GitHub에 올릴 때는 이 보고서가 “실제로 어떤 장애를 재현했고 어떻게 판단했는지”를 보여주는 포트폴리오 산출물이 된다.

---

## 장애 보고서 템플릿

```text
제목:

상황:

영향 범위:

초기 가설:

확인한 명령어:

관찰 결과:

원인:

복구 방법:

재발 방지:

배운 점:
```

---

## 예시 제목

```text
Redis Stream Consumer ACK 누락으로 인한 Pending 메시지 적체 분석
```

---

## 예시 상황

```text
Consumer A가 game:events Stream에서 메시지를 읽은 뒤 처리 중 종료되었다.

XACK가 호출되지 않아 메시지들이 game-workers Consumer Group의 Pending 상태로 남았다.
```

---

## 예시 확인 명령어

```redis
XINFO GROUPS game:events
XINFO CONSUMERS game:events game-workers
XPENDING game:events game-workers
XPENDING game:events game-workers - + 10
```

---

## 예시 관찰 결과

```text
game-workers 그룹의 pending 수가 5였다.

consumer-a에 pending 메시지 5개가 묶여 있었다.

각 메시지의 idle time이 복구 기준인 5초를 초과했다.

delivery count는 1이어서 최초 재처리 대상으로 판단했다.
```

---

## 예시 복구 방법

```text
recovery-consumer가 XAUTOCLAIM으로 idle time 5초 이상인 메시지를 가져왔다.

메시지를 재처리한 뒤 성공한 메시지에 대해 XACK를 호출했다.

복구 후 XPENDING 결과 pending 수가 0이 되었다.
```

---

## 재발 방지 관점

- Consumer 처리 로직에 예외 처리를 추가한다.
- 처리 실패와 ACK 실패를 구분해서 로그를 남긴다.
- Pending 메시지 수를 모니터링한다.
- Pending 메시지의 idle time을 모니터링한다.
- 오래된 Pending 메시지는 자동 복구 대상으로 삼는다.
- 반복 실패 메시지는 Dead Letter Stream으로 이동한다.
