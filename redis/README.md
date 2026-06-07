# Redis

백엔드 운영 관점에서 Redis를 학습하기 위한 기록이다.

첫 번째 주제는 Redis Stream 장애 추적이다. C# 예제로 Redis Stream을 사용하고, `Testcontainers` 패키지와 `IContainer`로 Redis 컨테이너를 실행해서 장애 상황을 재현한다.

## 학습 주제

- Redis Stream
- Consumer Group
- Pending 메시지
- ACK 누락
- 장애 추적
- 메시지 재처리와 복구

## 프로젝트 구조

```text
study-notes/
  redis/
    README.md
    redis_study_docs/
      README.md
      phase-00-learning-goal.md
      phase-01-testcontainers-redis.md
      ...
    reports/
      incident-report-ack-missing.md
    src/
      RedisStreamStudy/
        RedisStreamStudy.csproj
        Program.cs
        Infrastructure/
          RedisContainerFactory.cs
        Scenarios/
          BasicStreamScenario.cs
          ConsumerGroupScenario.cs
          FailureSimulationScenario.cs
          TroubleshootingScenario.cs
          RecoveryScenario.cs
          ...
```

## 학습 계획

상세 학습 계획은 [redis_study_docs/README.md](redis_study_docs/README.md)에 정리한다.

## 목표

Redis Stream을 단순히 사용하는 데서 끝내지 않고, Consumer 장애 상황에서 메시지가 어떻게 남고 어떻게 복구되는지 설명할 수 있을 정도로 정리한다.
