# Unreal Dedicated Server

원본 레포: [study-unreal-dedicated](https://github.com/Limza/study-unreal-dedicated)  
분류: Unreal, Dedicated Server  
크기: 약 6.9 MB

## 목적

Unreal Dedicated Server 구조와 실행 흐름을 학습한 레포입니다.

## 정리할 포인트

- Listen Server와 Dedicated Server 차이
- 서버 빌드와 실행 흐름
- 클라이언트 접속 흐름
- 서버 권위 구조
- 게임 서버 개발 관점에서 Unreal Dedicated Server가 갖는 의미

## 내 말로 정리

```text
Dedicated Server를 학습하면서 클라이언트와 서버가 같은 게임 로직을 공유하더라도,
실제 상태 판단은 서버 권위로 가져가야 한다는 점을 이해했습니다.
특히 멀티플레이 게임에서는 서버 빌드, 접속 흐름, 상태 복제 구조를 함께 봐야 한다고 생각합니다.
```

## 나중에 보강할 것

- 실행 명령 정리
- 접속 테스트 과정
- Replication 관련 개념
- 서버 권위와 일반 게임 서버 구조 비교
