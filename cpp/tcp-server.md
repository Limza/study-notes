# TCP 서버

원본 레포: [study-cplus-tcp_server](https://github.com/Limza/study-cplus-tcp_server)  
분류: C++, TCP server  

## 목적

C++ 기반 TCP 서버 구조를 학습한 레포입니다.

## 정리

- TCP 연결 수립과 세션 관리
- 패킷 수신, 파싱, 처리 흐름
- blocking / non-blocking IO 차이
- 멀티스레드 처리 시 동기화 문제
- 게임 서버에서 TCP를 사용할 때 고려할 점

TCP 서버를 공부하면서 연결 관리, 패킷 처리 흐름, 세션 단위 상태 관리가 중요하다는 점을 확인했습니다.
게임 서버에서는 단순히 데이터를 주고받는 것보다, 끊김 처리와 잘못된 패킷 처리, 동시성 문제를 함께 고려해야 합니다.

## TODO

- 서버 실행 방법
- 패킷 구조
- 세션 관리 방식
- 장애 상황 예시
