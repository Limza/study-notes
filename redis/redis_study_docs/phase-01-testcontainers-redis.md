---
tags:
  - redis
  - csharp
  - testcontainers
---

# Phase 01. C#에서 Redis 컨테이너 실행

> [!NOTE] 목표
> 로컬 Redis 설치나 수동 Docker 명령어 없이,  
> C# 코드에서 `IContainer`로 Redis 컨테이너를 실행한다.

---

## 이번 Phase에서 할 일

- .NET 콘솔 프로젝트를 만든다.
- `StackExchange.Redis`를 설치한다.
- `Testcontainers` 패키지를 설치한다.
- C# 코드에서 Redis 컨테이너를 시작한다.
- Redis에 `PING`을 보내 연결을 확인한다.

---

## 필요한 지식

### Testcontainers

Testcontainers는 코드에서 Docker 컨테이너를 생성하고 정리할 수 있게 해주는 라이브러리다.

실습 환경을 코드로 만들 수 있기 때문에, 매번 같은 조건에서 테스트하기 좋다.

---

### IContainer

`IContainer`는 Testcontainers에서 컨테이너를 표현하는 인터페이스다.

Redis 이미지, 포트 바인딩, 시작, 종료를 코드로 관리한다.

---

## GitHub에 올릴 기준 프로젝트 구조

이 학습은 `study-notes/redis` 디렉터리를 하나의 Redis 학습 프로젝트 루트로 본다.

문서는 `redis_study_docs/`에 두고, 직접 실행하는 C# 코드는 `src/` 아래에 둔다.

```text
study-notes/
  redis/
    README.md
    redis_study_docs/
      README.md
      phase-00-learning-goal.md
      phase-01-testcontainers-redis.md
      phase-02-basic-stream.md
      ...
    src/
      RedisStreamStudy/
        RedisStreamStudy.csproj
        Program.cs
        Infrastructure/
          RedisContainerFactory.cs
```

> [!TIP] 정리 기준
> GitHub에 올릴 때는 `study-notes/redis`만 따로 봐도
> 문서와 실습 코드가 함께 이해되도록 만든다.
>
> 이후 Phase에서 Stream, Consumer Group, 장애 재현 코드가 늘어나면
> `src/RedisStreamStudy` 안에 기능별 디렉터리를 추가한다.

---

## 프로젝트 생성 위치

터미널 기준 위치는 `study-notes/redis`다.

```powershell
cd study-notes/redis
dotnet new console -n RedisStreamStudy -o src/RedisStreamStudy
```

옵션 의미:

- `-n RedisStreamStudy`: 생성할 프로젝트 이름을 `RedisStreamStudy`로 지정한다.
- `-o src/RedisStreamStudy`: 생성된 파일을 `src/RedisStreamStudy` 디렉터리에 넣는다. `o`는 output의 의미다.

---

## 패키지 설치

```powershell
dotnet add src/RedisStreamStudy/RedisStreamStudy.csproj package StackExchange.Redis
dotnet add src/RedisStreamStudy/RedisStreamStudy.csproj package Testcontainers
```

---

## 이번 Phase에서 만들 파일

```text
study-notes/
  redis/
    src/
      RedisStreamStudy/
        RedisStreamStudy.csproj
        Program.cs
        Infrastructure/
          RedisContainerFactory.cs
```

---

## Redis 컨테이너 팩토리 예시

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Infrastructure/RedisContainerFactory.cs
```

```csharp
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace RedisStreamStudy.Infrastructure;

public static class RedisContainerFactory
{
    public static IContainer Create()
    {
        return new ContainerBuilder("redis:7-alpine")
            .WithPortBinding(6379, true)
            .WithCleanUp(true)
            .Build();
    }
}
```

---

## Redis 연결 예시

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Program.cs
```

```csharp
using RedisStreamStudy.Infrastructure;
using StackExchange.Redis;

namespace RedisStreamStudy;

public class Program
{
    public static async Task Main(string[] args)
    {
        await using var redisContainer = RedisContainerFactory.Create();

        await redisContainer.StartAsync();

        var redisPort = redisContainer.GetMappedPublicPort(6379);
        var connectionString = $"localhost:{redisPort}";

        await using var connection =
            await ConnectionMultiplexer.ConnectAsync(connectionString);

        var database = connection.GetDatabase();
        var pong = await database.PingAsync();

        Console.WriteLine($"Redis PING: {pong.TotalMilliseconds} ms");
    }
}
```

### `ConnectionMultiplexer` 이해

`ConnectionMultiplexer`는 Redis 연결 관리자다.

```text
ConnectionMultiplexer
  -> Redis 연결 관리자

GetDatabase()
  -> Redis 명령을 보낼 창구

PingAsync()
  -> Redis가 응답하는지 확인하는 명령
```

`GetDatabase()`는 새 연결을 하나 더 만드는 코드가 아니라, 이미 만든 연결을 통해 Redis 명령을 보낼 창구를 얻는 코드다.

콘솔 실습에서는 짧게 실행하고 끝나므로 `await using`으로 정리한다.  
웹 서버처럼 오래 실행되는 앱에서는 보통 `ConnectionMultiplexer`를 하나 만들어 오래 재사용한다.

---

## `async Task Main` 이해

예전 C# 콘솔 프로그램은 보통 `void Main`에서 시작했다.

```csharp
public static void Main(string[] args)
{
    Console.WriteLine("Hello");
}
```

하지만 Redis 연결, 컨테이너 시작, 네트워크 호출처럼 비동기 작업을 `await`하려면 `Main`도 비동기 메서드가 될 수 있다.

```csharp
public static async Task Main(string[] args)
{
    await RunAsync();
}
```

`async Task Main`도 일반 `async` 메서드처럼 await 지점에서 멈췄다가, 작업이 끝나면 이어서 실행된다.

```text
Main 실행
  -> await 지점에서 대기
  -> 작업 완료 후 이어서 실행
```

`Task<int>`를 반환하면 `int` 값이 프로그램 종료 코드가 된다.

```csharp
public static async Task<int> Main(string[] args)
{
    await RunAsync();
    return 0;
}
```

---

## 실습 순서

1. .NET 콘솔 프로젝트를 만든다.
2. 필요한 NuGet 패키지를 설치한다.
3. `RedisContainerFactory`를 만든다.
4. `Program.cs`에서 Redis 컨테이너를 시작한다.
5. mapped port로 Redis에 연결한다.
6. `PING`을 실행한다.
7. 프로그램 종료 시 컨테이너가 정리되는지 확인한다.

---

## 확인할 점

- Redis 컨테이너가 매번 새로 시작되는가?
- 포트 충돌 없이 실행되는가?
- 고정 `6379`가 아니라 mapped port를 사용하고 있는가?
- Redis 연결 객체를 재사용할 수 있는 구조인가?

---

## 자주 생길 수 있는 문제

| 문제 | 원인 후보 | 확인 |
| --- | --- | --- |
| 컨테이너 시작 실패 | Docker Desktop 미실행 | Docker가 켜져 있는지 확인 |
| Redis 연결 실패 | mapped port 미사용 | `GetMappedPublicPort(6379)` 사용 |
| 컨테이너 잔류 | Dispose 누락 | `await using` 또는 `DisposeAsync` 확인 |
