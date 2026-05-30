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
- `DotNet.Testcontainers`를 설치한다.
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

## 패키지 설치

```powershell
dotnet add package StackExchange.Redis
dotnet add package DotNet.Testcontainers
```

---

## 예상 프로젝트 구조

```text
src/
  RedisSandbox/
    RedisSandbox.csproj
    Program.cs
    RedisContainerFactory.cs
```

---

## Redis 컨테이너 팩토리 예시

```csharp
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

public static class RedisContainerFactory
{
    public static IContainer Create()
    {
        return new ContainerBuilder()
            .WithImage("redis:7-alpine")
            .WithPortBinding(6379, true)
            .WithCleanUp(true)
            .Build();
    }
}
```

---

## Redis 연결 예시

```csharp
using StackExchange.Redis;

await using var redisContainer = RedisContainerFactory.Create();

await redisContainer.StartAsync();

var redisPort = redisContainer.GetMappedPublicPort(6379);
var connectionString = $"localhost:{redisPort}";

await using var connection =
    await ConnectionMultiplexer.ConnectAsync(connectionString);

var database = connection.GetDatabase();
var pong = await database.PingAsync();

Console.WriteLine($"Redis PING: {pong.TotalMilliseconds} ms");
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

