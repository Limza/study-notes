---
tags:
  - redis
  - redis-stream
  - replication
  - testcontainers
  - failure-simulation
---

# Phase 08. Redis 노드 장애와 Stream 유실 재현

> [!NOTE] 목표
> Redis Stream에서 Consumer 장애는 Pending 추적으로 대응할 수 있다.  
> 하지만 Redis master 장애와 비동기 복제 지연이 겹치면
> Stream 메시지 자체가 replica에 남지 않을 수 있다.

---

## 이 Phase를 추가하는 이유

앞 단계에서는 Consumer가 죽었을 때 메시지가 Pending 상태로 남는 흐름을 확인했다.

하지만 그 전제는 Redis 안에 Stream 데이터가 남아 있다는 것이다.

Redis master가 성공 응답을 준 뒤, replica에 복제되기 전에 장애가 발생하면 최신 메시지가 사라질 수 있다.

이 Phase에서는 Redis Cluster 전체를 구성하지 않고, master-replica 구조로 핵심 원인만 재현한다.

---

## 확인하려는 장애

Stream 메시지 유실의 핵심 원인은 Stream 자료구조가 아니라 Redis의 비동기 복제다.

테스트에서는 아래 상황을 만든다.

1. master와 replica를 실행한다.
2. replica가 master를 복제하게 만든다.
3. baseline 메시지가 replica에 복제되는지 확인한다.
4. replica 복제를 끊는다.
5. master에 `XADD`로 Stream 메시지를 쓴다.
6. master는 성공 응답을 반환한다.
7. master 컨테이너를 중지한다.
8. replica에서 방금 쓴 Stream 메시지가 없는지 확인한다.

---

## 테스트 구조

| 역할 | 컨테이너 | 설명 |
| --- | --- | --- |
| master | `redis-master` | Producer가 `XADD`를 보내는 Redis |
| replica | `redis-replica` | master를 복제하는 Redis |
| app | C# 테스트 코드 | `IContainer`로 Redis 컨테이너를 실행 |

이 구조는 실제 Redis Cluster와 같지는 않다.

하지만 master가 성공 응답을 준 write가 replica에 복제되기 전에 master가 죽으면, replica에는 그 데이터가 없을 수 있다는 점을 확인하기에는 충분하다.

---

## Redis 명령 흐름

복제 상태 확인:

```redis
INFO REPLICATION
```

replica 복제 끊기:

```redis
REPLICAOF NO ONE
```

master에 Stream 메시지 쓰기:

```redis
XADD game:events * type match.completed matchId 1001
```

replica에서 Stream 조회:

```redis
XRANGE game:events - +
```

기대 결과는 replica에 방금 쓴 메시지가 없는 것이다.

---

## 이번 Phase에서 만들 파일

Redis master-replica 실험은 기존 단일 Redis 컨테이너 시나리오와 분리한다.

```text
study-notes/
  redis/
    src/
      RedisStreamStudy/
        Program.cs
        Scenarios/
          ReplicationLossScenario.cs
```

`ReplicationLossScenario.cs`에는 master 컨테이너, replica 컨테이너, Docker network 생성 코드를 넣는다.

`Program.cs`에서는 이 Phase를 실습할 때 `ReplicationLossScenario`만 실행하도록 바꾼다.

---

## C# 테스트 코드 초안

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/ReplicationLossScenario.cs
```

클래스 / 메서드:

```text
ReplicationLossScenario.RunAsync
```

역할:

```text
master-replica Redis 컨테이너를 만들고, master 성공 응답 뒤 replica에 없는 Stream 메시지를 확인한다.
```

```csharp
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class ReplicationLossScenario
{
    public static async Task RunAsync()
    {
        // master와 replica가 컨테이너 이름으로 서로 찾을 수 있도록 Docker network를 만든다.
        // Guid를 붙여 매 실행마다 겹치지 않는 네트워크 이름을 만든다.
        await using INetwork network = new NetworkBuilder()
            .WithName($"redis-stream-loss-{Guid.NewGuid():N}")
            .Build();

        // Testcontainers network는 컨테이너 시작 전에 먼저 생성해야 한다.
        await network.CreateAsync();

        // Producer가 XADD를 보내는 master Redis 컨테이너를 만든다.
        // WithNetworkAliases("redis-master") 덕분에 같은 Docker network 안의 replica가
        // redis-master라는 이름으로 master 컨테이너를 찾을 수 있다.
        // WithPortBinding(6379, true)는 호스트 포트는 자동 배정하고, 컨테이너 내부 6379를 노출한다.
        // appendonly no는 AOF 영속화 변수를 줄이고 복제 지연 실험에 집중하기 위한 설정이다.
        await using IContainer master = new ContainerBuilder("redis:7.4")
            .WithName($"redis-master-{Guid.NewGuid():N}")
            .WithNetwork(network)
            .WithNetworkAliases("redis-master")
            .WithPortBinding(6379, true)
            .WithCommand("redis-server", "--appendonly", "no")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(6379))
            .Build();

        // master를 복제하는 replica Redis 컨테이너를 만든다.
        // redis-master는 위에서 지정한 master 컨테이너의 network alias다.
        // "--replicaof redis-master 6379"로 시작하면 replica가 부팅하면서 master에 붙는다.
        await using IContainer replica = new ContainerBuilder("redis:7.4")
            .WithName($"redis-replica-{Guid.NewGuid():N}")
            .WithNetwork(network)
            .WithNetworkAliases("redis-replica")
            .WithPortBinding(6379, true)
            .WithCommand("redis-server", "--replicaof", "redis-master", "6379", "--appendonly", "no")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(6379))
            .Build();

        // master를 먼저 시작한 뒤 replica를 시작해야 replica가 master에 붙을 수 있다.
        await master.StartAsync();
        await replica.StartAsync();

        // C# 코드가 호스트에서 Redis에 접속할 수 있도록 매핑된 포트를 가져온다.
        // 여기서 6379는 WithPortBinding(6379, true)에 지정한 컨테이너 내부 Redis 포트다.
        // master와 replica는 둘 다 컨테이너 내부에서는 6379를 쓰지만, 호스트 포트는 서로 다르게 자동 배정된다.
        var masterPort = master.GetMappedPublicPort(6379);
        var replicaPort = replica.GetMappedPublicPort(6379);

        // master와 replica에 각각 접속한다.
        // masterDb에는 쓰기 명령을 보내고, replicaDb에는 복제 결과 확인 명령을 보낸다.
        using var masterRedis = await ConnectionMultiplexer.ConnectAsync($"localhost:{masterPort}");
        using var replicaRedis = await ConnectionMultiplexer.ConnectAsync($"localhost:{replicaPort}");

        // Redis 명령을 보낼 database 객체를 꺼낸다.
        var masterDb = masterRedis.GetDatabase();
        var replicaDb = replicaRedis.GetDatabase();

        // 먼저 baseline 메시지를 써서 replica 복제가 정상인지 확인한다.
        // 이 메시지가 replica에 보이지 않으면 실험 전에 복제 연결부터 문제가 있는 것이다.
        await masterDb.ExecuteAsync("XADD", "game:events", "*", "type", "baseline", "matchId", "before-break");
        await Task.Delay(500);

        // replica에 baseline 메시지가 보이면 기본 복제 연결은 정상이다.
        var baseline = await replicaDb.StreamRangeAsync("game:events");
        Console.WriteLine($"Replica baseline count: {baseline.Length}");

        // replica를 master에서 떼어 내 복제가 밀린 상황을 강제로 만든다.
        // 실제 장애에서는 네트워크 지연이나 failover 타이밍 때문에 비슷한 간극이 생길 수 있다.
        await replica.ExecAsync(new[] { "redis-cli", "REPLICAOF", "NO", "ONE" });

        // master에는 쓰기 성공 응답을 받지만 replica에는 복제되지 않을 메시지를 쓴다.
        // StreamAddAsync가 성공해도, 그 성공이 replica 저장까지 보장한다는 뜻은 아니다.
        var lostCandidateId = await masterDb.StreamAddAsync(
            "game:events",
            new NameValueEntry[]
            {
                new("type", "match.completed"),
                new("matchId", "1001")
            });

        Console.WriteLine($"XADD success on master: {lostCandidateId}");

        // master 장애를 재현하기 위해 master 컨테이너를 중지한다.
        // 이 시점에 lostCandidateId가 replica로 복제되지 않았다면, replica에서는 해당 메시지를 볼 수 없다.
        await master.StopAsync();

        // master가 죽은 뒤 replica에 남은 Stream 메시지를 조회한다.
        var entriesAfterMasterDown = await replicaDb.StreamRangeAsync("game:events");

        Console.WriteLine($"Replica count after master down: {entriesAfterMasterDown.Length}");

        // baseline만 있고 lostCandidateId가 없으면 복제 전 유실 상황이 재현된 것이다.
        foreach (var entry in entriesAfterMasterDown)
        {
            // replica에 남아 있는 message id만 출력한다.
            // lostCandidateId가 출력되지 않으면 master 성공 응답 후 replica 미복제 상태였다는 뜻이다.
            Console.WriteLine(entry.Id);
        }
    }
}
```

`ReplicationLossScenario.RunAsync`는 Redis 컨테이너를 직접 만들기 때문에 `database`를 `Program.cs`에서 넘겨받지 않는다.

`Program.cs`의 호출 부분:

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Program.cs
```

```csharp
// ...

// 이 시나리오는 master-replica 컨테이너를 직접 만들기 때문에 database를 넘기지 않는다.
await ReplicationLossScenario.RunAsync();

// ...
```

---

## 결과 해석

`baseline` 메시지는 replica 복제가 정상인지 확인하는 기준 메시지다.

`match.completed / 1001` 메시지는 replica 복제를 끊은 뒤 master에 쓴 메시지다.

master는 `XADD` 성공 응답을 줬지만 replica에는 그 메시지가 없다.

이 결과는 Redis Stream의 Consumer 장애 추적은 유용하지만, Redis master 장애와 복제 지연까지 자동으로 보장하지는 않는다는 점을 보여준다.

---

## 다음에 확장할 수 있는 테스트

| 확장 테스트 | 확인할 내용 |
| --- | --- |
| `WAIT` 사용 | write 후 replica 복제를 기다리면 결과가 어떻게 달라지는지 확인 |
| AOF `everysec` | Redis 재시작 시 최근 메시지 보존 범위 확인 |
| AOF `always` | write latency와 유실 가능성 변화 확인 |
| Toxiproxy | master-replica 네트워크 지연과 단절을 실제처럼 재현 |
| Redis Cluster 3 master + 3 replica | 특정 hash slot master 장애와 failover 흐름 확인 |

---

## 운영 관점 정리

Redis Stream은 Consumer가 죽었을 때 Pending 메시지를 추적하고 복구하는 데 유리하다.

하지만 Redis master가 죽었을 때 replica에 복제되지 않은 최신 Stream 메시지는 사라질 수 있다.

따라서 Redis Stream을 운영 메시지 처리에 사용할 때는 Consumer 장애와 Redis 노드 장애를 분리해서 봐야 한다.

Consumer 장애는 `XPENDING`, `XINFO`, `XAUTOCLAIM`, `XACK`로 추적한다.

Redis 노드 장애는 replication, persistence, failover, `WAIT`, `min-replicas-to-write` 같은 설정과 함께 검토한다.
