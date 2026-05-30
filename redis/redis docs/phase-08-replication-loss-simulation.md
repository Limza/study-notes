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
> Redis Stream이 Consumer 장애에는 Pending 추적으로 대응할 수 있지만,  
> Redis master 장애와 비동기 복제 지연 상황에서는  
> Stream 메시지가 유실될 수 있다는 점을 테스트로 확인한다.

---

## 이 Phase를 추가하는 이유

앞 단계에서는 Consumer가 죽었을 때 메시지가 Pending 상태로 남는 흐름을 확인했다.

하지만 그 전제는 Redis에 Stream 데이터가 남아 있다는 것이다.

Redis 자체가 죽거나, master가 성공 응답을 준 뒤 replica에 복제되기 전에 장애가 발생하면 상황이 달라진다.

이 Phase에서는 Redis Cluster 전체를 바로 구성하지 않고, master-replica 구조로 핵심 원인을 먼저 재현한다.

---

## 확인하려는 장애

Redis Cluster에서 Stream 메시지가 유실될 수 있는 핵심 원인은 Stream 자료구조가 아니라 Redis의 비동기 복제다.

테스트에서는 다음 상황을 만든다.

1. master와 replica를 실행한다.
2. replica가 master를 복제하게 만든다.
3. replica 복제를 일부러 끊는다.
4. master에 `XADD`로 Stream 메시지를 쓴다.
5. master는 성공 응답을 반환한다.
6. master 컨테이너를 강제 종료한다.
7. replica를 master처럼 조회한다.
8. 방금 성공한 Stream 메시지가 replica에 없는지 확인한다.

---

## 테스트 구조

| 역할 | 컨테이너 | 설명 |
| --- | --- | --- |
| master | `redis-master` | Producer가 `XADD`를 보내는 Redis |
| replica | `redis-replica` | master를 복제하는 Redis |
| app | C# 테스트 코드 | `IContainer`로 Redis 컨테이너 실행 |

이 구조는 실제 Redis Cluster와 완전히 같지는 않다.

하지만 “master가 성공 응답을 준 write가 replica에 도착하기 전에 master가 죽으면 새 master에 데이터가 없을 수 있다”는 핵심을 확인하기에는 충분하다.

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

## C# 테스트 코드 초안

```csharp
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using StackExchange.Redis;

await using INetwork network = new NetworkBuilder()
    .WithName($"redis-stream-loss-{Guid.NewGuid():N}")
    .Build();

await network.CreateAsync();

await using IContainer master = new ContainerBuilder()
    .WithImage("redis:7.4")
    .WithName($"redis-master-{Guid.NewGuid():N}")
    .WithNetwork(network)
    .WithNetworkAliases("redis-master")
    .WithPortBinding(6379, true)
    .WithCommand("redis-server", "--appendonly", "no")
    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(6379))
    .Build();

await using IContainer replica = new ContainerBuilder()
    .WithImage("redis:7.4")
    .WithName($"redis-replica-{Guid.NewGuid():N}")
    .WithNetwork(network)
    .WithNetworkAliases("redis-replica")
    .WithPortBinding(6379, true)
    .WithCommand("redis-server", "--replicaof", "redis-master", "6379", "--appendonly", "no")
    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(6379))
    .Build();

await master.StartAsync();
await replica.StartAsync();

var masterPort = master.GetMappedPublicPort(6379);
var replicaPort = replica.GetMappedPublicPort(6379);

using var masterRedis = await ConnectionMultiplexer.ConnectAsync($"localhost:{masterPort}");
using var replicaRedis = await ConnectionMultiplexer.ConnectAsync($"localhost:{replicaPort}");

var masterDb = masterRedis.GetDatabase();
var replicaDb = replicaRedis.GetDatabase();

await masterDb.ExecuteAsync("XADD", "game:events", "*", "type", "baseline", "matchId", "before-break");
await Task.Delay(500);

var baseline = await replicaDb.StreamRangeAsync("game:events");
Console.WriteLine($"Replica baseline count: {baseline.Length}");

await replica.ExecAsync(new[] { "redis-cli", "REPLICAOF", "NO", "ONE" });

var lostCandidateId = await masterDb.StreamAddAsync(
    "game:events",
    new NameValueEntry[]
    {
        new("type", "match.completed"),
        new("matchId", "1001")
    });

Console.WriteLine($"XADD success on master: {lostCandidateId}");

await master.StopAsync();

var entriesAfterMasterDown = await replicaDb.StreamRangeAsync("game:events");

Console.WriteLine($"Replica count after master down: {entriesAfterMasterDown.Length}");

foreach (var entry in entriesAfterMasterDown)
{
    Console.WriteLine(entry.Id);
}

```

---

## 결과 해석

`baseline` 메시지는 replica에 복제된 뒤 확인하는 메시지다.

`match.completed / 1001` 메시지는 replica 복제를 끊은 뒤 master에 쓴 메시지다.

master는 `XADD` 성공 응답을 줬지만, replica에는 그 메시지가 없다.

이 결과는 Redis Stream이 Consumer 장애 추적에는 유용하지만, Redis master 장애와 복제 지연까지 자동으로 보장하지는 않는다는 것을 보여준다.

---

## 다음에 확장할 수 있는 테스트

| 확장 테스트 | 확인할 내용 |
| --- | --- |
| `WAIT` 사용 | write 후 replica 복제를 기다리면 결과가 어떻게 달라지는지 확인 |
| AOF `everysec` | Redis 재시작 시 최근 메시지 보존 범위 확인 |
| AOF `always` | write latency와 유실 가능성 변화 확인 |
| Toxiproxy | master-replica 네트워크 지연과 단절을 더 실제처럼 재현 |
| Redis Cluster 3 master + 3 replica | 특정 hash slot master 장애와 failover 흐름 확인 |

---

## 운영 관점 정리

Redis Stream은 Consumer가 죽었을 때 Pending 메시지를 추적하고 복구하는 데 유리하다.

하지만 Redis master가 죽었을 때 replica에 복제되지 않은 최신 Stream 메시지는 사라질 수 있다.

따라서 Redis Stream을 운영 큐처럼 사용할 때는 Consumer 장애와 Redis 노드 장애를 분리해서 봐야 한다.

Consumer 장애는 `XPENDING`, `XINFO`, `XAUTOCLAIM`, `XACK`로 추적한다.

Redis 노드 장애는 replication, persistence, failover, `WAIT`, `min-replicas-to-write` 같은 설정과 함께 검토한다.
