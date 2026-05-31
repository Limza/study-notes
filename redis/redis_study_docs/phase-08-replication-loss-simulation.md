---
tags:
  - redis
  - redis-stream
  - replication
  - testcontainers
  - failure-simulation
---

# Phase 08. Redis ?몃뱶 ?μ븷? Stream ?좎떎 ?ы쁽

> [!NOTE] 紐⑺몴
> Redis Stream??Consumer ?μ븷?먮뒗 Pending 異붿쟻?쇰줈 ??묓븷 ???덉?留?  
> Redis master ?μ븷? 鍮꾨룞湲?蹂듭젣 吏???곹솴?먯꽌?? 
> Stream 硫붿떆吏媛 ?좎떎?????덈떎???먯쓣 ?뚯뒪?몃줈 ?뺤씤?쒕떎.

---

## ??Phase瑜?異붽??섎뒗 ?댁쑀

???④퀎?먯꽌??Consumer媛 二쎌뿀????硫붿떆吏媛 Pending ?곹깭濡??⑤뒗 ?먮쫫???뺤씤?덈떎.

?섏?留?洹??꾩젣??Redis??Stream ?곗씠?곌? ?⑥븘 ?덈떎??寃껋씠??

Redis ?먯껜媛 二쎄굅?? master媛 ?깃났 ?묐떟??以 ??replica??蹂듭젣?섍린 ?꾩뿉 ?μ븷媛 諛쒖깮?섎㈃ ?곹솴???щ씪吏꾨떎.

??Phase?먯꽌??Redis Cluster ?꾩껜瑜?諛붾줈 援ъ꽦?섏? ?딄퀬, master-replica 援ъ“濡??듭떖 ?먯씤??癒쇱? ?ы쁽?쒕떎.

---

## ?뺤씤?섎젮???μ븷

Redis Cluster?먯꽌 Stream 硫붿떆吏媛 ?좎떎?????덈뒗 ?듭떖 ?먯씤? Stream ?먮즺援ъ“媛 ?꾨땲??Redis??鍮꾨룞湲?蹂듭젣??

?뚯뒪?몄뿉?쒕뒗 ?ㅼ쓬 ?곹솴??留뚮뱺??

1. master? replica瑜??ㅽ뻾?쒕떎.
2. replica媛 master瑜?蹂듭젣?섍쾶 留뚮뱺??
3. replica 蹂듭젣瑜??쇰????딅뒗??
4. master??`XADD`濡?Stream 硫붿떆吏瑜??대떎.
5. master???깃났 ?묐떟??諛섑솚?쒕떎.
6. master 而⑦뀒?대꼫瑜?媛뺤젣 醫낅즺?쒕떎.
7. replica瑜?master泥섎읆 議고쉶?쒕떎.
8. 諛⑷툑 ?깃났??Stream 硫붿떆吏媛 replica???녿뒗吏 ?뺤씤?쒕떎.

---

## ?뚯뒪??援ъ“

| ??븷 | 而⑦뀒?대꼫 | ?ㅻ챸 |
| --- | --- | --- |
| master | `redis-master` | Producer媛 `XADD`瑜?蹂대궡??Redis |
| replica | `redis-replica` | master瑜?蹂듭젣?섎뒗 Redis |
| app | C# ?뚯뒪??肄붾뱶 | `IContainer`濡?Redis 而⑦뀒?대꼫 ?ㅽ뻾 |

??援ъ“???ㅼ젣 Redis Cluster? ?꾩쟾??媛숈????딅떎.

?섏?留??쐌aster媛 ?깃났 ?묐떟??以 write媛 replica???꾩갑?섍린 ?꾩뿉 master媛 二쎌쑝硫???master???곗씠?곌? ?놁쓣 ???덈떎?앸뒗 ?듭떖???뺤씤?섍린?먮뒗 異⑸텇?섎떎.

---

## Redis 紐낅졊 ?먮쫫

蹂듭젣 ?곹깭 ?뺤씤:

```redis
INFO REPLICATION
```

replica 蹂듭젣 ?딄린:

```redis
REPLICAOF NO ONE
```

master??Stream 硫붿떆吏 ?곌린:

```redis
XADD game:events * type match.completed matchId 1001
```

replica?먯꽌 Stream 議고쉶:

```redis
XRANGE game:events - +
```

湲곕? 寃곌낵??replica??諛⑷툑 ??硫붿떆吏媛 ?녿뒗 寃껋씠??

---

## ?대쾲 Phase?먯꽌 留뚮뱾 ?뚯씪

Redis master-replica ?ㅽ뿕? 湲곗〈 ?⑥씪 Redis 而⑦뀒?대꼫 ?쒕굹由ъ삤? 遺꾨━?쒕떎.

```text
study-notes/
  redis/
    src/
      RedisStreamStudy/
        Program.cs
        Scenarios/
          ReplicationLossScenario.cs
```

`ReplicationLossScenario.cs`?먮뒗 master 而⑦뀒?대꼫, replica 而⑦뀒?대꼫, Docker network ?앹꽦 肄붾뱶瑜??ｋ뒗??

`Program.cs`?먯꽌????Phase瑜??ㅼ뒿????`ReplicationLossScenario`留??ㅽ뻾?섎룄濡?諛붽씔??

---

## C# ?뚯뒪??肄붾뱶 珥덉븞

```csharp
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using StackExchange.Redis;

await using INetwork network = new NetworkBuilder()
    .WithName($"redis-stream-loss-{Guid.NewGuid():N}")
    .Build();

await network.CreateAsync();

await using IContainer master = new ContainerBuilder("redis:7.4")
    .WithName($"redis-master-{Guid.NewGuid():N}")
    .WithNetwork(network)
    .WithNetworkAliases("redis-master")
    .WithPortBinding(6379, true)
    .WithCommand("redis-server", "--appendonly", "no")
    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(6379))
    .Build();

await using IContainer replica = new ContainerBuilder("redis:7.4")
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

## 寃곌낵 ?댁꽍

`baseline` 硫붿떆吏??replica??蹂듭젣?????뺤씤?섎뒗 硫붿떆吏??

`match.completed / 1001` 硫붿떆吏??replica 蹂듭젣瑜??딆? ??master????硫붿떆吏??

master??`XADD` ?깃났 ?묐떟??以ъ?留? replica?먮뒗 洹?硫붿떆吏媛 ?녿떎.

??寃곌낵??Redis Stream??Consumer ?μ븷 異붿쟻?먮뒗 ?좎슜?섏?留? Redis master ?μ븷? 蹂듭젣 吏?곌퉴吏 ?먮룞?쇰줈 蹂댁옣?섏????딅뒗?ㅻ뒗 寃껋쓣 蹂댁뿬以??

---

## ?ㅼ쓬???뺤옣?????덈뒗 ?뚯뒪??
| ?뺤옣 ?뚯뒪??| ?뺤씤???댁슜 |
| --- | --- |
| `WAIT` ?ъ슜 | write ??replica 蹂듭젣瑜?湲곕떎由щ㈃ 寃곌낵媛 ?대뼸寃??щ씪吏?붿? ?뺤씤 |
| AOF `everysec` | Redis ?ъ떆????理쒓렐 硫붿떆吏 蹂댁〈 踰붿쐞 ?뺤씤 |
| AOF `always` | write latency? ?좎떎 媛?μ꽦 蹂???뺤씤 |
| Toxiproxy | master-replica ?ㅽ듃?뚰겕 吏?곌낵 ?⑥젅?????ㅼ젣泥섎읆 ?ы쁽 |
| Redis Cluster 3 master + 3 replica | ?뱀젙 hash slot master ?μ븷? failover ?먮쫫 ?뺤씤 |

---

## ?댁쁺 愿???뺣━

Redis Stream? Consumer媛 二쎌뿀????Pending 硫붿떆吏瑜?異붿쟻?섍퀬 蹂듦뎄?섎뒗 ???좊━?섎떎.

?섏?留?Redis master媛 二쎌뿀????replica??蹂듭젣?섏? ?딆? 理쒖떊 Stream 硫붿떆吏???щ씪吏????덈떎.

?곕씪??Redis Stream???댁쁺 ?먯쿂???ъ슜???뚮뒗 Consumer ?μ븷? Redis ?몃뱶 ?μ븷瑜?遺꾨━?댁꽌 遊먯빞 ?쒕떎.

Consumer ?μ븷??`XPENDING`, `XINFO`, `XAUTOCLAIM`, `XACK`濡?異붿쟻?쒕떎.

Redis ?몃뱶 ?μ븷??replication, persistence, failover, `WAIT`, `min-replicas-to-write` 媛숈? ?ㅼ젙怨??④퍡 寃?좏븳??
