---
tags:
  - redis
  - incident-alert
  - monitoring
  - csharp
---

# Phase 07. 장애 알림 양식 출력

> [!NOTE] 목표
> Pending 메시지 상태를 사람이 보고서에 직접 옮기기 전에,
> C# 코드에서 Slack으로 보낼 수 있는 알림 payload를 만들고 콘솔에 출력한다.  
> 이번 Phase에서는 실제 Slack 전송 없이 JSON 모양만 확인한다.

---

## 이번 Phase에서 할 일

- Redis Stream의 Pending 상태를 조회한다.
- Pending 수, 가장 긴 idle time, 가장 큰 delivery count를 계산한다.
- 값에 따라 `normal`, `warning`, `critical` 심각도를 정한다.
- Slack 메시지 payload를 만든다.
- 만든 JSON을 `Console.WriteLine`으로 출력한다.

---

## 이번 Phase에서 만들 파일

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/PendingAlertScenario.cs
```

클래스 / 메서드:

```text
PendingAlertScenario.RunAsync
```

역할:

```text
Redis Stream Pending 상태를 읽고 Slack 알림 JSON을 만들어 콘솔에 출력한다.
```

---

## `PendingAlertScenario.cs` 작성

먼저 아래 전체 파일 예시를 만든다.

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Scenarios/PendingAlertScenario.cs
```

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class PendingAlertScenario
{
    private const long CriticalIdleMs = 5000;
    private const long CriticalDeliveryCount = 3;

    public static async Task RunAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        // StreamPendingAsync는 Redis CLI의 XPENDING stream group 요약 조회와 가깝다.
        // Group 전체에 ACK되지 않고 남은 메시지가 몇 개인지 확인한다.
        var pending = await database.StreamPendingAsync(streamKey, groupName);

		// pendingCount는 ACK되지 않고 Consumer Group에 남아 있는 메시지 수다.
        var pendingCount = pending.PendingMessageCount;

        // StreamPendingMessagesAsync는 XPENDING stream group - + count 상세 조회와 가깝다.
        // Pending 메시지마다 owner consumer, idle time, delivery count를 확인한다.
        var pendingMessages = await database.StreamPendingMessagesAsync(
            streamKey,
            groupName,
            count: 10,
            consumerName: RedisValue.Null);

        // maxIdleMs는 Pending 메시지 중 가장 오래 ACK되지 않은 시간을 밀리초 단위로 계산한 값이다.
        // 이 값이 너무 크면 Consumer가 멈췄거나 ACK 호출 전에 실패했을 가능성이 커진다.
        var maxIdleMs = pendingMessages.Length == 0
            ? 0
            : pendingMessages.Max(message => message.IdleTimeInMilliseconds);

        // delivery count는 같은 메시지가 Consumer에게 몇 번 전달됐는지 나타낸다.
        // 값이 높으면 재처리를 반복했지만 계속 실패했을 가능성이 있다.
        var maxDeliveryCount = pendingMessages.Length == 0
            ? 0
            : pendingMessages.Max(message => message.DeliveryCount);

        var severity = GetSeverity(pendingCount, maxIdleMs, maxDeliveryCount);

        // Slack으로 보낼 수 있는 메시지 양식이다.
        // 이번 Phase에서는 실제 Webhook 호출 없이 payload 모양만 확인한다.
        var slackMessage = new SlackAlertMessage(
            Text: $"[{severity}] Redis Stream Pending messages",
            Blocks:
            [
                new SlackBlock("header", new SlackText("plain_text", "Redis Stream Pending messages")),
                new SlackBlock(
                    "section",
                    null,
                    [
                        new SlackText("mrkdwn", $"*Severity*\n{severity}"),
                        new SlackText("mrkdwn", $"*Stream*\n{streamKey}"),
                        new SlackText("mrkdwn", $"*Group*\n{groupName}"),
                        new SlackText("mrkdwn", $"*Pending*\n{pendingCount}"),
                        new SlackText("mrkdwn", $"*Max idle*\n{maxIdleMs}ms"),
                        new SlackText("mrkdwn", $"*Max delivery*\n{maxDeliveryCount}")
                    ]),
                new SlackBlock(
                    "section",
                    new SlackText(
                        "mrkdwn",
                        $"*Check*\n```XPENDING {streamKey} {groupName} - + 10```")),
                new SlackBlock(
                    "section",
                    new SlackText(
                        "mrkdwn",
                        $"*Recovery candidate*\n```XAUTOCLAIM {streamKey} {groupName} recovery-consumer {CriticalIdleMs} 0-0 COUNT 10```"))
            ]);

        var options = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        // Slack 메시지로 바꾸기 쉬운 payload JSON이다.
        Console.WriteLine();
        Console.WriteLine("=== SLACK MESSAGE JSON ===");
        Console.WriteLine(JsonSerializer.Serialize(slackMessage, options));
    }

    private static string GetSeverity(
        long pendingCount,
        long maxIdleMs,
        long maxDeliveryCount)
    {
        // Pending 메시지가 없으면 알림이 해소된 상태로 본다.
        if (pendingCount == 0)
        {
            return "normal";
        }

        // 오래 방치됐거나 반복 전달된 메시지가 있으면 즉시 확인해야 하는 상태로 본다.
        if (maxIdleMs >= CriticalIdleMs || maxDeliveryCount >= CriticalDeliveryCount)
        {
            return "critical";
        }

        // Pending은 있지만 아직 복구 기준을 넘지 않았으면 관찰 상태로 본다.
        return "warning";
    }

    private sealed record SlackAlertMessage(
        string Text,
        SlackBlock[] Blocks);

    private sealed record SlackBlock(
        string Type,
        SlackText? Text = null,
        SlackText[]? Fields = null);

    private sealed record SlackText(
        string Type,
        string Text);
}
```

---

## `Program.cs` 호출 추가

파일 위치:

```text
study-notes/redis/src/RedisStreamStudy/Program.cs
```

`FailureSimulationScenario`가 Pending 메시지를 만든 뒤에 `PendingAlertScenario`를 호출한다.

```csharp
// 먼저 Consumer가 메시지를 읽고 ACK하지 않은 장애 상황을 만든다.
await FailureSimulationScenario.RunAsync(database, streamKey, groupName);

// Pending 상태를 알림 양식으로 만든 뒤 콘솔에 출력한다.
await PendingAlertScenario.RunAsync(database, streamKey, groupName);
```

`PendingAlertScenario`는 Pending 상태를 읽는 코드이므로,
반드시 Pending을 만드는 시나리오 뒤에서 실행해야 한다.

---

## Redis 명령어와 C# API 연결

`StreamPendingAsync`는 Redis CLI의 아래 명령어와 가깝다.

```redis
XPENDING game:events game-workers
```

이 명령어는 Consumer Group 전체의 Pending 요약을 보여준다.
코드에서는 `pending.PendingMessageCount`로 Pending 수를 가져온다.

`StreamPendingMessagesAsync`는 Redis CLI의 아래 명령어와 가깝다.

```redis
XPENDING game:events game-workers - + 10
```

이 명령어는 Pending 메시지별 상세 정보를 보여준다.
코드에서는 각 메시지의 idle time과 delivery count를 읽어 알림 판단 값으로 쓴다.

---

## 알림 판단 값

| 값 | 의미 |
| --- | --- |
| `pendingCount` | ACK되지 않고 Consumer Group에 남아 있는 메시지 수 |
| `maxIdleMs` | Pending 메시지 중 가장 오래 ACK되지 않은 시간 |
| `maxDeliveryCount` | Pending 메시지 중 가장 많이 재전달된 횟수 |
| `severity` | Slack 메시지 제목과 필드에 들어갈 심각도 |

`pendingCount`만 보면 아직 처리 중인지 장애인지 구분하기 어렵다.
그래서 `maxIdleMs`를 같이 본다.

오래 ACK되지 않은 메시지가 있으면 Consumer가 죽었거나,
처리 도중 예외가 나서 `XACK`까지 가지 못했을 가능성이 있다.

---

## 심각도 기준

```text
pendingCount == 0
=> normal

pendingCount > 0 이고 maxIdleMs < 5000
=> warning

pendingCount > 0 이고 maxIdleMs >= 5000
=> critical

maxDeliveryCount >= 3
=> critical
```

`CriticalIdleMs = 5000`은 학습용 기준이다.
실제 운영에서는 평균 메시지 처리 시간보다 충분히 크게 잡아야 한다.

`CriticalDeliveryCount = 3`은 같은 메시지가 여러 번 재전달됐는지 보기 위한 기준이다.
delivery count가 계속 커지면 자동 재처리보다 Dead Letter Stream으로 보내는 쪽을 검토해야 한다.

---

## 출력 예시

콘솔에는 Slack 메시지 JSON만 출력한다.

```json
{
  "text": "[critical] Redis Stream Pending messages",
  "blocks": [
    {
      "type": "header",
      "text": {
        "type": "plain_text",
        "text": "Redis Stream Pending messages"
      }
    },
    {
      "type": "section",
      "fields": [
        {
          "type": "mrkdwn",
          "text": "*Severity*\ncritical"
        },
        {
          "type": "mrkdwn",
          "text": "*Stream*\ngame:events"
        },
        {
          "type": "mrkdwn",
          "text": "*Group*\ngame-workers"
        },
        {
          "type": "mrkdwn",
          "text": "*Pending*\n5"
        },
        {
          "type": "mrkdwn",
          "text": "*Max idle*\n6000ms"
        },
        {
          "type": "mrkdwn",
          "text": "*Max delivery*\n1"
        }
      ]
    },
    {
      "type": "section",
      "text": {
        "type": "mrkdwn",
        "text": "*Check*\n```XPENDING game:events game-workers - + 10```"
      }
    },
    {
      "type": "section",
      "text": {
        "type": "mrkdwn",
        "text": "*Recovery candidate*\n```XAUTOCLAIM game:events game-workers recovery-consumer 5000 0-0 COUNT 10```"
      }
    }
  ]
}
```

이번 Phase에서는 이 JSON을 실제 Slack으로 보내지 않는다.
출력 결과를 확인한 뒤 다음 단계에서 `HttpClient`로 Webhook 전송을 붙이면 된다.

---

## 실행 흐름

```text
1. FailureSimulationScenario가 메시지를 읽고 ACK하지 않는다.
2. Redis Consumer Group에 Pending 메시지가 생긴다.
3. PendingAlertScenario가 Pending 상태를 조회한다.
4. pendingCount, maxIdleMs, maxDeliveryCount를 계산한다.
5. 심각도를 정한다.
6. Slack 메시지 JSON을 출력한다.
```

이번 Phase의 핵심은 장애 보고서 양식을 만드는 것이 아니다.
Redis 상태를 Slack 알림 payload로 바꾸는 C# 출력 양식을 먼저 만드는 것이다.
