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
