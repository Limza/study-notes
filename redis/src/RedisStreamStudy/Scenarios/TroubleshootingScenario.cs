using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class TroubleshootingScenario
{
    public static async Task RunAsync(
        IDatabase database,
        string streamKey,
        string groupName)
    {
        // 1단계: Stream에 메시지가 실제로 쌓여 있는지 확인한다.
        // StreamLengthAsync는 Redis의 XLEN 명령에 해당한다.
        // 값이 0이면 Producer가 메시지를 넣지 못했거나 다른 Stream key를 보고 있을 수 있다.
        var length = await database.StreamLengthAsync(streamKey);
        Console.WriteLine($"Stream length: {length}");

        // 2단계: Stream에 어떤 Consumer Group이 붙어 있는지 확인한다.
        // StackExchange.Redis에 전용 고수준 API가 애매한 명령은 ExecuteAsync로 직접 보낼 수 있다.
        // XINFO GROUPS는 Group 이름, Consumer 수, Pending 수, 마지막 전달 ID 등을 보여준다.
        // 여기서 groupName이 안 보이면 Consumer Group 생성이 안 된 것이다.
        var groupsInfo = await database.ExecuteAsync("XINFO", "GROUPS", streamKey);
        Console.WriteLine($"Groups info: {groupsInfo}");

        // 3단계: Group 안의 Consumer별 상태를 확인한다.
        // XINFO CONSUMERS는 각 Consumer가 가진 Pending 수와 idle 시간을 보여준다.
        // idle 시간이 길고 Pending이 많으면 해당 Consumer가 멈췄을 가능성을 의심한다.
        var consumersInfo = await database.ExecuteAsync("XINFO", "CONSUMERS", streamKey, groupName);
        Console.WriteLine($"Consumers info: {consumersInfo}");

        // 4단계: Group 전체 Pending 상태를 요약해서 확인한다.
        // XPENDING stream group 형태는 Pending 개수와 Pending message id 범위를 보여준다.
        // Pending이 0이면 현재 ACK되지 않은 메시지가 없다는 뜻이다.
        var pendingSummary = await database.ExecuteAsync("XPENDING", streamKey, groupName);
        Console.WriteLine($"Pending summary: {pendingSummary}");

        // 5단계: Pending 메시지를 하나씩 상세 조회한다.
        // "-"는 가장 오래된 Pending message id부터 보겠다는 뜻이다.
        // "+"는 가장 최신 Pending message id까지 보겠다는 뜻이다.
        // "10"은 그 범위 안에서 오래된 순서로 최대 10개만 가져오겠다는 뜻이다.
        // 결과에서는 message id, owner consumer, idle time, delivery count를 확인한다.
        var pendingDetails = await database.ExecuteAsync("XPENDING", streamKey, groupName, "-", "+", "10");
        Console.WriteLine($"Pending details: {pendingDetails}");
    }
}
