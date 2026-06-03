using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class BasicStreamScenario
{
    public static async Task RunAsync(IDatabase database)
    {
        // 메시지를 저장할 Redis Stream key를 정한다.
        // 이 key 하나 안에 여러 message id가 시간 순서대로 쌓인다.
        var streamKey = "game:events";

        // 같은 Stream에 테스트 메시지 3개를 차례대로 추가한다.
        // 반복문 한 바퀴가 Redis Stream 메시지 한 건에 해당한다.
        for (var i = 1; i <= 3; i++)
        {
            // StreamAddAsync는 Redis의 XADD 명령에 해당한다.
            // 첫 번째 인자는 메시지를 넣을 Stream key다.
            // 두 번째 인자는 하나의 message id에 함께 저장할 field-value 목록이다.
            var messageId = await database.StreamAddAsync(
                streamKey,
                new NameValueEntry[]
                {
                    // eventType, matchId, playerId, score는 각각 Redis key가 아니다.
                    // game:events Stream 안에 들어가는 메시지 한 건의 field 이름이다.
                    new("eventType", "match.completed"),
                    new("matchId", $"match-{i:000}"),
                    new("playerId", $"player-{i:000}"),
                    new("score", (1000 + i * 100).ToString())
                }
            );

            Console.WriteLine($"Added message with ID: {messageId}");
        }

        // StreamLengthAsync는 Redis의 XLEN 명령에 해당한다.
        // Stream key 안에 쌓인 전체 메시지 개수를 확인한다.
        var streamLength = await database.StreamLengthAsync(streamKey);
        Console.WriteLine($"Stream length: {streamLength}");

        // StreamReadAsync는 Redis의 XREAD 명령에 해당한다.
        // "0-0"은 Stream의 가장 처음 message id부터 읽겠다는 뜻이다.
        // count: 10은 최대 10개까지만 읽겠다는 뜻이다.
        var entries = await database.StreamReadAsync(
            streamKey,
            "0-0",
            count: 10);

        // 읽어 온 각 Stream 메시지를 출력한다.
        // entry.Id는 Redis가 붙인 message id이고, entry.Values는 field-value 목록이다.
        foreach (var entry in entries)
        {
            Console.WriteLine($"Message ID: {entry.Id}");
            
            foreach (var value in entry.Values)
            {
                // value.Name은 field 이름이고, value.Value는 field 값이다.
                Console.WriteLine($"  {value.Name}: {value.Value}");
            }
        }
    }
}
