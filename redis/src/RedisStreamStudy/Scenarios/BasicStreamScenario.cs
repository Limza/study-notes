using StackExchange.Redis;

namespace RedisStreamStudy.Scenarios;

public static class BasicStreamScenario
{
    public static async Task RunAsync(IDatabase database)
    {
        var streamKey = "game:events";

        // Stream에 메시지 추가
        for (var i = 0; i < 3; i++)
        {
            // StreamAddAsync 은 Redis의 XADD 명령어
            var messageId = await database.StreamAddAsync(
                streamKey,
                new NameValueEntry[]
                {
                    new("eventType", "match.completed"),
                    new("matchId", $"match-{i:000}"),
                    new("playerId", $"player-{i:000}"),
                    new("score", (1000 + i * 100).ToString())
                }
            );

            Console.WriteLine($"Added message with ID: {messageId}");
        }

        // StreamLengthAsync 는 Redis의 XLEN 명령어
        var streamLength = await database.StreamLengthAsync(streamKey);
        Console.WriteLine($"Stream length: {streamLength}");

        // StreamReadAsync 는 Redis의 XREAD 명령어
        var entries = await database.StreamReadAsync(
            streamKey,
            "0-0", // "0-0"은 스트림의 처음부터 읽겠다는 의미
            count: 10); // 최대 10개의 메시지를 읽음

        // 읽은 메세지 출력
        foreach (var entry in entries)
        {
            Console.WriteLine($"Message ID: {entry.Id}");
            
            foreach (var value in entry.Values)
            {
                Console.WriteLine($"  {value.Name}: {value.Value}");
            }
        }
    }
}
