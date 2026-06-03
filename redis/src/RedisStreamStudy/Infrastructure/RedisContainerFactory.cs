using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace RedisStreamStudy.Infrastructure;

public static class RedisContainerFactory
{
    private const int RedisContainerPort = 6379;

    public static IContainer Create(int hostPort)
    {
        return new ContainerBuilder("redis:latest")
            .WithPortBinding(hostPort, RedisContainerPort)
            .WithCleanUp(true)
            .Build();
    }
}
