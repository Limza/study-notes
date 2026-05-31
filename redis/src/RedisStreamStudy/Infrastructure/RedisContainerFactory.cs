using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace RedisStreamStudy.Infrastructure;

public static class RedisContainerFactory
{
    public static IContainer Create()
    {
        return new ContainerBuilder("redis:latest")
            .WithPortBinding(6379, true)
            .WithCleanUp(true)
            .Build();
    }
}
