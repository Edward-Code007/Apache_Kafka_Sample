using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Kafka_Users_Consumer;

public class Worker(IConsumer consumer) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
       await consumer.ConsumeAsync("users",stoppingToken);
    }
}
