public interface IConsumer
{
    public Task ConsumeAsync(string topic,CancellationToken stoppingToken);
}