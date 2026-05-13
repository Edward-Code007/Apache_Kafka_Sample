public interface IConsumer<T>
{
    public Task ConsumeAsync(string topic);
}