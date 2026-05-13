public interface IProducer<T>
{
    Task ProduceAsync(string topic,string key,T value);
}