using Confluent.Kafka;

public interface IProducer<T>
{
    Task<ResultPattern<PersistenceStatus>> ProduceAsync(string topic, string key, T value);
}