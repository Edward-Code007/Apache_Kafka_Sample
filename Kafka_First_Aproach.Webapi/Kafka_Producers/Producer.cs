using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

public class Producer<T> : IProducer<T>
{
    private ProducerOpts _producerConfig { init; get; }
    private ILogger<Producer<T>> _logger { get; set; }
    public Producer(ILogger<Producer<T>> logger, IOptions<ProducerOpts> producerConfig)
    {
        this._producerConfig = producerConfig.Value;
        this._logger = logger;
    }
    public async Task ProduceAsync(string topic, string key, T value)
    {
        var producerInstance = BuildProducer();
        await StartProducer(producerInstance, topic, key, value);
    }
    public IProducer<string, string> BuildProducer()
    {
        var prodConfig = new ProducerConfig
        {
            Acks = (Acks)_producerConfig.Acks,
            BootstrapServers = _producerConfig.Bootstrap_servers,
            ClientId = _producerConfig.ClientId,
            BatchSize = 32000,
            Partitioner = Partitioner.Random
        };

        var producerInstance =
        new ProducerBuilder<string, string>(prodConfig)
        .SetKeySerializer(Serializers.Utf8)
        .SetValueSerializer(Serializers.Utf8)
        .Build();
        return producerInstance;
    }
    public async Task StartProducer(IProducer<string, string> producerInstance, string topic, string key, T value)
    {
        try
        {
            await producerInstance.ProduceAsync(topic, new Message<string, string>
            {
                Key = key,
                Value = JsonSerializer.Serialize(value)
            });
        }
        catch (Exception exc)
        {
            this._logger.LogError($"Error Fatal: {exc.Message}");
            throw;
        }
    }

}