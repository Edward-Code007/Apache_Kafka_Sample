using Confluent.Kafka;
using Microsoft.Extensions.Options;

public class Consumer(ILogger<Consumer> _logger, IOptions<OptConsumer> optConsumer) : IConsumer
{
    public async Task ConsumeAsync(string topic, CancellationToken stoppingToken)
    {
        var consumerInstance = BuildConsumer();
        consumerInstance.Subscribe(topic);

        await ConsumeData(consumerInstance,  stoppingToken);
    }
    public Confluent.Kafka.IConsumer<string, string> BuildConsumer()
    {
        var _optConsumer = optConsumer.Value;
        var consumerConfig = new ConsumerConfig
        {
            AutoOffsetReset = AutoOffsetReset.Latest,
            BootstrapServers = _optConsumer.Bootstrap_Server,
            GroupId = _optConsumer.GroupId,
            ClientId = _optConsumer.ClientId,
            AllowAutoCreateTopics = true,
        };
        var consumerInstance = new ConsumerBuilder<string, string>(consumerConfig)
        .SetKeyDeserializer(Deserializers.Utf8)
        .SetValueDeserializer(Deserializers.Utf8)
        .Build();
        return consumerInstance;
    }
    public async Task ConsumeData(Confluent.Kafka.IConsumer<string, string> consumerInstance,CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var data = consumerInstance.Consume(150);
                if (data is not null)
                {
                    await TryWritetoDisk(data.Message,"./data_users",true);
                    _logger.LogInformation("Write Success: " + data.Message);
                }
            }
        }
        finally
        {
            consumerInstance.Close();
        }
    }
    public async Task TryWritetoDisk(Message<string, string> message, string path, bool append)
    {
       using StreamWriter writer = new StreamWriter(path, append);
       await writer.WriteLineAsync(message.Value);      
    }
}