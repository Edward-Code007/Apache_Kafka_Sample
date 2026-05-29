using Confluent.Kafka;
using Microsoft.Extensions.Options;

public class Consumer(ILogger<Consumer> _logger,IOptions<OptConsumer> optConsumer) : IConsumer
{
    public async Task ConsumeAsync(string topic,CancellationToken stoppingToken)
    {
       var consumerInstance = BuildConsumer();
        consumerInstance.Subscribe(topic);

        StreamWriter writer = new StreamWriter("./data_users",true);
        ConsumeData(consumerInstance,writer,stoppingToken);
    }
    private Confluent.Kafka.IConsumer<string,string> BuildConsumer()
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
    private async void ConsumeData(Confluent.Kafka.IConsumer<string,string> consumerInstance,StreamWriter writer,CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var data = consumerInstance.Consume(100);
                    if(data is not null)
                    {
                    var message = data.Message;
                   await writer.WriteLineAsync(message.Value);
                   _logger.LogInformation(message.Value);     
                   _logger.LogInformation("Escrito Correctamente");     
                    }
            }

        }
        finally
        {
            writer.Close();
            writer.Dispose();
            consumerInstance.Close();
            
        }
    }
}