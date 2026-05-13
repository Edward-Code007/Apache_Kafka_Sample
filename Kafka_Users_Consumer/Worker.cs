using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Kafka_Users_Consumer;

public class Worker(ILogger<Worker> logger,IOptions<OptConsumer> optConsumer) : BackgroundService
{
    private OptConsumer _optConsumer = optConsumer.Value;
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
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
        .SetValueDeserializer(Deserializers.Utf8).Build();
        consumerInstance.Subscribe("users");

        StreamWriter writer = new StreamWriter("./data_users",true);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var data = consumerInstance.Consume(100);
                
                    if(data is not null)
                    {
                    var message = data.Message;
                   await writer.WriteLineAsync(message.Value);
                   logger.LogInformation(message.Value);     
                   logger.LogInformation("Escrito Correctamente");     
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
