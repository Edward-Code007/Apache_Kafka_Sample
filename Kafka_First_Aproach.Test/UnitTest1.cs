using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace Kafka_First_Aproach.Test;

public class ProducerTest
{
    [Fact]
    public void BuildProducer_ShouldReturnProducerInstance()
    {
         // Arrange: mock de ILogger
        var mockLogger = new Mock<ILogger<Producer<string>>>();

        // Arrange: mock de IOptions<ProducerOpts>
        var producerOpts = new ProducerOpts
        {
            Acks = AckKafka.Leader,
            Bootstrap_servers = "localhost:9092",
            ClientId = "test-client"
        };
        var mockOptions = new Mock<IOptions<ProducerOpts>>();
        mockOptions.Setup(o => o.Value).Returns(producerOpts);

        var producer = new Producer<string>(mockLogger.Object, mockOptions.Object);

        // Act
        var result = producer.BuildProducer();

        // Assert
        Assert.NotNull(result);
        Assert.IsAssignableFrom<Confluent.Kafka.IProducer<string, string>>(result);
    }


    }

