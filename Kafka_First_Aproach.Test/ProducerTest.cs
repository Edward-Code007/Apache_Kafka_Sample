using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace Kafka_First_Aproach.Test;

public class ProducerTest
{
    private readonly ProducerOpts _opts = new ProducerOpts
    {
        Acks = AckKafka.Leader,
        Bootstrap_servers = "localhost:9092",
        ClientId = "test-client"
    };
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
    [Fact]
    public async Task ProduceMessage_ShouldCallProduceAync()
    {
        var logger = Mock.Of<ILogger<Producer<string>>>();
        var options = Options.Create(_opts);

        var mockProducer = new Mock<IProducer<string, string>>();
        mockProducer
            .Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<Message<string, string>>()))
            .ReturnsAsync(new DeliveryResult<string, string>());

        var producer = new Producer<string>(logger, options);

        await producer.ProduceMessage(mockProducer.Object, "topic-test", "key1", "valor");

        mockProducer.Verify(p => p.ProduceAsync(
            "topic-test",
            It.Is<Message<string, string>>(m => m.Key == "key1" && m.Value == JsonSerializer.Serialize("valor"))
        ), Times.Once);
    }
    [Fact]
    public async Task ProduceAsync_ShouldCreateProducerNCallProduceMessage()
    {
        var logger = Mock.Of<ILogger<Producer<string>>>();
        var options = Options.Create(_opts);

        var producer = new Producer<string>(logger, options);
        //Validar que no se lance excepcion
        await producer.ProduceAsync("topic-test", "key1", "valor");
        Assert.True(true);
    }

}