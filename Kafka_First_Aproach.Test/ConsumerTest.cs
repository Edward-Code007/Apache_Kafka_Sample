using Xunit;
using Moq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Confluent.Kafka;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

public class ConsumerTests
{
    private readonly OptConsumer _opts = new OptConsumer
    {
        Bootstrap_Server = "localhost:9092",
        GroupId = "test-group",
        ClientId = "test-client"};
    [Fact]
    public void BuildConsumer_ReturnsValidInstance()
    {
        var logger = Mock.Of<ILogger<Consumer>>();
        var options = Options.Create(_opts);

        var consumer = new Consumer(logger, options);
        var instance = consumer.BuildConsumer();

        Assert.NotNull(instance);
        Assert.IsAssignableFrom<IConsumer<string, string>>(instance);
    }
    [Fact]
    public async Task TryWritetoDisk_WritesFileSuccessfully()
    {
        var logger = Mock.Of<ILogger<Consumer>>();
        var options = Options.Create(_opts);
        var consumer = new Consumer(logger, options);

        string path = "./test_output.txt";
        if (File.Exists(path)) File.Delete(path);

        var message = new Message<string, string> { Key = "k1", Value = "contenido-prueba" };

        await consumer.TryWritetoDisk(message, path, true);

        string contenido = File.ReadAllText(path);
        Assert.Contains("contenido-prueba", contenido);
    }
    [Fact]
    public async Task ConsumeData_ProcessesMessagesAndLogsInformation()
    {
        var mockLogger = new Mock<ILogger<Consumer>>();
        var options = Options.Create(_opts);

        var mockConsumer = new Mock<IConsumer<string, string>>();
        mockConsumer.Setup(c => c.Consume(It.IsAny<int>()))
            .Returns(new ConsumeResult<string, string>
            {
                Message = new Message<string, string> { Key = "k1", Value = "valor-test" }
            });

        var consumer = new Consumer(mockLogger.Object, options);

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(300); // cancelar rápido para no bloquear

        await consumer.ConsumeData(mockConsumer.Object, cts.Token);

        mockLogger.Verify(l => l.Log(
            It.Is<LogLevel>(lvl => lvl == LogLevel.Information),
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString().Contains("Write Success")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception, string>>()), Times.AtLeastOnce);
    }
}
