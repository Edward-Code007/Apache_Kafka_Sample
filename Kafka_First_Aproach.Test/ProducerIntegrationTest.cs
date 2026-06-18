using DotNet.Testcontainers.Builders;
using Microsoft.AspNetCore.Mvc.Testing;
using Testcontainers.Kafka;


public class ProducerIntegrationTest
{
    [Fact]
    public void HappyPathUserCreate_ShouldReturnResultOK()
    {
       var kafkaConfig = new KafkaConfiguration(KafkaVendor.Confluent);
       var kafkaContainer = new KafkaContainer(kafkaConfig);
    }
}