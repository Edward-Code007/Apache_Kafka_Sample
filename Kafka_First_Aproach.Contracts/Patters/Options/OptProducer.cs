using Confluent.Kafka;

public enum AckKafka
{
    Leader = Acks.Leader,
    None = Acks.None,
    All = Acks.All
}

public class ProducerOpts
{
    public string Bootstrap_servers{get;set;}
    public AckKafka Acks{get;set;}
    public string ClientId{get;set;}
    public string GroupId{get;set;}

}
public class ServerOpt
{
    public string Hostname{get;set;}
    public string Port{get;set;}
}