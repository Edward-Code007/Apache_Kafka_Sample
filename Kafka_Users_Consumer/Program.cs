using Kafka_Users_Consumer;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

builder.Services.Configure<OptConsumer>(builder.Configuration.GetSection("Consumer"));

var host = builder.Build();
host.Run();
