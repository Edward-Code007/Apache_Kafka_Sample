using Kafka_Users_Consumer;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.Configure<OptConsumer>(builder.Configuration.GetSection("Consumer"));
builder.Services.AddHostedService<Worker>();
builder.Services.AddScoped<IConsumer,Consumer>();


var host = builder.Build();
host.Run();
