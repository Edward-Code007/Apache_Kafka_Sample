using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.Configure<ProducerOpts>(
    builder.Configuration.GetSection("Producer"));
builder.Services.AddScoped(typeof(IProducer<>),typeof(Producer<>));

var app = builder.Build();


// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapPost("/user",async (CreateUserDTO user,IProducer<CreateUser> producer) =>
{
    CreateUser userNew = new CreateUser(Guid.CreateVersion7().ToString(),user.name,user.lastname);
   await producer.ProduceAsync("users",userNew.UUID,userNew);
   return Results.Ok("Usuario Siendo Procesado");
});


app.Lifetime.ApplicationStarted.Register(() =>
{
    var server = app.Services.GetRequiredService<IServer>();
    var logger = app.Services.GetRequiredService<ILogger<Program>>();
    var addresses = server.Features.Get<IServerAddressesFeature>();

    foreach (var address in addresses.Addresses)
    {
        logger.LogInformation($"Escucnahdo en {address}");
    }
});

app.Run();

