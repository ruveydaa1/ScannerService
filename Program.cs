using ScannerService.Application.Interfaces;
using ScannerService.Infrastructure.Kafka;
using ScannerService.Infrastructure.Semgrep;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.AddControllers();

// HttpClient
builder.Services.AddHttpClient<ISemgrepClient, SemgrepClient>(client =>
{
    client.BaseAddress = new Uri("http://localhost:5278");
    client.Timeout = TimeSpan.FromMinutes(10);
});

// Infrastructure services
builder.Services.AddSingleton<KafkaScanResultProducer>();

builder.Services.AddSingleton<KafkaScanProgressProducer>();

// Kafka consumer
builder.Services.AddHostedService<KafkaScanConsumer>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapControllers();

app.Run();