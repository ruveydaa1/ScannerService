using ScannerService.Application.Interfaces;
using ScannerService.Infrastructure.Kafka;
using ScannerService.Infrastructure.Semgrep;
using ScannerService.Infrastructure.Http;
using ScannerService.Infrastructure.Trivy;
using ScannerService.Infrastructure.Gitleaks;
using ScannerService.Infrastructure.SonarQube;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddOpenApi();

builder.Services.AddControllers();

// Semgrep HttpClient
builder.Services.AddHttpClient<ISemgrepClient, SemgrepClient>(client =>
{
    client.BaseAddress = new Uri("http://localhost:5278");
    client.Timeout = TimeSpan.FromMinutes(10);
});

// Trivy HttpClient
builder.Services.AddHttpClient<ITrivyClient, TrivyClient>(client =>
{
    client.BaseAddress = new Uri("http://localhost:5279");
    client.Timeout = TimeSpan.FromMinutes(10);
});

// Gitleaks HttpClient
builder.Services.AddHttpClient<IGitleaksClient, GitleaksClient>(client =>
{
    client.BaseAddress = new Uri("http://localhost:5280");
    client.Timeout = TimeSpan.FromMinutes(10);
});

// SonarQube HttpClient
builder.Services.AddHttpClient<ISonarQubeClient, SonarQubeClient>(client =>
{
    client.BaseAddress = new Uri("http://localhost:5264");
    client.Timeout = TimeSpan.FromMinutes(15);
});

// Scan result sender HttpClient
builder.Services.AddHttpClient<IScanResultSender, ScanResultSender>(client =>
{
    client.BaseAddress = new Uri("http://localhost:5245");
    client.Timeout = TimeSpan.FromMinutes(10);
});

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