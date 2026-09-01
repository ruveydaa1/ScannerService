using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using ScannerService.Domain.Models;

namespace ScannerService.Infrastructure.Kafka;

public class KafkaScanResultProducer
{
    private readonly IConfiguration _configuration;

    public KafkaScanResultProducer(
        IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public async Task SendScanResultAsync(
        ScanResult scanResult)
    {
        var bootstrapServers =
            _configuration["Kafka:BootstrapServers"]
            ?? "localhost:9092";

        var topic =
            _configuration["Kafka:ResultTopic"]
            ?? "asset-scan-result";

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        var json =
            JsonSerializer.Serialize(scanResult);

        Console.WriteLine(
            $"Kafka result size: {json.Length} characters");

        Console.WriteLine(
            $"Findings count: {scanResult.Findings?.Count ?? 0}");

        using var producer =
            new ProducerBuilder<Null, string>(config)
                .Build();

        await producer.ProduceAsync(
            topic,
            new Message<Null, string>
            {
                Value = json
            });
    }
}