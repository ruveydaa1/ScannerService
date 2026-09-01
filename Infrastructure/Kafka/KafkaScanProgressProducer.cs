using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using ScannerService.Domain.Models;

namespace ScannerService.Infrastructure.Kafka;

public class KafkaScanProgressProducer
{
    private readonly IConfiguration _configuration;

    public KafkaScanProgressProducer(
        IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public async Task SendProgressAsync(
        ScanProgress progress)
    {
        var bootstrapServers =
            _configuration["Kafka:BootstrapServers"]
            ?? "localhost:9092";

        var topic =
            _configuration["Kafka:ProgressTopic"]
            ?? "asset-scan-progress";

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        var json =
            JsonSerializer.Serialize(progress);

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