using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ScannerService.Application.Interfaces;
using ScannerService.Domain.Models;

namespace ScannerService.Infrastructure.Kafka;

public class KafkaScanConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ISemgrepClient _semgrepClient;
    private readonly KafkaScanResultProducer _resultProducer;
    private readonly KafkaScanProgressProducer _progressProducer;
    private readonly ILogger<KafkaScanConsumer> _logger;

    public KafkaScanConsumer(
        IConfiguration configuration,
        ISemgrepClient semgrepClient,
        KafkaScanResultProducer resultProducer,
        KafkaScanProgressProducer progressProducer,
        ILogger<KafkaScanConsumer> logger)
    {
        _configuration = configuration;
        _semgrepClient = semgrepClient;
        _resultProducer = resultProducer;
        _progressProducer = progressProducer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var bootstrapServers =
            _configuration["Kafka:BootstrapServers"]
            ?? "localhost:9092";

        var topic =
            _configuration["Kafka:ScanTopic"]
            ?? "asset-scan";

        var groupId =
            _configuration["Kafka:ConsumerGroup"]
            ?? "scanner-service-workers";

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer =
            new ConsumerBuilder<Ignore, string>(config)
                .Build();

        consumer.Subscribe(topic);

        _logger.LogInformation(
            "Kafka scan consumer started. Topic: {Topic}",
            topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result =
                        consumer.Consume(stoppingToken);

                    if (string.IsNullOrWhiteSpace(
                            result.Message.Value))
                    {
                        continue;
                    }

                    var scanRequest =
                        JsonSerializer.Deserialize<ScanRequest>(
                            result.Message.Value);

                    if (scanRequest == null)
                    {
                        _logger.LogWarning(
                            "Kafka message could not be deserialized.");

                        continue;
                    }

                    _logger.LogInformation(
                        "Scan request received. AssetId: {AssetId}, ScanRunId: {ScanRunId}",
                        scanRequest.AssetId,
                        scanRequest.ScanRunId);

                    // 1. Scan başladı
                    await _progressProducer.SendProgressAsync(
                        new ScanProgress
                        {
                            AssetId = scanRequest.AssetId,
                            ScanRunId = scanRequest.ScanRunId,
                            Status = "Started",
                            Message = "Scan started.",
                            Timestamp = DateTime.UtcNow
                        });

                    _logger.LogInformation(
                        "Scan progress sent: Scan started. AssetId: {AssetId}, ScanRunId: {ScanRunId}",
                        scanRequest.AssetId,
                        scanRequest.ScanRunId);

                    // 2. Semgrep taraması başlıyor
                    await _progressProducer.SendProgressAsync(
                        new ScanProgress
                        {
                            AssetId = scanRequest.AssetId,
                            ScanRunId = scanRequest.ScanRunId,
                            Status = "Scanning",
                            Message = "Semgrep scan started.",
                            Timestamp = DateTime.UtcNow
                        });

                    _logger.LogInformation(
                        "Scan progress sent: Semgrep scan started. AssetId: {AssetId}, ScanRunId: {ScanRunId}",
                        scanRequest.AssetId,
                        scanRequest.ScanRunId);

                    // 3. Semgrep taramasını çalıştır
                    var scanResult =
                        await _semgrepClient.ScanAsync(
                            scanRequest,
                            stoppingToken);

                    // 4. Semgrep tamamlandı
                    await _progressProducer.SendProgressAsync(
                        new ScanProgress
                        {
                            AssetId = scanRequest.AssetId,
                            ScanRunId = scanRequest.ScanRunId,
                            Status = "Completed",
                            Message =
                                $"Semgrep scan completed. {scanResult.Findings.Count} finding(s) found.",
                            Timestamp = DateTime.UtcNow
                        });

                    _logger.LogInformation(
                        "Scan progress sent: Semgrep scan completed. AssetId: {AssetId}, ScanRunId: {ScanRunId}, Findings: {FindingsCount}",
                        scanRequest.AssetId,
                        scanRequest.ScanRunId,
                        scanResult.Findings.Count);

                    // 5. Scan sonucunu UserManagement'e gönder
                    await _resultProducer
                        .SendScanResultAsync(scanResult);

                    _logger.LogInformation(
                        "Scan result sent to Kafka. AssetId: {AssetId}, ScanRunId: {ScanRunId}, Success: {Success}",
                        scanResult.AssetId,
                        scanResult.ScanRunId,
                        scanResult.Success);

                    // 6. Scan tamamlandı
                    await _progressProducer.SendProgressAsync(
                        new ScanProgress
                        {
                            AssetId = scanRequest.AssetId,
                            ScanRunId = scanRequest.ScanRunId,
                            Status = "Finished",
                            Message = "Scan completed.",
                            Timestamp = DateTime.UtcNow
                        });

                    _logger.LogInformation(
                        "Scan progress sent: Scan completed. AssetId: {AssetId}, ScanRunId: {ScanRunId}",
                        scanRequest.AssetId,
                        scanRequest.ScanRunId);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(
                        ex,
                        "An error occurred while consuming the Kafka message.");
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "An unexpected error occurred during the scan.");
                }
            }
        }
        finally
        {
            consumer.Close();

            _logger.LogInformation(
                "Kafka scan consumer stopped.");
        }
    }
}