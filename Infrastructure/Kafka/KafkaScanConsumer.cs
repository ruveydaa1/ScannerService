
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
    private readonly ITrivyClient _trivyClient;
    private readonly IScanResultSender _scanResultSender;
    private readonly KafkaScanProgressProducer _progressProducer;
    private readonly ILogger<KafkaScanConsumer> _logger;

    public KafkaScanConsumer(
        IConfiguration configuration,
        ISemgrepClient semgrepClient,
        ITrivyClient trivyClient,
        IScanResultSender scanResultSender,
        KafkaScanProgressProducer progressProducer,
        ILogger<KafkaScanConsumer> logger)
    {
        _configuration = configuration;
        _semgrepClient = semgrepClient;
        _trivyClient = trivyClient;
        _scanResultSender = scanResultSender;
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

                    if (string.IsNullOrWhiteSpace(result.Message.Value))
                        continue;

                    var scanRequest =
                        JsonSerializer.Deserialize<ScanRequest>(
                            result.Message.Value);

                    if (scanRequest == null)
                    {
                        _logger.LogWarning(
                            "Kafka message could not be deserialized.");
                        continue;
                    }

                    var scanner = scanRequest.Scanner.Trim();

                    _logger.LogInformation(
                        "Scan request received. AssetId: {AssetId}, ScanRunId: {ScanRunId}, Scanner: {Scanner}",
                        scanRequest.AssetId,
                        scanRequest.ScanRunId,
                        scanner);

                    await _progressProducer.SendProgressAsync(
                        new ScanProgress
                        {
                            AssetId = scanRequest.AssetId,
                            ScanRunId = scanRequest.ScanRunId,
                            Status = "Started",
                            Message = $"{scanner} scan started.",
                            Timestamp = DateTime.UtcNow
                        });

                    await _progressProducer.SendProgressAsync(
                        new ScanProgress
                        {
                            AssetId = scanRequest.AssetId,
                            ScanRunId = scanRequest.ScanRunId,
                            Status = "Scanning",
                            Message = $"{scanner} scan is running.",
                            Timestamp = DateTime.UtcNow
                        });

                    ScanResult scanResult;

                    try
                    {
                        if (scanner.Equals(
                                "Semgrep",
                                StringComparison.OrdinalIgnoreCase))
                        {
                            scanResult = await _semgrepClient.ScanAsync(
                                scanRequest,
                                stoppingToken);
                        }
                        else if (scanner.Equals(
                                     "Trivy",
                                     StringComparison.OrdinalIgnoreCase))
                        {
                            scanResult = await _trivyClient.ScanAsync(
                                scanRequest,
                                stoppingToken);
                        }
                        else
                        {
                            scanResult = new ScanResult
                            {
                                AssetId = scanRequest.AssetId,
                                ScanRunId = scanRequest.ScanRunId,
                                Scanner = scanner,
                                Source = scanRequest.Source,
                                Success = false,
                                Error = $"Desteklenmeyen tarayıcı: {scanner}"
                            };
                        }
                    }
                    catch (OperationCanceledException)
                        when (stoppingToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "{Scanner} servisine bağlanırken hata oluştu. AssetId: {AssetId}, ScanRunId: {ScanRunId}",
                            scanner,
                            scanRequest.AssetId,
                            scanRequest.ScanRunId);

                        scanResult = new ScanResult
                        {
                            AssetId = scanRequest.AssetId,
                            ScanRunId = scanRequest.ScanRunId,
                            Scanner = scanner,
                            Source = scanRequest.Source,
                            Success = false,
                            Error = $"{scanner} servisine ulaşılamadı: {ex.Message}"
                        };
                    }

                    await _scanResultSender.SendAsync(
    scanResult,
    stoppingToken);

                    _logger.LogInformation(
                        "Scan result sent to UserManagement. AssetId: {AssetId}, ScanRunId: {ScanRunId}, Scanner: {Scanner}, Success: {Success}",
                        scanResult.AssetId,
                        scanResult.ScanRunId,
                        scanResult.Scanner,
                        scanResult.Success);

                    await _progressProducer.SendProgressAsync(
                        new ScanProgress
                        {
                            AssetId = scanRequest.AssetId,
                            ScanRunId = scanRequest.ScanRunId,
                            Status = scanResult.Success ? "Completed" : "Failed",
                            Message = scanResult.Success
                                ? $"{scanner} scan completed. {scanResult.Findings.Count} finding(s) found."
                                : $"{scanner} scan failed: {scanResult.Error}",
                            Timestamp = DateTime.UtcNow
                        });

                    await _progressProducer.SendProgressAsync(
                        new ScanProgress
                        {
                            AssetId = scanRequest.AssetId,
                            ScanRunId = scanRequest.ScanRunId,
                            Status = "Finished",
                            Message = $"{scanner} scan finished.",
                            Timestamp = DateTime.UtcNow
                        });
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
