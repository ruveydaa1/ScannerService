using System.Net.Http.Json;
using ScannerService.Application.Interfaces;
using ScannerService.Domain.Models;

namespace ScannerService.Infrastructure.Http;

public class ScanResultSender : IScanResultSender
{
    private readonly HttpClient _httpClient;

    public ScanResultSender(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task SendAsync(
        ScanResult scanResult,
        CancellationToken cancellationToken)
    {
        var response =
            await _httpClient.PostAsJsonAsync(
                "api/scan-runs/result",
                scanResult,
                cancellationToken);

        response.EnsureSuccessStatusCode();
    }
}