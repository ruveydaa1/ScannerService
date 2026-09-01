using System.Net.Http.Json;
using ScannerService.Application.Interfaces;
using ScannerService.Domain.Models;

namespace ScannerService.Infrastructure.Semgrep;

public class SemgrepClient : ISemgrepClient
{
    private readonly HttpClient _httpClient;
    private readonly IConfiguration _configuration;

    public SemgrepClient(
        HttpClient httpClient,
        IConfiguration configuration)
    {
        _httpClient = httpClient;
        _configuration = configuration;
    }

    public async Task<ScanResult> ScanAsync(
        ScanRequest request,
        CancellationToken cancellationToken = default)
    {
        var apiKey =
            _configuration["SemgrepService:InternalApiKey"];

        if (string.IsNullOrWhiteSpace(apiKey))
        {
            return new ScanResult
            {
                AssetId = request.AssetId,
                ScanRunId = request.ScanRunId,
                Success = false,
                Scanner = "Semgrep",
                Source = request.Source,
                Error = "SemgrepService API key yapılandırılmamış."
            };
        }

        _httpClient.DefaultRequestHeaders.Remove(
            "X-Internal-Api-Key");

        _httpClient.DefaultRequestHeaders.Add(
            "X-Internal-Api-Key",
            apiKey);

        var response = await _httpClient.PostAsJsonAsync(
            "/api/semgrep/scan",
            request,
            cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            return new ScanResult
            {
                AssetId = request.AssetId,
                ScanRunId = request.ScanRunId,
                Success = false,
                Scanner = "Semgrep",
                Source = request.Source,
                Error =
                    $"SemgrepService hata döndürdü. HTTP Status: {(int)response.StatusCode}"
            };
        }

        var result =
            await response.Content.ReadFromJsonAsync<ScanResult>(
                cancellationToken: cancellationToken);

        if (result == null)
        {
            return new ScanResult
            {
                AssetId = request.AssetId,
                ScanRunId = request.ScanRunId,
                Success = false,
                Scanner = "Semgrep",
                Source = request.Source,
                Error =
                    "SemgrepService boş veya geçersiz sonuç döndürdü."
            };
        }

        return result;
    }
}