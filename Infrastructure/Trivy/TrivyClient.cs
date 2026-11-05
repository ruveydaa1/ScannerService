
using System.Net.Http.Json;
using ScannerService.Application.Interfaces;
using ScannerService.Domain.Models;

namespace ScannerService.Infrastructure.Trivy;

public class TrivyClient : ITrivyClient
{
    private readonly HttpClient _httpClient;
    private readonly IConfiguration _configuration;

    public TrivyClient(
    HttpClient httpClient,
    IConfiguration configuration)
    {
        _httpClient = httpClient;
        _configuration = configuration;

        Console.WriteLine(
            $"Trivy BaseAddress: {_httpClient.BaseAddress}");
    }

    public async Task<ScanResult> ScanAsync(
        ScanRequest request,
        CancellationToken cancellationToken = default)
    {
        var apiKey =
            _configuration["TrivyService:InternalApiKey"];

        if (string.IsNullOrWhiteSpace(apiKey))
        {
            return new ScanResult
            {
                AssetId = request.AssetId,
                ScanRunId = request.ScanRunId,
                Success = false,
                Scanner = "Trivy",
                Source = request.Source,
                Error = "TrivyService API key yapılandırılmamış."
            };
        }

        _httpClient.DefaultRequestHeaders.Remove(
            "X-Internal-Api-Key");

        _httpClient.DefaultRequestHeaders.Add(
            "X-Internal-Api-Key",
            apiKey);

        var response = await _httpClient.PostAsJsonAsync(
            "/api/trivy/scan",
            request,
            cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            return new ScanResult
            {
                AssetId = request.AssetId,
                ScanRunId = request.ScanRunId,
                Success = false,
                Scanner = "Trivy",
                Source = request.Source,
                Error =
                    $"TrivyService hata döndürdü. HTTP Status: {(int)response.StatusCode}"
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
                Scanner = "Trivy",
                Source = request.Source,
                Error =
                    "TrivyService boş veya geçersiz sonuç döndürdü."
            };
        }

        return result;
    }
}
