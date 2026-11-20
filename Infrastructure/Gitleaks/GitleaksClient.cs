
using System.Net.Http.Json;
using Microsoft.Extensions.Configuration;
using ScannerService.Application.Interfaces;
using ScannerService.Domain.Models;

namespace ScannerService.Infrastructure.Gitleaks;

public class GitleaksClient : IGitleaksClient
{
    private readonly HttpClient _httpClient;
    private readonly IConfiguration _configuration;

    public GitleaksClient(
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
        var apiKey = _configuration["GitleaksService:InternalApiKey"];

        if (string.IsNullOrWhiteSpace(apiKey))
        {
            throw new InvalidOperationException(
                "GitleaksService API key is not configured.");
        }

        using var httpRequest = new HttpRequestMessage(
            HttpMethod.Post,
            "/api/gitleaks/scan");

        httpRequest.Headers.Add("X-Internal-Api-Key", apiKey);
        httpRequest.Content = JsonContent.Create(request);

        using var response = await _httpClient.SendAsync(
            httpRequest,
            cancellationToken);

        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<ScanResult>(
            cancellationToken: cancellationToken);

        return result ?? throw new InvalidOperationException(
            "GitleaksService returned an empty response.");
    }
}
