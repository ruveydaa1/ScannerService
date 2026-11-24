using System.Net.Http.Json;
using Microsoft.Extensions.Configuration;
using ScannerService.Application.Interfaces;
using ScannerService.Domain.Models;

namespace ScannerService.Infrastructure.SonarQube;

public class SonarQubeClient : ISonarQubeClient
{
    private readonly HttpClient _httpClient;
    private readonly IConfiguration _configuration;

    public SonarQubeClient(
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
        var apiKey = _configuration["SonarQubeService:InternalApiKey"];

        if (string.IsNullOrWhiteSpace(apiKey))
        {
            throw new InvalidOperationException(
                "SonarQubeService API key is not configured.");
        }

        using var httpRequest = new HttpRequestMessage(
            HttpMethod.Post,
            "/api/sonarqube/scan");

        httpRequest.Headers.Add("X-Internal-Api-Key", apiKey);
        httpRequest.Content = JsonContent.Create(request);

        using var response = await _httpClient.SendAsync(
            httpRequest,
            cancellationToken);

        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<ScanResult>(
            cancellationToken: cancellationToken);

        return result ?? throw new InvalidOperationException(
            "SonarQubeService returned an empty response.");
    }
}