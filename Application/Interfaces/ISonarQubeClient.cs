using ScannerService.Domain.Models;

namespace ScannerService.Application.Interfaces;

public interface ISonarQubeClient
{
    Task<ScanResult> ScanAsync(
        ScanRequest request,
        CancellationToken cancellationToken = default);
}