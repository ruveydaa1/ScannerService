using ScannerService.Domain.Models;

namespace ScannerService.Application.Interfaces;

public interface ISemgrepClient
{
    Task<ScanResult> ScanAsync(
        ScanRequest request,
        CancellationToken cancellationToken = default);
}