using ScannerService.Domain.Models;

namespace ScannerService.Application.Interfaces;

public interface ITrivyClient
{
    Task<ScanResult> ScanAsync(
        ScanRequest request,
        CancellationToken cancellationToken = default);
}