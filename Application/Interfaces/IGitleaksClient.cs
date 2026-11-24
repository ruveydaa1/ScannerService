
using ScannerService.Domain.Models;

namespace ScannerService.Application.Interfaces;

public interface IGitleaksClient
{
    Task<ScanResult> ScanAsync(
        ScanRequest request,
        CancellationToken cancellationToken = default);
}
