using ScannerService.Domain.Models;

namespace ScannerService.Application.Interfaces;

public interface IScannerService
{
    Task<ScanResult> ScanAsync(
        ScanRequest request,
        Func<ScanProgress, Task>? onProgress = null);
}