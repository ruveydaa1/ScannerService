using ScannerService.Domain.Models;

namespace ScannerService.Application.Interfaces;

public interface IScanResultSender
{
    Task SendAsync(
        ScanResult scanResult,
        CancellationToken cancellationToken);
}