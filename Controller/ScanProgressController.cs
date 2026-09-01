using Microsoft.AspNetCore.Mvc;
using ScannerService.Infrastructure.Kafka;
using ScannerService.Domain.Models;

namespace ScannerService.Controllers;

[ApiController]
[Route("api/scan-progress")]
public class ScanProgressController : ControllerBase
{
    private readonly KafkaScanProgressProducer _progressProducer;

    public ScanProgressController(
        KafkaScanProgressProducer progressProducer)
    {
        _progressProducer = progressProducer;
    }

    [HttpPost]
    public async Task<IActionResult> SendProgress(
        [FromBody] ScanProgress progress)
    {
        await _progressProducer.SendProgressAsync(progress);

        return Ok();
    }
}