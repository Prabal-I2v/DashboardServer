using Dashboard.Models;
using Dashboard.QueryUtils;
using Microsoft.AspNetCore.Mvc;

namespace Dashboard.Controllers;

[ApiController]
[Route("[controller]")]
public class DashboardController : ControllerBase
{
    private readonly ILogger<DashboardController> _logger;

    public DashboardController(ILogger<DashboardController> logger)
    {
        _logger = logger;
    }
    [HttpGet]
    public IActionResult Get()
    {
        return Ok("Hello3");

    }
    [HttpPost]
    [Route("GetWidgetOutputModel")]
    public ChartsOutputModel GetWidgetOutputModel([FromBody] WidgetRequestModel requestModel)
    {
        return QueryManager.GetQueryData(requestModel);

    }
}