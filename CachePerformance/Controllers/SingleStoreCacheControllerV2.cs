using Microsoft.AspNetCore.Mvc;

namespace CachePerformance.Controllers
{
    public class SingleStoreCacheControllerV2 : Controller
    {
        public IActionResult Index()
        {
            return View();
        }
    }
}
