using Microsoft.Extensions.DependencyInjection;
using Model.Extensions;
using Repositories.Extensions;

namespace Services.Extensions
{
    public static class CacheServiceExtension
    {
        public static void AddServices(this IServiceCollection services)
        {
            services.AddRepositories();
            services.AddDapperRepositories();
            services.AddScoped<IStudentService, StudentService>();
            services.AddScoped<IStudentServiceV2, StudentServiceV2>();
        }
    }
}
