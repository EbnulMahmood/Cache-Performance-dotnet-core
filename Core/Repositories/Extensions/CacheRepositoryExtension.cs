using Microsoft.Extensions.DependencyInjection;

namespace Repositories.Extensions
{
    public static class CacheRepositoryExtension
    {
        public static void AddRepositories(this IServiceCollection services)
        {
            services.AddScoped<IStudentRepository, StudentRepository>();
        }
    }
}
