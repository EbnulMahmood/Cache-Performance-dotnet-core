using Microsoft.Extensions.DependencyInjection;

namespace Model.Extensions
{
    public static class DapperExtension
    {
        public static void AddDapperRepositories(this IServiceCollection services)
        {
            services.AddSingleton<IDapperDataAccess, DapperDataAccess>();
        }
    }
}
