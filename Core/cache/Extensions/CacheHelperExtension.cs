using Hazelcast;
using Microsoft.Extensions.DependencyInjection;

namespace Cache.Extensions
{
    public static class CacheHelperExtension
    {
        public static void AddCacheService(this IServiceCollection services, HazelcastOptions hazelcastOptions)
        {
            services.AddSingleton<IStudentCacheHelper, StudentCacheHelper>(_ => new StudentCacheHelper(hazelcastOptions));
        }
    }
}
