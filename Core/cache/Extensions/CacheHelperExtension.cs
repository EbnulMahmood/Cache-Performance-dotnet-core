using Hazelcast;
using Microsoft.Extensions.DependencyInjection;

namespace Cache.Extensions
{
    public static class CacheHelperExtension
    {
        public static void AddCacheService(this IServiceCollection services, HazelcastOptions hazelcastOptions)
        {
            services.AddSingleton<IHazelcastStudentHelper, HazelcastStudentHelper>(_ => new HazelcastStudentHelper(hazelcastOptions));
            services.AddSingleton<ICouchbaseStudentHelper, CouchbaseStudentHelper>();
        }
    }
}
