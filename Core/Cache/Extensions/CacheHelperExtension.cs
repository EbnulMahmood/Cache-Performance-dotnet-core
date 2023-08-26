using Cache.ApacheIgnite;
using Hazelcast;
using Microsoft.Extensions.DependencyInjection;

namespace Cache.Extensions
{
    public static class CacheHelperExtension
    {
        public static void AddCacheService(this IServiceCollection services, HazelcastOptions hazelcastOptions)
        {
            services.AddSingleton<IHazelcastStudentHelper, HazelcastStudentHelper>(_ => new HazelcastStudentHelper(hazelcastOptions));
            services.AddSingleton<IHazelcastStudentHelperV1, HazelcastStudentHelperV1>(_ => new HazelcastStudentHelperV1(hazelcastOptions));
            services.AddSingleton<IHazelcastStudentHelperV2, HazelcastStudentHelperV2>(_ => new HazelcastStudentHelperV2(hazelcastOptions));
            services.AddSingleton<ICouchbaseStudentHelper, CouchbaseStudentHelper>();
            services.AddSingleton<ISingleStoreHelper, SingleStoreHelper>();
            services.AddSingleton<ISingleStoreHelperV2, SingleStoreHelperV2>();
        }

        public static void AddIgniteCacheService(this IServiceCollection services, params string[] endPoints)
        {
            services.AddSingleton<IClient>(new IgniteConnection(endPoints));
            services.AddScoped<IIgniteCacheServicce, IgniteCacheService>();
            services.AddScoped<IIgniteStudentCacheHelper, IgniteStudentCacheHelper>();
        }
    }
}
