using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Client.Cache;

namespace ApacheIgnite.Entity
{
    public class Subject
    {
        [QuerySqlField(IsIndexed = true)]
        public long Id { get; set; }

        [QuerySqlField]
        public string Name { get; set; }

        public string Descripion { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }

        public static CacheClientConfiguration GetSubjectCacheConfiguration()
        {
            return new CacheClientConfiguration
            {
                Name = "Subject",
                QueryEntities = new[]
                {
                    new QueryEntity
                    {
                        KeyType = typeof(long),
                        ValueType = typeof(Subject),
                        Fields = new[]
                        {
                            new QueryField("Id", typeof(long)),
                            new QueryField("Name", typeof(string))
                        },
                        Indexes = new[]
                        {
                            new QueryIndex("Id")
                        }
                    }
                },
                EnableStatistics = true,
                CacheMode = CacheMode.Replicated
            };
        }
    }
}
