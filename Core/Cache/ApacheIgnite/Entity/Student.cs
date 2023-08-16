using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Client.Cache;

namespace ApacheIgnite.Entity
{
    public class Student
    {
        [QuerySqlField(IsIndexed = true)]
        public long Id { get; set; }

        [QuerySqlField]
        public string Name { get; set; }

        [QuerySqlField]
        public string RollNumber { get; set; }

        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }

        public static CacheClientConfiguration GetStudentCacheConfiguration()
        {
            return new CacheClientConfiguration
            {
                Name = "Student",
                QueryEntities = new[]
                {
                    new QueryEntity
                    {
                        KeyType = typeof(long),
                        ValueType = typeof(Student),
                        Fields = new[]
                        {
                            new QueryField("Id", typeof(long)),
                            new QueryField("Name", typeof(string)),
                            new QueryField("RollNumber", typeof(string))
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
