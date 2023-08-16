using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Client.Cache;

namespace ApacheIgnite.Entity
{
    public class Mark
    {
        [QuerySqlField(IsIndexed = true)]
        public long Id { get; set; }

        [QuerySqlField]
        public double MarkValue { get; set; }

        [QuerySqlField(IsIndexed = true)]
        public long StudentId { get; set; }

        [QuerySqlField(IsIndexed = true)]
        public long SubjectId { get; set; }

        [QuerySqlField(IsIndexed = true)]
        public long ExamId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }

        public static CacheClientConfiguration GetMarksCacheConfiguration()
        {
            return new CacheClientConfiguration
            {
                Name = "Mark",
                QueryEntities = new[]
                {
                    new QueryEntity
                    {
                        KeyType = typeof(long),
                        ValueType = typeof(Mark),
                        Fields = new[]
                        {
                            new QueryField("Id", typeof(long)),
                            new QueryField("MarkValue", typeof(double)),
                            new QueryField("StudentId", typeof(long)),
                            new QueryField("SubjectId", typeof(long)),
                            new QueryField("ExamId", typeof(long))
                        },
                        Indexes = new[]
                        {
                            new QueryIndex("Id"),
                            new QueryIndex("StudentId"),
                            new QueryIndex("SubjectId"),
                            new QueryIndex("ExamId")
                        }
                    }
                },
                EnableStatistics = true,
                CacheMode = CacheMode.Replicated
            };
        }
    }
}
