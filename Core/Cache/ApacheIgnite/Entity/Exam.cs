using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Client.Cache;

namespace ApacheIgnite.Entity;

public class Exam
{
    [QuerySqlField(IsIndexed = true)]
    public long Id { get; set; }

    [QuerySqlField]
    public string Name { get; set; }

    [QuerySqlField]
    public DateTimeOffset ExamDate { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset ModifiedAt { get; set; }

    public static CacheClientConfiguration GetExamCacheConfiguration()
    {
        return new CacheClientConfiguration
        {
            Name = "Exam",
            QueryEntities = new[]
            {
                new QueryEntity
                {
                    KeyType = typeof(long),
                    ValueType = typeof(Exam),
                    Fields = new[]
                    {
                        new QueryField("Id", typeof(long)),
                        new QueryField("Name", typeof(string)),
                        new QueryField("ExamDate", typeof(DateTimeOffset)),
                    },
                    Indexes = new[]
                    {
                        new QueryIndex("Id")
                    }
                }
            },
            EnableStatistics = true,
            CacheMode = CacheMode.Replicated,
        };
    }
}
