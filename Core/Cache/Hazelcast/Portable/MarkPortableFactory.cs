using Hazelcast.Serialization;

namespace Cache.Hazelcast.Portable
{
    public sealed class PMark : IPortable
    {
        public const int ClassId = 20;

        public long Id { get; set; }
        public double MarkValue { get; set; }
        public long StudentId { get; set; }
        public long SubjectId { get; set; }
        public long ExamId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }

        int IPortable.FactoryId => MarkPortableFactory.FactoryId;
        int IPortable.ClassId => ClassId;

        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteLong("id", Id);
            writer.WriteDouble("markValue", MarkValue);
            writer.WriteLong("studentId", StudentId);
            writer.WriteLong("subjectId", SubjectId);
            writer.WriteLong("examId", ExamId);
            writer.WriteLong("createdAt", CreatedAt.ToFileTime());
            writer.WriteLong("modifiedAt", ModifiedAt.ToFileTime());
        }

        public void ReadPortable(IPortableReader reader)
        {
            Id = reader.ReadLong("id");
            MarkValue = reader.ReadLong("markValue");
            StudentId = reader.ReadLong("studentId");
            SubjectId = reader.ReadLong("subjectId");
            ExamId = reader.ReadLong("examId");
            CreatedAt = DateTimeOffset.FromFileTime(reader.ReadLong("createdAt"));
            ModifiedAt = DateTimeOffset.FromFileTime(reader.ReadLong("modifiedAt"));
        }
    }

    public sealed class MarkPortableFactory : IPortableFactory
    {
        public const int FactoryId = 20;

        public IPortable? Create(int classId)
        {
            if (classId == PMark.ClassId) return new PMark();
            return null;
        }
    }
}
