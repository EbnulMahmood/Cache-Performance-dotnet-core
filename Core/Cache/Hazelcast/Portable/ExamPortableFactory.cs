using Hazelcast.Serialization;

namespace Cache.Hazelcast.Portable
{
    public sealed class PExam : IPortable
    {
        public const int ClassId = 15;

        public long Id { get; set; }
        public string? Name { get; set; }
        public DateTimeOffset ExamDate { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }

        int IPortable.FactoryId => ExamPortableFactory.FactoryId;
        int IPortable.ClassId => ClassId;

        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteLong("id", Id);
            writer.WriteString("name", Name);
            writer.WriteLong("examDate", ExamDate.ToFileTime());
            writer.WriteLong("createdAt", CreatedAt.ToFileTime());
            writer.WriteLong("modifiedAt", ModifiedAt.ToFileTime());
        }

        public void ReadPortable(IPortableReader reader)
        {
            Id = reader.ReadLong("id");
            Name = reader.ReadString("name");
            ExamDate = DateTimeOffset.FromFileTime(reader.ReadLong("examDate"));
            CreatedAt = DateTimeOffset.FromFileTime(reader.ReadLong("createdAt"));
            ModifiedAt = DateTimeOffset.FromFileTime(reader.ReadLong("modifiedAt"));
        }
    }

    public sealed class ExamPortableFactory : IPortableFactory
    {
        public const int FactoryId = 15;

        public IPortable? Create(int classId)
        {
            if (classId == PExam.ClassId) return new PExam();
            return null;
        }
    }
}
