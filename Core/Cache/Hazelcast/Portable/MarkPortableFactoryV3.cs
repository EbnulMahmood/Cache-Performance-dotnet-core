using Hazelcast.Serialization;

namespace Cache.Hazelcast.Portable
{
    public sealed class PMarkV3 : IPortable
    {
        public const int ClassId = 25;

        public long Id { get; set; }
        public double MarkValue { get; set; }
        public PStudent? PStudent { get; set; }
        public PSubject? PSubject { get; set; }
        public PExam? PExam { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }

        int IPortable.FactoryId => MarkPortableFactoryV3.FactoryId;
        int IPortable.ClassId => ClassId;

        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteLong("id", Id);
            writer.WriteDouble("markValue", MarkValue);
            writer.WritePortable("student", PStudent);
            writer.WritePortable("subject", PSubject);
            writer.WritePortable("exam", PExam);
            writer.WriteLong("createdAt", CreatedAt.ToFileTime());
            writer.WriteLong("modifiedAt", ModifiedAt.ToFileTime());
        }

        public void ReadPortable(IPortableReader reader)
        {
            Id = reader.ReadLong("id");
            MarkValue = reader.ReadDouble("markValue");
            PStudent = reader.ReadPortable<PStudent>("student");
            PSubject = reader.ReadPortable<PSubject>("subject");
            PExam = reader.ReadPortable<PExam>("exam");
            CreatedAt = DateTimeOffset.FromFileTime(reader.ReadLong("createdAt"));
            ModifiedAt = DateTimeOffset.FromFileTime(reader.ReadLong("modifiedAt"));
        }
    }

    public sealed class MarkPortableFactoryV3 : IPortableFactory
    {
        public const int FactoryId = 25;

        public IPortable? Create(int classId)
        {
            if (classId == PMarkV3.ClassId) return new PMarkV3();
            return null;
        }
    }
}
