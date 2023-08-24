using Hazelcast.Serialization;

namespace Cache.Hazelcast.Portable
{
    public sealed class PMarkV5 : IPortable
    {
        public const int ClassId = 30;

        public double MarkValue { get; set; }
        public string? StudentName { get; set; }
        public string? StudentRollNumber { get; set; }
        public string? SubjectName { get; set; }
        public string? SubjectDescription { get; set; }
        public string? ExamName { get; set; }
        public DateTimeOffset ExamDate { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }

        int IPortable.FactoryId => MarkPortableFactoryV5.FactoryId;
        int IPortable.ClassId => ClassId;

        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteDouble(nameof(MarkValue), MarkValue);
            writer.WriteString(nameof(StudentName), StudentName);
            writer.WriteString(nameof(StudentRollNumber), StudentRollNumber);
            writer.WriteString(nameof(SubjectName), SubjectName);
            writer.WriteString(nameof(SubjectDescription), SubjectDescription);
            writer.WriteString(nameof(ExamName), ExamName);
            writer.WriteLong(nameof(ExamDate), ExamDate.ToFileTime());
            writer.WriteLong(nameof(CreatedAt), CreatedAt.ToFileTime());
            writer.WriteLong(nameof(ModifiedAt), ModifiedAt.ToFileTime());
        }

        public void ReadPortable(IPortableReader reader)
        {
            MarkValue = reader.ReadDouble(nameof(MarkValue));
            StudentName = reader.ReadString(nameof(StudentName));
            StudentRollNumber = reader.ReadString(nameof(StudentRollNumber));
            SubjectName = reader.ReadString(nameof(SubjectName));
            SubjectDescription = reader.ReadString(nameof(SubjectDescription));
            ExamName = reader.ReadString(nameof(ExamName));
            ExamDate = DateTimeOffset.FromFileTime(reader.ReadLong(nameof(ExamDate)));
            CreatedAt = DateTimeOffset.FromFileTime(reader.ReadLong(nameof(CreatedAt)));
            ModifiedAt = DateTimeOffset.FromFileTime(reader.ReadLong(nameof(ModifiedAt)));
        }
    }

    public sealed class MarkPortableFactoryV5 : IPortableFactory
    {
        public const int FactoryId = 30;

        public IPortable? Create(int classId)
        {
            if (classId == PMarkV5.ClassId) return new PMarkV5();
            return null;
        }
    }
}
