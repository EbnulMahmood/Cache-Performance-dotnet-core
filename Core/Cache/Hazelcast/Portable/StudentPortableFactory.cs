using Hazelcast.Serialization;

namespace Cache.Hazelcast.Portable
{
    public sealed class PStudent : IPortable
    {
        public const int ClassId = 5;

        public long Id { get; set; }
        public string? Name { get; set; }
        public string? RollNumber { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }

        int IPortable.FactoryId => StudentPortableFactory.FactoryId;
        int IPortable.ClassId => ClassId;

        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteLong("id", Id);
            writer.WriteString("name", Name);
            writer.WriteString("rollNumber", RollNumber);
            writer.WriteLong("createdAt", CreatedAt.ToFileTime());
            writer.WriteLong("modifiedAt", ModifiedAt.ToFileTime());
        }

        public void ReadPortable(IPortableReader reader)
        {
            Id = reader.ReadLong("id");
            Name = reader.ReadString("name");
            RollNumber = reader.ReadString("rollNumber");
            CreatedAt = DateTimeOffset.FromFileTime(reader.ReadLong("createdAt"));
            ModifiedAt = DateTimeOffset.FromFileTime(reader.ReadLong("modifiedAt"));
        }
    }

    public sealed class StudentPortableFactory : IPortableFactory
    {
        public const int FactoryId = 5;

        public IPortable? Create(int classId)
        {
            if (classId == PStudent.ClassId) return new PStudent();
            return null;
        }
    }
}
