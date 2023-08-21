using Hazelcast.Serialization;

namespace Cache.Hazelcast.Portable
{
    public sealed class PSubject : IPortable
    {
        public const int ClassId = 10;

        public long Id { get; set; }
        public string? Name { get; set; }
        public string? Description { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }

        int IPortable.FactoryId => SubjectPortableFactory.FactoryId;
        int IPortable.ClassId => ClassId;

        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteLong("id", Id);
            writer.WriteString("name", Name);
            writer.WriteString("description", Description);
            writer.WriteLong("createdAt", CreatedAt.ToFileTime());
            writer.WriteLong("modifiedAt", ModifiedAt.ToFileTime());
        }

        public void ReadPortable(IPortableReader reader)
        {
            Id = reader.ReadLong("id");
            Name = reader.ReadString("name");
            Description = reader.ReadString("description");
            CreatedAt = DateTimeOffset.FromFileTime(reader.ReadLong("createdAt"));
            ModifiedAt = DateTimeOffset.FromFileTime(reader.ReadLong("modifiedAt"));
        }
    }

    public sealed class SubjectPortableFactory : IPortableFactory
    {
        public const int FactoryId = 10;

        public IPortable? Create(int classId)
        {
            if (classId == PSubject.ClassId) return new PSubject();
            return null;
        }
    }
}
