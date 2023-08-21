using Hazelcast.Serialization;

namespace Cache.Hazelcast.Portable
{
    internal sealed class Customer : IPortable
    {
        public const int ClassId = 1;

        public string? Name { get; set; }
        public int Id { get; set; }
        public DateTime LastOrder { get; set; }

        int IPortable.FactoryId => CustomerPortableFactory.FactoryId;
        int IPortable.ClassId => ClassId;

        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteInt("id", Id);
            writer.WriteString("name", Name);
            writer.WriteLong("lastOrder", LastOrder.ToFileTimeUtc());
        }

        public void ReadPortable(IPortableReader reader)
        {
            Id = reader.ReadInt("id");
            Name = reader.ReadString("name");
            LastOrder = DateTime.FromFileTimeUtc(reader.ReadLong("lastOrder"));
        }
    }

    internal sealed class CustomerPortableFactory : IPortableFactory
    {
        public const int FactoryId = 1;

        public IPortable? Create(int classId)
        {
            if (classId == Customer.ClassId) return new Customer();
            return null;
        }
    }
}
