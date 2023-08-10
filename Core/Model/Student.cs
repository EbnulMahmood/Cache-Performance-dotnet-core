namespace Model
{
    public sealed class Student
    {
        public long Id { get; set; }
        public string? Name { get; set; }
        public string? RollNumber { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }
    }
}
