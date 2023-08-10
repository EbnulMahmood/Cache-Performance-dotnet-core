namespace Model
{
    public sealed class Mark
    {
        public long Id { get; set; }
        public double MarkValue { get; set; }
        public Student? Student { get; set; }
        public Subject? Subject { get; set; }
        public Exam? Exam { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }
    }
}
