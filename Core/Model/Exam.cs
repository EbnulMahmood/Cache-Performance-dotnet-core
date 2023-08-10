namespace Model
{
    public sealed class Exam
    {
        public long Id { get; set; }
        public string? Name { get; set; }
        public DateTimeOffset ExamDate { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }
    }
}
