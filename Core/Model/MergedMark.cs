namespace Model
{
    public sealed class MergedMark
    {
        public long Id { get; set; }
        public double MarkValue { get; set; }
        public string? StudentName { get; set; }
        public string? StudentRollNumber { get; set; }
        public string? SubjectName { get; set; }
        public string? SubjectDescription { get; set; }
        public string? ExamName { get; set; }
        public DateTimeOffset ExamDate { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }
    }
}
