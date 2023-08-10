namespace Common.Dto
{
    public sealed class MarkDto
    {
        public long Id { get; set; }
        public double MarkValue { get; set; }
        public long StudentId { get; set; }
        public long SubjectId { get; set; }
        public long ExamId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ModifiedAt { get; set; }
    }
}
