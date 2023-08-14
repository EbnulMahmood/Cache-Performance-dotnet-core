namespace Common.Dto
{
    public sealed class StudentSubjectMarksDto
    {
        public string? StudentName { get; set; }
        public string? SubjectName { get; set; }
        public double HighestMark { get; set; }
        public int ExamCount { get; set; }
    }
}
