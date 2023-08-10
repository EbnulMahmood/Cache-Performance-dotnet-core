namespace Common.Dto
{
    public sealed class StudentExamMarksDto
    {
        public string? Name { get; set; }
        public string? RollNumber { get; set; }
        public int ExamCount { get; set; }
        public double TotalMarks { get; set; }
    }
}
