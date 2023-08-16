using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common.Dto
{
    public sealed class StudentSubjectMarksStoreDto
    {
        public string? StudentName { get; set; }
        public string? SubjectName { get; set; }
        public double HighestMark { get; set; }
        public long ExamCount { get; set; }
    }
    public sealed class StudentPerformanceStoreDto
    {
        public string? StudentName { get; set; }
        public double AverageMark { get; set; }
        public long ExamCount { get; set; }
    }
    public sealed class StudentExamMarksStoreDto
    {
        public string? Name { get; set; }
        public string? RollNumber { get; set; }
        public long ExamCount { get; set; }
        public double TotalMarks { get; set; }
    }
}
