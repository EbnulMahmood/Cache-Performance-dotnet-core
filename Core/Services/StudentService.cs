﻿using Bogus;
using Common;
using Common.Dto;
using Model;
using Repositories;

namespace Services
{
    public interface IStudentService : IStudentQueryServiceAsync
    {
        Task SaveStudentListAsync(List<Student> studentList, CancellationToken token = default);
        Task SaveSubjectListAsync(List<Subject> subjectList, CancellationToken token = default);
        Task SaveExamListAsync(List<Exam> examList, CancellationToken token = default);
        Task SaveMarkListAsync(List<Mark> markList, CancellationToken token = default);
        Task<IEnumerable<Exam>> LoadExamListAsync(CancellationToken token = default);
        Task<IEnumerable<Mark>> LoadMarkListAsync(CancellationToken token = default);
        Task<IEnumerable<Student>> LoadStudentListAsync(CancellationToken token = default);
        Task<IEnumerable<Subject>> LoadSubjectListAsync(CancellationToken token = default);
        (List<Student>, List<Subject>, List<Exam>, List<Mark>) GenerateData(int min, int max);
    }

    internal sealed class StudentService : IStudentService
    {
        private readonly IStudentRepository _studentRepository;
        private readonly IDapperDataAccess _dataAccess;

        public StudentService(IStudentRepository studentRepository
            , IDapperDataAccess dataAccess)
        {
            _studentRepository = studentRepository;
            _dataAccess = dataAccess;
        }

        public async Task<IEnumerable<StudentSubjectMarksDto>> LoadSubjectWiseHighestMarksAndExamCountAsync(CancellationToken token = default)
        {
            try
            {
                const string query = $@"
SELECT 
    s.Name AS StudentName
    ,sub.Name AS SubjectName
    ,MAX(m.MarkValue) AS HighestMark
    ,COUNT(DISTINCT m.ExamId) AS ExamCount
FROM Marks m
JOIN Students s ON s.Id = m.StudentId
JOIN Subjects sub ON m.SubjectId = sub.Id
GROUP BY s.Id, s.Name, sub.Id, sub.Name
ORDER BY s.Name, sub.Name";

                return await _dataAccess.LoadDataAsync<StudentSubjectMarksDto, dynamic>(query, new { });
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<IEnumerable<StudentSubjectMarksDto>> LoadTopPerformingStudentsBySubjectAsync(CancellationToken token = default)
        {
            try
            {
                const string query = $@"
SELECT
    s.Name AS StudentName,
    sub.Name AS SubjectName,
    MAX(m.MarkValue) AS HighestMark,
    COUNT(DISTINCT e.Id) AS ExamCount
FROM Marks m
JOIN Students s ON s.Id = m.StudentId
JOIN Subjects sub ON sub.Id = m.SubjectId
JOIN Exams e ON e.Id = m.ExamId
GROUP BY s.Id, s.Name, sub.Id, sub.Name
HAVING MAX(m.MarkValue) = 100";

                return await _dataAccess.LoadDataAsync<StudentSubjectMarksDto, dynamic>(query, new { });
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<IEnumerable<StudentPerformanceDto>> LoadTopStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default)
        {
            try
            {
                const string query = $@"
SELECT 
    TOP (@TopCount)
    s.Name AS StudentName
    ,ROUND(AVG(m.MarkValue), 2) AS AverageMark
    ,COUNT(DISTINCT e.Id) AS ExamCount
FROM Marks m
JOIN Students s ON s.Id = m.StudentId
JOIN Exams e ON e.Id = m.ExamId
GROUP BY s.Id, s.Name
ORDER BY AverageMark DESC";

                return await _dataAccess.LoadDataAsync<StudentPerformanceDto, dynamic>(query, new { TopCount = topCount });
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<IEnumerable<StudentPerformanceDto>> LoadLowPerformingStudentsByAverageMarkAsync(int bottomCount = 1, CancellationToken token = default)
        {
            try
            {
                const string query = @"
SELECT 
    TOP (@BottomCount)
    s.Name AS StudentName,
    ROUND(AVG(m.MarkValue), 2) AS AverageMark,
    COUNT(DISTINCT e.Id) AS ExamCount
FROM Marks m
JOIN Students s ON s.Id = m.StudentId
JOIN Exams e ON e.Id = m.ExamId
GROUP BY s.Id, s.Name
ORDER BY AverageMark ASC";

                return await _dataAccess.LoadDataAsync<StudentPerformanceDto, dynamic>(query, new { BottomCount = bottomCount });
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<IEnumerable<StudentPerformanceDto>> LoadHighPerformingStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default)
        {
            try
            {
                const string query = @"
SELECT 
    TOP (@TopCount)
    s.Name AS StudentName,
    ROUND(AVG(m.MarkValue), 2) AS AverageMark,
    COUNT(DISTINCT e.Id) AS ExamCount
FROM Marks m
JOIN Students s ON s.Id = m.StudentId
JOIN Exams e ON e.Id = m.ExamId
GROUP BY s.Id, s.Name
ORDER BY AverageMark DESC";

                return await _dataAccess.LoadDataAsync<StudentPerformanceDto, dynamic>(query, new { TopCount = topCount });
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<IEnumerable<StudentExamMarksDto>> LoadStudentsWithLowestMarksAsync(int numberOfStudents = 1, CancellationToken token = default)
        {
            try
            {
                const string query = @"
SELECT 
    TOP (@NumberOfStudents) s.Name
    ,s.RollNumber
    ,COUNT(DISTINCT m.ExamId) AS ExamCount
    ,ROUND(SUM(m.MarkValue), 2) AS TotalMarks
FROM Marks m
JOIN Students s ON s.Id = m.StudentId
GROUP BY s.Id, s.Name, s.RollNumber
HAVING COUNT(DISTINCT m.ExamId) = (
    SELECT MAX(ExamCount)
    FROM (
        SELECT COUNT(DISTINCT ExamId) AS ExamCount
        FROM Marks
        GROUP BY StudentId
    ) subq
)
ORDER BY SUM(m.MarkValue) ASC";

                return await _dataAccess.LoadDataAsync<StudentExamMarksDto, dynamic>(query, new { NumberOfStudents = numberOfStudents });
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<IEnumerable<StudentExamMarksDto>> LoadStudentsWithHighestMarksAsync(int numberOfStudents = 1, CancellationToken token = default)
        {
            try
            {
                const string query = @"
SELECT 
TOP (@numberOfStudents) s.Name
,s.RollNumber
,COUNT(DISTINCT m.ExamId) AS ExamCount
,ROUND(SUM(m.MarkValue), 2) AS TotalMarks
FROM Marks m
JOIN Students s ON s.Id = m.StudentId
GROUP BY s.Id, s.Name, s.RollNumber
HAVING COUNT(DISTINCT m.ExamId) = (
    SELECT MIN(ExamCount)
    FROM (
        SELECT COUNT(DISTINCT ExamId) AS ExamCount
        FROM Marks
        GROUP BY StudentId
    ) subq
)
ORDER BY SUM(m.MarkValue) DESC";

                return await _dataAccess.LoadDataAsync<StudentExamMarksDto, dynamic>(query, new { NumberOfStudents = numberOfStudents });
            }
            catch (Exception)
            {

                throw;
            }
        }

        #region Seed Data
        public async Task SaveStudentListAsync(List<Student> studentList, CancellationToken token = default)
        {
            try
            {
                await _studentRepository.SaveStudentListAsync(studentList, token);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task SaveSubjectListAsync(List<Subject> subjectList, CancellationToken token = default)
        {
            try
            {
                await _studentRepository.SaveSubjectListAsync(subjectList, token);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task SaveExamListAsync(List<Exam> examList, CancellationToken token = default)
        {
            try
            {
                await _studentRepository.SaveExamListAsync(examList, token);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task SaveMarkListAsync(List<Mark> markList, CancellationToken token = default)
        {
            try
            {
                await _studentRepository.SaveMarkListAsync(markList, token);

            }
            catch (Exception)
            {

                throw;
            }
        }

        private static List<Subject> GenerateSubjectList(int min, int max)
        {
            var subjectList = new List<Subject>();
            var subjects = new string[] { "Mathematics", "Physics", "Chemistry", "Biology", "History", "Geography", "English", "French", "Spanish", "Art" };
            var random = new Random();
            for (int i = min; i < max; i++)
            {
                var objSubject = new Faker<Subject>()
                    .RuleFor(x => x.Name, f => subjects[i])
                    .RuleFor(x => x.Description, f => f.Lorem.Sentence())
                    .RuleFor(x => x.CreatedAt, f => f.Date.Past(2))
                    .RuleFor(x => x.ModifiedAt, f => f.Date.Future(1))
                    .Generate();
                subjectList.Add(objSubject);
            }
            return subjectList;
        }

        private static List<Exam> GenerateExamList(int min, int max)
        {
            var examList = new List<Exam>();
            var exams = new string[] { "Midterm Exam", "Final Exam", "Quiz 1", "Quiz 2", "Unit Test 1", "Unit Test 2", "Pop Quiz", "Semester Exam", "Lab Exam", "Oral Exam" };
            var random = new Random();
            for (int i = min; i < max; i++)
            {
                var objExam = new Faker<Exam>()
                    .RuleFor(x => x.Name, f => exams[i])
                    .RuleFor(x => x.ExamDate, f => f.Date.Future(1))
                    .RuleFor(x => x.CreatedAt, f => f.Date.Past(2))
                    .RuleFor(x => x.ModifiedAt, f => f.Date.Future(1))
                    .Generate();
                examList.Add(objExam);
            }
            return examList;
        }

        private static List<Student> GenerateStudentList(int min, int max)
        {
            var studentList = new List<Student>();
            for (int i = min; i < max; i++)
            {
                var objStudent = new Faker<Student>()
                    .RuleFor(x => x.Name, f => f.Person.FullName)
                    .RuleFor(x => x.RollNumber, f => $"{f.Random.Number(10000, 99999)}{f.Random.Number(0000, 9999)}")
                    .RuleFor(x => x.CreatedAt, f => f.Date.Past(2))
                    .RuleFor(x => x.ModifiedAt, f => f.Date.Future(1))
                    .Generate();
                studentList.Add(objStudent);
            }
            return studentList;
        }

        public (List<Student>, List<Subject>, List<Exam>, List<Mark>) GenerateData(int min, int max)
        {
            try
            {
                var markList = new List<Mark>();
                var subjectList = GenerateSubjectList(min, 10);
                var examList = GenerateExamList(min, 10);
                var studentList = GenerateStudentList(min, max);

                var random = new Random();
                int totalExam = examList.Count * max;
                for (int i = min; i < totalExam; i++)
                {
                    var objMark = new Faker<Mark>()
                        .RuleFor(x => x.MarkValue, f => Math.Round(f.Random.Double(0, 100), 1))
                        .RuleFor(x => x.Student, _ => studentList[random.Next(studentList.Count)])
                        .RuleFor(x => x.Subject, _ => subjectList[random.Next(subjectList.Count)])
                        .RuleFor(x => x.Exam, _ => examList[random.Next(examList.Count)])
                        .RuleFor(x => x.CreatedAt, f => f.Date.Past(2))
                        .RuleFor(x => x.ModifiedAt, f => f.Date.Future(1))
                        .Generate();
                    markList.Add(objMark);
                }

                return (studentList, subjectList, examList, markList);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<IEnumerable<Exam>> LoadExamListAsync(CancellationToken token = default)
        {
            try
            {
                return await _studentRepository.LoadExamListAsync(token);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<IEnumerable<Mark>> LoadMarkListAsync(CancellationToken token = default)
        {
            try
            {
                return await _studentRepository.LoadMarkListAsync(token);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<IEnumerable<Student>> LoadStudentListAsync(CancellationToken token = default)
        {
            try
            {
                return await _studentRepository.LoadStudentListAsync(token);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<IEnumerable<Subject>> LoadSubjectListAsync(CancellationToken token = default)
        {
            try
            {
                return await _studentRepository.LoadSubjectListAsync(token);
            }
            catch (Exception)
            {

                throw;
            }
        }
        #endregion
    }
}
