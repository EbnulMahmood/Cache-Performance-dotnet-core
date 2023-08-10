using Common;
using Common.Dto;
using Hazelcast;
using Hazelcast.Core;
using Model;
using System.Text.Json;

namespace Cache
{
    public interface IStudentCacheHelper : IStudentQueryService
    {
        Task<int> CacheStudentListAsync(IEnumerable<Student> studentList, CancellationToken token = default);
        Task<int> CacheSubjectListAsync(IEnumerable<Subject> subjectList, CancellationToken token = default);
        Task<int> CacheExamListAsync(IEnumerable<Exam> examList, CancellationToken token = default);
        Task<int> CacheMarkListAsync(IEnumerable<Mark> markList, CancellationToken token = default);
    }

    internal sealed class StudentCacheHelper : IStudentCacheHelper
    {
        private readonly HazelcastOptions _options;
        private const string _mapStudent = "students";
        private const string _mapSubject = "subjects";
        private const string _mapExam = "exams";
        private const string _mapMark = "marks";

        public StudentCacheHelper(HazelcastOptions options)
        {
            _options = options;
        }

        public async Task<IEnumerable<StudentSubjectMarksDto>> LoadSubjectWiseHighestMarksAndExamCountAsync(CancellationToken token = default)
        {
            try
            {
                var studentSubjectMarksDtoList = new List<StudentSubjectMarksDto>();

                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
    s.Name AS StudentName,
    sub.Name AS SubjectName,
    MAX(m.MarkValue) AS HighestMark,
    CAST(COUNT(DISTINCT m.ExamId) AS int) AS ExamCount
FROM marks m
JOIN students s ON s.__key = m.StudentId
JOIN subjects sub ON sub.__key = m.SubjectId
GROUP BY s.__key, s.Name, sub.__key, sub.Name
ORDER BY s.Name, sub.Name", cancellationToken: token);

                studentSubjectMarksDtoList = await result.Select(row =>
                    new StudentSubjectMarksDto
                    {
                        StudentName = row.GetColumn<string>("StudentName"),
                        SubjectName = row.GetColumn<string>("SubjectName"),
                        HighestMark = row.GetColumn<double>("HighestMark"),
                        ExamCount = row.GetColumn<int>("ExamCount"),
                    }).ToListAsync(token).ConfigureAwait(false);

                return studentSubjectMarksDtoList;
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
                var studentSubjectMarksDtoList = new List<StudentSubjectMarksDto>();

                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
    s.Name AS StudentName,
    sub.Name AS SubjectName,
    MAX(m.MarkValue) AS HighestMark,
    CAST(COUNT(DISTINCT m.ExamId) AS int) AS ExamCount
FROM marks m
JOIN students s ON s.__key = m.StudentId
JOIN subjects sub ON sub.__key = m.SubjectId
JOIN exams e ON e.__key = m.ExamId
GROUP BY s.__key, s.Name, sub.__key, sub.Name
HAVING MAX(m.MarkValue) = 100", cancellationToken: token);

                studentSubjectMarksDtoList = await result.Select(row =>
                    new StudentSubjectMarksDto
                    {
                        StudentName = row.GetColumn<string>("StudentName"),
                        SubjectName = row.GetColumn<string>("SubjectName"),
                        HighestMark = row.GetColumn<double>("HighestMark"),
                        ExamCount = row.GetColumn<int>("ExamCount"),
                    }).ToListAsync(token).ConfigureAwait(false);

                return studentSubjectMarksDtoList;
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
                var studentPerformanceDtoList = new List<StudentPerformanceDto>();

                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
    s.Name AS StudentName,
    ROUND(AVG(m.MarkValue), 2) AS AverageMark,
    CAST(COUNT(DISTINCT m.ExamId) AS int) AS ExamCount
FROM marks m
JOIN students s ON s.__key = m.StudentId
JOIN exams e ON e.__key = m.ExamId
GROUP BY s.__key, s.Name
ORDER BY AverageMark DESC
LIMIT ?", cancellationToken: token, parameters: topCount);

                studentPerformanceDtoList = await result.Select(row =>
                    new StudentPerformanceDto
                    {
                        StudentName = row.GetColumn<string>("StudentName"),
                        AverageMark = row.GetColumn<double>("AverageMark"),
                        ExamCount = row.GetColumn<int>("ExamCount"),
                    }).ToListAsync(token).ConfigureAwait(false);

                return studentPerformanceDtoList;
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
                var studentPerformanceDtoList = new List<StudentPerformanceDto>();

                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
    s.Name AS StudentName,
    ROUND(AVG(m.MarkValue), 2) AS AverageMark,
    CAST(COUNT(DISTINCT m.ExamId) AS int) AS ExamCount
FROM marks m
JOIN students s ON s.__key = m.StudentId
JOIN exams e ON e.__key = m.ExamId
GROUP BY s.__key, s.Name
ORDER BY AverageMark ASC
LIMIT ?", cancellationToken: token, parameters: bottomCount);

                studentPerformanceDtoList = await result.Select(row =>
                    new StudentPerformanceDto
                    {
                        StudentName = row.GetColumn<string>("StudentName"),
                        AverageMark = row.GetColumn<double>("AverageMark"),
                        ExamCount = row.GetColumn<int>("ExamCount"),
                    }).ToListAsync(token).ConfigureAwait(false);

                return studentPerformanceDtoList;
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
                var studentPerformanceDtoList = new List<StudentPerformanceDto>();

                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
    s.Name AS StudentName,
    ROUND(AVG(m.MarkValue), 2) AS AverageMark,
    CAST(COUNT(DISTINCT m.ExamId) AS int) AS ExamCount
FROM marks m
JOIN students s ON s.__key = m.StudentId
JOIN exams e ON e.__key = m.ExamId
GROUP BY s.__key, s.Name
ORDER BY AverageMark DESC
LIMIT ?", cancellationToken: token, parameters: topCount);

                studentPerformanceDtoList = await result.Select(row =>
                    new StudentPerformanceDto
                    {
                        StudentName = row.GetColumn<string>("StudentName"),
                        AverageMark = row.GetColumn<double>("AverageMark"),
                        ExamCount = row.GetColumn<int>("ExamCount"),
                    }).ToListAsync(token).ConfigureAwait(false);

                return studentPerformanceDtoList;
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
                var studentExamMarksDtoList = new List<StudentExamMarksDto>();

                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
    s.Name,
    s.RollNumber,
    CAST(COUNT(DISTINCT m.ExamId) AS int) AS ExamCount,
    ROUND(SUM(m.MarkValue), 2) AS TotalMarks
FROM marks m
JOIN students s ON s.__key = m.StudentId
GROUP BY s.__key, s.Name, s.RollNumber
HAVING COUNT(DISTINCT m.ExamId) = (
    SELECT MAX(ExamCount)
    FROM (
        SELECT COUNT(DISTINCT ExamId) AS ExamCount
        FROM marks
        GROUP BY StudentId
    ) subq
)
ORDER BY SUM(m.MarkValue) ASC
LIMIT ?", cancellationToken: token, parameters: numberOfStudents);

                studentExamMarksDtoList = await result.Select(row =>
                    new StudentExamMarksDto
                    {
                        Name = row.GetColumn<string>("name"),
                        RollNumber = row.GetColumn<string>("RollNumber"),
                        ExamCount = row.GetColumn<int>("ExamCount"),
                        TotalMarks = row.GetColumn<double>("TotalMarks"),
                    }).ToListAsync(token).ConfigureAwait(false);

                return studentExamMarksDtoList;
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
                var studentExamMarksDtoList = new List<StudentExamMarksDto>();

                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
    s.Name,
    s.RollNumber,
    CAST(COUNT(DISTINCT m.ExamId) AS int) AS ExamCount,
    ROUND(SUM(m.MarkValue), 2) AS TotalMarks
FROM marks m
JOIN students s ON s.__key = m.StudentId
GROUP BY s.__key, s.Name, s.RollNumber
HAVING COUNT(DISTINCT m.ExamId) = (
    SELECT MIN(ExamCount)
    FROM (
        SELECT COUNT(DISTINCT ExamId) AS ExamCount
        FROM marks
        GROUP BY StudentId
    ) subq
)
ORDER BY SUM(m.MarkValue) DESC
LIMIT ?", cancellationToken: token, parameters: numberOfStudents);

                studentExamMarksDtoList = await result.Select(row =>
                    new StudentExamMarksDto
                    {
                        Name = row.GetColumn<string>("name"),
                        RollNumber = row.GetColumn<string>("RollNumber"),
                        ExamCount = row.GetColumn<int>("ExamCount"),
                        TotalMarks = row.GetColumn<double>("TotalMarks"),
                    }).ToListAsync(token).ConfigureAwait(false);

                return studentExamMarksDtoList;
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<int> CacheStudentListAsync(IEnumerable<Student> studentList, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);
                await using var map = await client.GetMapAsync<long, HazelcastJsonValue>(_mapStudent).ConfigureAwait(false);

                var studentsDictionary = new Dictionary<long, HazelcastJsonValue>();

                foreach (var student in studentList)
                {
                    string jsonString = JsonSerializer.Serialize(student);
                    var jsonObj = new HazelcastJsonValue(jsonString);
                    studentsDictionary.Add(student.Id, jsonObj);
                }

                await client.Sql.ExecuteCommandAsync($@"
CREATE OR REPLACE MAPPING 
{map.Name} (
__key BIGINT,
Id BIGINT,
Name VARCHAR,
RollNumber VARCHAR,
CreatedAt TIMESTAMP WITH TIME ZONE,
ModifiedAt TIMESTAMP WITH TIME ZONE)
TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json-flat')", cancellationToken: token).ConfigureAwait(false);

                await map.SetAllAsync(studentsDictionary).ConfigureAwait(false);
                int count = await map.GetSizeAsync().ConfigureAwait(false);

                await map.DisposeAsync().ConfigureAwait(false);
                await client.DisposeAsync().ConfigureAwait(false);

                return count;
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<int> CacheSubjectListAsync(IEnumerable<Subject> subjectList, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);
                await using var map = await client.GetMapAsync<long, HazelcastJsonValue>(_mapSubject).ConfigureAwait(false);

                var subjectsDictionary = new Dictionary<long, HazelcastJsonValue>();

                foreach (var subject in subjectList)
                {
                    string jsonString = JsonSerializer.Serialize(subject);
                    var jsonObj = new HazelcastJsonValue(jsonString);
                    subjectsDictionary.Add(subject.Id, jsonObj);
                }

                await client.Sql.ExecuteCommandAsync($@"
CREATE OR REPLACE MAPPING 
{map.Name} (
__key BIGINT,
Id BIGINT,
Name VARCHAR,
Description VARCHAR,
CreatedAt TIMESTAMP WITH TIME ZONE,
ModifiedAt TIMESTAMP WITH TIME ZONE)
TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json-flat')", cancellationToken: token).ConfigureAwait(false);

                await map.SetAllAsync(subjectsDictionary).ConfigureAwait(false);
                int count = await map.GetSizeAsync().ConfigureAwait(false);

                await map.DisposeAsync().ConfigureAwait(false);
                await client.DisposeAsync().ConfigureAwait(false);

                return count;
            }
            catch (Exception)
            {

                throw;

            }
        }

        public async Task<int> CacheExamListAsync(IEnumerable<Exam> examList, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);
                await using var map = await client.GetMapAsync<long, HazelcastJsonValue>(_mapExam).ConfigureAwait(false);

                var examsDictionary = new Dictionary<long, HazelcastJsonValue>();

                foreach (var exam in examList)
                {
                    string jsonString = JsonSerializer.Serialize(exam);
                    var jsonObj = new HazelcastJsonValue(jsonString);
                    examsDictionary.Add(exam.Id, jsonObj);
                }

                await client.Sql.ExecuteCommandAsync($@"
CREATE OR REPLACE MAPPING 
{map.Name} (
__key BIGINT,
Id BIGINT,
Name VARCHAR,
ExamDate TIMESTAMP WITH TIME ZONE,
CreatedAt TIMESTAMP WITH TIME ZONE,
ModifiedAt TIMESTAMP WITH TIME ZONE)
TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json-flat')", cancellationToken: token).ConfigureAwait(false);

                await map.SetAllAsync(examsDictionary).ConfigureAwait(false);
                int count = await map.GetSizeAsync().ConfigureAwait(false);

                await map.DisposeAsync().ConfigureAwait(false);
                await client.DisposeAsync().ConfigureAwait(false);

                return count;
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<int> CacheMarkListAsync(IEnumerable<Mark> markList, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);
                await using var map = await client.GetMapAsync<long, HazelcastJsonValue>(_mapMark).ConfigureAwait(false);

                var marksDictionary = new Dictionary<long, HazelcastJsonValue>();

                foreach (var mark in markList)
                {
                    string jsonString = JsonSerializer.Serialize(
                        new MarkDto
                        {
                            Id = mark.Id,
                            MarkValue = mark.MarkValue,
                            StudentId = mark.Student.Id,
                            SubjectId = mark.Subject.Id,
                            ExamId = mark.Exam.Id,
                            CreatedAt = mark.CreatedAt,
                            ModifiedAt = mark.ModifiedAt,
                        });
                    var jsonObj = new HazelcastJsonValue(jsonString);
                    marksDictionary.Add(mark.Id, jsonObj);
                }

                await client.Sql.ExecuteCommandAsync($@"
CREATE OR REPLACE MAPPING 
{map.Name} (
__key BIGINT,
Id BIGINT,
MarkValue DOUBLE,
StudentId BIGINT,
SubjectId BIGINT,
ExamId BIGINT,
CreatedAt TIMESTAMP WITH TIME ZONE,
ModifiedAt TIMESTAMP WITH TIME ZONE)
TYPE IMap OPTIONS ('keyFormat'='bigint', 'valueFormat'='json-flat')", cancellationToken: token).ConfigureAwait(false);

                await map.SetAllAsync(marksDictionary).ConfigureAwait(false);
                int count = await map.GetSizeAsync().ConfigureAwait(false);

                await map.DisposeAsync().ConfigureAwait(false);
                await client.DisposeAsync().ConfigureAwait(false);

                return count;
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}
