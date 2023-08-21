using Cache.Hazelcast.Portable;
using Common;
using Common.Dto;
using Hazelcast;
using Hazelcast.Query;
using Model;

namespace Cache
{
    public interface IHazelcastStudentHelperV2 : IStudentQueryServiceAsync
    {
        Task<int> CacheStudentListAsync(IEnumerable<Student> studentList, CancellationToken token = default);
        Task<int> CacheSubjectListAsync(IEnumerable<Subject> subjectList, CancellationToken token = default);
        Task<int> CacheExamListAsync(IEnumerable<Exam> examList, CancellationToken token = default);
        Task<int> CacheMarkListAsync(IEnumerable<Mark> markList, CancellationToken token = default);
    }

    internal sealed class HazelcastStudentHelperV2 : IHazelcastStudentHelperV2
    {
        private readonly HazelcastOptions _options;
        private const string _mapStudent = "studentsv2";
        private const string _mapSubject = "subjectsv2";
        private const string _mapExam = "examsv2";
        private const string _mapMark = "marksv2";

        public HazelcastStudentHelperV2(HazelcastOptions options)
        {
            _options = options;
        }

        public async Task<IEnumerable<StudentSubjectMarksDto>> LoadSubjectWiseHighestMarksAndExamCountAsync(CancellationToken token = default)
        {
            try
            {
                var studentSubjectMarksDtoList = new List<StudentSubjectMarksDto>();

                var hazelcastOptions = new HazelcastOptionsBuilder().Build();

                var factoryMark = new MarkPortableFactory();
                var factoryStudent = new StudentPortableFactory();
                var factorySubject = new SubjectPortableFactory();

                hazelcastOptions.Serialization
                    .AddPortableFactory(MarkPortableFactory.FactoryId, factoryMark)
                    .AddPortableFactory(StudentPortableFactory.FactoryId, factoryStudent)
                    .AddPortableFactory(SubjectPortableFactory.FactoryId, factorySubject);
                await using var client = await HazelcastClientFactory.StartNewClientAsync(hazelcastOptions, cancellationToken: token).ConfigureAwait(false);

                var mapMark = await client.GetMapAsync<long, PMark>(_mapMark).ConfigureAwait(false);
                var mapStudent = await client.GetMapAsync<long, PMark>(_mapStudent).ConfigureAwait(false);
                var mapSubject = await client.GetMapAsync<long, PMark>(_mapSubject).ConfigureAwait(false);

                var keysMark = await mapMark.GetKeysAsync().ConfigureAwait(false);
                var keysStudent = await mapStudent.GetKeysAsync().ConfigureAwait(false);
                var keysSubject = await mapSubject.GetKeysAsync().ConfigureAwait(false);

                //                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                //                await using var result = await client.Sql.ExecuteQueryAsync($@"
                //SELECT 
                //    s.Name AS StudentName,
                //    sub.Name AS SubjectName,
                //    MAX(m.MarkValue) AS HighestMark,
                //    CAST(COUNT(DISTINCT m.ExamId) AS int) AS ExamCount
                //FROM marks m
                //JOIN students s ON s.__key = m.StudentId
                //JOIN subjects sub ON sub.__key = m.SubjectId
                //GROUP BY s.__key, s.Name, sub.__key, sub.Name
                //ORDER BY s.Name, sub.Name", cancellationToken: token);

                //                studentSubjectMarksDtoList = await result.Select(row =>
                //                    new StudentSubjectMarksDto
                //                    {
                //                        StudentName = row.GetColumn<string>("StudentName"),
                //                        SubjectName = row.GetColumn<string>("SubjectName"),
                //                        HighestMark = row.GetColumn<double>("HighestMark"),
                //                        ExamCount = row.GetColumn<int>("ExamCount"),
                //                    }).ToListAsync(token).ConfigureAwait(false);

                //await foreach (var row in result)
                //{
                //    studentSubjectMarksDtoList.Add(
                //        new StudentSubjectMarksDto
                //        {
                //            StudentName = row.GetColumn<string>("StudentName"),
                //            SubjectName = row.GetColumn<string>("SubjectName"),
                //            HighestMark = row.GetColumn<double>("HighestMark"),
                //            ExamCount = row.GetColumn<int>("ExamCount"),
                //        }
                //    );
                //}

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

        // SCALAR QUERY not supported
        public async Task<IEnumerable<StudentExamMarksDto>> LoadStudentsWithLowestMarksAsync(int numberOfStudents = 1, CancellationToken token = default)
        {
            try
            {
                var studentExamMarksDtoList = new List<StudentExamMarksDto>();

                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
    s.Name
    ,s.RollNumber
    ,CAST(m.ExamCount AS INT) AS ExamCount
    ,ROUND(m.TotalMarks, 2) AS TotalMarks
FROM (
  SELECT 
    StudentId
    ,COUNT(DISTINCT ExamId) AS ExamCount
    ,SUM(MarkValue) AS TotalMarks
  FROM marks
  GROUP BY StudentId
) m
JOIN students s ON s.__key = m.StudentId
JOIN (
  SELECT 
    MAX(ExamCount) AS MaxExamCount
  FROM (
    SELECT 
        COUNT(DISTINCT ExamId) AS ExamCount
    FROM marks
    GROUP BY StudentId
  ) subq
) mec ON m.ExamCount = mec.MaxExamCount
ORDER BY m.TotalMarks ASC
LIMIT ?", cancellationToken: token, parameters: numberOfStudents);

                studentExamMarksDtoList = await result.Select(row =>
                    new StudentExamMarksDto
                    {
                        Name = row.GetColumn<string>("Name"),
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

        // SCALAR QUERY not supported
        public async Task<IEnumerable<StudentExamMarksDto>> LoadStudentsWithHighestMarksAsync(int numberOfStudents = 1, CancellationToken token = default)
        {
            try
            {
                var studentExamMarksDtoList = new List<StudentExamMarksDto>();

                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
    s.Name
    ,s.RollNumber
    ,CAST(m.ExamCount AS INT) AS ExamCount
    ,ROUND(m.TotalMarks, 2) AS TotalMarks
FROM (
  SELECT 
    StudentId
    ,COUNT(DISTINCT ExamId) AS ExamCount
    ,SUM(MarkValue) AS TotalMarks
  FROM marks
  GROUP BY StudentId
) m
JOIN students s ON s.__key = m.StudentId
JOIN (
  SELECT 
    MIN(ExamCount) AS MinExamCount
  FROM (
    SELECT 
        COUNT(DISTINCT ExamId) AS ExamCount
    FROM marks
    GROUP BY StudentId
  ) subq
) mec ON m.ExamCount = mec.MinExamCount
ORDER BY m.TotalMarks DESC
LIMIT ?", cancellationToken: token, parameters: numberOfStudents);

                studentExamMarksDtoList = await result.Select(row =>
                    new StudentExamMarksDto
                    {
                        Name = row.GetColumn<string>("Name"),
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
                var hazelcastOptions = new HazelcastOptionsBuilder().Build();
                var factory = new StudentPortableFactory();
                hazelcastOptions.Serialization
                    .AddPortableFactory(StudentPortableFactory.FactoryId, factory);

                await using var client = await HazelcastClientFactory.StartNewClientAsync(hazelcastOptions, cancellationToken: token).ConfigureAwait(false);
                var map = await client.GetMapAsync<long, PStudent>(_mapStudent).ConfigureAwait(false);

                var studentsDictionary = new Dictionary<long, PStudent>();

                foreach (var student in studentList)
                {
                    studentsDictionary.Add(student.Id, new PStudent
                    {
                        Id = student.Id,
                        Name = student.Name,
                        RollNumber = student.RollNumber,
                        CreatedAt = student.CreatedAt,
                        ModifiedAt = student.ModifiedAt
                    });
                }

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
                var hazelcastOptions = new HazelcastOptionsBuilder().Build();
                var factory = new SubjectPortableFactory();
                hazelcastOptions.Serialization
                    .AddPortableFactory(SubjectPortableFactory.FactoryId, factory);

                await using var client = await HazelcastClientFactory.StartNewClientAsync(hazelcastOptions, cancellationToken: token).ConfigureAwait(false);
                var map = await client.GetMapAsync<long, PSubject>(_mapSubject).ConfigureAwait(false);

                var subjectsDictionary = new Dictionary<long, PSubject>();

                foreach (var subject in subjectList)
                {
                    subjectsDictionary.Add(subject.Id, new PSubject
                    {
                        Id = subject.Id,
                        Name = subject.Name,
                        Description = subject.Description,
                        CreatedAt = subject.CreatedAt,
                        ModifiedAt = subject.ModifiedAt
                    });
                }

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
                var hazelcastOptions = new HazelcastOptionsBuilder().Build();
                var factory = new ExamPortableFactory();
                hazelcastOptions.Serialization
                    .AddPortableFactory(ExamPortableFactory.FactoryId, factory);

                await using var client = await HazelcastClientFactory.StartNewClientAsync(hazelcastOptions, cancellationToken: token).ConfigureAwait(false);
                var map = await client.GetMapAsync<long, PExam>(_mapExam).ConfigureAwait(false);

                var examsDictionary = new Dictionary<long, PExam>();

                foreach (var exam in examList)
                {
                    examsDictionary.Add(exam.Id, new PExam
                    {
                        Id = exam.Id,
                        Name = exam.Name,
                        ExamDate = exam.ExamDate,
                        CreatedAt = exam.CreatedAt,
                        ModifiedAt = exam.ModifiedAt
                    });
                }

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
                var hazelcastOptions = new HazelcastOptionsBuilder().Build();
                var factory = new MarkPortableFactory();
                hazelcastOptions.Serialization
                    .AddPortableFactory(MarkPortableFactory.FactoryId, factory);

                await using var client = await HazelcastClientFactory.StartNewClientAsync(hazelcastOptions, cancellationToken: token).ConfigureAwait(false);
                var map = await client.GetMapAsync<long, PMark>(_mapMark).ConfigureAwait(false);

                var marksDictionary = new Dictionary<long, PMark>();

                foreach (var mark in markList)
                {
                    marksDictionary.Add(mark.Id, new PMark
                    {
                        Id = mark.Id,
                        MarkValue = mark.MarkValue,
                        StudentId = mark.Student.Id,
                        SubjectId = mark.Subject.Id,
                        ExamId = mark.Exam.Id,
                        CreatedAt = mark.CreatedAt,
                        ModifiedAt = mark.ModifiedAt
                    });
                }

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
