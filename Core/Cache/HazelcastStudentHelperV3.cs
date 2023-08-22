using Cache.Hazelcast.Portable;
using Common;
using Common.Dto;
using Hazelcast;
using Hazelcast.Query;
using Model;

namespace Cache
{
    public interface IHazelcastStudentHelperV3 : IStudentQueryServiceAsync
    {
        Task<int> CacheMarkListAsync(IEnumerable<Mark> markList, CancellationToken token = default);
    }

    internal sealed class HazelcastStudentHelperV3 : IHazelcastStudentHelperV3
    {
        private readonly HazelcastOptions _options;
        private const string _mapStudent = "studentsv3";
        private const string _mapSubject = "subjectsv3";
        private const string _mapExam = "examsv3";
        private const string _mapMark = "marksv3";

        public HazelcastStudentHelperV3(HazelcastOptions options)
        {
            _options = options;
        }

        public async Task<IEnumerable<StudentSubjectMarksDto>> LoadSubjectWiseHighestMarksAndExamCountAsync(CancellationToken token = default)
        {
            try
            {
                var studentSubjectMarksDtoList = new List<StudentSubjectMarksDto>();

                var hazelcastOptions = new HazelcastOptionsBuilder().Build();

                var factoryMark = new MarkPortableFactoryV3();
                var factoryStudent = new StudentPortableFactory();
                var factorySubject = new SubjectPortableFactory();
                var factoryExam = new ExamPortableFactory();

                hazelcastOptions.Serialization
                    .AddPortableFactory(MarkPortableFactoryV3.FactoryId, factoryMark)
                    .AddPortableFactory(StudentPortableFactory.FactoryId, factoryStudent)
                    .AddPortableFactory(SubjectPortableFactory.FactoryId, factorySubject)
                    .AddPortableFactory(ExamPortableFactory.FactoryId, factoryExam);
                await using var client = await HazelcastClientFactory.StartNewClientAsync(hazelcastOptions, cancellationToken: token).ConfigureAwait(false);
                var mapMark = await client.GetMapAsync<long, PMarkV3>(_mapMark).ConfigureAwait(false);


                //var result1 = await mapMark.GetValuesAsync().ConfigureAwait(false);
                //var res1 = result1.ToList();
                //int count1 = res1.Count;

                //var predicate1 = Predicates.Sql("markValue = 100");
                //var result2 = await mapMark.GetValuesAsync(predicate1).ConfigureAwait(false);
                //var res2 = result2.ToList();
                //int count2 = res2.Count;


                var predicate2 = Predicates.Sql("student.name = 'Ken Corkery'");

                var result3 = await mapMark.GetValuesAsync(predicate2).ConfigureAwait(false);
                var res3 = result3.ToList();
                int count3 = res3.Count;

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
FROM marksv3 m
JOIN studentsv3 s ON s.__key = m.StudentId
JOIN subjectsv3 sub ON sub.__key = m.SubjectId
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
FROM marksv3 m
JOIN studentsv3 s ON s.__key = m.StudentId
JOIN examsv3 e ON e.__key = m.ExamId
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
FROM marksv3 m
JOIN studentsv3 s ON s.__key = m.StudentId
JOIN examsv3 e ON e.__key = m.ExamId
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
FROM marksv3 m
JOIN studentsv3 s ON s.__key = m.StudentId
JOIN examsv3 e ON e.__key = m.ExamId
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
  FROM marksv3
  GROUP BY StudentId
) m
JOIN studentsv3 s ON s.__key = m.StudentId
JOIN (
  SELECT 
    MAX(ExamCount) AS MaxExamCount
  FROM (
    SELECT 
        COUNT(DISTINCT ExamId) AS ExamCount
    FROM marksv3
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
  FROM marksv3
  GROUP BY StudentId
) m
JOIN studentsv3 s ON s.__key = m.StudentId
JOIN (
  SELECT 
    MIN(ExamCount) AS MinExamCount
  FROM (
    SELECT 
        COUNT(DISTINCT ExamId) AS ExamCount
    FROM marksv3
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

        public async Task<int> CacheMarkListAsync(IEnumerable<Mark> markList, CancellationToken token = default)
        {
            try
            {
                var hazelcastOptions = new HazelcastOptionsBuilder().Build();
                var factory = new MarkPortableFactoryV3();
                hazelcastOptions.Serialization
                    .AddPortableFactory(MarkPortableFactoryV3.FactoryId, factory);

                await using var client = await HazelcastClientFactory.StartNewClientAsync(hazelcastOptions, cancellationToken: token).ConfigureAwait(false);
                var map = await client.GetMapAsync<long, PMarkV3>(_mapMark).ConfigureAwait(false);

                var marksDictionary = new Dictionary<long, PMarkV3>();

                foreach (var mark in markList)
                {
                    marksDictionary.Add(mark.Id, new PMarkV3
                    {
                        Id = mark.Id,
                        MarkValue = mark.MarkValue,
                        PStudent = new PStudent
                        {
                            Id = mark.Student.Id,
                            Name = mark.Student.Name,
                            RollNumber = mark.Student.RollNumber,
                            CreatedAt = mark.Student.CreatedAt,
                            ModifiedAt = mark.Student.ModifiedAt
                        },
                        PSubject = new PSubject
                        {
                            Id = mark.Subject.Id,
                            Name = mark.Subject.Name,
                            Description = mark.Subject.Description,
                            CreatedAt = mark.Subject.CreatedAt,
                            ModifiedAt = mark.Subject.ModifiedAt
                        },
                        PExam = new PExam
                        {
                            Id = mark.Exam.Id,
                            Name = mark.Exam.Name,
                            ExamDate = mark.Exam.ExamDate,
                            CreatedAt = mark.Exam.CreatedAt,
                            ModifiedAt = mark.Exam.ModifiedAt
                        },
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
