using Cache.Hazelcast.Portable;
using Common;
using Common.Dto;
using Hazelcast;
using Hazelcast.Models;
using Hazelcast.Query;
using Model;
using System.Security.Cryptography;

namespace Cache
{
    public interface IHazelcastStudentHelperV5 : IStudentQueryServiceAsync
    {
        Task<int> CacheMarkListAsync(IEnumerable<MergedMark> mergedMarkList, CancellationToken token = default);
    }

    internal sealed class HazelcastStudentHelperV5 : IHazelcastStudentHelperV5
    {
        private readonly HazelcastOptions _options;
        private const string _mapStudent = "studentsv5";
        private const string _mapSubject = "subjectsv5";
        private const string _mapExam = "examsv5";
        private const string _mapMark = "MergedMarks";

        public HazelcastStudentHelperV5(HazelcastOptions options)
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
    mm.StudentName AS StudentName
    ,mm.SubjectName AS SubjectName
    ,MAX(mm.MarkValue) AS HighestMark
    ,COUNT(DISTINCT mm.ExamName) AS ExamCount
FROM {_mapMark} mm
GROUP BY mm.StudentName, mm.SubjectName
ORDER BY mm.StudentName, mm.SubjectName", cancellationToken: token);

                studentSubjectMarksDtoList = await result.Select(row =>
                    new StudentSubjectMarksDto
                    {
                        StudentName = row.GetColumn<string>("StudentName"),
                        SubjectName = row.GetColumn<string>("SubjectName"),
                        HighestMark = row.GetColumn<double>("HighestMark"),
                        ExamCount = (int)row.GetColumn<long>("ExamCount"),
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
    mm.StudentName AS StudentName,
    mm.SubjectName AS SubjectName,
    MAX(mm.MarkValue) AS HighestMark,
    COUNT(DISTINCT mm.ExamName) AS ExamCount
FROM {_mapMark} mm
GROUP BY mm.StudentName, mm.SubjectName
HAVING MAX(mm.MarkValue) = 100", cancellationToken: token);

                studentSubjectMarksDtoList = await result.Select(row =>
                    new StudentSubjectMarksDto
                    {
                        StudentName = row.GetColumn<string>("StudentName"),
                        SubjectName = row.GetColumn<string>("SubjectName"),
                        HighestMark = row.GetColumn<double>("HighestMark"),
                        ExamCount = (int)row.GetColumn<long>("ExamCount"),
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
    mm.StudentName AS StudentName
    ,ROUND(AVG(mm.MarkValue), 2) AS AverageMark
    ,COUNT(DISTINCT mm.ExamName) AS ExamCount
FROM {_mapMark} mm
GROUP BY mm.StudentName
ORDER BY AverageMark DESC
LIMIT ?", cancellationToken: token, parameters: topCount);

                studentPerformanceDtoList = await result.Select(row =>
                    new StudentPerformanceDto
                    {
                        StudentName = row.GetColumn<string>("StudentName"),
                        AverageMark = row.GetColumn<double>("AverageMark"),
                        ExamCount = (int)row.GetColumn<long>("ExamCount"),
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
    mm.StudentName AS StudentName,
    ROUND(AVG(mm.MarkValue), 2) AS AverageMark,
    COUNT(DISTINCT mm.ExamName) AS ExamCount
FROM {_mapMark} mm
GROUP BY mm.StudentName
ORDER BY AverageMark ASC
LIMIT ?", cancellationToken: token, parameters: bottomCount);

                studentPerformanceDtoList = await result.Select(row =>
                    new StudentPerformanceDto
                    {
                        StudentName = row.GetColumn<string>("StudentName"),
                        AverageMark = row.GetColumn<double>("AverageMark"),
                        ExamCount = (int)row.GetColumn<long>("ExamCount"),
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
    mm.StudentName AS StudentName,
    ROUND(AVG(mm.MarkValue), 2) AS AverageMark,
    COUNT(DISTINCT mm.ExamName) AS ExamCount
FROM {_mapMark} mm
GROUP BY mm.StudentName
ORDER BY AverageMark DESC
LIMIT ?", cancellationToken: token, parameters: topCount);

                studentPerformanceDtoList = await result.Select(row =>
                    new StudentPerformanceDto
                    {
                        StudentName = row.GetColumn<string>("StudentName"),
                        AverageMark = row.GetColumn<double>("AverageMark"),
                        ExamCount = (int)row.GetColumn<long>("ExamCount"),
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
  FROM marksv5
  GROUP BY StudentId
) m
JOIN studentsv5 s ON s.__key = m.StudentId
JOIN (
  SELECT 
    MAX(ExamCount) AS MaxExamCount
  FROM (
    SELECT 
        COUNT(DISTINCT ExamId) AS ExamCount
    FROM marksv5
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
  FROM marksv5
  GROUP BY StudentId
) m
JOIN studentsv5 s ON s.__key = m.StudentId
JOIN (
  SELECT 
    MIN(ExamCount) AS MinExamCount
  FROM (
    SELECT 
        COUNT(DISTINCT ExamId) AS ExamCount
    FROM marksv5
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

        public async Task<int> CacheMarkListAsync(IEnumerable<MergedMark> mergedMarkList, CancellationToken token = default)
        {
            try
            {
                var obj = new PMarkV5();

                var hazelcastOptions = new HazelcastOptionsBuilder().Build();
                var factory = new MarkPortableFactoryV5();
                hazelcastOptions.Serialization
                    .AddPortableFactory(MarkPortableFactoryV5.FactoryId, factory);

                await using var client = await HazelcastClientFactory.StartNewClientAsync(hazelcastOptions, cancellationToken: token).ConfigureAwait(false);

                await client.Sql.ExecuteCommandAsync($@"
CREATE OR REPLACE MAPPING 
{_mapMark} (
  __key BIGINT,
  {nameof(obj.MarkValue)} DOUBLE,
  {nameof(obj.StudentName)} VARCHAR,
  {nameof(obj.StudentRollNumber)} VARCHAR,
  {nameof(obj.SubjectName)} VARCHAR,
  {nameof(obj.SubjectDescription)} VARCHAR,
  {nameof(obj.ExamName)} VARCHAR,
  {nameof(obj.ExamDate)} TIMESTAMP WITH TIME ZONE,
  {nameof(obj.CreatedAt)} TIMESTAMP WITH TIME ZONE,
  {nameof(obj.ModifiedAt)} TIMESTAMP WITH TIME ZONE
) TYPE IMap OPTIONS (
  'keyFormat'='bigint',
  'valueFormat'='json-flat'
)", cancellationToken: token).ConfigureAwait(false);

                var map = await client.GetMapAsync<long, PMarkV5>(_mapMark).ConfigureAwait(false);

                // Create a sorted index on the __key attribute
                await map.AddIndexAsync(IndexType.Sorted, "__key").ConfigureAwait(false);

                // Create a sorted index on the MarkValue attribute
                await map.AddIndexAsync(IndexType.Sorted, nameof(obj.MarkValue)).ConfigureAwait(false);

                // Create a hash index on the StudentName attribute
                await map.AddIndexAsync(IndexType.Hashed, nameof(obj.StudentName)).ConfigureAwait(false);

                // Create a hash index on the StudentRollNumber attribute
                await map.AddIndexAsync(IndexType.Hashed, nameof(obj.StudentRollNumber)).ConfigureAwait(false);

                // Create a hash index on the SubjectName attribute
                await map.AddIndexAsync(IndexType.Hashed, nameof(obj.SubjectName)).ConfigureAwait(false);

                // Create a hash index on the ExamName attribute
                await map.AddIndexAsync(IndexType.Hashed, nameof(obj.ExamName)).ConfigureAwait(false);

                // Create a sorted index on the ExamDate attribute
                await map.AddIndexAsync(IndexType.Sorted, nameof(obj.ExamDate)).ConfigureAwait(false);

                // Create a sorted index on the CreatedAt attribute
                await map.AddIndexAsync(IndexType.Sorted, nameof(obj.CreatedAt)).ConfigureAwait(false);

                // Create a sorted index on the ModifiedAt attribute
                await map.AddIndexAsync(IndexType.Sorted, nameof(obj.ModifiedAt)).ConfigureAwait(false);

                var marksDictionary = new Dictionary<long, PMarkV5>();

                foreach (var mark in mergedMarkList)
                {
                    marksDictionary.Add(mark.Id, new PMarkV5
                    {
                        MarkValue = mark.MarkValue,
                        StudentName = mark.StudentName,
                        StudentRollNumber = mark.StudentRollNumber,
                        SubjectName = mark.SubjectName,
                        SubjectDescription = mark.SubjectDescription,
                        ExamName = mark.ExamName,
                        ExamDate = mark.ExamDate,
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
