using Common;
using Common.Dto;
using Couchbase;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.Extensions.DependencyInjection;
using Couchbase.Query;
using Model;

namespace Cache
{
    public interface ICouchbaseStudentHelper : IStudentQueryService
    {
        Task<int> CacheStudentListAsync(IEnumerable<Student> studentList, CancellationToken token = default);
        Task<int> CacheSubjectListAsync(IEnumerable<Subject> subjectList, CancellationToken token = default);
        Task<int> CacheExamListAsync(IEnumerable<Exam> examList, CancellationToken token = default);
        Task<int> CacheMarkListAsync(IEnumerable<Mark> markList, CancellationToken token = default);
    }

    internal sealed class CouchbaseStudentHelper : ICouchbaseStudentHelper
    {
        private readonly IBucketProvider _bucketProvider;
        private const string _bucketName = "Academy";
        private const string _scopeName = "AcademyScope";

        private const string _collectionStudent = "Students";
        private const string _collectionSubject = "Subjects";
        private const string _collectionExam = "Exams";
        private const string _collectionMark = "Marks";

        public CouchbaseStudentHelper(IBucketProvider bucketProvider)
        {
            _bucketProvider = bucketProvider;
        }

        public async Task<IEnumerable<StudentSubjectMarksDto>> LoadSubjectWiseHighestMarksAndExamCountAsync(CancellationToken token = default)
        {
            var bucket = await _bucketProvider.GetBucketAsync(_bucketName).ConfigureAwait(false);
            var cluster = bucket.Cluster;
            var result = await cluster.QueryAsync<StudentSubjectMarksDto>(
                $@"SELECT
--s.id AS StudentId,
--ARRAY_AGG(s.name) AS StudentName,
MAX(s.name) AS StudnetName,
--sub.id AS SubjectId,
--ARRAY_AGG(sub.name) AS SubjectName,
MAX(sub.name) AS SubjectName,
--ARRAY_AGG(m.markValue) AS Marks,
--ARRAY_AGG(TONUMBER(m.examId)) AS ExamIds,
--ARRAY_LENGTH(ARRAY_AGG (DISTINCT m.examId)) AS ExamCount,
ARRAY_LENGTH(ARRAY_AGG (DISTINCT m.examId)) AS ExamCount,
MAX(m.markValue) AS HighestMark
FROM Academy.AcademyScope.Marks AS m
JOIN Academy.AcademyScope.Students AS s ON META(s).id = m.studentId
JOIN Academy.AcademyScope.Subjects AS sub ON m.subjectId = META(sub).id
GROUP BY s.id, sub.id
--LIMIT 20
", options =>
                {
                    options.ReadOnly(true);
                    options.CancellationToken(token);
                });
            if (result.MetaData?.Status is not QueryStatus.Success)
            {
                throw new CouchbaseException("Query Execution Error");
            }

            return await result.ToListAsync(cancellationToken: token).ConfigureAwait(false);
        }

        public async Task<IEnumerable<StudentSubjectMarksDto>> LoadTopPerformingStudentsBySubjectAsync(CancellationToken token = default)
        {
            var bucket = await _bucketProvider.GetBucketAsync(_bucketName).ConfigureAwait(false);
            var cluster = bucket.Cluster;
            var result = await cluster.QueryAsync<StudentSubjectMarksDto>(
                $@"SELECT MAX(s.name) AS StudentName ,
       MAX(sub.name) AS SubjectName,
       MAX(m.markValue) AS HighestMark,
       ARRAY_LENGTH(ARRAY_AGG(DISTINCT META(e).id)) AS ExamCount
FROM Academy.AcademyScope.Marks AS m
    JOIN Academy.AcademyScope.Students AS s ON META(s).id = m.studentId
    JOIN Academy.AcademyScope.Subjects AS sub ON META(sub).id = m.subjectId
    JOIN Academy.AcademyScope.Exams AS e ON META(e).id = m.examId
GROUP BY s.id
", options =>
                {
                    options.ReadOnly(true);
                    options.CancellationToken(token);
                });
            if (result.MetaData?.Status is not QueryStatus.Success)
            {
                throw new CouchbaseException("Query Execution Error");
            }

            return await result.ToListAsync(cancellationToken: token).ConfigureAwait(false);
        }

        public async Task<IEnumerable<StudentPerformanceDto>> LoadTopStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default)
        {
            var bucket = await _bucketProvider.GetBucketAsync(_bucketName).ConfigureAwait(false);
            var cluster = bucket.Cluster;
            var result = await cluster.QueryAsync<StudentPerformanceDto>(
                $@"SELECT
MAX(s.name) AS StudentName
,ROUND(AVG(m.markValue), 2) AS AverageMark
,COUNT(DISTINCT e.id) AS ExamCount
FROM Academy.AcademyScope.Marks AS m
JOIN Academy.AcademyScope.Students AS s ON META(s).id = m.studentId
JOIN Academy.AcademyScope.Exams AS e ON META(e).id = m.examId
GROUP BY s.id
ORDER BY AverageMark DESC
LIMIT {topCount}", options =>
                {
                    options.ReadOnly(true);
                    options.CancellationToken(token);
                });
            if (result.MetaData?.Status is not QueryStatus.Success)
            {
                throw new CouchbaseException("Query Execution Error");
            }

            return await result.ToListAsync(cancellationToken: token).ConfigureAwait(false);
        }

        public async Task<IEnumerable<StudentPerformanceDto>> LoadLowPerformingStudentsByAverageMarkAsync(int bottomCount = 1, CancellationToken token = default)
        {
            var bucket = await _bucketProvider.GetBucketAsync(_bucketName).ConfigureAwait(false);
            var cluster = bucket.Cluster;
            var result = await cluster.QueryAsync<StudentPerformanceDto>(
                $@"SELECT
MAX(s.name) AS StudentName
,ROUND(AVG(m.markValue), 2) AS AverageMark
,COUNT(DISTINCT e.id) AS ExamCount
FROM Academy.AcademyScope.Marks AS m
JOIN Academy.AcademyScope.Students AS s ON META(s).id = m.studentId
JOIN Academy.AcademyScope.Exams AS e ON META(e).id = m.examId
GROUP BY s.id
ORDER BY AverageMark ASC
LIMIT {bottomCount}", options =>
                {
                    options.ReadOnly(true);
                    options.CancellationToken(token);
                });
            if (result.MetaData?.Status is not QueryStatus.Success)
            {
                throw new CouchbaseException("Query Execution Error");
            }

            return await result.ToListAsync(cancellationToken: token).ConfigureAwait(false);
        }

        public Task<IEnumerable<StudentPerformanceDto>> LoadHighPerformingStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<StudentExamMarksDto>> LoadStudentsWithLowestMarksAsync(int numberOfStudents = 1, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public async Task<IEnumerable<StudentExamMarksDto>> LoadStudentsWithHighestMarksAsync(int numberOfStudents = 1, CancellationToken token = default)
        {
            try
            {
                var bucket = await _bucketProvider.GetBucketAsync(_bucketName).ConfigureAwait(false);
                var cluster = bucket.Cluster;
                var result = await cluster.QueryAsync<StudentExamMarksDto>(
@$"
", options =>
{
    //options.Timeout(TimeSpan.FromSeconds(15));
    options.Readonly(true);
    options.CancellationToken(token);
})
                .ConfigureAwait(false);

                if (result.MetaData?.Status is not QueryStatus.Success)
                {
                    throw new CouchbaseException("Query execution error");
                }

                return await result.ToListAsync(cancellationToken: token).ConfigureAwait(false);
            }
            catch (DocumentNotFoundException)
            {
                // cache miss - get value from permanent storage

                // repopulate cache so subsequent calls get cache hit
                throw;
            }
            catch (TimeoutException)
            {
                // propagate, since time budget's up
                throw;
            }
            catch (CouchbaseException)
            {
                // error performing insert
                throw;
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
                var bucket = await _bucketProvider.GetBucketAsync(_bucketName).ConfigureAwait(false);
                var collectionStudent = await bucket.CollectionAsync(_collectionStudent).ConfigureAwait(false);

                foreach (var student in studentList)
                {
                    await collectionStudent.InsertAsync(student.Id.ToString(), new CouchbaseStudentDto
                    {
                        id = student.Id.ToString(),
                        name = student.Name,
                        rollNumber = student.RollNumber,
                        createdAt = student.CreatedAt,
                        modifiedAt = student.ModifiedAt,
                    }).ConfigureAwait(false);
                }
                return 0;
            }
            catch (DocumentExistsException)
            {
                // handle exception
                throw;
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
                var bucket = await _bucketProvider.GetBucketAsync(_bucketName).ConfigureAwait(false);
                var collectionSubject = await bucket.CollectionAsync(_collectionSubject).ConfigureAwait(false);

                foreach (var subject in subjectList)
                {
                    await collectionSubject.InsertAsync(subject.Id.ToString(), new CouchbaseSubjectDto
                    {
                        id = subject.Id.ToString(),
                        name = subject.Name,
                        description = subject.Description,
                        createdAt = subject.CreatedAt,
                        modifiedAt = subject.ModifiedAt,
                    }).ConfigureAwait(false);
                }
                return 0;
            }
            catch (DocumentExistsException)
            {
                // handle exception
                throw;
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
                var bucket = await _bucketProvider.GetBucketAsync(_bucketName).ConfigureAwait(false);
                var collectionExam = await bucket.CollectionAsync(_collectionExam).ConfigureAwait(false);

                foreach (var exam in examList)
                {
                    await collectionExam.InsertAsync(exam.Id.ToString(), new CouchbaseExamDto
                    {
                        id = exam.Id.ToString(),
                        name = exam.Name,
                        examDate = exam.ExamDate,
                        createdAt = exam.CreatedAt,
                        modifiedAt = exam.ModifiedAt,
                    }).ConfigureAwait(false);
                }
                return 0;
            }
            catch (DocumentExistsException)
            {
                // handle exception
                throw;
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
                var bucket = await _bucketProvider.GetBucketAsync(_bucketName).ConfigureAwait(false);
                var collectionMark = await bucket.CollectionAsync(_collectionMark).ConfigureAwait(false);

                foreach (var mark in markList)
                {
                    await collectionMark.InsertAsync(mark.Id.ToString(),
                        new CouchbaseMarkDto
                        {
                            id = mark.Id.ToString(),
                            markValue = mark.MarkValue,
                            examId = mark.Exam.Id.ToString(),
                            studentId = mark.Student.Id.ToString(),
                            subjectId = mark.Subject.Id.ToString(),
                            createdAt = mark.CreatedAt,
                            modifiedAt = mark.ModifiedAt,
                        }).ConfigureAwait(false);
                }
                return 0;
            }
            catch (DocumentExistsException)
            {
                // handle exception
                throw;
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}
