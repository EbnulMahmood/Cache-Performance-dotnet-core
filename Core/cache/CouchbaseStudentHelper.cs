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
        private const string _bucketName = "Demo";

        private const string _collectionStudent = "students";
        private const string _collectionSubject = "subjects";
        private const string _collectionExam = "exams";
        private const string _collectionMark = "marks";

        public CouchbaseStudentHelper(IBucketProvider bucketProvider)
        {
            _bucketProvider = bucketProvider;
        }

        public Task<IEnumerable<StudentSubjectMarksDto>> LoadSubjectWiseHighestMarksAndExamCountAsync(CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<StudentSubjectMarksDto>> LoadTopPerformingStudentsBySubjectAsync(CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<StudentPerformanceDto>> LoadTopStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<StudentPerformanceDto>> LoadLowPerformingStudentsByAverageMarkAsync(int bottomCount = 1, CancellationToken token = default)
        {
            throw new NotImplementedException();
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
