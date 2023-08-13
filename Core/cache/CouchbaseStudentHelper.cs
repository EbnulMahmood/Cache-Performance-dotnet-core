using Common;
using Common.Dto;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.Extensions.DependencyInjection;
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

        public Task<IEnumerable<StudentExamMarksDto>> LoadStudentsWithHighestMarksAsync(int numberOfStudents = 1, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public async Task<int> CacheStudentListAsync(IEnumerable<Student> studentList, CancellationToken token = default)
        {
            try
            {
                var bucket = await _bucketProvider.GetBucketAsync(_bucketName).ConfigureAwait(false);
                var collectionStudent = await bucket.CollectionAsync(_collectionStudent).ConfigureAwait(false);

                foreach (var student in studentList)
                {
                    await collectionStudent.InsertAsync(student.Id.ToString(), student).ConfigureAwait(false);
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
                    await collectionSubject.InsertAsync(subject.Id.ToString(), subject).ConfigureAwait(false);
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
                    await collectionExam.InsertAsync(exam.Id.ToString(), exam).ConfigureAwait(false);
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
                        new MarkDto
                        {
                            Id = mark.Id,
                            MarkValue = mark.MarkValue,
                            ExamId = mark.Exam.Id,
                            StudentId = mark.Student.Id,
                            SubjectId = mark.Subject.Id,
                            CreatedAt = mark.CreatedAt,
                            ModifiedAt = mark.ModifiedAt,
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
