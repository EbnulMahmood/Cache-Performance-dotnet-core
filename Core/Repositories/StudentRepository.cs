using Microsoft.EntityFrameworkCore;
using Model;

namespace Repositories
{
    public interface IStudentRepository
    {
        Task<IEnumerable<Exam>> LoadExamListAsync(CancellationToken token = default);
        Task<IEnumerable<Mark>> LoadMarkListAsync(CancellationToken token = default);
        Task<IEnumerable<Student>> LoadStudentListAsync(CancellationToken token = default);
        Task<IEnumerable<Subject>> LoadSubjectListAsync(CancellationToken token = default);
        Task SaveExamListAsync(List<Exam> examList, CancellationToken token = default);
        Task SaveMarkListAsync(List<Mark> markList, CancellationToken token = default);
        Task SaveStudentListAsync(List<Student> studentList, CancellationToken token = default);
        Task SaveSubjectListAsync(List<Subject> subjectList, CancellationToken token = default);
    }

    internal sealed class StudentRepository : IStudentRepository
    {
        private readonly AppDbContext _dbContext;

        public StudentRepository(AppDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public async Task SaveStudentListAsync(List<Student> studentList, CancellationToken token = default)
        {
            try
            {
                _dbContext.Students.AddRange(studentList);
                await _dbContext.SaveChangesAsync(token);
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
                _dbContext.Subjects.AddRange(subjectList);
                await _dbContext.SaveChangesAsync(token);
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
                _dbContext.Exams.AddRange(examList);
                await _dbContext.SaveChangesAsync(token);
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
                _dbContext.Marks.AddRange(markList);
                await _dbContext.SaveChangesAsync(token);
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
                return await _dbContext.Students.ToListAsync(token);
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
                return await _dbContext.Subjects.ToListAsync(token);
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
                return await _dbContext.Exams.ToListAsync(token);
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
                return await _dbContext.Marks.ToListAsync(token);
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}
