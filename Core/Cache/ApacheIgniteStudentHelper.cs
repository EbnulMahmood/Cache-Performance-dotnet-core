using Apache.Ignite.Core.Cache.Query;
using Cache.ApacheIgnite;
using Common;
using Common.Dto;
using Model;
using System.Reflection;
using CacheEntity = ApacheIgnite.Entity;

namespace Cache;

public interface IIgniteStudentCacheHelper : IStudentQueryServiceAsync
{
    Task<int> CacheStudentListAsync(IEnumerable<Student> studentList, CancellationToken token = default);
    Task<int> CacheSubjectListAsync(IEnumerable<Subject> subjectList, CancellationToken token = default);
    Task<int> CacheExamListAsync(IEnumerable<Exam> examList, CancellationToken token = default);
    Task<int> CacheMarkListAsync(IEnumerable<Mark> markList, CancellationToken token = default);
}

internal sealed class IgniteStudentCacheHelper : IIgniteStudentCacheHelper
{
    private readonly IIgniteCacheServicce _cacheService;
    private readonly string _examCacheName;
    private readonly string _markCacheName;
    private readonly string _studentCacheName;
    private readonly string _subjectCacheName;

    public IgniteStudentCacheHelper(IIgniteCacheServicce cacheService)
    {
        _cacheService = cacheService;
        _examCacheName = _cacheService.GetOrCreateCache<long, CacheEntity.Exam>(CacheEntity.Exam.GetExamCacheConfiguration()).Name;
        _markCacheName = _cacheService.GetOrCreateCache<long, CacheEntity.Mark>(CacheEntity.Mark.GetMarksCacheConfiguration()).Name;
        _studentCacheName = _cacheService.GetOrCreateCache<long, CacheEntity.Student>(CacheEntity.Student.GetStudentCacheConfiguration()).Name;
        _subjectCacheName = _cacheService.GetOrCreateCache<long, CacheEntity.Subject>(CacheEntity.Subject.GetSubjectCacheConfiguration()).Name;
    }

    public async Task<int> CacheExamListAsync(IEnumerable<Exam> examList, CancellationToken token = default)
    {
        try
        {
            var key = await _cacheService.GetCacheSizeAsync<long, CacheEntity.Exam>(_examCacheName);
            key = key + 1;
            var examDictionary = new Dictionary<long, CacheEntity.Exam>();
            int batch = 500;
            int afterBatch = examList.Count();

            while (afterBatch > 0)
            {
                foreach (var exam in examList)
                {
                    var cacheExam = new CacheEntity.Exam { Id = exam.Id, Name = exam.Name, ExamDate = exam.ExamDate, CreatedAt = exam.CreatedAt, ModifiedAt = exam.ModifiedAt };
                    examDictionary.Add(key, cacheExam);
                    key = key + 1;

                    if (examDictionary.Count == batch)
                    {
                        await _cacheService.PutAllAsync(_examCacheName, examDictionary);
                        examDictionary.Clear();
                        afterBatch = afterBatch - batch;
                    }
                }

                if (examDictionary.Count > 0)
                {
                    await _cacheService.PutAllAsync(_examCacheName, examDictionary);
                    afterBatch = afterBatch - examDictionary.Count;
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            throw;
        }
    }

    public async Task<int> CacheMarkListAsync(IEnumerable<Mark> markList, CancellationToken token = default)
    {
        try
        {
            var key = await _cacheService.GetCacheSizeAsync<long, CacheEntity.Mark>(_markCacheName);
            key = key + 1;
            var markDictionary = new Dictionary<long, CacheEntity.Mark>();
            int batch = 500;
            int afterBatch = markList.Count();

            while (afterBatch > 0)
            {
                foreach (var mark in markList)
                {
                    var cacheMark = new CacheEntity.Mark { Id = mark.Id, MarkValue = mark.MarkValue, StudentId = mark.Student.Id, SubjectId = mark.Subject.Id, ExamId = mark.Exam.Id, CreatedAt = mark.CreatedAt, ModifiedAt = mark.ModifiedAt };
                    markDictionary.Add(key, cacheMark);
                    key = key + 1;

                    if (markDictionary.Count == batch)
                    {
                        await _cacheService.PutAllAsync(_markCacheName, markDictionary);
                        markDictionary.Clear();
                        afterBatch = afterBatch - batch;
                    }
                }

                if (markDictionary.Count > 0)
                {
                    await _cacheService.PutAllAsync(_markCacheName, markDictionary);
                    afterBatch = afterBatch - markDictionary.Count;
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            throw;
        }
    }

    public async Task<int> CacheStudentListAsync(IEnumerable<Student> studentList, CancellationToken token = default)
    {
        try
        {
            var key = await _cacheService.GetCacheSizeAsync<long, CacheEntity.Student>(_studentCacheName);
            key = key + 1;
            var studentDictionary = new Dictionary<long, CacheEntity.Student>();
            int batch = 500;
            int afterBatch = studentList.Count();

            while (afterBatch > 0)
            {
                foreach (var student in studentList)
                {
                    var cacheStudent = new CacheEntity.Student { Id = student.Id, Name = student.Name, RollNumber = student.RollNumber, CreatedAt = student.CreatedAt, ModifiedAt = student.ModifiedAt };
                    studentDictionary.Add(key, cacheStudent);
                    key = key + 1;

                    if (studentDictionary.Count == batch)
                    {
                        await _cacheService.PutAllAsync(_studentCacheName, studentDictionary);
                        studentDictionary.Clear();
                        afterBatch = afterBatch - batch;
                    }
                }

                if (studentDictionary.Count > 0)
                {
                    await _cacheService.PutAllAsync(_studentCacheName, studentDictionary);
                    afterBatch = afterBatch - studentDictionary.Count;
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            throw;
        }
    }

    public async Task<int> CacheSubjectListAsync(IEnumerable<Subject> subjectList, CancellationToken token = default)
    {
        try
        {
            var key = await _cacheService.GetCacheSizeAsync<long, CacheEntity.Subject>(_subjectCacheName);
            key = key + 1;
            var subjectDictionary = new Dictionary<long, CacheEntity.Subject>();
            int batch = 500;
            int afterBatch = subjectList.Count();

            while (afterBatch > 0)
            {
                foreach (var subject in subjectList)
                {
                    var cacheSubject = new CacheEntity.Subject { Id = subject.Id, Name = subject.Name, Descripion = subject.Description, CreatedAt = subject.CreatedAt, ModifiedAt = subject.ModifiedAt };
                    subjectDictionary.Add(key, cacheSubject);
                    key = key + 1;

                    if (subjectDictionary.Count == batch)
                    {
                        await _cacheService.PutAllAsync(_subjectCacheName, subjectDictionary);
                        subjectDictionary.Clear();
                        afterBatch = afterBatch - batch;
                    }
                }

                if (afterBatch > 0)
                {
                    await _cacheService.PutAllAsync(_subjectCacheName, subjectDictionary);
                    afterBatch = afterBatch - subjectDictionary.Count;
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            throw;
        }
    }

    public async Task<IEnumerable<StudentPerformanceDto>> LoadHighPerformingStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default)
    {
        try
        {
            string query = $@"
SELECT
    s.Name AS StudentName,
    ROUND(AVG(m.MarkValue), 2) AS AverageMark,
    CAST(COUNT(DISTINCT e.Id) AS INT) AS ExamCount
FROM Mark m
INNER JOIN ""{_studentCacheName}"".Student s ON s.Id = m.StudentId
INNER JOIN ""{_examCacheName}"".Exam e ON e.Id = m.ExamId
GROUP BY s.Id, s.Name
ORDER BY AverageMark DESC
OFFSET 0 ROWS FETCH NEXT {topCount} ROWS ONLY
";

            var result = await Task.Run(() => _cacheService.ExecuteQuery<long, CacheEntity.Mark>(_markCacheName, query));
            var list = new List<StudentPerformanceDto>();

            if (result != null)
                list = FieldsQueryCursorToList<StudentPerformanceDto>(result);

            return list;
        }
        catch (Exception ex)
        {
            throw;
        }
    }

    public async Task<IEnumerable<StudentPerformanceDto>> LoadLowPerformingStudentsByAverageMarkAsync(int bottomCount = 1, CancellationToken token = default)
    {
        try
        {
            string query = $@"
SELECT 
    s.Name AS StudentName,
    ROUND(AVG(m.MarkValue), 2) AS AverageMark,
    CAST(COUNT(DISTINCT e.Id) AS INT) AS ExamCount
FROM Mark m
INNER JOIN ""{_studentCacheName}"".Student s ON s.Id = m.StudentId
INNER JOIN ""{_examCacheName}"".Exam e ON e.Id = m.ExamId
GROUP BY s.Id, s.Name
ORDER BY AverageMark ASC
OFFSET 0 ROWS FETCH NEXT {bottomCount} ROWS ONLY
";

            var result = await Task.Run(() => _cacheService.ExecuteQuery<long, CacheEntity.Mark>(_markCacheName, query));

            var list = new List<StudentPerformanceDto>();
            if (result != null)
                list = FieldsQueryCursorToList<StudentPerformanceDto>(result);

            return list;
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
            string query = $@"
SELECT 
    s.Name
    ,s.RollNumber
    ,CAST(m.ExamCount AS INT) AS ExamCount
    ,ROUND(m.TotalMarks, 2) AS TotalMarks
FROM ( 
        SELECT 
            StudentId, COUNT(DISTINCT ExamId) AS ExamCount
            ,SUM(MarkValue) AS TotalMarks 
        FROM ""{_markCacheName}"".Mark GROUP BY StudentId 
    ) m JOIN ""{_studentCacheName}"".Student s ON s.Id = m.StudentId 
JOIN ( 
        SELECT 
            MIN(ExamCount) AS MinExamCount 
        FROM ( 
                SELECT 
                    COUNT(DISTINCT ExamId) AS ExamCount 
                FROM ""{_markCacheName}"".Mark
                GROUP BY StudentId 
            ) subq 
    ) minq ON m.ExamCount = minq.MinExamCount 
ORDER BY m.TotalMarks DESC
LIMIT {numberOfStudents}
";

            var result = await Task.Run(() => _cacheService.ExecuteQuery<long, CacheEntity.Mark>(_markCacheName, query));

            var list = new List<StudentExamMarksDto>();
            if (result != null)
                list = FieldsQueryCursorToList<StudentExamMarksDto>(result);

            return list;
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
            string query = $@"
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
        FROM ""{_markCacheName}"".Mark GROUP BY StudentId
    ) m JOIN ""{_studentCacheName}"".Student s ON s.Id = m.StudentId 
JOIN ( 
        SELECT 
            MAX(ExamCount) AS MaxExamCount
        FROM ( 
                SELECT 
                    COUNT(DISTINCT ExamId) AS ExamCount
                FROM ""{_markCacheName}"".Mark GROUP BY StudentId 
            ) subq 
    ) maxq ON m.ExamCount = maxq.MaxExamCount 
ORDER BY m.TotalMarks ASC LIMIT {numberOfStudents}
";

            var result = await Task.Run(() => _cacheService.ExecuteQuery<long, CacheEntity.Mark>(_markCacheName, query));

            var list = new List<StudentExamMarksDto>();
            if (result != null)
                list = FieldsQueryCursorToList<StudentExamMarksDto>(result);

            return list;
        }
        catch (Exception)
        {
            throw;
        }
    }

    public async Task<IEnumerable<StudentSubjectMarksDto>> LoadSubjectWiseHighestMarksAndExamCountAsync(CancellationToken token = default)
    {
        try
        {
            string query = $@"
SELECT 
    s.Name AS StudentName
    ,sub.Name AS SubjectName
    ,MAX(m.MarkValue) AS HighestMark
    ,CAST(COUNT(DISTINCT m.ExamId) AS INT) AS ExamCount
FROM Mark m
INNER JOIN ""{_studentCacheName}"".Student s ON s.Id = m.StudentId
INNER JOIN ""{_subjectCacheName}"".Subject sub ON m.SubjectId = sub.Id
GROUP BY s.Id, s.Name, sub.Id, sub.Name
ORDER BY s.Name, sub.Name
";

            var result = await Task.Run(() => _cacheService.ExecuteQuery<long, CacheEntity.Mark>(_markCacheName, query));

            var list = new List<StudentSubjectMarksDto>();
            if (result != null)
                list = FieldsQueryCursorToList<StudentSubjectMarksDto>(result);

            return list;
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
            string query = $@"
SELECT
    s.Name AS StudentName,
    sub.Name AS SubjectName,
    MAX(m.MarkValue) AS HighestMark,
    CAST(COUNT(DISTINCT e.Id) AS INT) AS ExamCount
FROM Mark m
INNER JOIN ""{_studentCacheName}"".Student s ON s.Id = m.StudentId
INNER JOIN ""{_subjectCacheName}"".Subject sub ON sub.Id = m.SubjectId
INNER JOIN ""{_examCacheName}"".Exam e ON e.Id = m.ExamId
GROUP BY s.Id, s.Name, sub.Id, sub.Name
HAVING MAX(m.MarkValue) = 100
";

            var result = await Task.Run(() => _cacheService.ExecuteQuery<long, CacheEntity.Mark>(_markCacheName, query));

            var list = new List<StudentSubjectMarksDto>();
            if (result != null)
                list = FieldsQueryCursorToList<StudentSubjectMarksDto>(result);

            return list;
        }
        catch (Exception ex)
        {
            throw;
        }
    }

    public async Task<IEnumerable<StudentPerformanceDto>> LoadTopStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default)
    {
        try
        {
            string query = $@"
SELECT 
    s.Name AS StudentName
    ,ROUND(AVG(m.MarkValue), 2) AS AverageMark
    ,CAST(COUNT(DISTINCT e.Id) AS INT) AS ExamCount
FROM Mark m
JOIN ""{_studentCacheName}"".Student s ON s.Id = m.StudentId
JOIN ""{_examCacheName}"".Exam e ON e.Id = m.ExamId
GROUP BY s.Id, s.Name
ORDER BY AverageMark DESC
OFFSET 0 ROWS FETCH NEXT {topCount} ROWS ONLY
";

            var result = await Task.Run(() => _cacheService.ExecuteQuery<long, CacheEntity.Mark>(_markCacheName, query));

            var list = new List<StudentPerformanceDto>();
            if (result != null)
                list = FieldsQueryCursorToList<StudentPerformanceDto>(result);

            return list;
        }
        catch (Exception ex)
        {
            throw;
        }
    }

    private List<T> FieldsQueryCursorToList<T>(IFieldsQueryCursor objects)
    {
        var result = new List<T>();
        var type = typeof(T);
        var fields = GetPropertyNames(type);

        foreach (var item in objects)
        {
            var obj = GetInstance(type);

            if (obj == null)
                throw new NullReferenceException();

            if (fields.Count != item.Count)
                throw new Exception("Invalid object..!");

            for (int i = 0; i < item.Count; i++)
            {
                var propertyName = fields[i];
                type.GetProperty(propertyName)!.SetValue(obj, item[i]);
            }

            result.Add((T)obj);
        }

        return result;
    }

    private object? GetInstance(Type type)
    {
        ConstructorInfo? constructor = type.GetConstructor(new Type[] { });

        return (constructor == null) ? null : constructor.Invoke(new object[] { });
    }

    private List<string> GetPropertyNames(Type type)
    {
        var result = new List<string>();
        var fields = type.GetProperties();
        foreach (var field in fields)
        {
            var name = field.Name;
            result.Add(name);
        }

        return result;
    }
}