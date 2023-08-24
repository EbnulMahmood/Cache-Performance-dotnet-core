using Common;
using Common.Dto;
using Couchbase.Extensions.DependencyInjection;
using Couchbase.Management.Users;
using Microsoft.Extensions.Configuration;
using Microsoft.Identity.Client;
using Model;
using Newtonsoft.Json.Linq;
using SingleStoreConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using static Couchbase.Core.Diagnostics.Tracing.OuterRequestSpans.ManagerSpan;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;

namespace Cache
{
    public interface ISingleStoreHelperV2
    {
        Task<int> CacheMergedMarkListAsync(IEnumerable<MergedMark> markList, CancellationToken token = default);
        Task<List<StudentSubjectMarksStoreDto>> LoadSubjectWiseHighestMarksAndExamCountAsync(CancellationToken token = default);
        Task<List<StudentPerformanceStoreDto>> LoadHighPerformingStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default);
        Task<List<StudentPerformanceStoreDto>> LoadLowPerformingStudentsByAverageMarkAsync(int bottomCount = 1, CancellationToken token = default);
        Task<List<StudentExamMarksStoreDto>> LoadStudentsWithHighestMarksAsync(int numberOfStudents = 1, CancellationToken token = default);
        Task<List<StudentExamMarksStoreDto>> LoadStudentsWithLowestMarksAsync(int numberOfStudents = 1, CancellationToken token = default);
        Task<List<StudentSubjectMarksStoreDto>> LoadTopPerformingStudentsBySubjectAsync(CancellationToken token = default);
        Task<List<StudentPerformanceStoreDto>> LoadTopStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default);
    }
    public class SingleStoreHelperV2 : ISingleStoreHelperV2
    {
        private readonly IConfiguration _configuration;
        private IDbCommand dbCommand;
        private string insertCommand;
        public const int batchSize = 5000;
        public SingleStoreHelperV2(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        #region Seed Data
        public async Task<int> CacheMergedMarkListAsync(IEnumerable<MergedMark> markList, CancellationToken token = default)
        {
            var markLists = markList.ToList();
            string[] batch = new string[batchSize];
            string table = "MergedMarks";
            int totalRows = 0;
            try
            {
                for (int j = 0; j < 200; j++)
                {
                    int i = 0;
                    int value = j * batchSize;
                    for (int k = 0; k < batchSize; k++)
                    {
                        batch[i] = $"({markLists[value].Id}, {markLists[value].MarkValue}, \"{markLists[value].StudentName}\", \"{markLists[value].StudentRollNumber}\", \"{markLists[value].SubjectName}\", \"{markLists[value].SubjectDescription}\", \"{markLists[value].ExamName}\", '{markLists[value].ExamDate.DateTime.ToString("yyyy-MM-dd 00:00:00")}', '{markLists[value].CreatedAt.DateTime.ToString("yyyy-MM-dd 00:00:00")}', '{markLists[value].ModifiedAt.DateTime.ToString("yyyy-MM-dd 00:00:00")}')";
                        i++;
                        value++;
                    }
                    insertCommand = $"INSERT INTO {table} (Id, MarkValue, StudentName, StudentRollNumber, SubjectName, SubjectDescription, ExamName,ExamDate,CreatedAt, ModifiedAt) VALUES {string.Join(",", batch)}";

                    using (var con = new SingleStoreConnection())
                    {
                        con.ConnectionString = GetConnectionStringValue();
                        await con.OpenAsync();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = insertCommand;
                            totalRows += await dbCommand.ExecuteNonQueryAsync();
                        }
                    }
                }
                return totalRows;
            }
            catch (Exception)
            {
                throw;
            }
        }

        #endregion

        #region Load Data
        public async Task<List<StudentPerformanceStoreDto>> LoadHighPerformingStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default)
        {
            try
            {
                var query = $@"
SELECT 
s.Name AS StudentName,
ROUND(AVG(m.MarkValue), 2) AS AverageMark,
COUNT(DISTINCT e.Id) AS ExamCount
FROM Marks m
JOIN Students s ON s.Id = m.StudentId
JOIN Exams e ON e.Id = m.ExamId
GROUP BY s.Id, s.Name
ORDER BY AverageMark DESC
LIMIT {topCount}
";
                using (var con = new SingleStoreConnection())
                {
                    con.ConnectionString = GetConnectionStringValue();
                    await con.OpenAsync();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = query;
                        var readerObj = await dbCommand.ExecuteReaderAsync();
                        var list = DataReaderMapToList<StudentPerformanceStoreDto>(readerObj);
                        return list;
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<List<StudentPerformanceStoreDto>> LoadLowPerformingStudentsByAverageMarkAsync(int bottomCount = 1, CancellationToken token = default)
        {
            try
            {
                var query = $@"
SELECT 
s.Name AS StudentName,
ROUND(AVG(m.MarkValue), 2) AS AverageMark,
COUNT(DISTINCT e.Id) AS ExamCount
FROM Marks m
JOIN Students s ON s.Id = m.StudentId
JOIN Exams e ON e.Id = m.ExamId
GROUP BY s.Id, s.Name
ORDER BY AverageMark ASC
LIMIT {bottomCount}
";
                using (var con = new SingleStoreConnection())
                {
                    con.ConnectionString = GetConnectionStringValue();
                    await con.OpenAsync();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = query;
                        var readerObj = await dbCommand.ExecuteReaderAsync();
                        var list = DataReaderMapToList<StudentPerformanceStoreDto>(readerObj);
                        return list;
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<List<StudentExamMarksStoreDto>> LoadStudentsWithHighestMarksAsync(int numberOfStudents = 1, CancellationToken token = default)
        {
            try
            {
                var query = $@"
SELECT 
 s.Name
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
ORDER BY SUM(m.MarkValue) DESC
LIMIT {numberOfStudents}
";
                using (var con = new SingleStoreConnection())
                {
                    con.ConnectionString = GetConnectionStringValue();
                    await con.OpenAsync();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = query;
                        var readerObj = await dbCommand.ExecuteReaderAsync();
                        var list = DataReaderMapToList<StudentExamMarksStoreDto>(readerObj);
                        return list;
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<List<StudentExamMarksStoreDto>> LoadStudentsWithLowestMarksAsync(int numberOfStudents = 1, CancellationToken token = default)
        {
            try
            {
                var query = $@"
SELECT 
s.Name
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
ORDER BY SUM(m.MarkValue) ASC
LIMIT {numberOfStudents}
";
                using (var con = new SingleStoreConnection())
                {
                    con.ConnectionString = GetConnectionStringValue();
                    await con.OpenAsync();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = query;
                        var readerObj = await dbCommand.ExecuteReaderAsync();
                        var list = DataReaderMapToList<StudentExamMarksStoreDto>(readerObj);
                        return list;
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<List<StudentSubjectMarksStoreDto>> LoadSubjectWiseHighestMarksAndExamCountAsync(CancellationToken token = default)
        {
            try
            {
                var query = $@"
SELECT 
s.Name AS StudentName
,sub.Name AS SubjectName
,MAX(m.MarkValue) AS HighestMark
,COUNT(DISTINCT m.ExamId) AS ExamCount
FROM Marks m
JOIN Students s ON s.Id = m.StudentId
JOIN Subjects sub ON m.SubjectId = sub.Id
GROUP BY s.Id, s.Name, sub.Id, sub.Name
ORDER BY s.Name, sub.Name
";
                using (var con = new SingleStoreConnection())
                {
                    con.ConnectionString = GetConnectionStringValue();
                    await con.OpenAsync();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = query;
                        var readerObj = await dbCommand.ExecuteReaderAsync();
                        var list = DataReaderMapToList<StudentSubjectMarksStoreDto>(readerObj);
                        return list;
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<List<StudentSubjectMarksStoreDto>> LoadTopPerformingStudentsBySubjectAsync(CancellationToken token = default)
        {
            try
            {
                var query = $@"
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
HAVING MAX(m.MarkValue) = 100
";
                using (var con = new SingleStoreConnection())
                {
                    con.ConnectionString = GetConnectionStringValue();
                    await con.OpenAsync();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = query;
                        var readerObj = await dbCommand.ExecuteReaderAsync();
                        var list = DataReaderMapToList<StudentSubjectMarksStoreDto>(readerObj);
                        return list;
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<List<StudentPerformanceStoreDto>> LoadTopStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default)
        {
            try
            {
                var query = $@"
SELECT 
s.Name AS StudentName
,ROUND(AVG(m.MarkValue), 2) AS AverageMark
,COUNT(DISTINCT e.Id) AS ExamCount
FROM Marks m
JOIN Students s ON s.Id = m.StudentId
JOIN Exams e ON e.Id = m.ExamId
GROUP BY s.Id, s.Name
ORDER BY AverageMark DESC
LIMIT {topCount}
";
                using (var con = new SingleStoreConnection())
                {
                    con.ConnectionString = GetConnectionStringValue();
                    await con.OpenAsync();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = query;
                        var readerObj = await dbCommand.ExecuteReaderAsync();
                        var list = DataReaderMapToList<StudentPerformanceStoreDto>(readerObj);
                        return list;
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }
        #endregion

        #region Helper Functions 
        private string GetConnectionStringValue()
        {
            return _configuration.GetValue<string>("SingleStore:ConnectionString");
        }
        private List<T> DataReaderMapToList<T>(SingleStoreDataReader dr)
        {
            List<T> list = new List<T>();
            T obj = default(T);
            while (dr.Read())
            {
                obj = Activator.CreateInstance<T>();
                foreach (PropertyInfo prop in obj.GetType().GetProperties())
                {
                    if (!object.Equals(dr[prop.Name], DBNull.Value))
                    {
                        prop.SetValue(obj, dr[prop.Name], null);
                    }
                }
                list.Add(obj);
            }
            return list;
        }
        #endregion
    }
}
