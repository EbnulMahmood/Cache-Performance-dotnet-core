using Cache;
using Microsoft.AspNetCore.Mvc;
using Services;

namespace CachePerformance.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CouchbaseStudentsCacheController : ControllerBase
    {
        private readonly IStudentService _studentService;
        private readonly ICouchbaseStudentHelper _couchbaseStudentHelper;

        public CouchbaseStudentsCacheController(IStudentService studentService
            , ICouchbaseStudentHelper couchbaseStudentHelper)
        {
            _studentService = studentService;
            _couchbaseStudentHelper = couchbaseStudentHelper;
        }


        [HttpGet]
        [Route("/couchbase/cache/subject-wise-highest-marks/and-exam-count")]
        public async Task<IActionResult> LoadSubjectWiseHighestMarksAndExamCount(CancellationToken token = default)
        {
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var studentSubjectMarks = await _couchbaseStudentHelper.LoadSubjectWiseHighestMarksAndExamCountAsync(token).ConfigureAwait(false);
                var count = studentSubjectMarks.Count();
                watch.Stop();

                return Ok($"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes");
            }
            catch (Exception)
            {

                throw;
            }
        }


        //[HttpGet]
        //[Route("/couchbase/cache/students-with-highest-marks/{numberOfStudent}:int")]
        //public async Task<IActionResult> LoadStudentsWithHighestMarks(int numberOfStudent = 1)
        //{
        //    try
        //    {
        //        var studentSubjectMarks = await _couchbaseStudentHelper.LoadStudentsWithHighestMarksAsync(numberOfStudent).ConfigureAwait(false);
        //        return Ok(studentSubjectMarks);
        //    }
        //    catch (Exception)
        //    {

        //        throw;
        //    }
        //}

        [HttpGet]
        [Route("/couchbase/cache/top-performing-students/by-subject")]
        public async Task<IActionResult> LoadTopPerformingStudentsBySubject()
        {
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var studentSubjectMarks = await _couchbaseStudentHelper.LoadTopPerformingStudentsBySubjectAsync();
                var count = studentSubjectMarks.Count();
                watch.Stop();

                return Ok($"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes");
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/couchbase/cache/top-students-by-average-mark/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadTopStudentsByAverageMark(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _couchbaseStudentHelper.LoadTopStudentsByAverageMarkAsync(numberOfStudent);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/couchbase/cache/low-performing-students/by-average-mark/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadLowPerformingStudentsByAverageMark(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _couchbaseStudentHelper.LoadLowPerformingStudentsByAverageMarkAsync(numberOfStudent);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {
                throw;
            }
        }

        #region Seed Data
        [HttpPost]
        [Route("/couchbase/cache/seed-students")]
        public async Task<IActionResult> SeedData(CancellationToken token = default)
        {
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();

                var entitiesSubject = await _studentService.LoadSubjectListAsync(token);
                var entitiesExam = await _studentService.LoadExamListAsync(token);
                var entitiesStudent = await _studentService.LoadStudentListAsync(token);
                var entitiesMark = await _studentService.LoadMarkListAsync(token);

                int subjectCount = await _couchbaseStudentHelper.CacheSubjectListAsync(entitiesSubject, token).ConfigureAwait(false);
                int examCount = await _couchbaseStudentHelper.CacheExamListAsync(entitiesExam, token).ConfigureAwait(false);
                int studentCount = await _couchbaseStudentHelper.CacheStudentListAsync(entitiesStudent, token).ConfigureAwait(false);
                int markCount = await _couchbaseStudentHelper.CacheMarkListAsync(entitiesMark, token).ConfigureAwait(false);

                watch.Stop();
                return Ok($"{subjectCount + examCount + studentCount + markCount} Records Load and Save Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes");
            }
            catch (Exception)
            {

                throw;
            }
        }
        #endregion
    }
}
