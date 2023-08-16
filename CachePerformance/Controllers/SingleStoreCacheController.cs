using Cache;
using Microsoft.AspNetCore.Mvc;
using Services;

namespace CachePerformance.Controllers
{
    public class SingleStoreCacheController : ControllerBase
    {
        private readonly IStudentService _studentService;
        private readonly ISingleStoreHelper _singleStoreHelper;

        public SingleStoreCacheController(IStudentService studentService, ISingleStoreHelper singleStoreHelper)
        {
            _studentService = studentService;
            _singleStoreHelper = singleStoreHelper;
        }

        #region Load Data
        [HttpGet]
        [Route("/SingleStore/cache/subject-wise-highest-marks/and-exam-count")]
        public async Task<IActionResult> LoadSubjectWiseHighestMarksAndExamCount(CancellationToken token = default)
        {
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var studentSubjectMarks = await _singleStoreHelper.LoadSubjectWiseHighestMarksAndExamCountAsync(token).ConfigureAwait(false);
                var count = studentSubjectMarks.Count;
                watch.Stop();
                return Ok($"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes");
            }
            catch (Exception)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/SingleStore/cache/top-performing-students/by-subject")]
        public async Task<IActionResult> LoadTopPerformingStudentsBySubject()
        {
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var studentSubjectMarks = await _singleStoreHelper.LoadTopPerformingStudentsBySubjectAsync().ConfigureAwait(false);
                var count = studentSubjectMarks.Count;
                watch.Stop();
                return Ok($"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes");
                //return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/SingleStore/cache/top-students-by-average-mark/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadTopStudentsByAverageMark(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _singleStoreHelper.LoadTopStudentsByAverageMarkAsync(numberOfStudent).ConfigureAwait(false);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/SingleStore/cache/low-performing-students/by-average-mark/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadLowPerformingStudentsByAverageMark(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _singleStoreHelper.LoadLowPerformingStudentsByAverageMarkAsync(numberOfStudent).ConfigureAwait(false);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/SingleStore/cache/high-performing-students/by-average-mark/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadHighPerformingStudentsByAverageMark(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _singleStoreHelper.LoadHighPerformingStudentsByAverageMarkAsync(numberOfStudent).ConfigureAwait(false);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/SingleStore/cache/students-with-lowest-marks/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadStudentsWithLowestMarks(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _singleStoreHelper.LoadStudentsWithLowestMarksAsync(numberOfStudent).ConfigureAwait(false);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/SingleStore/cache/students-with-highest-marks/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadStudentsWithHighestMarks(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _singleStoreHelper.LoadStudentsWithHighestMarksAsync(numberOfStudent).ConfigureAwait(false);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {
                throw;
            }
        }
        #endregion

        #region Seed Data
        [HttpPost]
        [Route("/singlestore/cache/seed-students")]
        public async Task<IActionResult> SeedData(CancellationToken token = default)
        {
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();

                var entitiesSubject = await _studentService.LoadSubjectListAsync(token);
                var entitiesExam = await _studentService.LoadExamListAsync(token);
                var entitiesStudent = await _studentService.LoadStudentListAsync(token);
                var entitiesMark = await _studentService.LoadMarkListAsync(token);

                int subjectCount =  await _singleStoreHelper.CacheSubjectListAsync(entitiesSubject, token);
                int examCount = await _singleStoreHelper.CacheExamListAsync(entitiesExam, token).ConfigureAwait(false);
                int studentCount = await _singleStoreHelper.CacheStudentListAsync(entitiesStudent, token).ConfigureAwait(false);
                int markCount = await _singleStoreHelper.CacheMarkListAsync(entitiesMark, token).ConfigureAwait(false);

                watch.Stop();
                return Ok($"{subjectCount + examCount + studentCount + markCount} Records Load and Save Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes");

            }
            catch (Exception ex)
            {
                throw;
            }
        }
        #endregion
    }
}
