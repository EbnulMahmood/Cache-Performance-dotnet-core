using Cache;
using Microsoft.AspNetCore.Mvc;
using Services;

namespace CachePerformance.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class StudentsCacheController : ControllerBase
    {
        private readonly IStudentService _studentService;
        private readonly IStudentCacheHelper _studentCacheHelper;

        public StudentsCacheController(IStudentService studentService
            , IStudentCacheHelper studentCacheHelper)
        {
            _studentService = studentService;
            _studentCacheHelper = studentCacheHelper;
        }

        [HttpGet]
        [Route("/cache/subject-wise-highest-marks/and-exam-count")]
        public async Task<IActionResult> LoadSubjectWiseHighestMarksAndExamCount(CancellationToken token = default)
        {
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var studentSubjectMarks = await _studentCacheHelper.LoadSubjectWiseHighestMarksAndExamCountAsync(token);
                var count = studentSubjectMarks.Count();
                watch.Stop();

                return Ok($"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes");
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/cache/top-performing-students/by-subject")]
        public async Task<IActionResult> LoadTopPerformingStudentsBySubject()
        {
            try
            {
                var studentSubjectMarks = await _studentCacheHelper.LoadTopPerformingStudentsBySubjectAsync();
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/cache/top-students-by-average-mark/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadTopStudentsByAverageMark(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _studentCacheHelper.LoadTopStudentsByAverageMarkAsync(numberOfStudent);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/cache/low-performing-students/by-average-mark/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadLowPerformingStudentsByAverageMark(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _studentCacheHelper.LoadLowPerformingStudentsByAverageMarkAsync(numberOfStudent);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/cache/high-performing-students/by-average-mark/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadHighPerformingStudentsByAverageMark(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _studentCacheHelper.LoadHighPerformingStudentsByAverageMarkAsync(numberOfStudent);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/cache/students-with-lowest-marks/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadStudentsWithLowestMarks(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _studentCacheHelper.LoadStudentsWithLowestMarksAsync(numberOfStudent);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/cache/students-with-highest-marks/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadStudentsWithHighestMarks(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _studentCacheHelper.LoadStudentsWithHighestMarksAsync(numberOfStudent);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        #region Seed Data
        [HttpPost]
        [Route("/cache/seed-students")]
        public async Task<IActionResult> SeedData(CancellationToken token = default)
        {
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();

                var entitiesSubject = await _studentService.LoadSubjectListAsync(token);
                var entitiesExam = await _studentService.LoadExamListAsync(token);
                var entitiesStudent = await _studentService.LoadStudentListAsync(token);
                var entitiesMark = await _studentService.LoadMarkListAsync(token);

                int subjectCount = await _studentCacheHelper.CacheSubjectListAsync(entitiesSubject, token);
                int examCount = await _studentCacheHelper.CacheExamListAsync(entitiesExam, token);
                int studentCount = await _studentCacheHelper.CacheStudentListAsync(entitiesStudent, token);
                int markCount = await _studentCacheHelper.CacheMarkListAsync(entitiesMark, token);

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
