//using Cache;
//using CachePerformance.Helpers;
//using Microsoft.AspNetCore.Mvc;
//using Services;
//using System.Diagnostics;

//namespace CachePerformance.Controllers
//{
//    [Route("api/[controller]")]
//    [ApiController]
//    public class HazelcastStudentsCacheV3Controller : ControllerBase
//    {
//        private readonly IStudentService _studentService;
//        private readonly IHazelcastStudentHelperV3 _hazelcastStudentHelper;

//        public HazelcastStudentsCacheV3Controller(IStudentService studentService
//            , IHazelcastStudentHelperV3 hazelcastStudentHelper)
//        {
//            _studentService = studentService;
//            _hazelcastStudentHelper = hazelcastStudentHelper;
//        }

//        [HttpGet]
//        [Route("/hazelcast/v3/cache/subject-wise-highest-marks/and-exam-count")]
//        public async Task<IActionResult> LoadSubjectWiseHighestMarksAndExamCount(CancellationToken token = default)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _hazelcastStudentHelper.LoadSubjectWiseHighestMarksAndExamCountAsync(token).ConfigureAwait(false);
//                var count = studentSubjectMarks.Count();
//                watch.Stop();

//                return Ok(Constants.GetTime(count, watch));
//            }
//            catch (Exception)
//            {

//                throw;
//            }
//        }

//        [HttpGet]
//        [Route("/hazelcast/v3/cache/top-performing-students/by-subject")]
//        public async Task<IActionResult> LoadTopPerformingStudentsBySubject(CancellationToken token = default)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _hazelcastStudentHelper.LoadTopPerformingStudentsBySubjectAsync(token).ConfigureAwait(false);
//                var count = studentSubjectMarks.Count();
//                watch.Stop();

//                return Ok(Constants.GetTime(count, watch));
//            }
//            catch (Exception)
//            {

//                throw;
//            }
//        }

//        [HttpGet]
//        [Route("/hazelcast/v3/cache/top-students-by-average-mark/{numberOfStudent}/{showTime}")]
//        public async Task<IActionResult> LoadTopStudentsByAverageMark(int numberOfStudent = 1, bool showTime = true, CancellationToken token = default)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _hazelcastStudentHelper.LoadTopStudentsByAverageMarkAsync(numberOfStudent, token).ConfigureAwait(false);
//                var count = studentSubjectMarks.Count();
//                watch.Stop();

//                if (showTime)
//                {
//                    return Ok(Constants.GetTime(count, watch));
//                }
//                else
//                {
//                    return Ok(studentSubjectMarks);
//                }
//            }
//            catch (Exception)
//            {

//                throw;
//            }
//        }

//        [HttpGet]
//        [Route("/hazelcast/v3/cache/low-performing-students/by-average-mark/{numberOfStudent}/{showTime}")]
//        public async Task<IActionResult> LoadLowPerformingStudentsByAverageMark(int numberOfStudent = 1, bool showTime = true, CancellationToken token = default)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _hazelcastStudentHelper.LoadLowPerformingStudentsByAverageMarkAsync(numberOfStudent, token).ConfigureAwait(false);
//                var count = studentSubjectMarks.Count();
//                watch.Stop();

//                if (showTime)
//                {
//                    return Ok(Constants.GetTime(count, watch));
//                }
//                else
//                {
//                    return Ok(studentSubjectMarks);
//                }
//            }
//            catch (Exception)
//            {

//                throw;
//            }
//        }

//        [HttpGet]
//        [Route("/hazelcast/v3/cache/high-performing-students/by-average-mark/{numberOfStudent}/{showTime}")]
//        public async Task<IActionResult> LoadHighPerformingStudentsByAverageMark(int numberOfStudent = 1, bool showTime = true, CancellationToken token = default)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _hazelcastStudentHelper.LoadHighPerformingStudentsByAverageMarkAsync(numberOfStudent, token).ConfigureAwait(false);
//                var count = studentSubjectMarks.Count();
//                watch.Stop();

//                if (showTime)
//                {
//                    return Ok(Constants.GetTime(count, watch));
//                }
//                else
//                {
//                    return Ok(studentSubjectMarks);
//                }
//            }
//            catch (Exception)
//            {

//                throw;
//            }
//        }

//        [HttpGet]
//        [Route("/hazelcast/v3/cache/students-with-lowest-marks/{numberOfStudent}/{showTime}")]
//        public async Task<IActionResult> LoadStudentsWithLowestMarks(int numberOfStudent = 1, bool showTime = true, CancellationToken token = default)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _hazelcastStudentHelper.LoadStudentsWithLowestMarksAsync(numberOfStudent, token).ConfigureAwait(false);
//                var count = studentSubjectMarks.Count();
//                watch.Stop();

//                if (showTime)
//                {
//                    return Ok(Constants.GetTime(count, watch));
//                }
//                else
//                {
//                    return Ok(studentSubjectMarks);
//                }
//            }
//            catch (Exception)
//            {

//                throw;
//            }
//        }

//        [HttpGet]
//        [Route("/hazelcast/v3/cache/students-with-highest-marks/{numberOfStudent}/{showTime}")]
//        public async Task<IActionResult> LoadStudentsWithHighestMarks(int numberOfStudent = 1, bool showTime = true, CancellationToken token = default)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _hazelcastStudentHelper.LoadStudentsWithHighestMarksAsync(numberOfStudent, token).ConfigureAwait(false);
//                var count = studentSubjectMarks.Count();
//                watch.Stop();

//                if (showTime)
//                {
//                    return Ok(Constants.GetTime(count, watch));
//                }
//                else
//                {
//                    return Ok(studentSubjectMarks);
//                }
//            }
//            catch (Exception)
//            {

//                throw;
//            }
//        }

//        #region Seed Data
//        [HttpPost]
//        [Route("/hazelcast/v3/cache/seed-students")]
//        public async Task<IActionResult> SeedData(CancellationToken token = default)
//        {
//            try
//            {
//                var entitiesSubject = await _studentService.LoadSubjectListAsync(token);
//                var entitiesExam = await _studentService.LoadExamListAsync(token);
//                var entitiesStudent = await _studentService.LoadStudentListAsync(token);
//                var entitiesMark = await _studentService.LoadMarkListAsync(token);

//                var watch = Stopwatch.StartNew();
//                int markCount = await _hazelcastStudentHelper.CacheMarkListAsync(entitiesMark, token).ConfigureAwait(false);

//                watch.Stop();
//                return Ok(Constants.GetTime(markCount, watch));
//            }
//            catch (Exception)
//            {

//                throw;
//            }
//        }
//        #endregion
//    }
//}
