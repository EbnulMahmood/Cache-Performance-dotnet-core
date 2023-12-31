﻿//using CachePerformance.Helpers;
//using Microsoft.AspNetCore.Mvc;
//using Services;
//using System.Diagnostics;

//namespace CachePerformance.Controllers
//{
//    [Route("api/[controller]")]
//    [ApiController]
//    public class StudentsController : ControllerBase
//    {
//        private readonly IStudentService _studentService;

//        public StudentsController(IStudentService studentService)
//        {
//            _studentService = studentService;
//        }

//        [HttpGet]
//        [Route("/subject-wise-highest-marks/and-exam-count")]
//        public async Task<IActionResult> LoadSubjectWiseHighestMarksAndExamCount()
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _studentService.LoadSubjectWiseHighestMarksAndExamCountAsync();
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
//        [Route("/top-performing-students/by-subject")]
//        public async Task<IActionResult> LoadTopPerformingStudentsBySubject()
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _studentService.LoadTopPerformingStudentsBySubjectAsync();
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
//        [Route("/top-students-by-average-mark/{numberOfStudent}/{showTime}")]
//        public async Task<IActionResult> LoadTopStudentsByAverageMark(int numberOfStudent = 1, bool showTime = true)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _studentService.LoadTopStudentsByAverageMarkAsync(numberOfStudent);
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
//        [Route("/low-performing-students/by-average-mark/{numberOfStudent}/{showTime}")]
//        public async Task<IActionResult> LoadLowPerformingStudentsByAverageMark(int numberOfStudent = 1, bool showTime = true)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _studentService.LoadLowPerformingStudentsByAverageMarkAsync(numberOfStudent);
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
//        [Route("/high-performing-students/by-average-mark/{numberOfStudent}/{showTime}")]
//        public async Task<IActionResult> LoadHighPerformingStudentsByAverageMark(int numberOfStudent = 1, bool showTime = true)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _studentService.LoadHighPerformingStudentsByAverageMarkAsync(numberOfStudent);
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
//        [Route("/students-with-lowest-marks/{numberOfStudent}/{showTime}")]
//        public async Task<IActionResult> LoadStudentsWithLowestMarks(int numberOfStudent = 1, bool showTime = true)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _studentService.LoadStudentsWithLowestMarksAsync(numberOfStudent);
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
//        [Route("/students-with-highest-marks/{numberOfStudent}/{showTime}")]
//        public async Task<IActionResult> LoadStudentsWithHighestMarks(int numberOfStudent = 1, bool showTime = true)
//        {
//            try
//            {
//                var watch = Stopwatch.StartNew();
//                var studentSubjectMarks = await _studentService.LoadStudentsWithHighestMarksAsync(numberOfStudent);
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
//        //[HttpPost]
//        //[Route("/data/seed-students")]
//        //public async Task<IActionResult> SeedData(CancellationToken token = default)
//        //{
//        //    try
//        //    {
//        //        var watch = System.Diagnostics.Stopwatch.StartNew();

//        //        var (entitiesStudent, entitiesSubject, entitiesExam, entitiesMark) = _studentService.GenerateData(Constants.Zero, Constants.OneHundredThousand);

//        //        await _studentService.SaveStudentListAsync(entitiesStudent, token);
//        //        await _studentService.SaveSubjectListAsync(entitiesSubject, token);
//        //        await _studentService.SaveExamListAsync(entitiesExam, token);
//        //        await _studentService.SaveMarkListAsync(entitiesMark, token);

//        //        watch.Stop();
//        //        return Ok($"Records Save Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes");
//        //    }
//        //    catch (Exception)
//        //    {

//        //        throw;
//        //    }
//        //}
//        #endregion
//    }
//}
