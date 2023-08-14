﻿using Cache;
using Microsoft.AspNetCore.Mvc;
using Services;

namespace CachePerformance.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class IgniteStudentsCacheController : ControllerBase
    {
        private readonly IStudentService _studentService;
        private readonly IIgniteStudentCacheHelper _igniteStudentCacheHelper;

        public IgniteStudentsCacheController(IStudentService studentService
            , IIgniteStudentCacheHelper igniteStudentCacheHelper)
        {
            _studentService = studentService;
            _igniteStudentCacheHelper = igniteStudentCacheHelper;
        }


        [HttpGet]
        [Route("/ignite/cache/subject-wise-highest-marks/and-exam-count")]
        public async Task<IActionResult> LoadSubjectWiseHighestMarksAndExamCount(CancellationToken token = default)
        {
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var studentSubjectMarks = await _igniteStudentCacheHelper.LoadSubjectWiseHighestMarksAndExamCountAsync(token).ConfigureAwait(false);
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
        [Route("/ignite/cache/top-performing-students/by-subject")]
        public async Task<IActionResult> LoadTopPerformingStudentsBySubject()
        {
            try
            {
                var studentSubjectMarks = await _igniteStudentCacheHelper.LoadTopPerformingStudentsBySubjectAsync().ConfigureAwait(false);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/ignite/cache/top-students-by-average-mark/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadTopStudentsByAverageMark(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _igniteStudentCacheHelper.LoadTopStudentsByAverageMarkAsync(numberOfStudent).ConfigureAwait(false);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/ignite/cache/low-performing-students/by-average-mark/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadLowPerformingStudentsByAverageMark(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _igniteStudentCacheHelper.LoadLowPerformingStudentsByAverageMarkAsync(numberOfStudent).ConfigureAwait(false);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/ignite/cache/high-performing-students/by-average-mark/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadHighPerformingStudentsByAverageMark(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _igniteStudentCacheHelper.LoadHighPerformingStudentsByAverageMarkAsync(numberOfStudent).ConfigureAwait(false);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/ignite/cache/students-with-lowest-marks/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadStudentsWithLowestMarks(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _igniteStudentCacheHelper.LoadStudentsWithLowestMarksAsync(numberOfStudent).ConfigureAwait(false);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/ignite/cache/students-with-highest-marks/{numberOfStudent}:int")]
        public async Task<IActionResult> LoadStudentsWithHighestMarks(int numberOfStudent = 1)
        {
            try
            {
                var studentSubjectMarks = await _igniteStudentCacheHelper.LoadStudentsWithHighestMarksAsync(numberOfStudent).ConfigureAwait(false);
                return Ok(studentSubjectMarks);
            }
            catch (Exception)
            {

                throw;
            }
        }

        #region Seed Data
        [HttpPost]
        [Route("/ignite/cache/seed-students")]
        public async Task<IActionResult> SeedData(CancellationToken token = default)
        {
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();

                var entitiesSubject = await _studentService.LoadSubjectListAsync(token);
                var entitiesExam = await _studentService.LoadExamListAsync(token);
                var entitiesStudent = await _studentService.LoadStudentListAsync(token);
                var entitiesMark = await _studentService.LoadMarkListAsync(token);

                int subjectCount = await _igniteStudentCacheHelper.CacheSubjectListAsync(entitiesSubject, token).ConfigureAwait(false);
                int examCount = await _igniteStudentCacheHelper.CacheExamListAsync(entitiesExam, token).ConfigureAwait(false);
                int studentCount = await _igniteStudentCacheHelper.CacheStudentListAsync(entitiesStudent, token).ConfigureAwait(false);
                int markCount = await _igniteStudentCacheHelper.CacheMarkListAsync(entitiesMark, token).ConfigureAwait(false);

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