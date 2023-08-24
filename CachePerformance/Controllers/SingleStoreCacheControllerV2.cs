﻿using Cache;
using CachePerformance.Helpers;
using Microsoft.AspNetCore.Mvc;
using Services;
using System.Diagnostics;

namespace CachePerformance.Controllers
{
    public class SingleStoreCacheControllerV2 : ControllerBase
    {
        private readonly IStudentServiceV2 _studentService;
        private readonly ISingleStoreHelperV2 _singleStoreHelper;

        public SingleStoreCacheControllerV2(IStudentServiceV2 studentService, ISingleStoreHelperV2 singleStoreHelper)
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
                var watch = Stopwatch.StartNew();
                var studentSubjectMarks = await _singleStoreHelper.LoadSubjectWiseHighestMarksAndExamCountAsync(token).ConfigureAwait(false);
                var count = studentSubjectMarks.Count;
                watch.Stop();

                return Ok(Constants.GetTime(count, watch));
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
                var watch = Stopwatch.StartNew();
                var studentSubjectMarks = await _singleStoreHelper.LoadTopPerformingStudentsBySubjectAsync().ConfigureAwait(false);
                var count = studentSubjectMarks.Count;
                watch.Stop();

                return Ok(Constants.GetTime(count, watch));
            }
            catch (Exception)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/SingleStore/cache/top-students-by-average-mark/{numberOfStudent}/{showTime}")]
        public async Task<IActionResult> LoadTopStudentsByAverageMark(int numberOfStudent = 1, bool showTime = true)
        {
            try
            {
                var watch = Stopwatch.StartNew();
                var studentSubjectMarks = await _singleStoreHelper.LoadTopStudentsByAverageMarkAsync(numberOfStudent).ConfigureAwait(false);
                var count = studentSubjectMarks.Count();
                watch.Stop();

                if (showTime)
                {
                    return Ok(Constants.GetTime(count, watch));
                }
                else
                {
                    return Ok(studentSubjectMarks);
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/SingleStore/cache/low-performing-students/by-average-mark/{numberOfStudent}/{showTime}")]
        public async Task<IActionResult> LoadLowPerformingStudentsByAverageMark(int numberOfStudent = 1, bool showTime = true)
        {
            try
            {
                var watch = Stopwatch.StartNew();
                var studentSubjectMarks = await _singleStoreHelper.LoadLowPerformingStudentsByAverageMarkAsync(numberOfStudent).ConfigureAwait(false);
                var count = studentSubjectMarks.Count();
                watch.Stop();

                if (showTime)
                {
                    return Ok(Constants.GetTime(count, watch));
                }
                else
                {
                    return Ok(studentSubjectMarks);
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/SingleStore/cache/high-performing-students/by-average-mark/{numberOfStudent}/{showTime}")]
        public async Task<IActionResult> LoadHighPerformingStudentsByAverageMark(int numberOfStudent = 1, bool showTime = true)
        {
            try
            {
                var watch = Stopwatch.StartNew();
                var studentSubjectMarks = await _singleStoreHelper.LoadHighPerformingStudentsByAverageMarkAsync(numberOfStudent).ConfigureAwait(false);
                var count = studentSubjectMarks.Count();
                watch.Stop();

                if (showTime)
                {
                    return Ok(Constants.GetTime(count, watch));
                }
                else
                {
                    return Ok(studentSubjectMarks);
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/SingleStore/cache/students-with-lowest-marks/{numberOfStudent}/{showTime}")]
        public async Task<IActionResult> LoadStudentsWithLowestMarks(int numberOfStudent = 1, bool showTime = true)
        {
            try
            {
                var watch = Stopwatch.StartNew();
                var studentSubjectMarks = await _singleStoreHelper.LoadStudentsWithLowestMarksAsync(numberOfStudent).ConfigureAwait(false);
                var count = studentSubjectMarks.Count();
                watch.Stop();

                if (showTime)
                {
                    return Ok(Constants.GetTime(count, watch));
                }
                else
                {
                    return Ok(studentSubjectMarks);
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        [HttpGet]
        [Route("/SingleStore/cache/students-with-highest-marks/{numberOfStudent}/{showTime}")]
        public async Task<IActionResult> LoadStudentsWithHighestMarks(int numberOfStudent = 1, bool showTime = true)
        {
            try
            {
                var watch = Stopwatch.StartNew();
                var studentSubjectMarks = await _singleStoreHelper.LoadStudentsWithHighestMarksAsync(numberOfStudent).ConfigureAwait(false);
                var count = studentSubjectMarks.Count();
                watch.Stop();

                if (showTime)
                {
                    return Ok(Constants.GetTime(count, watch));
                }
                else
                {
                    return Ok(studentSubjectMarks);
                }
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
                var entitiesMergerMark = await _studentService.LoadMergedMarksListAsync(token);
                var watch = Stopwatch.StartNew();
                int mergedMarkCount = await _singleStoreHelper.CacheMergedMarkListAsync(entitiesMergerMark, token).ConfigureAwait(false);
                watch.Stop();
                return Ok($"{mergedMarkCount} Records Load and Save Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes");
            }
            catch (Exception)
            {

                throw;
            }
        }
        #endregion
    }
}
