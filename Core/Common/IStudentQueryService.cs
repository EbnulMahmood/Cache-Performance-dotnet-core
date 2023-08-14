using Common.Dto;

namespace Common
{
    public interface IStudentQueryService
    {
        /// <summary>
        /// Asynchronously retrieves the subject-wise highest marks and exam count of students from the database.
        /// </summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains an enumerable collection of StudentSubjectMarks objects.</returns>
        /// <exception cref="Exception">Thrown when an error occurs while executing the query.</exception>
        Task<IEnumerable<StudentSubjectMarksDto>> LoadSubjectWiseHighestMarksAndExamCountAsync(CancellationToken token = default);
        /// <summary>
        /// Asynchronously gets the subject-wise highest marks and exam count of students who got the highest marks (100) among them.
        /// </summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of objects containing the student name, subject name, highest mark, and exam count.</returns>
        Task<IEnumerable<StudentSubjectMarksDto>> LoadTopPerformingStudentsBySubjectAsync(CancellationToken token = default);
        /// <summary>
        /// Asynchronously gets the top-performing students based on their average mark across all subjects and exams.
        /// </summary>
        /// <param name="topCount">The number of top-performing students to retrieve.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of objects containing the student name, average mark, and exam count.</returns>
        Task<IEnumerable<StudentPerformanceDto>> LoadTopStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default);
        /// <summary>
        /// Asynchronously loads the specified number of students with the lowest average marks.
        /// </summary>
        /// <param name="bottomCount">The number of students to return. Defaults to 1.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <see cref="StudentPerformanceDto"/> objects representing the students with the lowest average marks.</returns>
        /// <exception cref="Exception">Thrown when an error occurs while loading data.</exception>
        Task<IEnumerable<StudentPerformanceDto>> LoadLowPerformingStudentsByAverageMarkAsync(int bottomCount = 1, CancellationToken token = default);
        /// <summary>
        /// Asynchronously loads the specified number of students with the highest average marks.
        /// </summary>
        /// <param name="topCount">The number of students to return. Defaults to 1.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <see cref="StudentPerformanceDto"/> objects representing the students with the highest average marks.</returns>
        /// <exception cref="Exception">Thrown when an error occurs while loading data.</exception>
        Task<IEnumerable<StudentPerformanceDto>> LoadHighPerformingStudentsByAverageMarkAsync(int topCount = 1, CancellationToken token = default);
        /// <summary>
        /// Gets the students who attended the maximum number of exams and got the lowest marks from all students.
        /// </summary>
        /// <param name="numberOfStudents">The number of students to return.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of students with the lowest marks.</returns>
        Task<IEnumerable<StudentExamMarksDto>> LoadStudentsWithLowestMarksAsync(int numberOfStudents = 1, CancellationToken token = default);
        /// <summary>
        /// Gets the students who attended the minimum number of exams and got the highest marks from all students.
        /// </summary>
        /// <param name="numberOfStudents">The number of students to return.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of students with the highest marks.</returns>
        Task<IEnumerable<StudentExamMarksDto>> LoadStudentsWithHighestMarksAsync(int numberOfStudents = 1, CancellationToken token = default);
    }
}
