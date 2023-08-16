using Common.Dto;

namespace Common
{
    public interface IStudentQueryService
    {
        /// <summary>
        /// Retrieves the subject-wise highest marks and exam count of students from the database.
        /// </summary>
        /// <returns>An enumerable collection of StudentSubjectMarks objects.</returns>
        /// <exception cref="Exception">Thrown when an error occurs while executing the query.</exception>
        IEnumerable<StudentSubjectMarksDto> LoadSubjectWiseHighestMarksAndExamCount();
        /// <summary>
        /// Gets the subject-wise highest marks and exam count of students who got the highest marks (100) among them.
        /// </summary>
        /// <returns>A list of objects containing the student name, subject name, highest mark, and exam count.</returns>
        IEnumerable<StudentSubjectMarksDto> LoadTopPerformingStudentsBySubject();

        /// <summary>
        /// Gets the top-performing students based on their average mark across all subjects and exams.
        /// </summary>
        /// <param name="topCount">The number of top-performing students to retrieve.</param>
        /// <returns>A list of objects containing the student name, average mark, and exam count.</returns>
        IEnumerable<StudentPerformanceDto> LoadTopStudentsByAverageMark(int topCount = 1);

        /// <summary>
        /// Loads the specified number of students with the lowest average marks.
        /// </summary>
        /// <param name="bottomCount">The number of students to return. Defaults to 1.</param>
        /// <returns>A list of objects representing the students with the lowest average marks.</returns>
        /// <exception cref="Exception">Thrown when an error occurs while loading data.</exception>
        IEnumerable<StudentPerformanceDto> LoadLowPerformingStudentsByAverageMark(int bottomCount = 1);
        /// <summary>
        /// Loads the specified number of students with the highest average marks.
        /// </summary>
        /// <param name="topCount">The number of students to return. Defaults to 1.</param>
        /// <returns>A list of objects representing the students with the highest average marks.</returns>
        /// <exception cref="Exception">Thrown when an error occurs while loading data.</exception>
        IEnumerable<StudentPerformanceDto> LoadHighPerformingStudentsByAverageMark(int topCount = 1);

        /// <summary>
        /// Gets the students who attended the maximum number of exams and got the lowest marks from all students.
        /// </summary>
        /// <param name="numberOfStudents">The number of students to return.</param>
        /// <returns>A list of students with the lowest marks.</returns>
        IEnumerable<StudentExamMarksDto> LoadStudentsWithLowestMarks(int numberOfStudents = 1);

        /// <summary>
        /// Gets the students who attended the minimum number of exams and got the highest marks from all students.
        /// </summary>
        /// <param name="numberOfStudents">The number of students to return.</param>
        /// <returns>A list of students with the highest marks.</returns>
        IEnumerable<StudentExamMarksDto> LoadStudentsWithHighestMarks(int numberOfStudents = 1);
    }
}
