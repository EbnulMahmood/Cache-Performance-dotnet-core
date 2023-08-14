namespace Common.Dto
{
    public sealed class CouchbaseMarkDto
    {
        public string? id { get; set; }
        public double markValue { get; set; }
        public string? studentId { get; set; }
        public string? subjectId { get; set; }
        public string? examId { get; set; }
        public DateTimeOffset createdAt { get; set; }
        public DateTimeOffset modifiedAt { get; set; }
    }

    public sealed class CouchbaseExamDto
    {
        public string? id { get; set; }
        public string? name { get; set; }
        public DateTimeOffset examDate { get; set; }
        public DateTimeOffset createdAt { get; set; }
        public DateTimeOffset modifiedAt { get; set; }
    }

    public sealed class CouchbaseStudentDto
    {
        public string? id { get; set; }
        public string? name { get; set; }
        public string? rollNumber { get; set; }
        public DateTimeOffset createdAt { get; set; }
        public DateTimeOffset modifiedAt { get; set; }
    }

    public sealed class CouchbaseSubjectDto
    {
        public string? id { get; set; }
        public string? name { get; set; }
        public string? description { get; set; }
        public DateTimeOffset createdAt { get; set; }
        public DateTimeOffset modifiedAt { get; set; }
    }
}
