using Microsoft.EntityFrameworkCore;

namespace Model
{
    public sealed class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
        {
        }

        public DbSet<Student> Students { get; set; }
        public DbSet<Subject> Subjects { get; set; }
        public DbSet<Exam> Exams { get; set; }
        public DbSet<Mark> Marks { get; set; }
        public DbSet<MergedMark> MergedMarks { get; set; }
    }
}
