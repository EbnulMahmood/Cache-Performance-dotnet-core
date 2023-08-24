using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CachePerformance.Migrations
{
    /// <inheritdoc />
    public partial class AddedMergedMark : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "MergedMarks",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    MarkValue = table.Column<double>(type: "float", nullable: false),
                    StudentName = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    StudentRollNumber = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    SubjectName = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    SubjectDescription = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    ExamName = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    ExamDate = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    CreatedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    ModifiedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_MergedMarks", x => x.Id);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "MergedMarks");
        }
    }
}
