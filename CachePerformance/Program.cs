using Hazelcast;
using Microsoft.EntityFrameworkCore;
using Model;
using System.Reflection;
using Cache.Extensions;
using Services.Extensions;
using Couchbase.Extensions.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("CacheDbContext"), b => b.MigrationsAssembly(Assembly.GetExecutingAssembly().FullName))
);

builder.Services.AddServices();

var couchbaseConfiguration = builder.Configuration.GetSection("Couchbase");
builder.Services.AddCouchbase(couchbaseConfiguration);

var hazelcastOptions = builder.Configuration.GetSection("hazelcast").Get<HazelcastOptions>();

builder.Services.AddCacheService(hazelcastOptions);

builder.Services.AddIgniteCacheService("172.18.4.176");


builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
