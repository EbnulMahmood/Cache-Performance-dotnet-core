using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data;

namespace Model
{
    public interface IDapperDataAccess
    {
        Task<TEntity> GetFirstOrDefaultDataAsync<TEntity, TParam>(string sql, TParam parameters, CommandType commandType = 0, int? commandTimeout = null);
        Task<IEnumerable<TEntity>> LoadDataAsync<TEntity, TParam>(string sql, TParam parameters, CommandType commandType = 0, int? commandTimeout = null);
    }

    internal sealed class DapperDataAccess : IDapperDataAccess
    {
        private readonly IConfiguration _config;
        private const string _connectionString = "CacheDbContext";
        public DapperDataAccess(IConfiguration config)
        {
            _config = config;
        }

        public async Task<TEntity> GetFirstOrDefaultDataAsync<TEntity, TParam>(string sql, TParam parameters, CommandType commandType = default, int? commandTimeout = null)
        {
            try
            {
                using IDbConnection connection = new SqlConnection(_config.GetConnectionString(_connectionString));

                return await connection.QueryFirstOrDefaultAsync<TEntity>(sql, parameters, commandType: commandType, commandTimeout: commandTimeout);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<IEnumerable<TEntity>> LoadDataAsync<TEntity, TParam>(string sql, TParam parameters, CommandType commandType = default, int? commandTimeout = null)
        {
            try
            {
                using IDbConnection connection = new SqlConnection(_config.GetConnectionString(_connectionString));

                return await connection.QueryAsync<TEntity>(sql, parameters, commandType: commandType, commandTimeout: commandTimeout);
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}
