using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data;

namespace Model
{
    public interface IDapperDataAccess
    {
        Task<TEntity> GetFirstOrDefaultDataAsync<TEntity, TParam>(string sql, TParam parameters, CommandType commandType = 0);
        Task<IEnumerable<TEntity>> LoadDataAsync<TEntity, TParam>(string sql, TParam parameters, CommandType commandType = 0);
    }

    internal sealed class DapperDataAccess : IDapperDataAccess
    {
        private readonly IConfiguration _config;
        private const string _conntectionString = "CacheDbContext";
        public DapperDataAccess(IConfiguration config)
        {
            _config = config;
        }

        public async Task<TEntity> GetFirstOrDefaultDataAsync<TEntity, TParam>(string sql, TParam parameters, CommandType commandType = default)
        {
            try
            {
                using IDbConnection connection = new SqlConnection(_config.GetConnectionString(_conntectionString));

                return await connection.QueryFirstOrDefaultAsync<TEntity>(sql, parameters, commandType: commandType);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<IEnumerable<TEntity>> LoadDataAsync<TEntity, TParam>(string sql, TParam parameters, CommandType commandType = default)
        {
            try
            {
                using IDbConnection connection = new SqlConnection(_config.GetConnectionString(_conntectionString));

                return await connection.QueryAsync<TEntity>(sql, parameters, commandType: commandType);
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}
