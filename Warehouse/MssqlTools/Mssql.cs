using Bygdrift.Warehouse;
using Microsoft.Data.SqlClient;
using RepoDb;
using System;

namespace Bygdrift.MssqlTools
{
    /// <summary>
    /// Access to edit Microsoft SQL database data
    /// </summary>
    public partial class Mssql : IDisposable
    {
        private SqlConnection _connection;

        /// <summary>
        /// MS Sql connection
        /// </summary>
        public Mssql(AppBase app) => App = app;

        /// <summary>
        /// Contains core info about dataLake, database and configurations
        /// </summary>
        public AppBase App { get; }

        /// <summary>
        /// The MS SQL connection
        /// </summary>
        public SqlConnection Connection
        {
            get
            {
                if (_connection == null)
                {
                    if (string.IsNullOrEmpty(App.MssqlConnectionString))
                    {
                        App.Log.LogError("The database connectionString, has to be set.");
                        throw new ArgumentNullException("The database connectionString, has to be set.");
                    }

                    SqlServerBootstrap.Initialize();
                    var builder = new SqlConnectionStringBuilder(App.MssqlConnectionString)
                    {
                        ConnectTimeout = 30,
                        CommandTimeout = 3600
                    };  

                    _connection = new SqlConnection(builder.ToString());
                }
                if (_connection.State == System.Data.ConnectionState.Closed)
                    try
                    {
                        _connection.Open();
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Could not login to MSSql database.", e);
                    }

                return _connection;
            }
            set { _connection = value; }
        }

        /// <summary>
        /// Called when disposing
        /// </summary>
        public void Dispose()
        {
            if (_connection != null)
            {
                _connection.Close();
                _connection = null;
            }
            GC.SuppressFinalize(this);
        }
    }
}
