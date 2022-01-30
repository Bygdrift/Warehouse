using Bygdrift.CsvTools;
using RepoDb;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Bygdrift.MssqlTools
{
    /// <summary>
    /// Access to edit Microsoft SQL database data
    /// </summary>
    public partial class Mssql
    {
        /// <summary>
        /// Get all data as csv
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <returns>Data as csv</returns>
        public Csv GetAsCsv(string tableName)
        {
            try
            {
                IEnumerable<dynamic> data = Connection.QueryAll($"[{App.ModuleName}].[{tableName}]");
                return new Csv().FromExpandObjects(data);
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// Get all data as csv
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The name of each column</param>
        /// <returns>Data as csv</returns>
        public Csv GetAsCsv(string tableName, params string[] columns)
        {
            var fields = Field.From(columns);
            IEnumerable<dynamic> data = Connection.QueryAll($"[{App.ModuleName}].[{tableName}]", fields: fields);
            return new Csv().FromExpandObjects(data);
        }

        /// <summary>
        /// Finds the first and last value in an ordered column
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableName"></param>
        /// <param name="columnName"></param>
        /// <param name="includeNulls">If all nulls should be excluded from the column</param>
        /// <returns></returns>
        public (T First, T Last) GetFirstAndLastAscending<T>(string tableName, string columnName, bool includeNulls)
        {
            var notNull = includeNulls ? "" : $"WHERE [{columnName}] is not null";
            var sqlFirst = $"IF OBJECT_ID('{App.ModuleName}.{tableName}') IS NOT NULL BEGIN SELECT TOP(1) [{columnName}] AS Item FROM [{App.ModuleName}].[{tableName}] {notNull} ORDER BY [{columnName}] ASC END; ELSE BEGIN SELECT NULL END;";
            var sqlLast = $"IF OBJECT_ID('{App.ModuleName}.{tableName}') IS NOT NULL BEGIN SELECT TOP(1) [{columnName}] AS Item FROM [{App.ModuleName}].[{tableName}] {notNull} ORDER BY [{columnName}] DESC END; ELSE BEGIN SELECT NULL END;";
            using var result = Connection.ExecuteQueryMultiple(sqlFirst + sqlLast);

            IDictionary<string, object> first = result.Extract().First();
            T firstVal = default;
            if (first.TryGetValue("Item", out object _firstVal))
                firstVal = ChangeType<T>(_firstVal);

            IDictionary<string, object> last = result.Extract().First();
            T lastVal = default;
            if (last.TryGetValue("Item", out object _lastVal))
                lastVal = ChangeType<T>(_lastVal);

            return (firstVal, lastVal);
        }

        private static T ChangeType<T>(object value)
        {
            if (typeof(T) == null || value == null)
                return default;

            try
            {
                return (T)Convert.ChangeType(value, typeof(T)); ;
            }
            catch (Exception)
            {
                return default;
            }
        }
    }
}
