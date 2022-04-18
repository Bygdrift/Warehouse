using Bygdrift.CsvTools;
using Bygdrift.Warehouse.MssqlTools.Models;
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

        public IEnumerable<ColumnType> GetColumnTypes(string tableName)
        {
           var sql = "SELECT C1.COLUMN_NAME, C1.DATA_TYPE, C1.CHARACTER_MAXIMUM_LENGTH, CASE WHEN KCU.COLUMN_NAME IS NULL THEN CAST(0 as bit) ELSE CAST(1 as bit)  END AS IsPrimaryKey\n" +
            "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU RIGHT JOIN INFORMATION_SCHEMA.COLUMNS C1 ON C1.TABLE_SCHEMA = KCU.TABLE_SCHEMA AND C1.TABLE_NAME = KCU.TABLE_NAME AND C1.COLUMN_NAME = KCU.COLUMN_NAME\n" +
            $"WHERE C1.TABLE_SCHEMA = '{App.ModuleName}' AND C1.TABLE_NAME = '{tableName}'";

            foreach (dynamic item in Connection.ExecuteQuery(sql))
                yield return new ColumnType(item.COLUMN_NAME, item.DATA_TYPE, item.CHARACTER_MAXIMUM_LENGTH, item.IsPrimaryKey);
        }

        /// <summary>
        /// If there are any rows, it will return the name on any empty column
        /// </summary>
        /// <param name="tableName">Th name of the table</param>
        /// <returns></returns>
        public string[] GetEmptyColumns(string tableName)
        {
            //var parameters = new { schemaName, tableName };  //Declares and sets @schemaName and @tableName
            var sql = "declare @tempTable TABLE(ColumnName sysname,NotNullCnt bigint);\n" +
                "declare @schemaName varchar(256);declare @tableName varchar(256);" +
                "SET @schemaName = '" + App.ModuleName + "';SET @tableName = '" + tableName + "';\n" +
                "declare @sql nvarchar(4000);declare @columnName sysname;declare @cnt bigint;\n" +
                "declare columnCursor cursor FOR\n" +
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE IS_NULLABLE = 'YES' AND TABLE_SCHEMA = @schemaName AND TABLE_NAME = @tableName;\n" +
                "open columnCursor;\n" +
                "fetch next FROM columnCursor INTO @columnName;\n" +
                "while @@FETCH_STATUS = 0\n" +
                "begin\n" +
                "SET @sql = 'select @cnt = COUNT(*) from [' + @schemaName + '].[' + @tableName + '] where [' + @columnName + '] is not null';\n" +
                "exec sp_executesql @sql, N'@cnt bigint output', @cnt = @cnt output;\n" +
                "INSERT INTO @tempTable SELECT @columnName, @cnt;\n" +
                "fetch next FROM columnCursor INTO @columnName;\n" +
                "end\n" +
                "close columnCursor;deallocate columnCursor;SELECT ColumnName FROM @tempTable WHERE NotNullCnt = 0;";

            var resp = Connection.ExecuteQuery(sql);
            var res = new List<string>();
            foreach (IDictionary<string, object> item in resp)
                foreach (var value in item.Values)
                    res.Add(value.ToString());

            return res.ToArray();
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
