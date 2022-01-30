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
        private void CreateSchemaIfNotExists()
        {
            Connection.ExecuteNonQuery($"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{App.ModuleName}') BEGIN EXEC('CREATE SCHEMA {App.ModuleName}') END");
        }

        /// <summary>
        /// Creates a table if it not already exists
        /// </summary>
        /// <returns>True if already exists. False if it was created</returns>
        private bool CreateTableIfNotExists(Csv csv, string tableName, string primaryKey = null)
        {
            CreateSchemaIfNotExists();
            var cols = "";
            foreach (var item in csv.Headers)
            {
                var sqlColumnType = GetCsvColumnTypeAsSqlType(csv, item.Key).Name;
                var header = item.Value;
                if (primaryKey != null && primaryKey == item.Value.ToString())
                    cols += $"[{header}] {sqlColumnType} NOT NULL PRIMARY KEY,\n";
                else
                    cols += $"[{header}] {sqlColumnType} NULL,\n";
            }
            if (string.IsNullOrEmpty(cols))
                return false;

            var sql = $"IF OBJECT_ID('{App.ModuleName}.{tableName}') IS NOT NULL BEGIN SELECT 1 END; ELSE BEGIN SELECT 0 CREATE TABLE [{App.ModuleName}].[{tableName}](\n{cols}) END;";
            try
            {
                //TODO: If there is an error in the following call, it will not throw an error as it should and I cannot fix it right away. Look at it later:
                return Connection.ExecuteScalar<bool>(sql, null, System.Data.CommandType.Text);
            }
            catch (Exception e)
            {
                App.Log.LogError(e, "Error in db load: {Message}. sql: {Sql}", e.Message, sql);
                throw;
            }
        }

        /// <summary>
        /// Delets the table if exists
        /// </summary>
        public string DeleteTable(string tableName)
        {
            try
            {
                Connection.ExecuteNonQuery($"DROP TABLE IF EXISTS [{App.ModuleName}].[{tableName}]");
            }
            catch (Exception e)
            {
                App.Log.LogError(e, "Mssql error in DeleteTable: {E}", e.Message);
                //if(e is SqlException)
                return e.Message;
            }
            return null;
        }

        /// <summary>
        /// Excecutes a SQL
        /// </summary>
        public string ExecuteNonQuery(string sql, dynamic param)
        {
            try
            {
                Connection.ExecuteNonQuery(sql, (object)param);
            }
            catch (Exception e)
            {
                App.Log.LogError(e, "Mssql error in ExecuteNonQuery, running {Sql}. Error: {E}", sql, e.Message);
                return e.Message;
            }
            return null;
        }

        /// <summary>
        /// Necesary if there has been any alterings
        /// </summary>
        private static void FlushRepoDb()
        {
            DbFieldCache.Flush(); // Remove all the cached DbField
            FieldCache.Flush(); // Remove all the cached DbField
            IdentityCache.Flush(); // Remove all the cached DbField
            PrimaryCache.Flush(); // Remove all the cached DbField
        }

        private static (string Name, int Length) GetCsvColumnTypeAsSqlType(Csv csv, int col)
        {
            csv.ColTypes.TryGetValue(col, out Type type);
            var length = csv.GetColRecordsMaxLength(col);
            length = length > 0 ? length : 1;
            var lengthText = length > 8000 ? "MAX" : length.ToString();
            var colType = Type.GetTypeCode(type) switch
            {
                TypeCode.Int64 => "bigint",
                TypeCode.Object => $"varchar({lengthText})",
                TypeCode.Boolean => "bit",
                TypeCode.Char => $"varchar({lengthText})",
                TypeCode.SByte => "binary",
                TypeCode.Byte => "binary",
                TypeCode.Int16 => "smallint",
                TypeCode.UInt16 => "smallint",
                TypeCode.Int32 => "int",
                TypeCode.UInt32 => "int",
                TypeCode.UInt64 => "bigint",
                TypeCode.Single => "real",
                TypeCode.Double => "float",
                TypeCode.Decimal => "decimal(18,12)",
                TypeCode.DateTime => "datetime",
                TypeCode.String => $"varchar({lengthText})",
                TypeCode.Empty => $"varchar({lengthText})",
                TypeCode.DBNull => $"varchar({lengthText})",
                _ => type.ToString().ToLower(),
            };

            return (colType, length);
        }

        /// <summary>
        /// If there are any rows
        /// </summary>
        /// <param name="tableName"></param>
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

        private static (string Name, int? Length) GetSqlColumnType(KeyValuePair<object, Dictionary<string, object>> sqlHeader)
        {
            var type = sqlHeader.Value["DATA_TYPE"].ToString();
            if (type.Contains("varchar"))
            {
                var maxLength = int.Parse(sqlHeader.Value["CHARACTER_MAXIMUM_LENGTH"].ToString());
                return ($"varchar({maxLength})", maxLength);
            }
            //if (type == "float")
            //    return ("float", 20);
            //if (type == "real")
            //    return ("real", 20);
            //if (type == "smallint")
            //    return ("smallint", 5);
            //if (type == "int")
            //    return ("int", 10);
            //if (type == "bigint")
            //    return ("bigint", 19);
            if (type == "decimal")
                return ("decimal(18,12)", 18);
            return (type, 40);
        }

        /// <summary>
        /// Data will be inserted in the table and if there are no table, it will be created.
        /// If a column types or names has been changed, it will be managed.
        /// </summary>
        /// <param name="csv"></param>
        /// <param name="tableName"></param>
        /// <param name="truncateTable">If true, the table gets truncated and filed with new data</param>
        /// <param name="removeEmptyColumns">If true, all columns that only contains null data, will be removed</param>
        /// <returns>eventual errors</returns>
        public string[] InserCsv(Csv csv, string tableName, bool truncateTable, bool removeEmptyColumns)
        {
            var errors = new List<string>();
            if (!PrepareData(csv, removeEmptyColumns))
                return null;

            PrepareTable(csv, tableName, null, truncateTable);
            var data = csv.ToExpandoList();

            try
            {
                if (csv.RowLimit.Max < 100)
                    Connection.InsertAll($"[{App.ModuleName}].[{tableName}]", data, csv.RowLimit.Max + 1, commandTimeout: 3600);
                else
                    Connection.BulkInsert($"[{App.ModuleName}].[{tableName}]", data, bulkCopyTimeout: 3600);
            }
            catch (Exception e)
            {
                Error(errors, e.Message);
            }
            return errors.Count == 0 ? null : errors.ToArray();
        }


        /// <summary>
        /// Data will be merged into the table and if there are no table, it will be created.
        /// If column types or names has been changed, it will be managed.
        /// Right now, this method can only alter columns one time and then merge data. So it cannot: Merge, Alter, Merge. But it can: Alter, Merge, Merge... Merge
        /// </summary>
        /// <param name="csv"></param>
        /// <param name="tableName"></param>
        /// <param name="primaryKey">Cannot be null - use the InsertCsv() method instead. If set, this column can't be null and must be unique values. If set and you try to insert a row that has an id that are already present, then the row will be updated</param>
        /// <param name="truncateTable">If true, the table gets truncated and filed with new data</param>
        /// <param name="removeEmptyColumns">If true, all columns that only contains null data, will be removed</param>
        /// <returns>eventual errors</returns>
        public string[] MergeCsv(Csv csv, string tableName, string primaryKey, bool truncateTable, bool removeEmptyColumns = false)
        {
            var errors = new List<string>();
            if (!PrepareData(csv, removeEmptyColumns))
                return null;

            if (primaryKey == null)
                return Error(errors, "PrimaryKey cannot be set to null. Use the InsertCsv() method instead.");
            if (!ValidatePrimaryKey(csv, tableName, primaryKey, out string[] validateErrors))
                return Error(errors, validateErrors);

            PrepareTable(csv, tableName, primaryKey, truncateTable);
            var data = csv.ToExpandoList();

            if (truncateTable)
            {
                try
                {
                    if (csv.RowLimit.Max < 100)
                        Connection.InsertAll($"[{App.ModuleName}].[{tableName}]", data, csv.RowLimit.Max + 1, commandTimeout: 3600);
                    else
                        Connection.BulkInsert($"[{App.ModuleName}].[{tableName}]", data, bulkCopyTimeout: 3600);
                }
                catch (Exception e)
                {
                    Error(errors, e.Message);
                }
            }
            else
            {
                try
                {
                    if (csv.RowLimit.Max < 100)
                    {
                        var batchsize = csv.RowLimit.Max - csv.RowLimit.Min + 1;
                        Connection.MergeAll($"[{App.ModuleName}].[{tableName}]", data, batchsize, commandTimeout: 3600);
                    }
                    else
                        Connection.BulkMerge($"[{App.ModuleName}].[{tableName}]", data, bulkCopyTimeout: 3600);
                }
                catch (Exception e)
                {
                    Error(errors, e.Message);
                }
            }

            return errors.Count == 0 ? null : errors.ToArray();
        }

        /// <returns>False if there was no content</returns>
        private static bool PrepareData(Csv csv, bool removeEmptyColumns)
        {
            csv.UniqueHeadersIgnoreCase(true);
            if (removeEmptyColumns)
                csv.RemoveEmptyColumns();

            if (csv.Headers.Count == 0)
                return false;

            for (int c = csv.ColLimit.Min; c < csv.ColLimit.Max; c++)  //Brackets cannot be in the column name
                csv.Headers[c] = csv.Headers[c].Replace("[", "(").Replace("]", ")");

            return true;
        }

        /// <summary>
        /// Removes rows that are older than a given expiration data
        /// </summary>
        /// <param name="tableName">The table name</param>
        /// <param name="columnName">The name on the column that contains dates that should be evaluated, wether they are expired</param>
        /// <param name="expirationTime">The expiration time - all older than this date, is removed.</param>
        public string[] RemoveOldRows(string tableName, string columnName, DateTime expirationTime)
        {
            try
            {
                var date = expirationTime.ToString("s");
                var sql = $"IF OBJECT_ID('{App.ModuleName}.{tableName}') IS NULL BEGIN SELECT 0 END; ELSE BEGIN SELECT 1 DELETE FROM[{App.ModuleName}].[{tableName}] WHERE[{columnName}] < '{date}'; END;";
                Connection.ExecuteQuery(sql);
            }
            catch (Exception e)
            {
                App.Log.LogError(e, "Error while removing rows: {E}", e.Message);
                return new string[] { e.Message };
            }
            return default;
        }

        /// <summary>
        /// Empties the table if exists
        /// </summary>
        public string TruncateTable(string tableName)
        {
            try
            {
                Connection.ExecuteNonQuery($"IF OBJECT_ID('{App.ModuleName}.{tableName}', 'U') IS NOT NULL BEGIN TRUNCATE TABLE [{App.ModuleName}].[{tableName}] END");
            }
            catch (Exception e)
            {
                App.Log.LogError(e, "Mssql error in TruncateTable: {E}", e.Message);
                //if(e is SqlException)
                return e.Message;
            }
            return null;
        }

        private void PrepareTable(Csv csv, string tableName, string primaryKey, bool truncateTable)
        {
            if (string.IsNullOrEmpty(App.ModuleName))
                throw new ArgumentNullException(nameof(App.ModuleName), "Schema name is null. It has to be set.");
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(nameof(tableName), "Table name is null. It has to be set.");
            if (csv.Headers.Count == 0)
                return;

            if (CreateTableIfNotExists(csv, tableName, primaryKey))
            {
                if (truncateTable)
                    TruncateTable(tableName);

                PrepareTableIfExist(csv, tableName, primaryKey);
            }
        }

        /// <summary>
        /// It the table exist, it will be updated so colType and names are mathing the csv
        /// If there are whole columns that are not containing data in the database table and that are not mentioned in the csv, they will be removed.
        /// </summary>
        /// <returns>If it was updated or not</returns>
        public bool PrepareTableIfExist(Csv csv, string tableName, string primaryKey)
        {
            var sql = $"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{App.ModuleName}' AND TABLE_NAME = '{tableName}'";
            var sqlColumns = new Csv().FromExpandObjects(Connection.ExecuteQuery(sql));

            if (sqlColumns == default)
                return false;

            //sql = $"IF select count(*) from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_SCHEMA = '{database.ModuleName}' AND TABLE_NAME = '{tableName}' AND COLUMN_NAME = '{primaryKey}' IS NOT NULL THEN TRUE ELSE FALSE END;";
            //var primaryKeySet = database.Connection.ExecuteScalarAsync<int?>(sql).Result > 0 ? true : false;

            var commands = "";
            var sqlHeadersAndTypes = sqlColumns.GetRowsRecordsWithHeaderNames("COLUMN_NAME");
            var sqlHeaders = sqlHeadersAndTypes.Select(o => o.Key.ToString());
            var csvHeaders = csv.Headers.Values.Select(o => o.ToString()).ToList();
            foreach (var sqlHeader in sqlHeadersAndTypes)
            {
                var sqlName = sqlHeader.Key.ToString();
                if (csv.TryGetColId(sqlName, out int csvColId, false))  //Not caseSensitive because SQL are not
                {
                    var csvColName = csv.Headers[(int)csvColId];
                    var (sqlColType, sqlColTypeLength) = GetSqlColumnType(sqlHeader);
                    var (csvColType, csvColTypeLength) = GetCsvColumnTypeAsSqlType(csv, (int)csvColId);

                    //Right now I am just upgrading every change to be a varchar because it will take some time to make this conversion
                    //I also have to handle if a primarykey changes type
                    if (!csvColType.Equals(sqlColType, StringComparison.InvariantCultureIgnoreCase))
                    {
                        var length = csvColTypeLength > sqlColTypeLength || sqlColTypeLength == null ? csvColTypeLength : sqlColTypeLength;
                        var newSqlType = $"varchar({length})";
                        if (!sqlColType.Equals(newSqlType, StringComparison.InvariantCultureIgnoreCase))
                        {
                            if (sqlName == primaryKey)
                            {
                                commands += "DECLARE @constraint varchar(128);\n";
                                commands += $"SELECT @constraint = CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '{App.ModuleName}' AND TABLE_NAME = '{tableName}';\n";
                                commands += $"if (@constraint) IS NOT NULL EXEC('ALTER TABLE [{App.ModuleName}].[{tableName}] DROP CONSTRAINT ' + @constraint);\n";
                                commands += $"ALTER TABLE [{App.ModuleName}].[{tableName}] ALTER COLUMN [{sqlName}] varchar({length}) NOT NULL;\n";
                                commands += $"ALTER TABLE [{App.ModuleName}].[{tableName}] ADD CONSTRAINT [{sqlName}_pk] PRIMARY KEY ([{sqlName}]);\n";
                            }
                            else
                                commands += $"ALTER TABLE [{App.ModuleName}].[{tableName}] ALTER COLUMN [{sqlName}] varchar({length}) ;\n";
                        }
                    }

                    if (sqlName != csvColName)  //There are casesensitive mismatch like 'Data' == 'data'
                        commands += $"EXEC sp_rename '{App.ModuleName}.{tableName}.{sqlName}', '{csvColName}', 'COLUMN';\n";

                    //if (!primaryKeySet)
                    //    commands += $"ALTER TABLE [{database.ModuleName}].[{tableName}] ALTER COLUMN [{sqlName}] varchar({csvColTypeLength}) ;\n";
                }
            }

            //Adds eventual missing columns
            var notInSqlHeaders = csvHeaders.Except(sqlHeaders);
            foreach (var name in notInSqlHeaders)
            {
                csv.TryGetColId(name, out int csvColId, false);//Not caseSensitive because SQL are not
                var csvColType = GetCsvColumnTypeAsSqlType(csv, csvColId).Name;
                commands += $"ALTER TABLE [{App.ModuleName}].[{tableName}] ADD [{name}] {csvColType};\n";
            }

            //Removes eventual empty columns from table:
            var notInCsvHeaders = sqlHeaders.Except(csvHeaders);
            if (notInCsvHeaders.Any())
            {
                var emptyColumns = GetEmptyColumns(tableName);
                foreach (var name in emptyColumns)
                    if (!csvHeaders.Any(o => o.Equals(name, StringComparison.InvariantCultureIgnoreCase)))
                        commands += $"ALTER TABLE [{App.ModuleName}].[{tableName}] DROP COLUMN [{name}];\n";
            }

            //TODO: If a coltype has changed from fx int to bigint or to varchar
            //Handle if their comes errors - they should bubble up

            if (!string.IsNullOrEmpty(commands))
            {
                try
                {
                    Connection.ExecuteNonQuery(commands);
                }
                catch (Exception e)
                {
                    //if(e is SqlException)
                    App.Log.LogError(e, "Error in db load: {Message}. Commands: {Commands}", e.Message, commands);
                    throw;
                }
                FlushRepoDb();
            }

            return true;
        }


        /// <summary>
        /// Validates if there are any duplicates in the primaryKey or if there are any nulls.
        /// </summary>
        /// <returns>true if there are no errors</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "<Pending>")]
        public bool ValidatePrimaryKey(Csv csv, string tableName, string primaryKey, out string[] errors)

        {
            if (!csv.Headers.Any())
            {
                errors = new string[] { "The csv is empty." };
                return false;
            }

            var res = new List<string>();

            var colRecords = csv.GetColRecords(primaryKey, true);
            if (colRecords == null)
                res.Add($"The primaryKey '{primaryKey}' in the table '{tableName}' does not exist.");
            else
            {
                if (csv.RowCount == 0)
                {
                    errors = res.ToArray();
                    return true;
                }

                var duplicates = colRecords.GroupBy(o => o.Value).Where(g => g.Count() > 1).Select(y => y.Key).ToArray();
                if (duplicates.Any())
                {
                    var duplicatesString = string.Join(',', duplicates.ToArray());
                    res.Add($"There must not be any duplicates in the column with the header '{primaryKey}' because it is set as a primary key in the database. There are {duplicates.Length} duplicates. They are: '{duplicatesString}'.");
                }

                var nulls = colRecords.Where(o => o.Value == null).ToList();
                if (nulls.Count > 0)
                    res.Add($"There must not be any null values in the column with the header '{primaryKey}' because it is set as a primary key in the database. There are {nulls} nulls.");
            }

            errors = res.ToArray();
            return !res.Any();
        }

        /// <returns>All accumulated errors</returns>
        private string[] Error(List<string> errors, string newError)
        {
            errors.Add(newError);
            App.Log.LogError(newError);
            return errors.ToArray();
        }

        /// <returns>All accumulated errors</returns>
        private string[] Error(List<string> errors, string[] newErrors)
        {
            foreach (var item in newErrors)
            {
                errors.Add(item);
                App.Log.LogError(item);
            }
            return errors.ToArray();
        }
    }
}
