using Bygdrift.CsvTools;
using Bygdrift.Warehouse.MssqlTools.Models;
using RepoDb;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Tests.MssqlTools")]
namespace Bygdrift.MssqlTools.Helpers
{
    internal class PrepareTableForCsv
    {
        private readonly Mssql mssql;
        private readonly Csv csv;
        private readonly string tableName;
        private readonly string primaryKey;

        internal PrepareTableForCsv(Mssql mssql, Csv csv, string tableName, string primaryKey, bool truncateTable)
        {
            this.mssql = mssql;
            this.csv = csv;
            this.tableName = tableName;
            this.primaryKey = primaryKey;
            var sql = "";
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(nameof(tableName), "Table name is null. It has to be set.");

            var colTypes = GetCsvAndSqlColumnTypes();
            if (colTypes == null || !colTypes.Any())
                return;

            if (colTypes.All(o => !o.SqlIsSet))
                sql = CreateTableAndColumns(colTypes);
            else
                sql = UpdateColumns(colTypes);

            if (!string.IsNullOrEmpty(sql))
            {
                try
                {
                    mssql.Connection.ExecuteNonQuery(sql);
                }
                catch (Exception e)
                {
                    mssql.App.Log.LogError(e, "Error in db load: {Message}. Commands: {Commands}", e.Message, sql);
                    throw new Exception($"Error in db load: {e.Message}. Commands: {sql}", e);
                }
                FlushRepoDb();
            }
        }

        private List<ColumnTypeExtend> GetCsvAndSqlColumnTypes()
        {
            var res = new List<ColumnTypeExtend>();
            var sqlColTypes = mssql.GetColumnTypes(tableName);
            var csvHeaders = csv.Headers.Values.Select(o => o.ToString()).ToList();

            foreach (var sqlColType in sqlColTypes)
            {
                if (csv.TryGetColId(sqlColType.Name, out int csvColId, false))  //Not caseSensitive because SQL are not
                {
                    var csvIsPrimaryKey = primaryKey != null && primaryKey.Equals(sqlColType.Name);
                    res.Add(new ColumnTypeExtend(sqlColType, csv.ColTypes[csvColId], csv.ColMaxLengths[csvColId], csvIsPrimaryKey));
                }
                else
                    res.Add(new ColumnTypeExtend(sqlColType));
            }

            var notInSqlHeaders = csvHeaders.Except(sqlColTypes.Select(o => o.Name));
            foreach (var name in notInSqlHeaders)
            {
                if (csv.TryGetColId(name, out int csvColId, false) && csv.ColTypes.Any() && csv.ColMaxLengths.Any())  //Not caseSensitive because SQL are not
                {
                    var isPrimaryKey = primaryKey != null && primaryKey.Equals(name);
                    res.Add(new ColumnTypeExtend(name, csv.ColTypes[csvColId], csv.ColMaxLengths[csvColId], isPrimaryKey));
                }
            }
            return res;
        }

        private string CreateTableAndColumns(List<ColumnTypeExtend> colTypes)
        {
            CreateSchemaIfNotExists();
            var cols = "";
            foreach (var colType in colTypes)
                cols += $"[{colType.Name}] {colType.CsvTypeExpression} " + (colType.CsvIsPrimaryKey ? "NOT NULL PRIMARY KEY" : "NULL") + ",\n";

            return $"CREATE TABLE [{mssql.App.ModuleName}].[{tableName}](\n{cols})";
        }

        private string UpdateColumns(List<ColumnTypeExtend> colTypes)
        {
            var sql = "";
            foreach (var colType in colTypes)
            {
                if (colType.TryGetUpdatedChangedType(out string typeExpression))
                    sql += SqlUpdateCommand(colType.Name, typeExpression, colType.SqlIsPrimaryKey, false);

                if (!colType.SqlIsSet)  //Add missing 
                    sql += SqlUpdateCommand(colType.Name, colType.CsvTypeExpression, colType.CsvIsPrimaryKey, true);
            }
            return sql;
        }

        private string SqlUpdateCommand(string name, string expression, bool isPrimaryKey, bool addColumn)
        {
            string alter = addColumn ? "ADD" : "ALTER COLUMN";
            if (!isPrimaryKey)
                return $"ALTER TABLE [{mssql.App.ModuleName}].[{tableName}] {alter} [{name}] {expression};\n";

            var commands = "DECLARE @constraint varchar(128);\n";
            commands += $"SELECT @constraint = CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '{mssql.App.ModuleName}' AND TABLE_NAME = '{tableName}';\n";
            commands += $"if (@constraint) IS NOT NULL EXEC('ALTER TABLE [{mssql.App.ModuleName}].[{tableName}] DROP CONSTRAINT ' + @constraint);\n";
            commands += $"ALTER TABLE [{mssql.App.ModuleName}].[{tableName}] {alter} [{name}] {expression} NOT NULL;\n";
            commands += $"ALTER TABLE [{mssql.App.ModuleName}].[{tableName}] ADD CONSTRAINT [{name}_pk] PRIMARY KEY ([{name}]);\n";
            return commands;
        }

        private void CreateSchemaIfNotExists()
        {
            mssql.Connection.ExecuteNonQuery($"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{mssql.App.ModuleName}') BEGIN EXEC('CREATE SCHEMA {mssql.App.ModuleName}') END");
        }

        /// <summary>
        /// Necesary if there has been any alterings
        /// </summary>
        public static void FlushRepoDb()
        {
            DbFieldCache.Flush(); // Remove all the cached DbField
            FieldCache.Flush(); // Remove all the cached DbField
            IdentityCache.Flush(); // Remove all the cached DbField
            PrimaryCache.Flush(); // Remove all the cached DbField
        }
    }
}
