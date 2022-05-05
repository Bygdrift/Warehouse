using Bygdrift.CsvTools;
using Bygdrift.MssqlTools.Helpers;
using RepoDb;
using System;
using System.Collections.Generic;

namespace Bygdrift.MssqlTools
{
    /// <summary>
    /// Access to edit Microsoft SQL database data
    /// </summary>
    public partial class Mssql
    {

        /// <summary>
        /// Data will be inserted in the table and if there are no table, it will be created.
        /// If a column types or names has been changed, it will be managed.
        /// </summary>
        /// <param name="csv"></param>
        /// <param name="tableName"></param>
        /// <param name="truncateTable">If true, the table gets truncated and filed with new data</param>
        /// <param name="removeEmptyColumns">If true, all columns that only contains null data, will be removed</param>
        /// <returns>Null if no errors or else an array of errors. Errors are also send to AppBase</returns>
        public string[] InserCsv(Csv csv, string tableName, bool truncateTable, bool removeEmptyColumns)
        {
            var errors = new List<string>();
            if (!PrepareData(csv, removeEmptyColumns))
                return null;

            _ = new PrepareTableForCsv(this, csv, tableName, null, truncateTable);
            var data = csv.ToExpandoList();

            try
            {
                if (truncateTable)
                    TruncateTable(tableName);

                if (csv.RowLimit.Max < 100)
                    Connection.InsertAll($"[{App.ModuleName}].[{tableName}]", data, csv.RowLimit.Max, commandTimeout: 3600);
                else
                    Connection.BulkInsert($"[{App.ModuleName}].[{tableName}]", data, bulkCopyTimeout: 3600);
            }
            catch (Exception e)
            {
                AddErrors(errors, e.Message);
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
        /// <returns>Null if no errors or else an array of errors. Errors are also send to AppBase</returns>
        public string[] MergeCsv(Csv csv, string tableName, string primaryKey, bool truncateTable, bool removeEmptyColumns = false)
        {
            var errors = new List<string>();
            if (!PrepareData(csv, removeEmptyColumns))
                return null;

            if (primaryKey == null)
                return AddErrors(errors, "PrimaryKey cannot be set to null. Use the InsertCsv() method instead.");

            var validation = ValidatePrimaryKey(csv, tableName, primaryKey);
            if (validation != null)
                return AddErrors(errors, validation);

            _ = new PrepareTableForCsv(this, csv, tableName, primaryKey, truncateTable);
            
            var data = csv.ToExpandoList();

            if (truncateTable)
            {
                try
                {
                    TruncateTable(tableName);
                    //Connection.DeleteAll($"[{App.ModuleName}].[{tableName}]", commandTimeout:3600);

                    if (csv.RowLimit.Max < 100)
                        Connection.InsertAll($"[{App.ModuleName}].[{tableName}]", data, csv.RowLimit.Max + 1, commandTimeout: 3600);
                    else
                        Connection.BulkInsert($"[{App.ModuleName}].[{tableName}]", data, bulkCopyTimeout: 3600);
                }
                catch (Exception e)
                {
                    AddErrors(errors, e.Message);
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
                    AddErrors(errors, e.Message);
                }
            }

            return errors.Count == 0 ? null : errors.ToArray();
        }

        /// <returns>False if there is no content</returns>
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

        /// <returns>All accumulated errors</returns>
        private string[] AddErrors(List<string> errors, string newError)
        {
            errors.Add(newError);
            App.Log.LogError(newError);
            return errors.ToArray();
        }

        /// <returns>All accumulated errors</returns>
        private string[] AddErrors(List<string> errors, string[] newErrors)
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
