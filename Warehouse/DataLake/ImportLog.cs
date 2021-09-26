using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using Bygdrift.Warehouse.DataLake.CsvTools;
using Bygdrift.Warehouse.Modules;
using Bygdrift.Warehouse.DataLake.DataLakeTools;

namespace Bygdrift.Warehouse.DataLake
{
    /// <summary>
    /// Creates / updates a csv log with import informations
    /// </summary>
    class ImportLog
    {
        public static RefineBase CreateLog(string connectionString, string container, string module, string logTableName, IEnumerable<RefineBase> refines, bool uploadToDataLake)
        {
            var res = new RefineBase(null, null, logTableName, true, FolderStructure.Path);
            res.CsvSet.AddHeaders("Table, Uploaded, Headers, ErrorsCount, Errors");

            var r = 0;
            foreach (var item in refines)
                if (item.TableName != logTableName)
                {
                    res.CsvSet.AddRecord(0, r, item.TableName);
                    res.CsvSet.AddRecord(1, r, item.CsvFileDateTime.ToUniversalTime());
                    res.CsvSet.AddRecord(2, r, item.CsvSet.Headers.Count.ToString());
                    res.CsvSet.AddRecord(3, r, item.Errors != null ? item.Errors.Count() : 0);
                    res.CsvSet.AddRecord(4, r, ErrorsAsString(item.Errors));
                    r++;
                }

            if(uploadToDataLake)
                Save(connectionString, container, module, logTableName, res.CsvSet, DateTime.UtcNow);

            return res;
        }

        private static void Save(string connectionString, string container, string module, string logTableName, CsvSet csv, DateTime utcNow)
        {
            var dataLake = new DataLakeTools.DataLake(connectionString, container, module);
            dataLake.SaveCsv(null, logTableName + ".csv", csv);
        }

        private static string ErrorsAsString(List<string> errors)
        {
            var res = "";
            if (errors != null)
                foreach (var item in errors)
                    res += item + "\r\n";

            return res;
        }
    }
}

