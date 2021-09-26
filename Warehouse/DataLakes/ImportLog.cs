using System;
using System.Collections.Generic;
using System.Linq;
using Bygdrift.Warehouse.Modules;
using Bygdrift.CsvTools;

namespace Bygdrift.Warehouse.DataLakes
{
    /// <summary>
    /// Creates / updates a csv log with import informations
    /// </summary>
    class ImportLog
    {
        public static RefineBase CreateLog(string connectionString, string container, string module, string logTableName, IEnumerable<RefineBase> refines, bool uploadToDataLake)
        {
            var res = new RefineBase(null, null, logTableName, true, FolderStructure.Path);
            res.Csv.AddHeaders("Table, Uploaded, Headers, ErrorsCount, Errors");

            var r = 0;
            foreach (var item in refines)
                if (item.TableName != logTableName)
                {
                    res.Csv.AddRecord(0, r, item.TableName);
                    res.Csv.AddRecord(1, r, item.CsvFileDateTime.ToUniversalTime());
                    res.Csv.AddRecord(2, r, item.Csv.Headers.Count.ToString());
                    res.Csv.AddRecord(3, r, item.Errors != null ? item.Errors.Count() : 0);
                    res.Csv.AddRecord(4, r, ErrorsAsString(item.Errors));
                    r++;
                }

            if(uploadToDataLake)
                Save(connectionString, container, module, logTableName, res.Csv, DateTime.UtcNow);

            return res;
        }

        private static void Save(string connectionString, string container, string module, string logTableName, Csv csv, DateTime utcNow)
        {
            var dataLake = new DataLake(connectionString, container, module);
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

