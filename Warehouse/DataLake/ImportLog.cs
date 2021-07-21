using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using Bygdrift.Warehouse.DataLake.CsvTools;
using Bygdrift.Warehouse.Modules;

namespace Bygdrift.Warehouse.DataLake
{
    /// <summary>
    /// Creates / updates a csv log with import informations
    /// </summary>
    class ImportLog
    {
        public static CsvSet CreateLog(IConfigurationRoot config, string module, string logTableName, List<IRefine> refines, bool uploadToDataLake)
        {
            var ingest = new Ingest(config, module, logTableName);
            var csv = new CsvSet();
            csv.AddHeaders("Table, Uploaded, Headers, ErrorsCount, Errors");

            var r = 0;
            foreach (var item in refines)
                if (item.TableName != logTableName)
                {
                    csv.AddRecord(0, r, item.TableName);
                    csv.AddRecord(1, r, item.FileDate);
                    csv.AddRecord(2, r, item.CsvSet.Headers.Count.ToString());
                    csv.AddRecord(3, r, item.Errors != null ? item.Errors.Count() : 0);
                    csv.AddRecord(4, r, ErrorsAsString(item.Errors));
                    r++;
                }
            if(uploadToDataLake)
                ingest.SaveAsCurrent(csv, DateTime.UtcNow, false);

            return csv;
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

