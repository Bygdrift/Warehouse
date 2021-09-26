using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Bygdrift.Warehouse.DataLake.CsvTools
{
    public static class FilterMethods
    {
        /// <summary>
        /// Returns a csv with all records that are filered
        /// </summary>
        /// <param name="headerName">The header to filter on</param>
        /// <param name="values">If a value is present in the looked up column, the row will be included</param>
        /// <returns></returns>
        public static CsvSet Filter(this CsvSet csv, string headerName, string value) => Filter(csv, headerName, new[] { value });


        /// <summary>
        /// Returns a csv with all records that are filered
        /// </summary>
        /// <param name="headerName">The header to filter on</param>
        /// <param name="values">If a value is presneted in the looked up column, the row will be included</param>
        /// <returns></returns>
        public static CsvSet Filter(this CsvSet csv, string headerName, params object[] values)
        {
            var res = new CsvSet();
            res.AddHeaders(csv.Headers);

            var r = 0;
            foreach (var lookupValue in values.Distinct())
                foreach (var recordRows in csv.GetRecordRows(headerName, lookupValue))
                    res.AddRow(r++, recordRows.Value);

            return res;
        }
    }
}
