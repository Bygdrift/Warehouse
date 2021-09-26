using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Bygdrift.Warehouse.DataLake.CsvTools
{
    public static class GetMethods
    {
        /// <summary>
        /// Builds a combination off all values from headers
        /// </summary>
        /// <returns>All data combinedd with a comma in keay, while value i row number</returns>
        public static Dictionary<string, int> GetCompositeKeys(this CsvSet csv, params string[] headers)
        {
            if (csv == null)
                return default;

            var res = new Dictionary<string, int>();
            var cols = new List<int>();
            foreach (var header in headers)
                if (csv.TryGetRecordCol(header, out int col))
                    cols.Add(col);

            var separator = headers.Length > 0 ? "," : "";
            for (int r = csv.RowLimit.Min; r <= csv.RowLimit.Max; r++)
            {
                var rowVal = "";
                foreach (var col in cols)
                  rowVal += (csv.Records.TryGetValue((col, r), out object val) ? val.ToString() : "") + separator;

                res.TryAdd(separator.Length > 0 ? rowVal.TrimEnd(',') : rowVal, r);
            }

            return res;
        }

        public static Dictionary<int, object> GetRecordCol(this CsvSet csv, int col, bool includeNullValues = true)
        {
            var res = new Dictionary<int, object>();
            for (int r = csv.RowLimit.Min; r <= csv.RowLimit.Max; r++)
                if (csv.Records.TryGetValue((col, r), out object val))
                    res.Add(r, val);
                else if (includeNullValues)
                    res.Add(r, default);

            return res;
        }

        public static (int Col, Dictionary<int, object> Records) GetRecordCol(this CsvSet csv, object headerName, bool includeNullValues = true)
        {
            return csv.TryGetRecordCol(headerName, out int header) ? (header, csv.GetRecordCol(header, includeNullValues)) : default;
        }

        /// <param name="lookupHeader">The name of the header to lookup the value in</param>
        /// <param name="returnHeader">The header column to return. If null, the lookupHeader will be returned</param>
        /// <param name="lookupValue">The value to lookup within the headerName column</param>
        public static Dictionary<int, object> GetRecordCol(this CsvSet csv, object lookupHeader, object returnHeader, object lookupValue, bool includeNullValues = true)
        {
            var res = new Dictionary<int, object>();
            if (csv.TryGetRecordCol(returnHeader, out int returnColumn) || csv.TryGetRecordCol(lookupHeader, out returnColumn))
            {
                var rowRecords = csv.GetRecordRows(lookupHeader, lookupValue);
                foreach (var rowRecord in rowRecords)
                    res.Add(rowRecord.Key, rowRecord.Value[returnColumn]);
            }
            return res;
        }

        /// <summary>
        /// All rows that satisfies the expresion, are returned
        /// </summary>
        public static Dictionary<int, Dictionary<int, object>> GetRecordCols(this CsvSet csv, Func<KeyValuePair<(int Col, int Row), object>, bool> expr, bool includeNullValues = true)
        {
            var res = new Dictionary<int, Dictionary<int, object>>();
            var cols = csv.Records.Where(expr).Select(o => o.Key.Row).Distinct();
            foreach (var c in cols)
            {
                var row = new Dictionary<int, object>();
                for (int r = csv.ColLimit.Min; r <= csv.ColLimit.Max; r++)
                    if (csv.Records.TryGetValue((c, r), out object val))
                        row.Add(r, val);
                    else if (includeNullValues)
                        row.Add(r, default);

                res.Add(c, row);
            }
            return res;
        }


        public static Dictionary<int, object> GetRecordRow(this CsvSet csv, int row, bool includeNullValues = true)
        {
            var res = new Dictionary<int, object>();

            for (int c = csv.ColLimit.Min; c <= csv.ColLimit.Max; c++)
                if (csv.Records.TryGetValue((c, row), out object val))
                    res.Add(c, val);
                else if (includeNullValues)
                    res.Add(c, default);

            return res;
        }

        public static Dictionary<int, Dictionary<int, object>> GetRecordRows(this CsvSet csv, bool includeNullValues = true)
        {
            var res = new Dictionary<int, Dictionary<int, object>>();
            foreach (var r in csv.Records.Select(o => o.Key.Row).Distinct())
            {
                var row = new Dictionary<int, object>();
                for (int c = csv.ColLimit.Min; c <= csv.ColLimit.Max; c++)
                    if (csv.Records.TryGetValue((c, r), out object val))
                        row.Add(c, val);
                    else if (includeNullValues)
                        row.Add(c, default);

                res.Add(r, row);
            }
            return res;
        }

        /// <summary>
        /// All rows that satisfies the expresion, are returned
        /// </summary>
        public static Dictionary<int, Dictionary<int, object>> GetRecordRows(this CsvSet csv, Func<KeyValuePair<(int Col, int Row), object>, bool> expr, bool includeNullValues = true)
        {
            var res = new Dictionary<int, Dictionary<int, object>>();
            foreach (var r in csv.Records.Where(expr).Select(o => o.Key.Row).Distinct())
            {
                var row = new Dictionary<int, object>();
                for (int c = csv.ColLimit.Min; c <= csv.ColLimit.Max; c++)
                    if (csv.Records.TryGetValue((c, r), out object val))
                        row.Add(c, val);
                    else if (includeNullValues)
                        row.Add(c, default);

                res.Add(r, row);
            }
            return res;
        }

        /// <param name="headerName">The name of the header to lookup the value in</param>
        /// <param name="value">The value to lookup within the headerName column</param>
        public static Dictionary<int, Dictionary<int, object>> GetRecordRows(this CsvSet csv, object headerName, object value, bool includeNullValues = true)
        {
            return csv.TryGetRecordCol(headerName, out int col) ? csv.GetRecordRows(o => o.Key.Col == col && o.Value.Equals(value), includeNullValues) : default;
        }

        /// <param name="csv"></param>
        /// <param name="filters">{"headerName 1": "Value 1", "headerName n": "Value n"}</param>
        /// <param name="includeNullValues"></param>
        /// <returns></returns>
        public static Dictionary<int, Dictionary<int, object>> GetRecordRows(this CsvSet csv, JObject filters, bool includeNullValues = true)
        {
            var res = new Dictionary<int, Dictionary<int, object>>();
            var itemsToRemove = new List<int>();

            foreach (var filter in filters)
            {
                if (csv.TryGetRecordCol(filter.Key, out int headerCol))
                {
                    var rows = csv.GetRecordRows(o => o.Key.Col.Equals(headerCol) && o.Value.Equals(filter.Value.ToString()), includeNullValues);
                    if (!res.Any())
                        res = rows;
                    else
                        foreach (var item in res)
                            if (!rows.ContainsKey(item.Key))
                                itemsToRemove.Add(item.Key);
                }
            }

            foreach (var item in itemsToRemove)
                res.Remove(item);

            return res;
        }

        public static bool TryGetRecordCol(this CsvSet csv, object headerName, out int col)
        {
            try
            {
                var header = csv.Headers.Single(o => o.Value.ToString().Equals(headerName));
                col = header.Key;
                return header.Value != default;
            }
            catch (Exception)
            {
                throw new Exception("Error in reading headerName. A programmer must take care of this issue.");
            }
        }
    }
}
