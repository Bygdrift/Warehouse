using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;

namespace Bygdrift.Warehouse.DataLake.CsvTools
{
    /// <summary>
    /// Converts dynamic data to a format that can be ingested to the datalake as csv
    /// </summary>
    public class CsvSet
    {
        //internal static readonly Regex csvSplit = new Regex("(?:^|,)(\"(?:[^\"])*\"|[^,]*)", RegexOptions.Compiled);

        public Dictionary<int, object> Headers { get; internal set; }
        public Dictionary<int, Type> ColTypes { get; internal set; }
        public Dictionary<(int Col, int Row), object> Records { get; internal set; }

        public CsvSet() => Init();

        public CsvSet(string headers)
        {
            Init();
            AddHeaders(headers);
        }

        private void Init()
        {
            Headers = new Dictionary<int, object>();
            ColTypes = new Dictionary<int, Type>();
            Records = new Dictionary<(int col, int row), object>();
        }

        /// <param name="col">First col has lowest number and the same number used to referenced records</param>
        /// <param name="value">Headers name</param>
        public bool AddHeader(int col, object value)
        {
            value = UniqueHeader(value);
            if (!Headers.TryAdd(col, value))
                return false;

            if (ColTypes == null || !ColTypes.ContainsKey(col))
                ColTypes.Add(col, null);  //Remember to tak care of those nulls that doesnt gets parsed to string, decimal, lon and bool

            if (_colLimit.Min > col || _colLimit.Min == null)
                _colLimit.Min = col;
            if (_colLimit.Max < col || _colLimit.Max == null)
                _colLimit.Max = col;

            return true;
        }

        public bool AddHeader(object value, out int col)
        {
            col = _colLimit.Max == null ? 0 : ColLimit.Max + 1;
            return AddHeader(col, value);
        }

        /// <summary>First header col is 0</summary>
        public bool AddHeaders(string headers)
        {
            var res = true;
            foreach (var item in CsvReader.SplitString(headers))
                if (!AddHeader(item.Key, item.Value.ToString().Trim()))
                    res = false;

            return res;
        }

        public bool AddHeaders(Dictionary<int, object> headers)
        {
            var res = true;
            foreach (var item in headers)
                if (!AddHeader(item.Key, item.Value))
                    res = false;

            return res;
        }

        /// <summary>If header does not exist, then create it and retrun the col</summary>
        public int GetOrCreateHeader(object headerName)
        {
            var header = Headers.SingleOrDefault(o => o.Value.Equals(headerName));
            if (header.Value != null)
                return header.Key;

            var col = ColLimit.Max + 1;
            AddHeader(col, headerName);
            return col;
        }

        public bool AddRecord(int col, int row, object value)
        {
            if (!Records.TryAdd((col, row), value))
                return false;

            VerifyColType(col, value);

            if (_colLimit.Min > col || _colLimit.Min == null)
                _colLimit.Min = col;
            if (_colLimit.Max < col || _colLimit.Max == null)
                _colLimit.Max = col;
            if (_rowLimit.Min > row || _rowLimit.Min == null)
                _rowLimit.Min = row;
            if (_rowLimit.Max < row || _rowLimit.Max == null)
                _rowLimit.Max = row;

            return true;
        }

        /// <param name="rows">Data like ["a,b","c,d"]</param>
        public bool AddRecords(string[] rows)
        {
            var res = true;

            for (int r = 0; r < rows.Length; r++)
                foreach (var item in CsvReader.SplitString(rows[r]))
                    if (!AddRecord(item.Key, r, item.Value))
                        res = false;

            return res;
        }

        public bool AddRow(Dictionary<int, object> columnRecords)
        {
            var r = RowLimit.Max + 1;
            foreach (var record in columnRecords)
                if (!AddRecord(record.Key, r, record.Value))
                    return false;

            return true;
        }

        public bool AddRow(int row, Dictionary<int, object> columnRecords)
        {
            foreach (var record in columnRecords)
                if (!AddRecord(record.Key, row, record.Value))
                    return false;

            return true;
        }

        public void UpdateRecord(int col, int row, object value)
        {
            Records[(col, row)] = value;
            VerifyColType(col, value);
        }

        public void RenameHeader(object origName, object newName)
        {
            var res = Headers.SingleOrDefault(o => o.Value.Equals(origName));
            if (res.Value != null)
                Headers[res.Key] = newName;
        }

        public void RemoveColumn(int col)
        {
            if (col < ColLimit.Min || col > ColLimit.Max)
                return;

            var newHeaders = new Dictionary<int, object>();
            var newColTypes = new Dictionary<int, Type>();
            var newRecords = new Dictionary<(int Col, int Row), object>();

            int newCol = ColLimit.Min;
            for (int origCol = ColLimit.Min; origCol <= ColLimit.Max; origCol++)
            {
                if (origCol != col)
                {
                    if (Headers.TryGetValue(origCol, out object header))
                        newHeaders.Add(newCol, header);
                    newColTypes.Add(newCol, ColTypes[origCol]);

                    for (int row = RowLimit.Min; row <= RowLimit.Max; row++)
                        newRecords.Add((newCol, row), Records[(origCol, row)]);

                    newCol++;
                }
            }

            Headers = newHeaders;
            ColTypes = newColTypes;
            Records = newRecords;
            _colLimit.Max--;
        }

        /// <summary>
        /// If there are two or more headers with the same name, they wil get a suffix of _n so thre times Id will become Id, Id_2, Id_3
        /// </summary>
        private object UniqueHeader(object value)
        {
            var res = value;
            if (Headers.Any(o => o.Value.Equals(value)))
            {
                var i = 2;
                while (Headers.Any(o => o.Value.Equals(res)))
                    res = value + "_" + i++;
            }
            return res;
        }

        private (int? Min, int? Max) _colLimit = (null, null);
        public (int Min, int Max) ColLimit
        {
            get { return (_colLimit.Min ?? 0, _colLimit.Max ?? 0); }
        }

        private (int? Min, int? Max) _rowLimit = (null, null);
        public (int Min, int Max) RowLimit
        {
            get { return (_rowLimit.Min ?? 0, _rowLimit.Max ?? 0); }
        }

        private void VerifyColType(int col, object value)
        {
            var valueAsString = Convert.ToString(value, CultureInfo.InvariantCulture);
            if (ColTypes != null && ColTypes.ContainsKey(col))
            {
                var originType = ColTypes[col];
                if (originType != null && IsValueType(valueAsString, originType))
                    return;
            }
            else
                ColTypes.Add(col, null);

            if (long.TryParse(valueAsString, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out _))
            {
                ColTypes[col] = typeof(long);
                return;
            }
            if (decimal.TryParse(valueAsString, NumberStyles.Any, NumberFormatInfo.InvariantInfo, out _))
            {
                ColTypes[col] = typeof(decimal);
                return;
            }
            if (bool.TryParse(valueAsString, out _))
            {
                ColTypes[col] = typeof(bool);
                return;
            }
            if (DateTime.TryParse(valueAsString, out _))
            {
                ColTypes[col] = typeof(DateTime);
                return;
            }
            ColTypes[col] = typeof(string);
        }

        private bool IsValueType(string value, Type type)
        {
            if (type == typeof(string)) return true;
            if (type == typeof(long)) return long.TryParse(value, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out _);
            if (type == typeof(decimal)) return decimal.TryParse(value, NumberStyles.Any, NumberFormatInfo.InvariantInfo, out _);
            if (type == typeof(bool)) return bool.TryParse(value, out _);
            if (type == typeof(DateTime)) return DateTime.TryParse(value, out _);
            return false;
        }
    }
}