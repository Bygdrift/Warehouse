using Bygdrift.Warehouse.DataLake.CsvTools;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace Bygdrift.Warehouse.DataLake.CsvTools
{
    public class CsvReader
    {
        private CsvSet _csvSet;
        internal static readonly Regex csvSplit = new Regex("(?:^|,)(\"(?:[^\"])*\"|[^,]*)", RegexOptions.Compiled);

        public CsvSet CsvSet
        {
            get { return _csvSet; }
            private set { _csvSet = value; }
        }

        public CsvReader(Stream stream, int? take = null)
        {
            if (stream.Length == 0)
                return;

            stream.Position = 0;
            _csvSet = new CsvSet();
            using var reader = new StreamReader(stream, leaveOpen: true);

            ReadRow(reader.ReadLine(), null);  //Header

            int r = 0;
            string line;
            while ((line = reader.ReadLine()) != null && (take == null || take != null && r < take))
                ReadRow(line, r++);
        }

        private void ReadRow(string input, int? r)
        {
            if (string.IsNullOrEmpty(input))
                return;

            int c = 0;
            foreach (Match match in csvSplit.Matches(input))
            {
                var val = match.Value?.TrimStart(',').Trim('"');

                if (r == null)
                    _csvSet.AddHeader(c, val);
                else
                    _csvSet.AddRecord(c, (int)r, val);
                c++;
            }
        }

        internal static Dictionary<int, object> SplitString(string input)
        {
            var res = new Dictionary<int, object>();
            if (!string.IsNullOrEmpty(input))
            {
                int c = 0;
                foreach (Match match in csvSplit.Matches(input))
                {
                    var val = match.Value?.TrimStart(',').Trim('"');
                    res.Add(c++, val);
                }
            }
            return res;
        }
    }
}
