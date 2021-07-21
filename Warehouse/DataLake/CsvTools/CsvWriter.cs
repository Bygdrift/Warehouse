using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace Bygdrift.Warehouse.DataLake.CsvTools
{
    public static class CsvWriter
    {
        public static void Write(this CsvSet csv, string destPath, int? take = null)
        {
            if (!csv.Records.Any()) return;

            var stream = csv.Write(take);

            var directory = Path.GetDirectoryName(destPath);
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);

            using var fileStream = new FileStream(destPath, FileMode.Create, FileAccess.Write);
            stream.CopyTo(fileStream);
        }

        public static Stream Write(this CsvSet csv, int? take = null)
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture; // Ensures right decimal punctiation

            var stream = new MemoryStream();
            if (csv.Records.Count != 0)
            {
                using var writer = new StreamWriter(stream, encoding: Encoding.UTF8, leaveOpen: true);
                writer.WriteLine(WriteRow(csv, null));  //Header

                for (int r = csv.RowLimit.Min; r <= csv.RowLimit.Max; r++)
                {
                    if (take != null && r == take)
                        break;

                    writer.WriteLine(WriteRow(csv, r));
                }

                writer.Flush();
            }

            stream.Position = 0;
            return stream;
        }

        /// <param name="r">If r is null, it is a header. Else it is a normal row</param>
        private static StringBuilder WriteRow(CsvSet csv, int? r)
        {
            ///Every record must have same number of lines
            ///Able to have line breaks within af record by quoting the sentence
            ///Fields with embedded commas or double-quote characters must be quoted: 1997,Ford,E350,"Super, luxurious truck"
            ///Each of the embedded double-quote characters must be represented by a pair of double-quote characters: 1997,Ford,E350,"Super, ""luxurious"" truck"
            ///Trim leading and trailng spaces
            ///spaces outside quotes in a field are not allowed
            ///Its okay to have a single quote in a record
            var chars = new char[] { '"', ',', '\n' };
            var row = new StringBuilder();

            for (int c = csv.ColLimit.Min; c <= csv.ColLimit.Max; c++)
            {
                string val;
                if (r != null)
                {
                    if (csv.Records.TryGetValue((c, (int)r), out object recordVal))
                        val = recordVal != null ? recordVal.ToString().Trim() : "";
                    else
                        val = "";
                }
                else
                {
                    if (csv.Headers.TryGetValue(c, out object headerVal))
                        val = headerVal.ToString();
                    else
                        val = "Col_" + c;
                }

                if (val.IndexOfAny(chars) != -1)
                    val = "\"" + val.Replace("\"", "\"\"") + "\"";

                if (c < csv.ColLimit.Max)
                    val += ',';

                row.Append(val);
            }
            return row;
        }

    }
}
