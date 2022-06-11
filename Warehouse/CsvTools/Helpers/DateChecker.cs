using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Bygdrift.Warehouse.CsvTools.Helpers
{
    /// <summary>
    /// A class that verifies if a value is a date
    /// </summary>
    public class DateChecker
    {
        ///First look if string length is between dateFormats
        ///Then look if the amount of numbers are OK
        ///Then look if any of the delimiters are pressent
        ///Users should be able to add the dateFormats
        ///Remember to handle null
        ///Use the info from Azure about timezone

        public List<string> FormatsUsed = new();
        
        /// <summary>
        /// The min length of the string
        /// </summary>
        public int MinLength = 8;

        /// <summary>
        /// The max length of the string
        /// </summary>
        public int MaxLength = 19;

        /// <summary>
        /// The list of formats that the date checker verifies agianst. It is posible to add or replace these.
        /// </summary>
        public List<string> Formats = new()
        {
            "yyyy M d",
            "yyyy-M-d",
            "yyyy/M/d",
            "yyyy-M-dTH:m:s",
            "yyyy M d H:m:s",
            "yyyy-M-d H:m:s",
            "yyyy/M/d H:m:s",
            "d M yyyy",
            "d-M-yyyy",
            "d/M/yyyy",
            "d M yyyy H:m:s",
            "d-M-yyyy H:m:s",
            "d/M/yyyy H:m:s",
        };

        internal bool TryParse(string s, out DateTime value)
        {
            if (s.Length >= MinLength && s.Length <= MaxLength)
            {
                foreach (var item in FormatsUsed)
                    if (TryParse(s, item, out value))
                    {
                        //value = value.ToString("s");
                        return true;
                    }

                foreach (var item in Formats)
                {
                    if (TryParse(s, item, out value))
                    {
                        FormatsUsed.Add(item);
                        Formats.Remove(item);
                        return true;
                    }
                }
            }
            value = default!;
            return false;
        }

        private static bool TryParse(string s, string format, out DateTime value)
        {
            return DateTime.TryParseExact(s, format, CultureInfo.CurrentCulture, DateTimeStyles.None, out value);
        }
    }
}
