using Bygdrift.Warehouse.DataLake.CsvTools;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;

namespace WarehouseTests.DataLake.CsvTools
{
    [TestClass]
    public class CsvReaderTests
    {
        /// <summary>Path to project base</summary>
        public static readonly string BasePath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..\\..\\..\\"));

        [TestMethod]
        public void CreateCsvTest()
        {
            using var stream = new FileStream(Path.Combine(BasePath, "Files", "Outlook", "test.csv"), FileMode.Open);
            var csv = new CsvReader(stream, 10).CsvSet;
        }
    }
}
