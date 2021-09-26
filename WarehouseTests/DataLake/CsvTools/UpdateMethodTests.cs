using Bygdrift.Warehouse.DataLake.CsvTools;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace WarehouseTests.DataLake.CsvTools
{
    [TestClass]
    public class UpdateMethodTests
    {
        [TestMethod]
        public void TestMethod1()
        {
            var updater = new UpdateMethod(GetCsvToUpdate(), "Id");
            updater.Merge(GetCsvToMerge());
            var merged = updater.CsvToUpdate;
            Assert.IsTrue(merged.RowLimit.Max == 2);
            var lastRow = merged.GetRecordRow(2);
            Assert.IsTrue(lastRow.TryGetValue(2, out object lastRowValue));
            Assert.IsTrue((string)lastRowValue == "c");
        }

        [TestMethod]
        public void TestMethod2()
        {
            var updater = new UpdateMethod(GetCsvToUpdate(), "Id", "Date");
            updater.Merge(GetCsvToMerge());
            var merged = updater.CsvToUpdate;
            Assert.IsTrue(merged.RowLimit.Max == 2);
            var lastRow = merged.GetRecordRow(2);
            Assert.IsTrue(lastRow.TryGetValue(2, out object lastRowValue));
            Assert.IsTrue((string)lastRowValue == "c");
        }

        [TestMethod]
        public void TestMethod3()
        {
            var updater = new UpdateMethod(GetCsvToUpdate(), "Date");
            updater.Merge(GetCsvToMerge());
            var merged = updater.CsvToUpdate;
            Assert.IsTrue(merged.RowLimit.Max == 1);
        }


        private static CsvSet GetCsvToUpdate()
        {
            var csv = new CsvSet();
            csv.AddHeaders("Id, Date, Data");
            csv.AddRecord(0, 0, 1);
            csv.AddRecord(1, 0, new DateTime(2021, 1, 1));
            csv.AddRecord(2, 0, "a");
            csv.AddRecord(0, 1, 2);
            csv.AddRecord(1, 1, new DateTime(2021, 1, 2));
            csv.AddRecord(2, 1, "b");
            return csv;
        }

        private static CsvSet GetCsvToMerge()
        {
            var csv = new CsvSet();
            csv.AddHeaders("Id, Date, Data");
            csv.AddRecord(0, 0, 3);
            csv.AddRecord(1, 0, new DateTime(2021, 1, 1));
            csv.AddRecord(2, 0, "c");
            return csv;
        }
    }
}
