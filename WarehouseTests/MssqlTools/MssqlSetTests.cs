using Bygdrift.CsvTools;
using Bygdrift.Warehouse;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace Tests.MssqlTools
{
    [TestClass]
    public class MssqlSetCsvTests
    {
        private readonly AppBase app = new();

        [TestMethod]
        public void RemoveOldRows()
        {
            var table = "RemoveOldRows";
            var csv = new Csv("Id, Date").AddRow(1, DateTime.Now).AddRow(2, DateTime.Now.AddMonths(-5)).AddRow(3, DateTime.Now.AddMonths(-10));

            Assert.IsNull(app.Mssql.DeleteTable(table));
            Assert.IsNull(app.Mssql.MergeCsv(csv, table, "Id", false, false));
            Assert.IsNull(app.Mssql.RemoveOldRows(table, "Date", DateTime.Now.AddMonths(-6)));
            var csvFromReader = app.Mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader.Records.Count == 4);

            Assert.IsFalse(app.Log.GetErrorsAndCriticals().Any());
            app.Mssql.Dispose();
        }


        [TestMethod]
        public void TruncateTable()
        {
            var table = "TruncateTable";
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);

            Assert.IsNull(app.Mssql.DeleteTable(table));
            Assert.IsNull(app.Mssql.MergeCsv(csv, table, "Id", false, false));
            Assert.IsNull(app.Mssql.TruncateTable(table));
            var csvFromReader = app.Mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader.Records.Count == 0);
            Assert.IsFalse(app.Log.GetErrorsAndCriticals().Any());
            app.Mssql.Dispose();
        }
    }
}