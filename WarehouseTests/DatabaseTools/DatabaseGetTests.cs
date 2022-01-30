using Bygdrift.CsvTools;
using Bygdrift.MssqlTools;
using Bygdrift.Warehouse;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Linq;

namespace Tests.DatabaseTools
{
    [TestClass]
    public class DatabaseGetTests : IDisposable
    {
        private readonly AppBase app = new();

        [TestMethod]
        public void ValidatePrimaryKey()
        {
            var csv = new Csv("Id, Name");
            csv.AddRecord(1, 1, 1);
            csv.AddRecord(2, 1, 2);
            csv.AddRecord(3, 1, 2);
            csv.AddRecord(4, 1, null);

            var res = app.Mssql.ValidatePrimaryKey(csv, "ValidatePrimaryKey", "Id", out string[] validation);
            Assert.IsTrue(res == false);
            Assert.IsTrue(validation.Length == 2);

            res = app.Mssql.ValidatePrimaryKey(csv, "ValidatePrimaryKey", "NotExisting", out string[] validation2);
            Assert.IsTrue(res == false);
            Assert.IsTrue(validation2.Length == 1);

            csv = new Csv("Id, Name");
            res = app.Mssql.ValidatePrimaryKey(csv, "ValidatePrimaryKey", "Id", out string[] validation3);
            Assert.IsTrue(res == true);
            Assert.IsTrue(validation3.Length == 0);
        }

        [TestMethod]
        public void GetAsCsv()
        {
            app.Mssql.DeleteTable("GetAsCsv");
            var errors = app.Mssql.MergeCsv(CsvOne(), "GetAsCsv", "Id", false, false);
            Assert.IsTrue(errors == null);
            var csvFromReader = app.Mssql.GetAsCsv("GetAsCsv");
            Assert.IsTrue(csvFromReader.Headers.Count == 4);
            var csvFromReader2 = app.Mssql.GetAsCsv("GetAsCsv", "Id", "Data");
            Assert.IsTrue(csvFromReader2.Headers.Count == 2);
        }

        [TestMethod]
        public void GetFirstAndLastAsync()
        {
            //db.DropTable(schemaName, tableName);
            //var errors = db.MergeCsv(CsvOne(), schemaName, tableName, "Id", false, false);
            //Assert.IsTrue(errors == null);

            //var res = db.GetFirstAndLastAscending<int>(schemaName, tableName, "Id");
            //var res = app.Mssql.GetFirstAndLastAscending<DateTime>("GetFirstAndLast", "Date", false);
            //var res2 = db.GetFirstAndLastAscending<int>("something", tableName, "Id");

        }

        [TestMethod]
        public void SetReadDelets()
        {
            app.Mssql.DeleteTable("SetReadDelets");
            var errors = app.Mssql.MergeCsv(CsvOne(), "SetReadDelets", "Id", false, false);
            Assert.IsTrue(errors == null);
            var csvFromReader = app.Mssql.GetAsCsv("SetReadDelets");
            Assert.IsTrue(csvFromReader.ColLimit.Equals((1, 4)));
            Assert.IsTrue(csvFromReader.RowLimit.Equals((1, 2)));
            Assert.IsTrue(csvFromReader.GetColRecords(2).Values.Any(o => o.Equals("Some text")));  //Id changes and gets listed sorted by ID, so I cannot say return cscFromReader [1,0]
        }

        public void Dispose()
        {
            app.Mssql.Dispose();
        }

        private Csv CsvOne()
        {
            var res = new Csv("Id, Data, Date, age");
            res.AddRecord(1, 1, new Random().Next(1, 5000));
            res.AddRecord(1, 2, "Some text");
            res.AddRecord(1, 3, DateTime.Now);
            res.AddRecord(1, 4, 22);
            res.AddRecord(2, 1, new Random().Next(1, 5000));
            res.AddRecord(2, 2, "Some new text");
            res.AddRecord(2, 3, DateTime.Now);
            res.AddRecord(2, 4, 23);
            return res;
        }
    }
}