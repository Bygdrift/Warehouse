using Bygdrift.CsvTools;
using Bygdrift.Warehouse;
using Bygdrift.Warehouse.MssqlTools.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Tests.MssqlTools
{
    [TestClass]
    public class MssqlGetTests
    {
        private readonly AppBase app = new();

        [TestMethod]
        public void ValidatePrimaryKey()
        {
            var table = nameof(ValidatePrimaryKey);
            var csv = new Csv("Id, Name").AddRow(1).AddRow(2).AddRow(2).AddRecord(4, 1, null);

            var res = app.Mssql.ValidatePrimaryKey(csv, table, "Id");
            Assert.IsTrue(res.Length == 2);

            res = app.Mssql.ValidatePrimaryKey(csv, table, "NotExisting");
            Assert.IsTrue(res.Length == 1);

            csv = new Csv("Id, Name");
            res = app.Mssql.ValidatePrimaryKey(csv, table, "Id");
            Assert.IsNull(res);
        }

        [TestMethod]
        public void GetAsCsv()
        {
            var csv = new Csv("Id, Data, Date, age").AddRow(new Random().Next(1, 5000), "Some text", DateTime.Now, 22).AddRow(new Random().Next(1, 5000), "Some new text", DateTime.Now, 23);
            var table = PurgeCreateTable(nameof(GetAsCsv), csv);

            var csvFromReader = app.Mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader.Headers.Count == 4);
            var csvFromReader2 = app.Mssql.GetAsCsv(table, "Id", "Data");
            Assert.IsTrue(csvFromReader2.Headers.Count == 2);
        }

        [TestMethod]
        public void GetColumnTypes()
        {
            var csv = new Csv("Id, Data, Date, age, decimal").AddRow(new Random().Next(1, 5000), "Some text", DateTime.Now, 22, 36.8);
            var table = PurgeCreateTable(nameof(GetColumnTypes), csv);

            List<ColumnType> a = app.Mssql.GetColumnTypes(table).ToList();
        }

        [TestMethod]
        public void SetReadDeletes()
        {
            var csv = new Csv("Id, Data, Date, age").AddRow(new Random().Next(1, 5000), "Some text", DateTime.Now, 22).AddRow(new Random().Next(1, 5000), "Some new text", DateTime.Now, 23);
            var table = PurgeCreateTable(nameof(SetReadDeletes), csv);

            var csvFromReader = app.Mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader.ColLimit.Equals((1, 4)));
            Assert.IsTrue(csvFromReader.RowLimit.Equals((1, 2)));
            Assert.IsTrue(csvFromReader.GetColRecords(2).Values.Any(o => o.Equals("Some text")));  //Id changes and gets listed sorted by ID, so I cannot say return cscFromReader [1,0]
        }

        private string PurgeCreateTable(string tableName, Csv csv)
        {
            Assert.IsNull(app.Mssql.DeleteTable(tableName));
            Assert.IsNull(app.Mssql.MergeCsv(csv, tableName, "Id", false, false));
            return tableName;
        }

    }
}