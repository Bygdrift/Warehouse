using Bygdrift.CsvTools;
using Bygdrift.Warehouse;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;

namespace Tests.DatabaseTools
{
    [TestClass]
    public class DatabaseSetTests : IDisposable
    {
        /// <summary>Path to project base</summary>
        public static readonly string BasePath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..\\..\\..\\"));
        private readonly AppBase app = new();

        [TestMethod]
        public void CreateAndTruncateTableFails()
        {
            //this would fail without flush fail and I asked for a solution here: https://stackoverflow.com/questions/69635893/repodb-doesnt-merge-correct-after-altering-a-column
            //If RepoDB didn't get flushed, the reulst would be: Id = 1 Data = "Lo", Name = NULL
            Assert.IsNull(app.Mssql.DeleteTable("CreateAndTruncateTableFails"));
            Assert.IsNull(app.Mssql.MergeCsv(CsvOne(), "CreateAndTruncateTableFails", "Id", false, false));
            Assert.IsNull(app.Mssql.MergeCsv(CsvTwo(), "CreateAndTruncateTableFails", "Id", false, false));
            var csvFromReader = app.Mssql.GetAsCsv("CreateAndTruncateTableFails");
            Assert.IsTrue(csvFromReader.Records[(1, 2)].Equals("Some more text"));
            Assert.IsTrue(csvFromReader.Records[(1, 5)].Equals("Knud"));
        }

        [TestMethod]
        public void DeleteExpiredColumns()
        {
            Assert.IsNull(app.Mssql.DeleteTable("DeleteExpiredColumns"));
            var csv = new Csv("Id, Date");
            csv.AddRecord(1, 1, 1);
            csv.AddRecord(1, 2, DateTime.Now);
            csv.AddRecord(2, 1, 2);
            csv.AddRecord(2, 2, DateTime.Now.AddMonths(-5));
            csv.AddRecord(3, 1, 3);
            csv.AddRecord(3, 2, DateTime.Now.AddMonths(-10));
            var errors = app.Mssql.MergeCsv(csv, "DeleteExpiredColumns", "Id", false, false);
            Assert.IsNull(errors);
            app.Mssql.RemoveOldRows("DeleteExpiredColumns", "Date", DateTime.Now.AddMonths(-6));
            var csvFromReader = app.Mssql.GetAsCsv("DeleteExpiredColumns");
            Assert.IsTrue(csvFromReader.Records.Count == 4);
        }

        /// <summary>
        /// If there are a column left that are all empty, it should be removed
        /// </summary>
        [TestMethod]
        public void IfColumnsGetsRemoved()
        {
            app.Mssql.DeleteTable("IfColumnsGetsRemoved");
            var csv = CsvOne();
            csv.AddHeader("EmptyColumn");
            app.Mssql.MergeCsv(csv, "IfColumnsGetsRemoved", "Id", false, true);
            var csvFromReader = app.Mssql.GetAsCsv("IfColumnsGetsRemoved");
            Assert.IsFalse(csvFromReader.TryGetColId("EmptyColumn", out int _));
        }

        [TestMethod]
        public void InsertCsvThenMerge()
        {
            ///TODO: Here I create a table without a primary key and then I create one. Handle it!
            app.Mssql.DeleteTable("SaveCsvAndOverwrite");
            var errors = app.Mssql.InserCsv(CsvOne(), "SaveCsvAndOverwrite", false, false);
            Assert.IsNull(errors);
            errors = app.Mssql.MergeCsv(CsvTwo(), "SaveCsvAndOverwrite", "Id", false, false);
            Assert.IsNull(errors);
        }

        [TestMethod]
        public void LoadMuchData()
        {
            var path = Path.Combine(BasePath, "Files", "Csv", "Height and weight for 25000 persons.csv");
            var csv = new Csv().FromCsvFile(path);
            Assert.IsNull(app.Mssql.DeleteTable("LoadMuchData"));
            Assert.IsNull(app.Mssql.MergeCsv(csv, "LoadMuchData", "Index", false, false));
        }

        [TestMethod]
        public void LoadMuchParallelData()
        {
            Assert.IsNull(app.Mssql.DeleteTable("LoadMuchParallelData"));
            //var tasks = new List<Task<string[]>>();

            for (int i = 0; i < 3; i++)
            {
                var csv = new Csv("Id, Data, Date, Age");
                csv.AddRecord(1, 1, i);
                csv.AddRecord(1, 2, "Some text");
                csv.AddRecord(1, 3, DateTime.Now);
                csv.AddRecord(1, 4, new Random().Next(1, 5000));
                app.Mssql.MergeCsv(csv, "LoadMuchParallelData", "Id", false, false);
            }
            //var errors = Task.WhenAll(tasks).Result.SelectMany(o => o);
        }

        /// <summary>
        /// This is not a test that fails.It only serves for testing time and thats the reason for commenting this method out.
        /// </summary>
        /// <returns></returns>

        [TestMethod]
        public void LoadMuchData2()
        {
            Assert.IsNull(app.Mssql.DeleteTable("LoadMuchData2"));
            var csv = new Csv("id, text1, text2, number");
            for (int r = 1; r < 100000; r++)
            {
                csv.AddRecord(r, 1, r);
                csv.AddRecord(r, 2, "This is a text that should indcate some length");
                csv.AddRecord(r, 3, "This is a text that should indcate some length");
                csv.AddRecord(r, 4, 105643256);
            }
            Assert.IsNull(app.Mssql.MergeCsv(csv, "LoadMuchData2", "id", false, false));
            Assert.IsNull(app.Mssql.DeleteTable("LoadMuchData2"));
            Assert.IsNull(app.Mssql.InserCsv(csv, "LoadMuchData2", false, false));
        }

        [TestMethod]
        public void NullableNumbers()
        {
            app.Mssql.DeleteTable("SaveCsvAndOverwrite");
            var csv = new Csv("Id, Data, Date, Age");
            csv.AddRecord(1, 1, 1);
            csv.AddRecord(1, 2, "Some text");
            csv.AddRecord(1, 3, DateTime.Now);
            csv.AddRecord(1, 4, 22);
            csv.AddRecord(2, 1, 2);
            csv.AddRecord(2, 4, null);

            var errors = app.Mssql.MergeCsv(csv, "SaveCsvAndOverwrite", "Id", false, false);
            Assert.IsNull(errors);
            errors = app.Mssql.MergeCsv(CsvTwo(), "SaveCsvAndOverwrite", "Id", false, false);
            Assert.IsNull(errors);
        }

        [TestMethod]
        public void SaveCsvAndOverwrite()
        {
            app.Mssql.DeleteTable("SaveCsvAndOverwrite");
            var errors = app.Mssql.MergeCsv(CsvOne(), "SaveCsvAndOverwrite", "Id", false, false);
            Assert.IsNull(errors);
            Assert.IsNull(app.Mssql.TruncateTable("SaveCsvAndOverwrite"));
            errors = app.Mssql.MergeCsv(CsvTwo(), "SaveCsvAndOverwrite", "Id", false, false);
            Assert.IsNull(errors);
        }

        [TestMethod]
        public void SaveEmptyCsv()
        {
            app.Mssql.DeleteTable("SaveEmptyCsv");
            var errors = app.Mssql.MergeCsv(new Csv(), "SaveEmptyCsv", "Id", false, false);
            Assert.AreEqual(errors.Length, 1);
            errors = app.Mssql.InserCsv(new Csv(), "SaveEmptyCsv", false, false);
            Assert.IsNull(errors);
        }

        [TestMethod]
        public void SetUpdateWithoutPrimaryKey()
        {
            var csv = CsvOne();
            csv.Records[(1, 1)] = 1;
            app.Mssql.DeleteTable("SetUpdateWithoutPrimaryKey");

            //Raise an error because primaryKey must not be null in this method
            var errors = app.Mssql.MergeCsv(csv, "SetUpdateWithoutPrimaryKey", null, false, false);
            Assert.IsTrue(errors.Length == 1);

            errors = app.Mssql.InserCsv(csv, "SetUpdateWithoutPrimaryKey", false, false);
            Assert.IsTrue(errors == null);
            var csvFromReader1 = app.Mssql.GetAsCsv("SetUpdateWithoutPrimaryKey");
            Assert.IsTrue(csvFromReader1.Records[(1, 2)].Equals("Some text"));

            app.Mssql.InserCsv(CsvTwo(), "SetUpdateWithoutPrimaryKey", false, false);
            var csvFromReader2 = app.Mssql.GetAsCsv("SetUpdateWithoutPrimaryKey");
            Assert.IsTrue(csvFromReader2.Records[(2, 2)].Equals("Some more text"));
        }

        [TestMethod]
        public void SetUpdateWithPrimaryKey()
        {
            app.Mssql.DeleteTable("SetUpdateWithPrimaryKey");
            var errors = app.Mssql.MergeCsv(CsvOne(), "SetUpdateWithPrimaryKey", "Id", false, false);
            Assert.IsTrue(errors == null);
            var csvFromReader1 = app.Mssql.GetAsCsv("SetUpdateWithPrimaryKey");
            Assert.IsTrue(csvFromReader1.Records[(1, 2)].Equals("Some text"));

            app.Mssql.MergeCsv(CsvTwo(), "SetUpdateWithPrimaryKey", "Id", false, false);
            var csvFromReader2 = app.Mssql.GetAsCsv("SetUpdateWithPrimaryKey");
            Assert.IsTrue(csvFromReader2.Records[(1, 2)].Equals("Some more text"));
            Assert.IsTrue(csvFromReader2.Records[(1, 5)].Equals("Knud"));
        }

        public void Dispose()
        {
            app.Mssql.Dispose();
        }

        private Csv CsvOne()
        {
            var csv = new Csv("Id, Data, Date, Age");
            csv.AddRecord(1, 1, 1);
            csv.AddRecord(1, 2, "Some text");
            csv.AddRecord(1, 3, DateTime.Now);
            csv.AddRecord(1, 4, 22);
            return csv;
        }

        private Csv CsvTwo()
        {
            var csv = new Csv("Id, Data, Date, Age, Name");
            csv.AddRecord(1, 1, 1);
            csv.AddRecord(1, 2, "Some more text");
            csv.AddRecord(1, 3, DateTime.Now);
            csv.AddRecord(1, 4, 22);
            csv.AddRecord(1, 5, "Knud");
            return csv;
        }
    }
}