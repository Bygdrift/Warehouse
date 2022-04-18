using Bygdrift.CsvTools;
using Bygdrift.Warehouse;
using Bygdrift.Warehouse.MssqlTools.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RepoDb;
using System;
using System.IO;
using System.Linq;

namespace Tests.MssqlTools
{
    [TestClass]
    public class MssqlSetTests
    {
        /// <summary>Path to project base</summary>
        public static readonly string BasePath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..\\..\\..\\"));
        private AppBase app = new();

        //Is not working as expected.... It cannot add 1 and then change to decimal 1.2 in same method 
        //[TestMethod]
        //public void ChangeTypes()
        //{
        //    var table = nameof(ChangeTypes);
        //    Assert.IsNull(app.Mssql.DeleteTable(table));

        //    var csv = new Csv("Int, Bit").AddRow(1, true);
        //    Assert.IsNull(app.Mssql.InserCsv(csv, table, false, true));
        //    app.Mssql.Dispose();
        //    app = new();

        //    DbFieldCache.Flush(); // Remove all the cached DbField
        //    FieldCache.Flush(); // Remove all the cached DbField
        //    IdentityCache.Flush(); // Remove all the cached DbField
        //    PrimaryCache.Flush(); // Remove all the cached DbField

        //    var csv2 = new Csv("Int, Bit").AddRow(2.23554568, true);
        //    Assert.IsNull(app.Mssql.InserCsv(csv2, table, false, true));
            
        //    var columnTypes = app.Mssql.GetColumnTypes(table);
        //    var csvFromReader = app.Mssql.GetAsCsv(table);


        //    //Assert.IsFalse(csvFromReader.TryGetColId("EmptyColumn", out int _));
        //    app.Mssql.Dispose();
        //}

        /// <summary>
        /// this would fail without flush fail and I asked for a solution here: https://stackoverflow.com/questions/69635893/repodb-doesnt-merge-correct-after-altering-a-column
        /// If RepoDB didn't get flushed, the reulst would be: Id = 1 Data = "Lo", Name = NULL
        /// </summary>
        [TestMethod]
        public void CreateAndTruncateTable_Fails()
        {
            var csvOne = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");
            var table = "CreateAndTruncateTableFails";

            Assert.IsNull(app.Mssql.DeleteTable(table));
            Assert.IsNull(app.Mssql.MergeCsv(csvOne, table, "Id", false, false));
            Assert.IsNull(app.Mssql.MergeCsv(csvTwo, table, "Id", false, false));

            var csvFromReader = app.Mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader.GetRecord(1, 2).Equals("Some more text"));
            Assert.IsTrue(csvFromReader.GetRecord(1, 5).Equals("Knud"));

            Assert.IsFalse(app.Log.GetErrorsAndCriticals().Any());
            app.Mssql.Dispose();
        }


        /// <summary>
        /// If there are a column left that are all empty, it should be removed
        /// </summary>
        [TestMethod]
        public void IfColumnsGetsRemoved()
        {
            var table = "IfColumnsGetsRemoved";
            var csv = new Csv("Id, Data, Date, Age, EmptyColumn").AddRow(1, "Some text", DateTime.Now, 22);

            Assert.IsNull(app.Mssql.DeleteTable(table));
            Assert.IsNull(app.Mssql.MergeCsv(csv, table, "Id", false, true));
            var csvFromReader = app.Mssql.GetAsCsv(table);
            Assert.IsFalse(csvFromReader.TryGetColId("EmptyColumn", out int _));
            app.Mssql.Dispose();
        }

        [TestMethod]
        public void InsertCsvThenMerge()
        {
            ///TODO: Here I create a table without a primary key and then I create one. Handle it!
            var table = "SaveCsvAndOverwrite";
            var csvOne = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(app.Mssql.DeleteTable(table));
            Assert.IsNull(app.Mssql.InserCsv(csvOne, table, false, false));
            Assert.IsNull(app.Mssql.MergeCsv(csvTwo, table, "Id", false, false));
            app.Mssql.Dispose();
        }

        [TestMethod]
        public void LoadMuchData()
        {
            var table = "LoadMuchData";
            var path = Path.Combine(BasePath, "Files", "Csv", "Height and weight for 25000 persons.csv");
            var csv = new Csv().FromCsvFile(path);

            Assert.IsNull(app.Mssql.DeleteTable(table));
            Assert.IsNull(app.Mssql.MergeCsv(csv, table, "Index", false, false));
            Assert.IsFalse(app.Log.GetErrorsAndCriticals().Any());
            app.Mssql.Dispose();
        }

        [TestMethod]
        public void LoadMuchParallelData()
        {
            var table = "LoadMuchParallelData";
            Assert.IsNull(app.Mssql.DeleteTable(table));

            for (int i = 0; i < 3; i++)
            {
                var csv = new Csv("Id, Data, Date, Age").AddRow(i, "Some text", DateTime.Now, new Random().Next(1, 5000));
                Assert.IsNull(app.Mssql.MergeCsv(csv, table, "Id", false, false));
            }
            Assert.IsFalse(app.Log.GetErrorsAndCriticals().Any());
            app.Mssql.Dispose();
            //var errors = Task.WhenAll(tasks).Result.SelectMany(o => o);
        }

        /// <summary>
        /// This is not a test that fails.It only serves for testing time and thats the reason for commenting this method out.
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public void LoadMuchData2()
        {
            var table = "LoadMuchData2";
            var csv = new Csv("id, text1, text2, number");
            for (int r = 1; r < 100000; r++)
                csv.AddRow(r, "This is a text that should indcate some length", "This is a text that should indcate some length", 105643256);

            Assert.IsNull(app.Mssql.DeleteTable(table));
            Assert.IsNull(app.Mssql.MergeCsv(csv, table, "id", false, false));
            Assert.IsNull(app.Mssql.DeleteTable(table));
            Assert.IsNull(app.Mssql.InserCsv(csv, table, false, false));
            Assert.IsFalse(app.Log.GetErrorsAndCriticals().Any());
            app.Mssql.Dispose();
        }

        [TestMethod]
        public void NullableNumbers()
        {
            var table = "SaveCsvAndOverwrite";
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22).AddRecord(2, 1, 2).AddRecord(2, 4, null);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(app.Mssql.DeleteTable(table));
            Assert.IsNull(app.Mssql.MergeCsv(csv, table, "Id", false, false));
            Assert.IsNull(app.Mssql.MergeCsv(csvTwo, table, "Id", false, false));
            Assert.IsFalse(app.Log.GetErrorsAndCriticals().Any());
            app.Mssql.Dispose();
        }

        [TestMethod]
        public void SaveCsvAndOverwrite()
        {
            var table = "SaveCsvAndOverwrite";
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(app.Mssql.DeleteTable(table));
            Assert.IsNull(app.Mssql.MergeCsv(csv, table, "Id", false, false));
            Assert.IsNull(app.Mssql.TruncateTable(table));
            Assert.IsNull(app.Mssql.MergeCsv(csvTwo, table, "Id", false, false));
            Assert.IsFalse(app.Log.GetErrorsAndCriticals().Any());
            app.Mssql.Dispose();
        }

        [TestMethod]
        public void SaveEmptyCsv()
        {
            var table = "SaveEmptyCsv";
            var csv = new Csv();

            app.Mssql.DeleteTable(table);
            var errors = app.Mssql.MergeCsv(csv, table, "Id", false, false);
            Assert.AreEqual(errors.Length, 1);

            Assert.IsNull(app.Mssql.InserCsv(csv, table, false, false));
            Assert.IsTrue(app.Log.GetErrorsAndCriticals().Any());
            app.Mssql.Dispose();
        }

        [TestMethod]
        public void SetUpdateWithoutPrimaryKey()
        {
            var table = "SetUpdateWithoutPrimaryKey";
            var csv = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            app.Mssql.DeleteTable(table);

            //Raise an error because primaryKey must not be null in this method
            var errors = app.Mssql.MergeCsv(csv, table, null, false, false);
            Assert.IsTrue(errors.Length == 1);

            errors = app.Mssql.InserCsv(csv, table, false, false);
            Assert.IsTrue(errors == null);
            var csvFromReader1 = app.Mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader1.GetRecord(1, 2).Equals("Some text"));

            Assert.IsNull(app.Mssql.InserCsv(csvTwo, table, false, false));
            var csvFromReader2 = app.Mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader2.GetRecord(2, 2).Equals("Some more text"));
            app.Mssql.Dispose();
        }

        [TestMethod]
        public void SetUpdateWithPrimaryKey()
        {
            var table = "SetUpdateWithPrimaryKey";
            var csvOne = new Csv("Id, Data, Date, Age").AddRow(1, "Some text", DateTime.Now, 22);
            var csvTwo = new Csv("Id, Data, Date, Age, Name").AddRow(1, "Some more text", DateTime.Now, 22, "Knud");

            Assert.IsNull(app.Mssql.DeleteTable(table));
            Assert.IsNull(app.Mssql.MergeCsv(csvOne, table, "Id", false, false));

            var csvFromReader1 = app.Mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader1.Records[(1, 2)].Equals("Some text"));

            Assert.IsNull(app.Mssql.MergeCsv(csvTwo, table, "Id", false, false));

            var csvFromReader2 = app.Mssql.GetAsCsv(table);
            Assert.IsTrue(csvFromReader2.GetRecord(1,2).Equals("Some more text"));
            Assert.IsTrue(csvFromReader2.GetRecord(1,5).Equals("Knud"));
            app.Mssql.Dispose();
        }
    }
}