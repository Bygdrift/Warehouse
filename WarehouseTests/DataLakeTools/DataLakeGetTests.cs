using Bygdrift.Tools.CsvTool;
using Bygdrift.DataLakeTools;
using Bygdrift.Warehouse;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Tests.DataLakeTools
{
    [TestClass]
    public class DataLakeGetTests
    {
        private readonly AppBase app = new();

        [TestMethod]
        public void GetFile()
        {
            CleanAndAddFilesAsync().Wait();
            Assert.IsTrue(app.DataLake.GetFile("Refined/Test.csv").Stream.Length > 1);
            Assert.IsTrue(app.DataLake.GetFile("Refined/Subfolder/Subfolder/Test.csv").Stream.Length > 1);
            Assert.IsTrue(app.DataLake.GetFile("RefinedError/Test.csv").Stream == null);
            Assert.IsTrue(app.DataLake.GetFile("Refined/TestError.csv").Stream == null);
            Assert.IsTrue(app.DataLake.GetFile("Refined/test.csv").Stream == null);

            var firstFile = app.DataLake.GetFirstOrDefaultFile("Refined", FolderStructure.Path, false);
            Assert.AreEqual("Test.csv", firstFile.Value.FileName);
        }

        private async Task CleanAndAddFilesAsync()
        {
            await app.DataLake.DeleteDirectoryAsync("Refined");

            var csv = new Csv("Id");
            csv.AddRecord(0, 0, 1);
            await app.DataLake.SaveCsvAsync(csv, "Refined", "Test.csv", FolderStructure.Path);
            await app.DataLake.SaveCsvAsync(csv, "Refined/Subfolder/Subfolder", "Test.csv", FolderStructure.Path);
            await app.DataLake.SaveCsvAsync(csv, "Refined", "Test2.csv", FolderStructure.Path);
        }
    }
}
