using Bygdrift.DataLakeTools;
using Bygdrift.Warehouse;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests.DataLakeTools
{
    [TestClass]
    public class DataLakeQueueTests
    {
        public static readonly string BasePath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..\\..\\..\\"));
        private readonly DataLake dataLake;
        private static AppBase app = new();

        public DataLakeQueueTests()
        {
            dataLake = new DataLake(app);
        }


        [TestMethod]
        public async Task SaveManyParallelStreams()  //There will come errors on concurrent readings. Make sure to get the errors fetched into the log.
        {
            var path = Path.Combine(BasePath, "Files", "Csv", "Height and weight for 25000 persons.csv");
            var tasks = new List<Task<string>>();

            for (int i = 0; i < 5; i++)
            {
                var stream = new MemoryStream(File.ReadAllBytes(path));
                tasks.Add(dataLake.SaveStreamAsync(stream, "Raw", "test.txt", FolderStructure.Path));
            }

            var res = await Task.WhenAll(tasks);

            var logErrors = dataLake.App.Log.GetErrorsAndCriticals().ToList();
            Assert.IsTrue(logErrors.Count > 1);
        }

        [TestMethod]
        public async Task AppendStream()
        {
            var stream = new MemoryStream(Encoding.UTF8.GetBytes("Hejsa"));
            await dataLake.SaveStreamAsync(stream, "Raw", "test.txt", FolderStructure.Path);
            await dataLake.SaveStreamAsync(stream, "Raw", "test.txt", FolderStructure.Path, true);
            var streamOut = app.DataLake.GetFile("Raw/test.txt").Stream;
            StreamReader reader = new StreamReader(streamOut);
            var text = reader.ReadToEnd();
            Assert.AreEqual(text, "HejsaHejsa");
        }

        [TestMethod]
        public async Task SaveStream()
        {
            var stream = new MemoryStream(Encoding.UTF8.GetBytes("Hejsa"));

            var now = app.LoadedLocal;
            var res = await dataLake.SaveStreamAsync(stream, "Raw", "test.txt", FolderStructure.Path);
            var shouldGive = "Raw/test.txt";
            Assert.AreEqual(res, shouldGive);

            var res2 = await dataLake.SaveStreamAsync(stream, "Raw", "test.txt", FolderStructure.DatePath);
            var shouldGive2 = $"Raw/{now:yyyy}/{now:MM}/{now:dd}/test.txt";
            Assert.AreEqual(res2, shouldGive2);

            var res3 = await dataLake.SaveStreamAsync(stream, "Raw", "test.txt", FolderStructure.DateTimePath);
            var shouldGive3 = $"Raw/{now:yyyy}/{now:MM}/{now:dd}/{now:HH}/test.txt";
            Assert.AreEqual(res3, shouldGive3);
        }

        [TestMethod]
        public async Task SaveString()
        {
            var now = app.LoadedLocal;
            var res = await dataLake.SaveStringAsync("some test data", "Raw", "test.txt", FolderStructure.Path);
            Assert.AreEqual(res, "Raw/test.txt");

            res = await dataLake.SaveStringAsync("some test data", "Raw", "test.txt", FolderStructure.DatePath);
            Assert.AreEqual(res, $"Raw/{now:yyyy}/{now:MM}/{now:dd}/test.txt");

            res = await dataLake.SaveStringAsync("some test data", "Raw", "test.txt", FolderStructure.DateTimePath);
            Assert.AreEqual(res, $"Raw/{now:yyyy}/{now:MM}/{now:dd}/{now:HH}/test.txt");
        }

        [TestMethod]
        public async Task CreateEmptyDirectoryWillFail()
        {
            try
            {
                await app.DataLake.CreateDirectoryAsync("", FolderStructure.Path);
                Assert.Fail("No exception thrown");
            }
            catch (ArgumentException)
            {
            }
        }

    }
}
