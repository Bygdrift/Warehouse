using Bygdrift.DataLakeTools;
using Bygdrift.Warehouse;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace Tests.DataLakeTools
{
    [TestClass]
    public class DataLakeDeleteTests
    {
        private readonly AppBase app = new();

        [TestMethod]
        public async Task CreateAndDeleteBasePath()
        {
            await app.DataLake.CreateDirectoryAsync("", FolderStructure.Path);

            Assert.IsTrue(app.DataLake.BasePathExists(""));

            await app.DataLake.DeleteDirectoryAsync("", FolderStructure.Path);
            Assert.IsFalse(app.DataLake.BasePathExists(""));
        }
    }
}
