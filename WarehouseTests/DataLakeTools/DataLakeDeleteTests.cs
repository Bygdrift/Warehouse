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
            await app.DataLake.CreateDirectoryAsync("Refined", FolderStructure.Path);

            Assert.IsTrue(app.DataLake.BasePathExists("Refined"));

            await app.DataLake.DeleteDirectoryAsync("Refined", FolderStructure.Path);
            Assert.IsFalse(app.DataLake.BasePathExists("Refined"));
        }

        [TestMethod]
        public async Task CreateAndDeleteRoot()
        {
            await app.DataLake.CreateDirectoryAsync("Refined", FolderStructure.Path);
            Assert.IsTrue(app.DataLake.BasePathExists("Refined"));

            await app.DataLake.DeleteDirectoryAsync(null, FolderStructure.Path);
            Assert.IsFalse(app.DataLake.BasePathExists("Refined"));
        }
    }
}
