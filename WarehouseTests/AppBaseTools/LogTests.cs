using Bygdrift.Warehouse.Helpers.Logs;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace WarehouseTests.Modules
{
    [TestClass]
    public class LogTests
    {

        [TestMethod]
        public void AddLogMessage()
        {
            var log = new Log();
            log.LogError("-{A}- {B}-", "a", 1);
            log.LogError("-{A}- {B}-", "a");
            log.LogError("-{A}- {B}-");
            log.LogError("test");
            Assert.IsTrue(log.GetLogs(false).ElementAt(0) == "-a- 1-");
            Assert.IsTrue(log.GetLogs(false).ElementAt(1) == "-a- {B}-");
            Assert.IsTrue(log.GetLogs(false).ElementAt(2) == "-{A}- {B}-");
            Assert.IsTrue(log.GetLogs(true).ElementAt(3) == "test");
        }
    }
}