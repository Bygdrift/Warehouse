using Bygdrift.Tools.CsvTool;
using Bygdrift.Warehouse;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace WarehouseTests
{
    [TestClass]
    public class AppBaseTests
    {
        [TestMethod]
        public void basic()
        {
            var app = new AppBase();
            Assert.AreEqual(app.HostName, "https://<appFunctionName>.azurewebsites.net");
            Assert.AreEqual(app.ModuleName, "Warehouse");
            Assert.AreEqual(app.Mssql.ConnectionString, "Empty");
            Assert.IsTrue(app.IsRunningLocal);
        }

        [TestMethod]
        public void Time()
        {
            var app = new AppBase();
            Assert.IsNotNull(app.LoadedUtc);
            Assert.IsNotNull(app.LoadedLocal);
            Assert.AreEqual(app.LoadedLocal.ToUniversalTime(), app.LoadedUtc);
        }

        [TestMethod]
        public void Log()
        {
            var app = new AppBase();
            app.Log.LogError("Test");
            Assert.IsTrue(app.Log.Any());
            Assert.IsTrue(app.Log.HasErrorsOrCriticals());
            Assert.AreEqual(app.Log.GetErrorsAndCriticals().Count(), 1);
        }
    }
}