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

            var time = new DateTime(2022,1,1);
            var csv1 = new Csv(new Config(null, "u"), "date").AddRow(time).ToCsvString();
            var csv2 = new Csv(new Config("da-DK", "u"), "date").AddRow(time).ToCsvString();
            var csv3 = new Csv(new Config("en-US", "u"), "date").AddRow(time).ToCsvString();


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