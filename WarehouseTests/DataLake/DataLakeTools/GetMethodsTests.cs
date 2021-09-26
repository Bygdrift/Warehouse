using Bygdrift.Warehouse.DataLakes;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Linq;

namespace WarehouseTests
{
    [TestClass]
    public class GetMethodsTests
    {
        /// <summary>Get data from appSettings like Config["test"]</summary>
        public static IConfigurationRoot Config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.local.json").Build();

        [TestMethod]
        public void TestMethod1()
        {
            var dataLake = new DataLake(Config["DataLakeConnectionString"], Config["DataLakeContainer"], "FM.DaluxFM");
            var a = dataLake.GetFilesFromDataLake("Lots", "Refined", new DateTime(2021, 7, 28), new DateTime(2021, 7, 29)).ToList();
        }
    }
}
