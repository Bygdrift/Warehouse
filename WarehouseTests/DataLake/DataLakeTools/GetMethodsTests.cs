using Bygdrift.Warehouse.DataLakes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using WarehouseTests.Helpers;

namespace WarehouseTests
{
    [TestClass]
    public class GetMethodsTests : GenericTests
    {
        [TestMethod]
        public void TestMethod1()
        {
            var dataLake = new DataLake(Config["DataLakeConnectionString"], Config["DataLakeContainer"], "FM.DaluxFM");
            var a = dataLake.GetFilesFromDataLake("Lots", "Refined", new DateTime(2021, 7, 28), new DateTime(2021, 7, 29)).ToList();
        }
    }
}
