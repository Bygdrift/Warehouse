using Bygdrift.Warehouse.DataLake.CsvTools;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace Warehouse.Common.Storage.DataLake.Tests.CsvTools
{
    [TestClass]
    public class GetMethodTests
    {

        [TestMethod]
        public void GetCompositeKeys()
        {
            var csv = CreateCsv();
            var res = csv.GetCompositeKeys("Type", "Mail");
            Assert.IsTrue(res.Count == 9);
            Assert.IsTrue(res.First().Key == "Room,andegaarden-1.sal@hillerod.dk");
        }

        [TestMethod]
        public void GetRecordColByLookup()
        {
            var csv = CreateCsv();
            var res = csv.GetRecordCol("Type", "Mail", "Room");
            Assert.IsTrue(res.Count == 3);
        }

        private static CsvSet CreateCsv()
        {
            var csv = new CsvSet("Mail,Capacity,Location,Level,Building,Name,Type");
            csv.AddRecords(new[] {
                "andegaarden-1.sal@hillerod.dk,6,Trollesmindealle 27,1. sal,Rådhus,Andergården,Room",
                "byraadssalen-moedecenter@hillerod.dk,30,Trollesmindealle 27,Stuen,Rådhus,Byrådssalen,Room",
                "BilABA97100@hillerod.dk,,Ukendt,,,Ældre og sundhed - Bil BA 97100,Vehicle",
                "BocenterMoedelokale3@hillerod.dk,3,Ukendt,,,Bocenter - Mødelokale 3,Room",
                "Bocentrets_faelleskalender@hillerod.dk,,Ukendt,,,Bocenter-fælleskalender,Calendar",
                "BSS-Bil-CH16136@hillerod.dk,,Ukendt,,,BSS Bil CH16136,Vehicle",
                "ByogMiljo-Bil3@hillerod.dk,,Ukendt,,,By og Miljø - Bil 3,Vehicle",
                "byogmiljobil5@hillerod.dk,,Ukendt,,,By og Miljø - Bil 5,Vehicle",
                "byogmiljobil6@hillerod.dk,,Ukendt,,,By og Miljø - Bil 6,Vehicle",
                "ByogMiljo-Stor_Projektor@hillerod.dk,,Ukendt,,,By og Miljø – Stor Projektor,Asset",
            });
            return csv;
        }
    }
}
