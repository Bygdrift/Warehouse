using Bygdrift.Warehouse.DataLake.CsvTools;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace Warehouse.Common.Storage.DataLake.Tests.CsvTools
{
    [TestClass]
    public class CsvSetTests
    {


        [TestMethod]
        public void VerifyUniqueHeaders()
        {
            var csv = new CsvSet();
            csv.AddHeader(1, "Container");
            csv.AddHeader(2, "Uploaded");
            csv.AddHeader(3, "Container");
            csv.AddHeader(4, "Container");

            Assert.IsTrue(csv.Headers.First().Value.Equals("Container"));
            Assert.IsTrue(csv.Headers.Count(o => o.Value.Equals("Container")) == 1);
            Assert.IsTrue(csv.Headers.Count(o => o.Value.Equals("Container_2")) == 1);
            Assert.IsTrue(csv.Headers.Count(o => o.Value.Equals("Container_3")) == 1);
        }

        [TestMethod]
        public void VerifyCsvParsing()
        {
            var csv = new CsvSet();
            csv.AddRecord(1, 1, "21");
            csv.AddRecord(2, 1, "21.1");
            csv.AddRecord(3, 1, "true");
            csv.AddRecord(4, 1, "true");
            csv.AddRecord(5, 1, "test");
            csv.AddHeader(1, "Container");  //Ending with adding rows to provoke an eventual error

            var a = csv.Write();

            Assert.IsTrue(csv.Headers.First().Value.Equals("Container"));
            Assert.IsTrue(csv.Records.First().Value.ToString() == "21");
            Assert.IsTrue(csv.ColTypes[1] == typeof(long));
            Assert.IsTrue(csv.ColTypes[2] == typeof(decimal));
            Assert.IsTrue(csv.ColTypes[3] == typeof(bool));
            Assert.IsTrue(csv.ColTypes[4] == typeof(bool));
            Assert.IsTrue(csv.ColTypes[5] == typeof(string));
        }

        [TestMethod]
        public void VerifyLimits()
        {
            var csv = new CsvSet();
            csv.AddRecord(1, 1, "");
            Assert.IsTrue(csv.RowLimit == (1, 1) && csv.ColLimit == (1, 1));
            csv.AddRecord(1, 2, "");
            Assert.IsTrue(csv.RowLimit == (1, 2) && csv.ColLimit == (1, 1));
            csv.AddRecord(2, 1, "");
            Assert.IsTrue(csv.RowLimit == (1, 2) && csv.ColLimit == (1, 2));
            csv.AddRecord(2, 3, "");
            Assert.IsTrue(csv.RowLimit == (1, 3) && csv.ColLimit == (1, 2));
            csv.AddRecord(0, 3, "");
            Assert.IsTrue(csv.RowLimit == (1, 3) && csv.ColLimit == (0, 2));
            csv.AddRecord(0, 0, "");
            Assert.IsTrue(csv.RowLimit == (0, 3) && csv.ColLimit == (0, 2));

            csv = new CsvSet();
            csv.AddHeader(1, "");
            Assert.IsTrue(csv.RowLimit == (0, 0) && csv.ColLimit == (1, 1));
        }

        [TestMethod]
        public void VerifyCsvParsingWithOnlyOneRow()
        {
            var csv = new CsvSet();
            csv.AddRecord(0, 0, "21");
            csv.AddHeader(0, "Container");  //Ending with adding rows to provoke an eventual error
            var stream = csv.Write();
            Assert.IsTrue(stream.Length == 18);
        }

        [TestMethod]
        public void TryRemoveRow()
        {
            var csv = new CsvSet();
            csv.AddHeader(0, "A");
            csv.AddHeader(1, "B");
            csv.AddRecord(0, 0, "a");
            csv.AddRecord(1, 0, "1");

            csv.RemoveColumn(1);
            Assert.IsTrue(csv.Headers.First().Value.Equals("A"));
            Assert.IsTrue(csv.ColTypes.First().Value.Equals(typeof(string)));
            Assert.IsTrue(csv.Records.First().Value.Equals("a"));

            csv = new CsvSet();
            csv.AddHeader(0, "A");
            csv.AddHeader(1, "B");
            csv.AddRecord(0, 0, "a");
            csv.AddRecord(1, 0, "1");
            csv.AddRecord(2, 0, "2");  //There are no header on perpous

            csv.RemoveColumn(0);
            Assert.IsTrue(csv.Headers.First().Value.Equals("B"));
            Assert.IsTrue(csv.ColTypes.First().Value.Equals(typeof(long)));
            Assert.IsTrue(csv.Records.First().Value.Equals("1"));
        }

    }
}
