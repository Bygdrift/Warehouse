using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using Bygdrift.Warehouse.DataLake.CsvTools;

namespace Bygdrift.Warehouse.Modules
{
    public class ImportResult
    {
        public JObject CommonDataModel { get; set; }
        public RefineBase ImportLog { get; set; }
        public List<RefineBase> Refines { get; set; }

        public ImportResult()
        {
            Refines = new List<RefineBase>();
        }
    }
}
