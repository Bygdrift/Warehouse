using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using Bygdrift.Warehouse.DataLake.CsvTools;

namespace Bygdrift.Warehouse.Modules
{
    public class ImportResult
    {
        public bool AppSettingsOk { get; set; }
        public JObject CMDModel { get; set; }
        public CsvSet ImportLog { get; set; }
        public List<RefineBase> Refines { get; set; }

        public ImportResult(bool appSettingsOk)
        {
            AppSettingsOk = appSettingsOk;
            Refines = new List<RefineBase>();
        }
    }
}
