using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using Bygdrift.Warehouse.DataLake.CsvTools;

namespace Bygdrift.Warehouse.Modules
{
    public class ExportResult
    {
        //public bool RunModule { get; set; }
        public bool DoRunSchedule { get; set; }
        public bool AppSettingsOk { get; set; }
        public JObject CMDModel { get; set; }
        public CsvSet ImportLog { get; set; }
        public List<IRefine> Refines { get; set; }

        public ExportResult()
        {
            Refines = new List<IRefine>();
        }
    }
}
