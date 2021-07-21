using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using Bygdrift.Warehouse.DataLake.CsvTools;

namespace Bygdrift.Warehouse.Modules
{
    public interface IExporter
    {
        string ModuleName { get; }
        public string ScheduleExpression { get; }
        IEnumerable<IRefine> Export(bool ingestToDataLake);
        internal bool DoRunSchedule(DateTime now);
        internal bool VerifyAppSettings();
        internal JObject CreateCommonDataModel(List<IRefine> refines, bool uploadToDataLake);
        internal CsvSet CreateImportLog(List<IRefine> refines, bool uploadToDataLake);
    }
}