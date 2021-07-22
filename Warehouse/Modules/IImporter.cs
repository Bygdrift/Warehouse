using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using Bygdrift.Warehouse.DataLake.CsvTools;

namespace Bygdrift.Warehouse.Modules
{
    public interface IImporter
    {
        string ModuleName { get; }
        string ScheduleExpression { get; }
        IEnumerable<IRefine> Import(bool ingestToDataLake);
        internal bool VerifyAppSettings();
        internal JObject CreateCommonDataModel(List<IRefine> refines, bool uploadToDataLake);
        internal CsvSet CreateImportLog(List<IRefine> refines, bool uploadToDataLake);
    }
}