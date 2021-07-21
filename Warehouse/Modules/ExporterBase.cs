using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using Bygdrift.Warehouse.DataLake;
using Bygdrift.Warehouse.DataLake.CsvTools;

namespace Bygdrift.Warehouse.Modules
{
    public class ExporterBase : IExporter
    {
        public string ModuleName { get; }
        public string ScheduleExpression { get; }
        public readonly IConfigurationRoot Config;
        public readonly ILogger Log;
        public readonly List<string> MandatoryAppSettings = new List<string> { "RunModules", "DataLakeAccountName", "DataLakeAccountKey", "DataLakeServiceUrl", "DataLakeBasePath" };

        public ExporterBase(IConfigurationRoot config, ILogger log, string moduleName, string scheduleExpression, string[] mandatoryAppSettings)
        {
            Config = config;
            Log = log;
            ModuleName = moduleName;
            ScheduleExpression = scheduleExpression;
            if (mandatoryAppSettings != null)
                MandatoryAppSettings.AddRange(mandatoryAppSettings);
        }

        public static ExportResult Run(IExporter exporter, bool uploadToDataLake)
        {
            var result = new ExportResult();
            result.DoRunSchedule = exporter.DoRunSchedule(DateTime.UtcNow);
            result.AppSettingsOk = exporter.VerifyAppSettings();

            if (result.DoRunSchedule && result.AppSettingsOk)
            {
                var refines = exporter.Export(uploadToDataLake);
                if (refines != null)
                {
                    result.Refines.AddRange(refines);
                    result.CMDModel = exporter.CreateCommonDataModel(result.Refines, uploadToDataLake);
                    result.ImportLog = exporter.CreateImportLog(result.Refines, uploadToDataLake);
                }
            }
            return result;
        }

        public virtual IEnumerable<IRefine> Export(bool ingestToDataLake)
        {
            return default;
        }

        /// <summary>If called module should be run, due to the current module scheduleExpression</summary>
        bool IExporter.DoRunSchedule(DateTime now)
        {
            return new NextRun("Call_modules_each_hour").DoRun(now, ScheduleExpression);
        }

        /// <summary>If called module mandatory appSettings are present</summary>
        bool IExporter.VerifyAppSettings()
        {
            var res = true;
            foreach (var name in MandatoryAppSettings)
                if (Config[name] == null)
                {
                    Log.LogError($"The appSetting: {name} are missing.");
                    res = false;
                }

            return res;
        }

        JObject IExporter.CreateCommonDataModel(List<IRefine> refines, bool uploadToDataLake)
        {
            var dataLake = new DataLake.DataLake(Config, ModuleName, "current");

            if (!uploadToDataLake || refines.Any(o => o.IsUploaded))
            {
                var model = new CommonDataModel(Config, ModuleName, refines, uploadToDataLake);
                Log.LogInformation($"Created ImportLog and model.json. There are {refines.Count(o => o.IsUploaded)} csv files + importLog.csv and model.csv.");
                return model.Model;
            }
            else
                Log.LogError($"There should have been uploaded {refines.Count(o => o.IsUploaded)} files, but there where {refines.Select(o => o.Errors).Count() } errors, that are described in importLog.csv.");

            //Jeg skal slette alle filer som ikke har været uploadet til current
            //Ingest.DeleteCurrentDirectoryInDatalake(Config, FunctionApp.TimerTrigger.database);  //Notice

            return default;
        }

        CsvSet IExporter.CreateImportLog(List<IRefine> refines, bool uploadToDataLake)
        {
            return ImportLog.CreateLog(Config, ModuleName, "importLog", refines, uploadToDataLake);
        }
    }
}