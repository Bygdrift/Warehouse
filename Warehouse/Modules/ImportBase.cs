using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Linq;
using Bygdrift.Warehouse.DataLake;
using Bygdrift.Warehouse.DataLake.CsvTools;
using Warehouse.Modules;

namespace Bygdrift.Warehouse.Modules
{
    public class ImportBase
    {
        public string ModuleName { get; }
        public string ScheduleExpression { get; }
        public IConfigurationRoot Config { get; }
        public ILogger Log { get; }
        public string[] MandatoryAppSettings { get; }

        /// <summary>If per hour, then folder structure in datalake will be like decode/2021/07/28/11/</summary>
        public bool SavePerHour { get; }

        public ImportBase(IConfigurationRoot config, ILogger log, string moduleName, string scheduleExpression, params string[] mandatoryAppSettings)
        {
            Config = config;
            Log = log;
            ModuleName = moduleName;
            ScheduleExpression = scheduleExpression;
            if (mandatoryAppSettings != null)
                MandatoryAppSettings = mandatoryAppSettings;

            SavePerHour = NextRun.GetHourSpanBetweenRuns(ScheduleExpression) < 24;
        }

        public ImportResult ImportToDataLake(IEnumerable<RefineBase> refines, bool uploadToDataLake = true)
        {
            var result = new ImportResult(VerifyAppSettings());

            if (result.AppSettingsOk)
            {
                if (refines != null)
                {
                    if(uploadToDataLake)
                        UploadRefinesToDataLake(refines);

                    result.Refines.AddRange(refines);
                    result.ImportLog = CreateImportLog(result.Refines, uploadToDataLake);
                    result.CMDModel = CreateCommonDataModel(RefinesWithImportLog(result), uploadToDataLake);
                }
            }
            return result;
        }

        private void UploadRefinesToDataLake(IEnumerable<RefineBase> refines)
        {
            foreach (var refine in refines)
            {
                var ingest = new Ingest(Config, ModuleName, refine.TableName);

                if (refine.UploadAsRawFile)
                    ingest.SaveAsRaw(refine.UploadAsRawFileStream, refine.UploadAsRawFileExtension, refine.UploadFileDate, SavePerHour);
                if (refine.UploadAsDecodedFile)
                    ingest.SaveASDecoded(refine.CsvSet, refine.UploadFileDate, SavePerHour);
            }
        }


        //private void UploadFile(IConfigurationRoot config, DateTime fileDate, string rawFileExtension, Stream rawStream, bool uploadAsRaw, bool uploadAsDecoded)
        //{
        //    if (HasErrors)
        //        return;

        //    var savePerHour = NextRun.GetHourSpanBetweenRuns(Importer.ScheduleExpression) < 24;

        //    var ingest = new Ingest(config, Importer.ModuleName, TableName);
        //    if (uploadAsRaw)
        //        ingest.SaveAsRaw(rawStream, rawFileExtension, fileDate, savePerHour);
        //    if (uploadAsDecoded)
        //        ingest.SaveASDecoded(CsvSet, fileDate, savePerHour);

        //    FileDate = fileDate;
        //    IsUploaded = true;
        //    IsUploadedAsDecodedFile = uploadAsDecoded;
        //}


        /// <summary>If called module mandatory appSettings are present</summary>
        internal bool VerifyAppSettings()
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

        internal JObject CreateCommonDataModel(RefineBase[] refines, bool uploadToDataLake)
        {
            var dataLake = new DataLake.DataLake(Config, ModuleName, null);

            if (!uploadToDataLake || refines.Any(o => o.UploadAsDecodedFile))
            {
                var model = new CommonDataModel(Config, ModuleName, refines, uploadToDataLake);
                Log.LogInformation($"Created model.json, containing {refines.Count(o => o.UploadAsDecodedFile)} csv files (ImportLog.csv included).");
                return model.Model;
            }
            else
            {
                Log.LogError($"There should have been uploaded {refines.Count(o => o.UploadAsDecodedFile)} files, but there where {refines.Select(o => o.Errors).Count() } errors, that are described in ImportLog.csv.");
                return default;
            }

            //Jeg skal slette alle filer som ikke har været uploadet til current
            //Ingest.DeleteCurrentDirectoryInDatalake(Config, FunctionApp.TimerTrigger.database);  //Notice
        }

        //Adds ImportLog to Refines:
        private static RefineBase[] RefinesWithImportLog(ImportResult result)
        {
            var res = new List<RefineBase>();
            res.AddRange(result.Refines);

            var logRefine = new RefineBase(null, "ImportLog");
            logRefine.CsvSet = result.ImportLog;
            res.Add(logRefine);
            return res.ToArray();
        }

        internal CsvSet CreateImportLog(List<RefineBase> refines, bool uploadToDataLake)
        {
            return ImportLog.CreateLog(Config, ModuleName, "importLog", refines, uploadToDataLake);
        }
    }
}