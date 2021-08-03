using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Linq;
using Bygdrift.Warehouse.DataLake;
using Bygdrift.Warehouse.DataLake.CsvTools;
using System;
using NCrontab;
using Bygdrift.Warehouse.DataLake.DataLakeTools;

namespace Bygdrift.Warehouse.Modules
{
    public class ImportBase
    {
        public string ConnectionString { get; }
        public string Container { get; }
        public string ModuleName { get; }
        public string ScheduleExpression { get; }
        public ILogger Log { get; }

        /// <summary>If per hour, then folder structure in datalake will be like decode/2021/07/28/11/</summary>
        public bool SavePerHour { get; }

        public ImportBase(ILogger log, string connectionString, string container, string moduleName, string scheduleExpression)
        {
            Log = log;
            ConnectionString = connectionString;
            Container = container;
            ModuleName = moduleName;
            ScheduleExpression = scheduleExpression;
            SavePerHour = GetHourSpanBetweenRuns(ScheduleExpression) < 24;
        }

        public ImportResult ImportToDataLake(IEnumerable<RefineBase> refines, bool uploadToDataLake = true)
        {
            var result = new ImportResult();
            if (refines != null)
            {
                if (uploadToDataLake)
                    UploadRefinesToDataLake(refines);

                result.Refines.AddRange(refines);
                result.ImportLog = CreateImportLog(result.Refines, uploadToDataLake);
                result.CMDModel = CreateCommonDataModel(RefinesWithImportLog(result), uploadToDataLake);
            }
            return result;
        }

        private void UploadRefinesToDataLake(IEnumerable<RefineBase> refines)
        {
            foreach (var refine in refines)
            {
                var dataLake = new DataLake.DataLakeTools.DataLake(ConnectionString, Container, ModuleName);

                if (refine.UploadAsRawFile)
                    dataLake.SaveAsRaw(refine.TableName, refine.UploadAsRawFileStream, refine.UploadAsRawFileExtension, refine.UploadFileDate, SavePerHour);
                if (refine.UploadAsDecodedFile)
                    dataLake.SaveAsDecoded(refine.TableName, refine.CsvSet, refine.UploadFileDate, SavePerHour);
            }
        }

        private static double GetHourSpanBetweenRuns(string scheduleExpression)
        {
            var schedule = CrontabSchedule.Parse(scheduleExpression, new CrontabSchedule.ParseOptions { IncludingSeconds = true });
            var nextRunFromLastRun = schedule.GetNextOccurrence(DateTime.MinValue);
            return (schedule.GetNextOccurrence(nextRunFromLastRun.AddSeconds(1)) - nextRunFromLastRun).TotalHours;
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


        ///// <summary>If called module mandatory appSettings are present</summary>
        //internal bool VerifyAppSettings()
        //{
        //    var res = true;
        //    foreach (var name in MandatoryAppSettings)
        //        if (Config[name] == null)
        //        {
        //            Log.LogError($"The appSetting: {name} are missing.");
        //            res = false;
        //        }

        //    return res;
        //}

        internal JObject CreateCommonDataModel(RefineBase[] refines, bool uploadToDataLake)
        {
            var dataLake = new DataLake.DataLakeTools.DataLake(ConnectionString, Container, ModuleName);

            if (!uploadToDataLake || refines.Any(o => o.UploadAsDecodedFile))
            {
                var model = new CommonDataModel(ConnectionString, Container, ModuleName, refines, uploadToDataLake);
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
            return ImportLog.CreateLog(ConnectionString, Container, ModuleName, "importLog", refines, uploadToDataLake);
        }
    }
}