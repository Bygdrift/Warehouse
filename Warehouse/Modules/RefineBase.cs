using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using Bygdrift.Warehouse.DataLake;
using Bygdrift.Warehouse.DataLake.CsvTools;
using Warehouse.Modules;

namespace Bygdrift.Warehouse.Modules
{
    public class RefineBase
    {
        public List<string> Errors { get; internal set; }
        public CsvSet CsvSet { get; set; }
        public ImportBase Importer { get; }
        public string TableName { get; }
        internal DateTime UploadFileDate { get; set; }
        internal bool UploadAsDecodedFile { get; set; }
        internal string UploadAsDecodedPath { get; set; }
        internal bool UploadAsRawFile { get; set; }
        internal string UploadAsRawFileExtension { get; set; }
        internal Stream UploadAsRawFileStream { get; set; }

        public bool HasErrors { get { return Errors != null && Errors.Count > 0; } }

        public RefineBase(ImportBase importer, string tableName)
        {
            Importer = importer;
            TableName = tableName;
            CsvSet = new CsvSet();
        }

        public void AddError(string error)
        {
            Errors ??= new List<string>();
            Errors.Add(error);
        }

        /// <summary>
        /// When calling: importer.ImportToDataLake, the csv will be uploaded to datalake
        /// </summary>
        public void ImportCsvFileToDataLake(DateTime fileDate)
        {
            UploadAsDecodedFile = true;
            UploadFileDate = fileDate.ToUniversalTime();
            UploadAsDecodedPath = string.Join('/', Ingest.CreateDatePath(SubDirectory.Decode, UploadFileDate, Importer.SavePerHour), TableName + ".csv");
        }

        /// <summary>
        /// When calling: importer.ImportToDataLake, the raw file will be uploaded to datalake
        /// </summary>
        public void ImportRawFileToDataLake(DateTime fileDate, string rawFileExtension, Stream rawStream)
        {
            UploadFileDate = fileDate.ToUniversalTime();
            UploadAsRawFile = true;
            UploadAsRawFileExtension = rawFileExtension;
            UploadAsRawFileStream = rawStream;
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

        //public void UploadDecodedFile(IConfigurationRoot config, DateTime fileDate)
        //{
        //    UploadFile(config, fileDate, null, null, false, true);
        //}

        //public void UploadRawFile(IConfigurationRoot config, DateTime fileDate, string rawFileExtension, Stream rawStream)
        //{
        //    UploadFile(config, fileDate, rawFileExtension, rawStream, true, false);
        //}

        //public void UploadRawAndDecodedFile(IConfigurationRoot config, DateTime fileDate, string rawFileExtension, Stream rawStream)
        //{
        //    UploadFile(config, fileDate, rawFileExtension, rawStream, true, true);

        //}

        public IEnumerable<CsvSet> GetDecodedFilesFromDataLake(string tableName, DateTime from, DateTime to)
        {
            return default;
        }
    }
}
