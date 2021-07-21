using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using Bygdrift.Warehouse.DataLake;
using Bygdrift.Warehouse.DataLake.CsvTools;

namespace Bygdrift.Warehouse.Modules
{
    public class RefineBase : IRefine
    {
        public List<string> Errors { get; private set; }
        public DateTime? FileDate { get; set; }
        public CsvSet CsvSet { get; set; }
        public IExporter Exporter { get; }
        public string TableName { get; }
        public bool IsUploaded { get; set; }

        public bool HasErrors { get { return Errors != null && Errors.Count > 0; } }

        public RefineBase(IExporter exporter, string tableName)
        {
            Exporter = exporter;
            TableName = tableName;
            CsvSet = new CsvSet();
        }

        public virtual void Refine()
        {
        }

        public void AddError(string error)
        {
            Errors ??= new List<string>();
            Errors.Add(error);
        }

        public void UploadFile(IConfigurationRoot config, DateTime fileDate, string rawFileExtension, Stream rawStream, bool uploadAsRaw, bool uploadAsDecoded, bool uploadAsCurrent, bool uploadAsAccumulated)
        {
            if (HasErrors)
                return;

            var savePerHour = NextRun.GetHourSpanBetweenRuns(Exporter.ScheduleExpression) < 24;

            var ingest = new Ingest(config, Exporter.ModuleName, TableName);
            if (uploadAsRaw)
                ingest.SaveAsRaw(rawStream, rawFileExtension, fileDate, savePerHour);
            if (uploadAsDecoded)
                ingest.SaveASDecoded(CsvSet, fileDate, savePerHour);
            if (uploadAsCurrent)
                ingest.SaveAsCurrent(CsvSet, fileDate);
            if (uploadAsAccumulated)
                ingest.SaveAsAccumulated(CsvSet);

            FileDate = fileDate;
            IsUploaded = true;
        }

        public void UploadFile(IConfigurationRoot config, DateTime fileDate, bool uploadAsDecoded, bool uploadAsCurrent, bool uploadAsAccumulated)
        {
            UploadFile(config, fileDate, null, null, false, uploadAsDecoded, uploadAsCurrent, uploadAsAccumulated);
        }

        public IEnumerable<CsvSet> GetDecodedFilesFromDataLake(string tableName, DateTime from, DateTime to)
        {
            return default;
        }
    }

    public interface IRefine
    {
        CsvSet CsvSet { get; set; }
        List<string> Errors { get; }
        DateTime? FileDate { get; set; }
        bool HasErrors { get; }
        bool IsUploaded { get; set; }
        //string ModuleName { get; }
        IExporter Exporter { get; }

        string TableName { get; }

        void AddError(string error);
        IEnumerable<CsvSet> GetDecodedFilesFromDataLake(string tableName, DateTime from, DateTime to);
        void Refine();
        void UploadFile(IConfigurationRoot config, DateTime fileDate, bool uploadAsDecoded, bool uploadAsCurrent, bool uploadAsAccumulated);
        void UploadFile(IConfigurationRoot config, DateTime fileDate, string rawFileExtension, Stream rawStream, bool uploadAsRaw, bool uploadAsDecoded, bool uploadAsCurrent, bool uploadAsAccumulated);
    }
}
