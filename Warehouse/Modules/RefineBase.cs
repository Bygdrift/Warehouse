using System;
using System.Collections.Generic;
using System.IO;
using Bygdrift.Warehouse.DataLake.CsvTools;
using Microsoft.Extensions.Logging;

namespace Bygdrift.Warehouse.Modules
{
    public class RefineBase
    {
        public List<string> Errors { get; internal set; }
        public ImportBase Importer { get; }
        public string TableName { get; }
        public CsvSet CsvSet { get; set; }
        public DateTime CsvFileDateTime { get; }
        public bool CsvAddToCommonDataModel { get; }
        public FolderStructure CsvFolderStructure { get; }
        public string CsvBasePath { get; }
        public DateTime FilestreamDateTime { get; internal set; }
        public Stream FileStream { get; internal set; }
        public string FileStreamExtension { get; internal set; }
        public string FileStreamBasePath { get; internal set; }
        public FolderStructure FileStreamFolderStructure { get; internal set; }

        public bool HasErrors { get { return Errors != null && Errors.Count > 0; } }


        public RefineBase(ImportBase importer, string basePath, string tableName, bool addToCommonDataModel, FolderStructure folderStructure, DateTime? fileDateTime = null)
        {
            Importer = importer;
            TableName = tableName;
            CsvSet = new CsvSet();
            CsvAddToCommonDataModel = addToCommonDataModel;
            CsvFolderStructure = folderStructure;
            CsvFileDateTime = fileDateTime == null ? DateTime.UtcNow : ((DateTime)fileDateTime).ToUniversalTime();
            CsvBasePath = folderStructure != FolderStructure.Path ? string.Join('/', DataLake.DataLakeTools.DataLake.CreateDatePath(basePath, CsvFileDateTime, CsvFolderStructure == FolderStructure.DateTimePath)) : basePath;
        }

        public void AddError(string error)
        {
            Errors ??= new List<string>();
            Errors.Add(error);
            Importer.Log.LogError(error);
        }

        ///// <summary>
        ///// When calling: importer.ImportToDataLake, the csv will be uploaded to datalake
        ///// </summary>
        //public void ImportCsvToDataLake(CsvSet csvSet)
        //{
        //    CsvSet = csvSet;
        //    UploadToDateTimeFolder = true;
        //    BasePath = string.Join('/', DataLake.DataLakeTools.DataLake.CreateDatePath(SubDirectory.Refined, UploadFileDate, Importer.SavePerHour), TableName + ".csv");
        //}

        /// <summary>
        /// When calling: importer.ImportToDataLake, the csv will be uploaded to datalake. If there are a mathcing csv-file, data will be added to the existing csv.
        /// Consider how large these csv-files can be.
        /// </summary>
        //public void ImportCsvToDataLake(CsvSet csvSet, string basePath)
        //{
        //    CsvSet = csvSet;
        //    UploadToFolder = true;
        //    BasePath = basePath;
        //}

        /// <summary>
        /// Can upload files directly to Datalake, such as raw data. They will not be at part of Common Data Model, if they are uploaded like this
        /// </summary>
        public void ImportStreamToDataLake(Stream stream, string basePath, string fileExtension, FolderStructure folderStructure, DateTime? fileDateTime = null)
        {
            FileStream = stream;
            FilestreamDateTime = fileDateTime ?? DateTime.UtcNow;
            FileStreamBasePath = folderStructure != FolderStructure.Path ? string.Join('/', DataLake.DataLakeTools.DataLake.CreateDatePath(basePath, FilestreamDateTime, folderStructure == FolderStructure.DateTimePath)) : basePath;
            FileStreamExtension = fileExtension;
            FileStreamFolderStructure = folderStructure;
        }
    }

    public enum FolderStructure
    {
        /// <summary>Saved by a path</summary>
        Path,
        /// <summary>Each file saved in a folder with the given date</summary>
        DatePath,
        /// <summary>Each file saved in a folder with the given date and hour</summary>
        DateTimePath
    }
}
