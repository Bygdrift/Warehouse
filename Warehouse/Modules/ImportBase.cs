using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using Bygdrift.CsvTools;
using Bygdrift.Warehouse.DataLakes;

namespace Bygdrift.Warehouse.Modules
{
    public class ImportBase
    {
        public string ConnectionString { get; }
        public string Container { get; }
        public static string ModuleName { get; set; }
        public ILogger Log { get; }

        private DataLake _dataLake;

        public DataLake DataLake
        {
            get
            {

                _dataLake ??= new DataLake(ConnectionString, Container, ModuleName);
                return _dataLake;
            }
        }

        public ImportBase(ILogger log, string connectionString, string container, string moduleName)
        {
            Log = log;
            ConnectionString = connectionString;
            Container = container;
            ModuleName = moduleName;
        }

        public ImportResult ImportToDataLake(IEnumerable<RefineBase> refines, bool uploadToDataLake = true)
        {
            var result = new ImportResult();
            if (refines != null)
            {
                if (uploadToDataLake)
                    UploadCsvToDataLake(refines);

                result.Refines.AddRange(refines);
                result.ImportLog = ImportLog.CreateLog(ConnectionString, Container, ModuleName, "importLog", refines, uploadToDataLake);
                result.CommonDataModel = new CommonDataModel(ConnectionString, Container, ModuleName, refines, result.ImportLog, Log, uploadToDataLake).Model;

                if (refines.Where(o => o.Errors != null).Any())
                    Log.LogError($"There should have been uploaded {refines.Count(o => o.CsvAddToCommonDataModel)} csv files, but there where {refines.Select(o => o.Errors).Count() } errors, that are described in ImportLog.csv.");
                else
                    Log.LogInformation($"Created model.json, containing {refines.Count(o => o.CsvAddToCommonDataModel)} csv files (ImportLog.csv included).");

            }
            return result;
        }

        private void UploadCsvToDataLake(IEnumerable<RefineBase> refines)
        {
            foreach (var refine in refines)
            {
                if (refine.FileStream != null)
                    if (refine.FileStreamFolderStructure == FolderStructure.Path)
                        DataLake.SaveStream(refine.FileStreamBasePath, refine.TableName + "." + refine.FileStreamExtension, refine.FileStream);
                    else
                        DataLake.SaveStreamToDateTimeFolder(refine.FileStreamBasePath, refine.TableName, refine.FileStream, refine.FileStreamExtension, refine.FilestreamDateTime, refine.FileStreamFolderStructure == FolderStructure.DateTimePath);

                if (refine.Csv.Headers.Any())
                    if (refine.CsvFolderStructure == FolderStructure.Path)
                        DataLake.SaveCsv(refine.CsvBasePath, refine.TableName + ".csv", refine.Csv);
                    else
                        DataLake.SaveCsvToDateTimeFolder(refine.CsvBasePath, refine.TableName, refine.Csv, refine.CsvFileDateTime, refine.CsvFolderStructure == FolderStructure.DateTimePath);
            }
        }

        public Csv GetCsvFromDataLake(string Subdirectory, string filename)
        {
            var dataLake = new DataLake(ConnectionString, Container, ModuleName);
            return dataLake.GetCsv(Subdirectory, filename);
        }
    }
}