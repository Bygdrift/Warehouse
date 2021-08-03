using Bygdrift.Warehouse.DataLake.CsvTools;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;

namespace Bygdrift.Warehouse.DataLake.DataLakeTools
{
    public static class SetMethods
    {

        //public string Module { get; }
        //public string Table { get; }
        //public IConfigurationRoot Config { get; }

        //public SetMethods(IConfigurationRoot config, string module, string table)
        //{
        //    Config = config;
        //    Module = module;
        //    Table = table;
        //}

        //public static void DeleteCurrentDirectoryInDatalake(IConfigurationRoot config, string module)
        //{
        //    var dataLake = new DataLake(config, module, SubDirectory.Current.ToString());
        //    dataLake.DeleteSubDirectory();
        //}

        public static DataLake SaveAsRaw(this DataLake dataLake, string table, Stream rawStream, string rawFileExtension, DateTime fileDate, bool savePerHour, bool onlyOverwriteIfFileIsNewer = true)
        {
            var filename = table + '.' + rawFileExtension;

            //var dataLake = new DataLakeInit(Config, Module, CreateDatePath(SubDirectory.Raw, fileDate, savePerHour));
            var subDirectory = DataLake.CreateDatePath(SubDirectory.Raw, fileDate, savePerHour);
            var newestFileInFolder = dataLake.GetNewestFileInFolder(subDirectory, filename);
            if (newestFileInFolder == null || !onlyOverwriteIfFileIsNewer || onlyOverwriteIfFileIsNewer && newestFileInFolder?.LastModified.UtcDateTime < fileDate)
                dataLake.SaveStreamToDataLake(subDirectory, filename, rawStream);

            return dataLake;
        }

        public static DataLake SaveAsDecoded(this DataLake dataLake, string table, CsvSet csv, DateTime fileDate, bool savePerHour, bool onlyOverwriteIfFileIsNewer = true)
        {
            var filename = table + ".csv";
            //var dataLake = new DataLakeInit(Config, Module, CreateDatePath(SubDirectory.Decode, fileDate, savePerHour));
            var subDirectory = DataLake.CreateDatePath(SubDirectory.Decode, fileDate, savePerHour);
            var newestFileInFolder = dataLake.GetNewestFileInFolder(subDirectory, filename);
            if (newestFileInFolder == null || !onlyOverwriteIfFileIsNewer || onlyOverwriteIfFileIsNewer && newestFileInFolder?.LastModified.UtcDateTime < fileDate)
                dataLake.SaveCsvToDataLake(subDirectory, filename, csv);

            return dataLake;
        }

        public static void DeleteSubDirectory(this DataLake dataLake, string subDirectory)
        {
            var fileSystem = dataLake.DataLakeServiceClient.GetFileSystemClient(dataLake.Container);
            if (fileSystem.Exists())
            {
                var directory = fileSystem.GetDirectoryClient(string.Join('/', dataLake.Module, subDirectory));
                if (directory.Exists())
                    directory.Delete();
            }
        }

        //public void SaveAsCurrent(CsvSet csv, DateTime fileDate, bool onlyOverwriteIfFileIsNewer = true)
        //{
        //    var basePath = SubDirectory.Current.ToString();
        //    var filename = Table + ".csv";
        //    var dataLake = new DataLake(Config, Module, basePath);
        //    var newestFileInFolder = dataLake.GetNewestFileInFolder(filename);
        //    if (newestFileInFolder == null || !onlyOverwriteIfFileIsNewer || onlyOverwriteIfFileIsNewer && newestFileInFolder?.LastModified.UtcDateTime < fileDate)
        //        dataLake.SaveCsvToDataLake(filename, csv);
        //}

        //public void SaveAsAccumulated(CsvSet csv)
        //{
        //    var basePath = SubDirectory.Accumulate.ToString();
        //    var filename = Table + ".csv";
        //    var dataLake = new DataLake(Config, Module, basePath);
        //    dataLake.SaveCsvToDataLake(filename, csv);
        //}

    }

    public enum SubDirectory
    {
        //Accumulate,
        //Current,
        Decode,
        Raw,
    }
}
