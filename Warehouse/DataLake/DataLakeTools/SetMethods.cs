using Bygdrift.Warehouse.DataLake.CsvTools;
using System;
using System.IO;
using System.Text;

namespace Bygdrift.Warehouse.DataLake.DataLakeTools
{
    public static class SetMethods
    {
        //public static void SaveCsv(this DataLake dataLake, string table, string folderName, CsvSet csv)
        //{
        //    var filename = table + ".csv";
        //    var subDirectory = folderName;
        //    dataLake.SaveCsv(subDirectory, filename, csv);
        //}½

        /// <param name="fileName">Såsom "Lots.csv"</param>
        public static void SaveCsv(this DataLake dataLake, string basePath, string fileName, CsvSet csv)
        {
            if (csv.Records.Count > 0)
            {
                var stream = csv.Write();
                stream.Position = 0;
                var fileClient = dataLake.GetFileClient(basePath, fileName);
                fileClient.Upload(stream, true);
            }
        }

        public static void SaveCsvToDateTimeFolder(this DataLake dataLake, string basePath, string table, CsvSet csv, DateTime fileDate, bool savePerHour, bool onlyOverwriteIfFileIsNewer = true)
        {
            var filename = table + ".csv";
            var newestFileInFolder = dataLake.GetNewestFileInFolder(basePath, filename);
            if (newestFileInFolder == null || !onlyOverwriteIfFileIsNewer || onlyOverwriteIfFileIsNewer && newestFileInFolder?.LastModified.UtcDateTime < fileDate)
                dataLake.SaveCsv(basePath, filename, csv);
        }

        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="fileName">Såsom "Lots.csv"</param>
        public static void SaveStream(this DataLake dataLake, string basePath, string fileName, Stream stream)  //Inspiration: https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-dotnet
        {
            if (stream == null || stream.Length == 0)
                return;

            var fileClient = dataLake.GetFileClient(basePath, fileName);
            stream.Position = 0;
            fileClient.Upload(stream, true);
        }

        public static void SaveStreamToDateTimeFolder(this DataLake dataLake, string basePath, string table, Stream fileStream, string rawFileExtension, DateTime fileDate, bool savePerHour, bool onlyOverwriteIfFileIsNewer = true)
        {
            var filename = table + '.' + rawFileExtension;
            var newestFileInFolder = dataLake.GetNewestFileInFolder(basePath, filename);
            if (newestFileInFolder == null || !onlyOverwriteIfFileIsNewer || onlyOverwriteIfFileIsNewer && newestFileInFolder?.LastModified.UtcDateTime < fileDate)
                dataLake.SaveStream(basePath, filename, fileStream);
        }


        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="fileName">Såsom "Lots.csv"</param>
        public static void SaveString(this DataLake dataLake, string basePath, string fileName, string data)  //Inspiration: https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-dotnet
        {
            using var stream = new MemoryStream(Encoding.Default.GetBytes(data));
            dataLake.SaveStream(basePath, fileName, stream);
        }

        public static void DeleteSubDirectory(this DataLake dataLake, string basePath)
        {
            var fileSystem = dataLake.DataLakeServiceClient.GetFileSystemClient(dataLake.Container);
            if (fileSystem.Exists())
            {
                var directory = fileSystem.GetDirectoryClient(string.Join('/', dataLake.Module, basePath));
                if (directory.Exists())
                    directory.Delete();
            }
        }
    }
}
