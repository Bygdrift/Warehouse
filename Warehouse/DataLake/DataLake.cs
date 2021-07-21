using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Bygdrift.Warehouse.DataLake.CsvTools;

[assembly: InternalsVisibleTo("Warehouse.Common.Tests")]
namespace Bygdrift.Warehouse.DataLake
{
    class DataLake
    {
        public readonly Uri ServiceUri;
        public readonly string BasePath;
        public readonly string BaseDirectory;
        public readonly string SubDirectory;
        //private readonly IConfigurationRoot config;
        //private readonly string module;
        private readonly string storageAccountName;
        private readonly string storageAccountKey;
        private DataLakeServiceClient _dataLakeServiceClient;

        public DataLakeServiceClient DataLakeServiceClient
        {
            get
            {
                if (_dataLakeServiceClient == null)
                {
                    var sharedKeyCredential = new StorageSharedKeyCredential(storageAccountName, storageAccountKey);
                    _dataLakeServiceClient = new DataLakeServiceClient(ServiceUri, sharedKeyCredential);
                }
                return _dataLakeServiceClient;
            }
        }

        /// <param name="module">såsom "DaluxFM"</param>
        /// <param name="subDirectory">Såsom "current"</param>
        public DataLake(IConfigurationRoot config, string module, string subDirectory)
        {
            //this.config = config;
            //this.module = module;
            ServiceUri = new Uri(config["DataLakeServiceUrl"]);
            if (ServiceUri == null) throw new Exception("The value for DataLakeServiceUrl, has to be set in config.");
            storageAccountName = config["DataLakeAccountName"];
            if (storageAccountName == null) throw new Exception("The value for DataLakeAccountName, has to be set in config.");
            storageAccountKey = config["DataLakeAccountKey"];
            if (storageAccountKey == null) throw new Exception("The value for DataLakeAccountKey, has to be set in config.");
            BasePath = config["DataLakeBasePath"];
            if (BasePath == null) throw new Exception("The value for DataLakeBasePath, has to be set in config.");

            BaseDirectory = module.ToString().ToLower();
            SubDirectory = subDirectory.ToLower();
        }

        public IEnumerable<KeyValuePair<DateTime, CsvSet>> GetDecodedFilesFromDataLake(string tableName, DateTime from, DateTime to)
        {
            var fileSystem = DataLakeServiceClient.GetFileSystemClient(BasePath);
            if (!fileSystem.Exists())
                yield break;

            var directory = fileSystem.GetDirectoryClient(string.Join('/', BaseDirectory, Warehouse.DataLake.SubDirectory.decode));
            if (!directory.Exists())
                yield break;

            foreach (var yearFolder in fileSystem.GetPaths(directory.Path).Where(o => o.IsDirectory == true))
                if (int.TryParse(Path.GetFileName(yearFolder.Name), out int year))
                    foreach (var monthFolder in fileSystem.GetPaths(yearFolder.Name).Where(o => o.IsDirectory == true))
                        if (int.TryParse(Path.GetFileName(monthFolder.Name), out int month))
                            foreach (var dayFolder in fileSystem.GetPaths(monthFolder.Name).Where(o => o.IsDirectory == true))
                                if (int.TryParse(Path.GetFileName(dayFolder.Name), out int day))
                                {
                                    var res = GetStream(fileSystem, directory, tableName, from, to, year, month, day, 0);
                                    if (res != null)
                                        yield return (KeyValuePair<DateTime, CsvSet>)res;

                                    foreach (var hourFolder in fileSystem.GetPaths(dayFolder.Name).Where(o => o.IsDirectory == true))
                                        if (int.TryParse(Path.GetFileName(hourFolder.Name), out int hour))
                                        {
                                            res = GetStream(fileSystem, directory, tableName, from, to, year, month, day, hour);
                                            if (res != null)
                                                yield return (KeyValuePair<DateTime, CsvSet>)res;
                                        }
                                }
        }

        private KeyValuePair<DateTime, CsvSet>? GetStream(DataLakeFileSystemClient fileSystem, DataLakeDirectoryClient directory, string tableName, DateTime from, DateTime to, int year, int month, int day, int hour = 0)
        {
            var date = new DateTime(year, month, day, hour, 0, 0);
            var item = fileSystem.GetPaths(directory.Path).SingleOrDefault(o => o.IsDirectory == false && Path.GetExtension(o.Name).Equals(".csv", StringComparison.InvariantCultureIgnoreCase) && Path.GetFileNameWithoutExtension(o.Name).Equals(tableName, StringComparison.InvariantCultureIgnoreCase));
            if (item != null && from <= date && date <= to)
            {
                var fileclient = fileSystem.GetFileClient(item.Name);
                using var stream = fileclient.OpenRead();
                return new KeyValuePair<DateTime, CsvSet>(date, new CsvReader(stream).CsvSet);
            }
            return null;
        }

        /// <param name="fileName">Såsom "Lots.csv"</param>
        internal void SaveCsvToDataLake(string fileName, CsvSet csv)
        {
            if (csv.Records.Count > 0)
            {
                var stream = csv.Write();
                stream.Position = 0;
                var fileClient = GetFileClient(fileName);
                fileClient.Upload(stream, true);
            }
        }

        /// <param name="fileName">Såsom "Lots.csv"</param>
        internal void SaveStringToDataLake(string fileName, string data)  //Inspiration: https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-dotnet
        {
            var fileClient = GetFileClient(fileName);

            using var stream = new MemoryStream(Encoding.Default.GetBytes(data));
            fileClient.Upload(stream, true);
        }

        /// <param name="fileName">Såsom "Lots.csv"</param>
        internal void SaveStreamToDataLake(string fileName, Stream stream)  //Inspiration: https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-dotnet
        {
            if (stream == null || stream.Length == 0)
                return;

            var fileClient = GetFileClient(fileName);
            stream.Position = 0;
            fileClient.Upload(stream, true);
        }

        internal void DeleteSubDirectory()
        {
            var fileSystem = DataLakeServiceClient.GetFileSystemClient(BasePath);
            if (fileSystem.Exists())
            {
                var directory = fileSystem.GetDirectoryClient(string.Join('/', BaseDirectory, SubDirectory));
                if (directory.Exists())
                    directory.Delete();
            }
        }

        /// <param name="filename">If set, it wil only return files by that name.</param>
        internal Azure.Storage.Files.DataLake.Models.PathItem GetNewestFileInFolder(string filename)
        {
            var fileSystem = DataLakeServiceClient.GetFileSystemClient(BasePath);
            if (!fileSystem.Exists())
                return null;

            var directory = fileSystem.GetDirectoryClient(string.Join('/', BaseDirectory, SubDirectory));
            if (!directory.Exists())
                return null;

            var list = fileSystem.GetPaths(directory.Path).OrderByDescending(o => o.LastModified);

            return string.IsNullOrEmpty(filename) ? list.FirstOrDefault() : list.Where(o => o.Name.EndsWith(filename, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
        }

        /// <param name="filename">Såsom "Lots.csv"</param>
        private DataLakeFileClient GetFileClient(string filename)
        {
            var fileSystem = DataLakeServiceClient.GetFileSystemClient(BasePath);
            if (!fileSystem.Exists())
                fileSystem.Create();

            var directory = fileSystem.GetDirectoryClient(string.Join('/', BaseDirectory, SubDirectory));
            if (!directory.Exists())
                directory.Create();

            return directory.GetFileClient(filename);
        }
    }
}
