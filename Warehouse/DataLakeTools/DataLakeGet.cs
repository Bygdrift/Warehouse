using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Bygdrift.CsvTools;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Bygdrift.DataLakeTools
{
    /// <summary>
    /// Get data from dataLake
    /// </summary>
    public partial class DataLake
    {
        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="folderStructure"></param>
        public bool BasePathExists(string basePath, FolderStructure folderStructure = FolderStructure.Path)
        {
            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            var fileSystem = DataLakeServiceClient.GetFileSystemClient(Container);
            if (!fileSystem.Exists())
                return false;

            var directory = fileSystem.GetDirectoryClient(basePath);
            return directory.Exists();
        }

        /// <summary>
        /// If a file exist
        /// </summary>
        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="fileName">Such as "Lots.csv"</param>
        /// <param name="folderStructure"></param>
        /// <returns>If true or false</returns>
        public bool FileExist(string basePath, string fileName, FolderStructure folderStructure = FolderStructure.Path)
        {
            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            var directory = GetDirectoryClient(basePath, false);
            if (directory == null)
                return false;

            var fileClient = directory.GetFileClient(fileName);
            return fileClient.Exists();
        }

        /// <summary>
        /// Get data that is stored as a csv
        /// </summary>
        /// <param name="basePath"></param>
        /// <param name="filename"></param>
        /// <param name="folderStructure"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool GetCsv(string basePath, string filename, FolderStructure folderStructure, out Csv value)
        {
            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            value = null;
            var ext = Path.GetExtension(filename);
            if (string.IsNullOrEmpty(ext))
                filename += ".csv";
            else if (ext.ToLower() != ".csv")
                _ = new Exception($"Can only convert data from a csv-file and not a {ext}.");

            var res = GetFile(basePath, filename);
            if (res.Stream == null)
                return false;

            value = new Csv().FromCsvStream(res.Stream);
            return true;
        }

        private static Csv GetCsv(DataLakeFileSystemClient fileSystem, PathItem pathItem, string tableName, DateTime from, DateTime to, DateTime date)
        {
            var item = fileSystem.GetPaths(pathItem.Name).SingleOrDefault(o =>
                o.IsDirectory == false &&
                Path.GetExtension(o.Name).Equals(".csv", StringComparison.InvariantCultureIgnoreCase) &&
                Path.GetFileNameWithoutExtension(o.Name).Equals(tableName, StringComparison.InvariantCultureIgnoreCase));

            if (item == null || from > date || date > to)
                return null;

            var fileClient = fileSystem.GetFileClient(item.Name);
            using var stream = fileClient.OpenRead();
            return new Csv().FromCsvStream(stream);
        }

        /// <summary>
        /// Get multiple files that are stored as Csvs in a given timeslot
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="basePath"></param>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <returns></returns>
        public IEnumerable<KeyValuePair<DateTime, Csv>> GetCsvs(string tableName, string basePath, DateTime from, DateTime to)
        {
            var fileSystem = DataLakeServiceClient.GetFileSystemClient(Container);
            if (!fileSystem.Exists())
                yield break;

            var directory = fileSystem.GetDirectoryClient(basePath);
            if (!directory.Exists())
                yield break;

            foreach (var yearFolder in fileSystem.GetPaths(directory.Path).Where(o => o.IsDirectory == true))
                if (int.TryParse(Path.GetFileName(yearFolder.Name), out int year))
                    foreach (var monthFolder in fileSystem.GetPaths(yearFolder.Name).Where(o => o.IsDirectory == true))
                        if (int.TryParse(Path.GetFileName(monthFolder.Name), out int month))
                            foreach (var dayFolder in fileSystem.GetPaths(monthFolder.Name).Where(o => o.IsDirectory == true))
                                if (int.TryParse(Path.GetFileName(dayFolder.Name), out int day))
                                {
                                    var date = new DateTime(year, month, day);
                                    var res = GetCsv(fileSystem, dayFolder, tableName, from, to, date);
                                    if (res != null)
                                        yield return new KeyValuePair<DateTime, Csv>(date, res);

                                    foreach (var hourFolder in fileSystem.GetPaths(dayFolder.Name).Where(o => o.IsDirectory == true))
                                        if (int.TryParse(Path.GetFileName(hourFolder.Name), out int hour))
                                        {
                                            date = new DateTime(year, month, day, hour, 0, 0);
                                            res = GetCsv(fileSystem, hourFolder, tableName, from, to, date);
                                            if (res != null)
                                                yield return new KeyValuePair<DateTime, Csv>(date, res);
                                        }
                                }
        }

        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="createIfNotExist"></param>
        internal DataLakeDirectoryClient GetDirectoryClient(string basePath, bool createIfNotExist)
        {
            if(string.IsNullOrEmpty(basePath))
                throw new ArgumentNullException(nameof(basePath));

            var fileSystem = DataLakeServiceClient.GetFileSystemClient(Container);
            if (!fileSystem.Exists())
                if (createIfNotExist)
                    fileSystem.Create();
                else
                    return null;

            var directory = fileSystem.GetDirectoryClient(basePath);
            if (!directory.Exists())
                if (createIfNotExist)
                    directory.Create();
                else
                    directory = null;

            return directory;
        }

        /// <summary>
        /// Gets a Json file from Data lake and tries to convert it to T
        /// </summary>
        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="fileName">Such as "Lots.csv"</param>
        /// <param name="folderStructure"></param>
        /// <param name="value"></param>
        public bool GetJson<T>(string basePath, string fileName, FolderStructure folderStructure, out T value)
        {
            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            value = default;

            var directory = GetDirectoryClient(basePath, false);
            if (directory == null)
                return false;

            var fileClient = directory.GetFileClient(fileName);
            if (!fileClient.Exists())
                return false;

            var stream = fileClient.OpenRead();
            if (stream == null || stream.Length == 0)
                return false;

            var serializer = new JsonSerializer();
            using var streamReader = new StreamReader(stream);
            using var jsonTextReader = new JsonTextReader(streamReader);

            try
            {
                value = serializer.Deserialize<T>(jsonTextReader);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Gets a Json file from Data lake and tries to convert it to T
        /// </summary>
        /// <param name="filePath">Starts from the basePath like: 'Refined/Subfolder/Subfolder/Test.csv'</param>
        /// <param name="value"></param>
        public bool GetJson<T>(string filePath, out T value)
        {
            var fileName = Path.GetFileName(filePath);
            var basePath = filePath.Replace(fileName, string.Empty).Trim('/');
            return GetJson(basePath, fileName, FolderStructure.Path, out value);
        }

        /// <summary>
        /// Gets a file from Data lake
        /// </summary>
        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="fileName">Such as "Lots.csv"</param>
        /// <param name="folderStructure"></param>
        /// <returns>A stream and extre infos</returns>
        public (Stream Stream, long Length, DateTime? LastModified) GetFile(string basePath, string fileName, FolderStructure folderStructure = FolderStructure.Path)
        {
            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            var directory = GetDirectoryClient(basePath, false);
            if (directory == null)
                return (null, 0, null);

            var fileClient = directory.GetFileClient(fileName);
            if (!fileClient.Exists())
                return (null, 0, null);

            PathProperties props = fileClient.GetProperties();
            var stream = fileClient.OpenRead();

            return (stream, props.ContentLength, props.LastModified.DateTime);
        }

        /// <summary>
        /// Gets a file from Data lake
        /// </summary>
        /// <param name="filePath">Starts from the basePath like: 'Refined/Subfolder/Subfolder/Test.csv'</param>
        /// <returns>A stream and extre infos</returns>
        public (Stream Stream, string BasePath, string FileName, long Length, DateTime? LastModified) GetFile(string filePath)
        {
            var fileName = Path.GetFileName(filePath);
            var basePath = filePath.Replace(fileName, string.Empty).Trim('/');
            var res = GetFile(basePath, fileName);
            return res.Stream != null ? (res.Stream, basePath, fileName, res.Length, res.LastModified) : (null, basePath, fileName, 0, null);
        }

        /// <summary>
        /// Get paths to muliple files
        /// </summary>
        /// <param name="basePath"></param>
        /// <param name="folderStructure"></param>
        public string[] GetFilePaths(string basePath, FolderStructure folderStructure = FolderStructure.Path)
        {
            var res = new List<string>();

            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            var fileSystem = DataLakeServiceClient.GetFileSystemClient(Container);
            if (!fileSystem.Exists())
                return default;

            var directory = fileSystem.GetDirectoryClient(basePath);
            if (!directory.Exists())
                return default;

            foreach (var file in fileSystem.GetPaths(directory.Path).Where(o => o.IsDirectory == false))
                res.Add(file.Name);

            return res.ToArray();
        }

        /// <summary>
        /// Get th first or default file from dataLake
        /// </summary>
        /// <param name="basePath"></param>
        /// <param name="folderStructure"></param>
        /// <param name="includeStream">Should stream be returned or is it only info about the file?</param>
        public (string FileName, DateTime LastModified, Stream Stream)? GetFirstOrDefaultFile(string basePath, FolderStructure folderStructure = FolderStructure.Path, bool includeStream = true)
        {
            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            var fileSystem = DataLakeServiceClient.GetFileSystemClient(Container);
            if (!fileSystem.Exists())
                return null;

            var directory = fileSystem.GetDirectoryClient(basePath);
            if (!directory.Exists())
                return null;

            var list = fileSystem.GetPaths(directory.Path).OrderByDescending(o => o.LastModified);

            var file = list.FirstOrDefault(o => o.IsDirectory == false);

            if (file != null)
            {
                var fileName = new DirectoryInfo(file.Name).Name;
                if (includeStream)
                {
                    var fileClient = directory.GetFileClient(fileName);
                    var stream = fileClient.OpenRead();
                    return (fileName, file.LastModified.DateTime, stream);
                }
                else
                    return (fileName, file.LastModified.DateTime, null);
            }
            return null;
        }

        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="filename">If set, it wil only return files by that name.</param>
        /// <param name="folderStructure"></param>
        public PathItem GetNewestFileInFolder(string basePath, string filename, FolderStructure folderStructure = FolderStructure.Path)
        {
            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            var fileSystem = DataLakeServiceClient.GetFileSystemClient(Container);
            if (!fileSystem.Exists())
                return null;

            var directory = fileSystem.GetDirectoryClient(basePath);
            if (!directory.Exists())
                return null;

            var list = fileSystem.GetPaths(directory.Path).OrderByDescending(o => o.LastModified);

            return string.IsNullOrEmpty(filename) ? list.FirstOrDefault() : list.Where(o => o.Name.EndsWith(filename, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
        }
    }
}
