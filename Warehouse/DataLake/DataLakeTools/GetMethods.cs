using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Bygdrift.Warehouse.DataLake.CsvTools;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Bygdrift.Warehouse.DataLake.DataLakeTools
{
    public static class GetMethods
    {
        public static IEnumerable<KeyValuePair<DateTime, CsvSet>> GetFilesFromDataLake(this DataLake dataLake, string tableName, string basePath, DateTime from, DateTime to)
        {
            var fileSystem = dataLake.DataLakeServiceClient.GetFileSystemClient(dataLake.Container);
            if (!fileSystem.Exists())
                yield break;

            var directory = fileSystem.GetDirectoryClient(string.Join('/', dataLake.Module, basePath));
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
                                    var res = GetCsvSet(dataLake, fileSystem, dayFolder, tableName, from, to, date);
                                    if (res != null)
                                        yield return new KeyValuePair<DateTime, CsvSet>(date, res);

                                    foreach (var hourFolder in fileSystem.GetPaths(dayFolder.Name).Where(o => o.IsDirectory == true))
                                        if (int.TryParse(Path.GetFileName(hourFolder.Name), out int hour))
                                        {
                                            date = new DateTime(year, month, day, hour, 0, 0);
                                            res = GetCsvSet(dataLake, fileSystem, hourFolder, tableName, from, to, date);
                                            if (res != null)
                                                yield return new KeyValuePair<DateTime, CsvSet>(date, res);
                                        }
                                }
        }

        public static CsvSet GetCsvSet(this DataLake dataLake, DataLakeFileSystemClient fileSystem, PathItem pathItem, string tableName, DateTime from, DateTime to, DateTime date)
        {
            var item = fileSystem.GetPaths(pathItem.Name).SingleOrDefault(o =>
                o.IsDirectory == false &&
                Path.GetExtension(o.Name).Equals(".csv", StringComparison.InvariantCultureIgnoreCase) &&
                Path.GetFileNameWithoutExtension(o.Name).Equals(tableName, StringComparison.InvariantCultureIgnoreCase));

            if (item == null || from > date || date > to)
                return null;

            var fileClient = fileSystem.GetFileClient(item.Name);
            using var stream = fileClient.OpenRead();
            return new CsvReader(stream).CsvSet;
        }

        public static CsvSet GetCsvSet(this DataLake dataLake, string subDirectory, string filename)
        {
            var ext = Path.GetExtension(filename);
            if (string.IsNullOrEmpty(ext))
                filename += ".csv";
            else if (ext.ToLower() != ".csv")
                new Exception($"Can only convert data from a csv-file and not a {ext}.");

            var fileClient = GetFileClient(dataLake, subDirectory, filename);
            if (fileClient == null || !fileClient.Exists())
                return null;

            using Stream stream = fileClient.OpenRead();
            return new CsvReader(stream).CsvSet;
        }

        /// <param name="subDirectory">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="filename">If set, it wil only return files by that name.</param>
        public static PathItem GetNewestFileInFolder(this DataLake dataLake, string subDirectory, string filename)
        {
            var fileSystem = dataLake.DataLakeServiceClient.GetFileSystemClient(dataLake.Container);
            if (!fileSystem.Exists())
                return null;

            var directory = fileSystem.GetDirectoryClient(string.Join('/', dataLake.Module, subDirectory));
            if (!directory.Exists())
                return null;

            var list = fileSystem.GetPaths(directory.Path).OrderByDescending(o => o.LastModified);

            return string.IsNullOrEmpty(filename) ? list.FirstOrDefault() : list.Where(o => o.Name.EndsWith(filename, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
        }

        /// <param name="subDirectory">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="filename">Såsom "Lots.csv"</param>
        public static DataLakeFileClient GetFileClient(this DataLake dataLake, string subDirectory, string filename)
        {
            var fileSystem = dataLake.DataLakeServiceClient.GetFileSystemClient(dataLake.Container);
            if (!fileSystem.Exists())
                fileSystem.Create();

            var directory = fileSystem.GetDirectoryClient(subDirectory != null ? string.Join('/', dataLake.Module, subDirectory) : dataLake.Module);
            if (!directory.Exists())
                directory.Create();

            return directory.GetFileClient(filename);
        }

        ///// <param name="subDirectory">Such as "Raw". If null, then files are saved in the base directory</param>
        ///// <param name="filename">If set, it wil only return files by that name.</param>
        //public static PathItem GetFile(this DataLake dataLake, string subDirectory, string filename)
        //{
        //    var fileSystem = dataLake.DataLakeServiceClient.GetFileSystemClient(dataLake.Container);
        //    if (!fileSystem.Exists())
        //        return null;

        //    var directory = fileSystem.GetDirectoryClient(string.Join('/', dataLake.Module, subDirectory));
        //    if (!directory.Exists())
        //        return null;

        //    var list = fileSystem.GetPaths(directory.Path).OrderByDescending(o => o.LastModified);

        //    return string.IsNullOrEmpty(filename) ? list.FirstOrDefault() : list.Where(o => o.Name.EndsWith(filename, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
        //}
    }
}
