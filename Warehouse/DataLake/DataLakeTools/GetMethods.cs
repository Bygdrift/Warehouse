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
        public static IEnumerable<KeyValuePair<DateTime, CsvSet>> GetDecodedFilesFromDataLake(this DataLake dataLake, string tableName, DateTime from, DateTime to)
        {
            var fileSystem = dataLake.DataLakeServiceClient.GetFileSystemClient(dataLake.Container);
            if (!fileSystem.Exists())
                yield break;

            var directory = fileSystem.GetDirectoryClient(string.Join('/', dataLake.Module, SubDirectory.Decode));
            if (!directory.Exists())
                yield break;

            foreach (var yearFolder in fileSystem.GetPaths(directory.Path).Where(o => o.IsDirectory == true))
                if (int.TryParse(Path.GetFileName(yearFolder.Name), out int year))
                    foreach (var monthFolder in fileSystem.GetPaths(yearFolder.Name).Where(o => o.IsDirectory == true))
                        if (int.TryParse(Path.GetFileName(monthFolder.Name), out int month))
                            foreach (var dayFolder in fileSystem.GetPaths(monthFolder.Name).Where(o => o.IsDirectory == true))
                                if (int.TryParse(Path.GetFileName(dayFolder.Name), out int day))
                                {
                                    var res = GetStream(fileSystem, dayFolder, tableName, from, to, year, month, day, 0);
                                    if (res != null)
                                        yield return (KeyValuePair<DateTime, CsvSet>)res;

                                    foreach (var hourFolder in fileSystem.GetPaths(dayFolder.Name).Where(o => o.IsDirectory == true))
                                        if (int.TryParse(Path.GetFileName(hourFolder.Name), out int hour))
                                        {
                                            res = GetStream(fileSystem, hourFolder, tableName, from, to, year, month, day, hour);
                                            if (res != null)
                                                yield return (KeyValuePair<DateTime, CsvSet>)res;
                                        }
                                }
        }

        private static KeyValuePair<DateTime, CsvSet>? GetStream(DataLakeFileSystemClient fileSystem, PathItem pathItem, string tableName, DateTime from, DateTime to, int year, int month, int day, int hour = 0)
        {
            var date = new DateTime(year, month, day, hour, 0, 0);

            var item = fileSystem.GetPaths(pathItem.Name).SingleOrDefault(o =>
                o.IsDirectory == false &&
                Path.GetExtension(o.Name).Equals(".csv", StringComparison.InvariantCultureIgnoreCase) &&
                Path.GetFileNameWithoutExtension(o.Name).Equals(tableName, StringComparison.InvariantCultureIgnoreCase));

            if (item != null && from <= date && date <= to)
            {
                var fileclient = fileSystem.GetFileClient(item.Name);
                using var stream = fileclient.OpenRead();
                return new KeyValuePair<DateTime, CsvSet>(date, new CsvReader(stream).CsvSet);
            }

            return null;
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
    }
}
