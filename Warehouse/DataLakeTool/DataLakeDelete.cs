using System;
using System.Linq;
using System.Threading.Tasks;

namespace Bygdrift.DataLakeTools
{
    public partial class DataLake
    {
        /// <summary>
        /// Deletes a given directory
        /// </summary>
        /// <param name="basePath">If null or empty, then it will be the root and all will be deleted</param>
        /// <param name="folderStructure"></param>
        public async Task DeleteDirectoryAsync(string basePath, FolderStructure folderStructure = FolderStructure.Path)
        {
            if (string.IsNullOrEmpty(basePath))
            {
                await DeleteAllAsync();
                return;
            }

            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            var directory = GetDirectoryClient(basePath, false);
            if (directory != null && directory.Exists())
                await directory.DeleteAsync();
        }

        /// <summary>
        /// If a base directory are build with subfolders like "basePath/yyyy/mm/dd/and eventual HH", then this method will remove all folders thats equal or older than the given amount of days.
        /// Paths like: "basePath/abc" and "basePath/abc/..." will be ignored.
        /// </summary>
        /// <param name="basePath"></param>
        /// <param name="equalOrolderThanDays">If 7, then all subfolders equal, or older than 7 days, will be removed</param>
        public async Task DeleteDirectoriesOlderThanDaysAsync(string basePath, int equalOrolderThanDays)
        {
            var olderThan = DateTime.Now.AddDays(-equalOrolderThanDays);
            var directories = GetDirectories(basePath);
            var fileSystem = DataLakeServiceClient.GetFileSystemClient(Container);

            foreach (var year in GetDirectories(basePath))
            {
                var yearString = year.Name.Split('/').Last();
                if (int.TryParse(yearString, out int yearInt))
                {
                    if (new DateTime(yearInt, 12, 31) <= olderThan)
                        await fileSystem.DeleteDirectoryAsync(year.Name);
                    else if (new DateTime(yearInt, 1, 1) <= olderThan)
                        foreach (var month in GetDirectories(year.Name))
                        {
                            var monthString = month.Name.Split('/').Last();
                            if (int.TryParse(monthString, out int monthInt))
                            {
                                if (new DateTime(yearInt, monthInt, 31) <= olderThan)
                                    await fileSystem.DeleteDirectoryAsync(month.Name);
                                else if (new DateTime(yearInt, monthInt, 1) <= olderThan)
                                    foreach (var day in GetDirectories(month.Name))
                                    {
                                        if (int.TryParse(day.Name, out int dayInt))
                                        {
                                            var dayDate = new DateTime(yearInt, monthInt, dayInt);
                                            if (dayDate <= olderThan)
                                                await fileSystem.DeleteDirectoryAsync(day.Name);
                                        }
                                    }
                            }
                        }
                }
            }
        }

        private async Task DeleteAllAsync()
        {
            var fileSystem = DataLakeServiceClient.GetFileSystemClient(Container);
            if (!fileSystem.Exists())
                return;

            foreach (var path in fileSystem.GetPaths())
                if (path.IsDirectory == true)
                    await fileSystem.DeleteDirectoryAsync(path.Name);
                else
                    await fileSystem.DeleteFileAsync(path.Name);
        }

        /// <summary>
        /// Deletes a given file
        /// </summary>
        /// <param name="basePath">If null, then it will be the root</param>
        /// <param name="fileName">Like 'data.csv'</param>
        /// <param name="folderStructure"></param>
        public async Task DeleteFileAsync(string basePath, string fileName, FolderStructure folderStructure = FolderStructure.Path)
        {
            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            var directory = GetDirectoryClient(basePath, true);
            var fileClient = directory.GetFileClient(fileName);

            await fileClient.DeleteIfExistsAsync();
        }

    }
}
