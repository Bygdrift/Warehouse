using System;
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
