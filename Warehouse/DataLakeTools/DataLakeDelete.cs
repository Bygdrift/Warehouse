using System.Threading.Tasks;

namespace Bygdrift.DataLakeTools
{
    public partial class DataLake
    {
        /// <summary>
        /// Deletes a given directory
        /// </summary>
        /// <param name="basePath">If null, then it will be the root</param>
        /// <param name="folderStructure"></param>
        public async Task DeleteDirectoryAsync(string basePath, FolderStructure folderStructure = FolderStructure.Path)
        {
            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            var directory = GetDirectoryClient(basePath, false);
            if (directory != null && directory.Exists())
                await directory.DeleteAsync();
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
