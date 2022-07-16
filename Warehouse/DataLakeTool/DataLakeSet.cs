using Bygdrift.CsvTools;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Bygdrift.DataLakeTools
{
    public partial class DataLake
    {
        /// <summary>
        /// Creates a directory. If it already exists, nothing will happen
        /// </summary>
        /// <param name="basePath">If empty, there will not be created a directory.</param>
        /// <param name="folderStructure"></param>
        /// <returns>The folderPath</returns>
        public async Task<string> CreateDirectoryAsync(string basePath, FolderStructure folderStructure)
        {
            if (string.IsNullOrEmpty(basePath))
                throw new ArgumentNullException(nameof(basePath));

            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            var directory = GetDirectoryClient(basePath, true);

            if (!await directory.ExistsAsync())
                await directory.CreateAsync();

            return directory.Path;
        }

        /// <summary>
        /// Saves a csv to dataLake
        /// </summary>
        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="fileName">Such as "Lots.csv"</param>
        /// <param name="csv"></param>
        /// <param name="folderStructure">What structure data should be saved into</param>
        /// <param name="append">If the file already exists, then data will be appended to the end of this file</param>
        /// <returns>The filePath</returns>
        public async Task<string> SaveCsvAsync(Csv csv, string basePath, string fileName, FolderStructure folderStructure, bool append = false)
        {
            if (csv != null && csv != default)
            {
                using var stream = csv.ToCsvStream();
                if (stream.Length > 0)
                    return await SaveStreamAsync(stream, basePath, fileName, folderStructure, append);
            }
            return string.Empty;
        }

        /// <summary>
        /// Saves a csv to dataLake
        /// </summary>
        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="fileName">Such as "Lots.xlsx"</param>
        /// <param name="csv"></param>
        /// <param name="folderStructure">What structure data should be saved into</param>
        /// <param name="paneName">The name of the worksheet</param>
        /// <param name="tableName">The name of the table inside Excel. If null, no fancy table will be added</param>
        /// <param name="append">If the file already exists, then data will be appended to the end of this file</param>
        /// <returns>The filePath</returns>
        public async Task<string> SaveExcelAsync(Csv csv, string basePath, string fileName, FolderStructure folderStructure, string paneName, string tableName = null, bool append = false)
        {
            if (csv != null && csv != default)
            {
                using var stream = csv.ToExcelStream(paneName, tableName);
                if (stream.Length > 0)
                    return await SaveStreamAsync(stream, basePath, fileName, folderStructure, append);
            }
            return string.Empty;
        }

        /// <summary>
        /// Deserializes an object to json and saves it to the dataLake
        /// </summary>
        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="fileName">Such as "Lots.json"</param>
        /// <param name="data">The object to be saved</param>
        /// <param name="folderStructure">What structure data should be saved into</param>
        /// <param name="append">If the file already exists, then data will be appended to the end of this file</param>
        /// <returns>A file path to where the file is saved</returns>
        public async Task<string> SaveObjectAsync(object data, string basePath, string fileName, FolderStructure folderStructure, bool append = false)
        {
            if (data is null)
                return null;

            var json = JsonConvert.SerializeObject(data);
            using var stream = new MemoryStream(Encoding.Default.GetBytes(json));
            return await SaveStreamAsync(stream, basePath, fileName, folderStructure, append);
        }

        /// <summary>
        /// Saves a stream to the datalake in a folder format like: basePath/2021/11/21/fileName.rawFileExtension
        /// </summary>
        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="fileName">Such as "Lots.txt"</param>
        /// <param name="stream"></param>
        /// <param name="folderStructure">What structure data should be saved into</param>
        /// <param name="append">If the file already exists, then data will be appended to the end of this file</param>
        /// <returns>A file path to where the file is saved</returns>
        public async Task<string> SaveStreamAsync(Stream stream, string basePath, string fileName, FolderStructure folderStructure, bool append = false)
        {
            if (stream == null || stream.Length == 0)
                return default;

            if (folderStructure == FolderStructure.DatePath)
                basePath = CreateDatePath(basePath, false);
            if (folderStructure == FolderStructure.DateTimePath)
                basePath = CreateDatePath(basePath, true);

            var directory = GetDirectoryClient(basePath, true);
            var fileClient = directory.GetFileClient(fileName);
            stream.Position = 0;

            try
            {
                if (append)
                {
                    var blobProperties = fileClient.GetProperties();
                    fileClient.Append(stream, offset: blobProperties.Value.ContentLength);
                    fileClient.Flush(position: stream.Length + blobProperties.Value.ContentLength);
                }
                else
                {
                    await fileClient.UploadAsync(stream, true);
                }
                return string.Join('/', basePath, fileClient.Name);
            }
            catch (Exception e)
            {
                App.Log.LogError($"There were an error, trying to upload file to datalake. BasePath: '{basePath}', filename: '{fileName}'. Error: {e.Message}");
                return string.Empty;
            }
        }

        /// <summary>
        /// Saves a text into the dataLake
        /// </summary>
        /// <param name="basePath">Such as "Raw". If null, then files are saved in the base directory</param>
        /// <param name="fileName">Such as "Lots.csv"</param>
        /// <param name="data">The string to bae saved</param>
        /// <param name="folderStructure">What structure data should be saved into</param>
        /// <param name="append">If the file already exists, then data will be appended to the end of this file</param>
        /// <returns>A file path to where the file is saved</returns>
        public async Task<string> SaveStringAsync(string data, string basePath, string fileName, FolderStructure folderStructure, bool append = false)
        {
            if (string.IsNullOrEmpty(data))
                return null;

            using var stream = new MemoryStream(Encoding.Default.GetBytes(data));
            return await SaveStreamAsync(stream, basePath, fileName, folderStructure, append);
        }


    }
}
