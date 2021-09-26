using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Bygdrift.Warehouse.DataLake.CsvTools;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Bygdrift.Warehouse.DataLake.DataLakeTools
{
    public class DataLake
    {
        public Uri ServiceUri;
        public string Container;
        public string Module;

        //public string SubDirectory { get; set; }

        private string storageAccountName;
        private string storageAccountKey;
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

        ///// <param name="module">Such as "DaluxFM"</param>
        //public DataLake(IConfigurationRoot config, string module)
        //{
        //    SetConnectionString(config["DataLakeConnectionString"]);

        //    Container = config["DataLakeContainer"];
        //    Module = module;
        //}

        /// <param name="module">Such as "DaluxFM"</param>
        public DataLake(string connectionString, string container, string module)
        {
            SetConnectionString(connectionString);
            
            Container = container;
            if (Container == null)
                throw new Exception("The value for DataLakeBasePath, has to be set in config.");

            Module = module;
        }

        private void SetConnectionString(string connectionString)
        {
            if (connectionString == null)
                throw new Exception("The value for DataLakeConnectionString, has to be set in config.");

            var endpointProtocol = "";
            var endpointSuffix = "";
            foreach (var item in connectionString.Split(';'))
            {
                var count = item.IndexOf('=');
                var pair = new string[] { item.Substring(0, count), item.Substring(count + 1) };
                if (pair.Length == 2)
                {
                    switch (pair[0].ToLower())
                    {
                        case "defaultendpointsprotocol": endpointProtocol = pair[1]; break;
                        case "accountname": storageAccountName = pair[1]; break;
                        case "accountkey": storageAccountKey = pair[1]; break;
                        case "endpointsuffix": endpointSuffix = pair[1]; break;
                    }
                }
            }

            if (string.IsNullOrEmpty(endpointProtocol) ||
                string.IsNullOrEmpty(endpointSuffix) ||
                string.IsNullOrEmpty(storageAccountName) ||
                string.IsNullOrEmpty(storageAccountKey)
            )
                throw new Exception("The value for DataLakeConnectionString, is not correct and is missing one or more parameters.");

            ServiceUri = new Uri($"{endpointProtocol}://{storageAccountName}.dfs.{endpointSuffix}/");
        }

        public static string CreateDatePath(string subDirectory, DateTime fileDate, bool savePerHour)
        {
            var pathParts = new List<string> { subDirectory.ToString(), fileDate.ToString("yyyy"), fileDate.ToString("MM"), fileDate.ToString("dd") };
            if (savePerHour)
                pathParts.Add(fileDate.ToString("HH"));

            var basePath = string.Join('/', pathParts);
            return basePath;
        }
    }
}
