using Microsoft.Extensions.Configuration;
using System;
using System.IO;

namespace WarehouseTests.Helpers
{
    public class GenericTests
    {
        /// <summary>Get data from appSettings like Config["test"]</summary>
        public IConfigurationRoot Config;

        /// <summary>Path to project base</summary>
        public static string BasePath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..\\..\\..\\"));

        public GenericTests()
        {
            Config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.local.json").Build();
        }
    }
}