﻿using Bygdrift.Tools.CsvTool;
using Bygdrift.Tools.DataLakeTool;
using Bygdrift.Tools.LogTool;
using Bygdrift.Tools.MssqlTool;
using Bygdrift.Warehouse.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Globalization;
using System.IO;
using System.Linq;

namespace Bygdrift.Warehouse
{
    /// <summary>
    /// Contains core info about dataLake, database and configurations.
    /// By adding TSettings, you can get app settings out in your own class
    /// </summary>
    public class AppBase<TSettings> : AppBase where TSettings : new()
    {
        /// <summary>
        /// The core for distributing config settings and dataLake- and database connections
        /// </summary>
        public AppBase(ILogger logger = null) : base(logger)
        {
            LoadSettings();
        }

        /// <summary>
        /// Settings from the module
        /// </summary>
        public TSettings Settings { get; private set; }

        private void LoadSettings()
        {
            Settings = new TSettings();
            foreach (var prop in Settings.GetType().GetProperties())
            {
                var configSetting = prop.GetCustomAttributes(true).OfType<ConfigSetting>().FirstOrDefault();
                if (configSetting != null)
                    configSetting.GetData(this, prop, Settings);

                var configSecret = prop.GetCustomAttributes(true).OfType<ConfigSecret>().FirstOrDefault();
                if (configSecret != null)
                    configSecret.GetData(this, prop, Settings);
            }
        }
    }

    /// <summary>
    /// Contains core info about dataLake, database and configurations
    /// </summary>
    public class AppBase
    {
        private Config _csvConfig;
        private CultureInfo _cultureInfo;
        private IConfigurationRoot _config;
        private DataLake _dataLake;
        private DataLakeQueue _dataLakeQueue;
        private string _dataLakeConnectionString;
        private Tools.CsvTool.Helpers.DateHelper _dateHelper;
        //private string _dataLakeContainer;
        //private string _functionAppName;
        private string _hostName;
        private DateTime _loadedUtc;
        private DateTime _loadedLocal;
        private string _moduleName;
        private Mssql _mssql;
        private string _mssqlConnectionString;
        private bool? _isRunningLocal;
        private KeyVault _keyVault;
        private TimeZoneInfo _timeZoneInfo;


        /// <summary>
        /// The core for distributing config settings and dataLake- and database connections
        /// </summary>
        public AppBase(ILogger logger = null)
        {
            Log = logger != null ? new Log(logger) : new Log();
            _loadedUtc = DateTime.Now.ToUniversalTime();
            _loadedLocal = ToLocalTime(_loadedUtc);
            //KeyVault = new KeyVault(this);
        }

        /// <summary>
        /// All config settings from the appSettings are available here and can be called like: Config["DataLakeConnectionString"], although it sholdn't be necesary because you can get data from this AppBase and AppBase.Settings.
        /// When running local, it will use settings from 'local.settings.json', 'appsettings.json' and 'appsettings.development.json', 
        /// When running on Azure it will take data from environmentvariables.
        /// </summary>
        public IConfigurationRoot Config
        {
            get
            {
                return _config ??= new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                     .AddJsonFile("local.settings.json", true, true)
                     .AddJsonFile("appsettings.json", true, true)
                     .AddJsonFile("appsettings.development.json", true, true)
                     .AddEnvironmentVariables()
                     .Build();
            }
        }

        /// <summary>
        /// Base csv config with FormatKind = FormatKind.Local
        /// </summary>
        public Config CsvConfig
        {
            get { return _csvConfig ??= new Config(CultureInfo, TimeZoneInfo, DateFormatKind.Local); }
            set { _csvConfig = value; }
        }

        /// <summary>
        /// Current culture
        /// </summary>
        public CultureInfo CultureInfo
        {
            get
            {
                if (_cultureInfo == null)
                {
                    var cultureName = Config["CultureName"];
                    _cultureInfo = cultureName != null ? CultureInfo.CreateSpecificCulture(cultureName) : CultureInfo.InvariantCulture;
                }
                return _cultureInfo;
            }
            set { _cultureInfo = value; }
        }

        /// <summary>
        /// Link to Bygdrift dataLake tools for reading and writing. 
        /// </summary>
        public DataLake DataLake { get { return _dataLake ??= new DataLake(DataLakeConnectionString, ModuleName, Log, LoadedLocal); } }

        /// <summary>
        /// Link to Bygdrift Key Vault for reading. 
        /// </summary>
        public KeyVault KeyVault { get { return _keyVault ??= new KeyVault(this); } }

        /// <summary>
        /// Link to Bygdrift dataLake tools for reading and writing. 
        /// </summary>
        public DataLakeQueue DataLakeQueue { get { return _dataLakeQueue ??= new DataLakeQueue(DataLakeConnectionString, ModuleName, Log); } }

        internal string DataLakeConnectionString
        {
            get
            {
                if (_dataLakeConnectionString == null)
                {
                    _dataLakeConnectionString = KeyVault.GetSecret("Secret--DataLakeConnectionString");
                    if (string.IsNullOrEmpty(_dataLakeConnectionString))
                        throw new Exception("'DataLakeConnectionString' has not been set in " + (IsRunningLocal ? "settings.json." : "Key vault in Azure"));
                }
                return _dataLakeConnectionString;
            }
        }

        /// <summary>
        /// A class that verifies if a value is a date. It is possible to replace the predefined date formats, if another culture than the predefined should be used. Dot it by replacing the 
        /// </summary>
        public Tools.CsvTool.Helpers.DateHelper DateHelper { get { return _dateHelper ??= new Tools.CsvTool.Helpers.DateHelper(CsvConfig); } }

        /// <summary>
        /// If is running local or in Azure. Gets determined, by if there is a local.settings.json, appsettings.json or appsettings.Development.json in the bin folder.
        /// You can overwrite the setting
        /// </summary>
        public bool IsRunningLocal
        {
            get
            {
                if (_isRunningLocal == null)
                {
                    _isRunningLocal = false;
                    if (File.Exists(Path.Combine(Environment.CurrentDirectory, "local.settings.json")) ||
                        File.Exists(Path.Combine(Environment.CurrentDirectory, "appsettings.json")) ||
                        File.Exists(Path.Combine(Environment.CurrentDirectory, "appsettings.development.json")))
                    {
                        _isRunningLocal = true;
                    }
                }
                return (bool)_isRunningLocal;
            }
            set
            {
                _isRunningLocal = value;
            }
        }

        /// <summary>
        /// The default host name like: https://ModuleName.azurewebsites.net
        /// </summary>
        public string HostName
        {
            get
            {
                if (_hostName == null)
                {
                    _hostName = Config["HostName"];
                    if (_hostName == null)
                        throw new Exception("'HostName' is not in appSettings. The module has been stopped.");
                }
                return _hostName;
            }
        }

        /// <summary>
        /// The date when App was initiated. Can be changed
        /// </summary>
        public DateTime LoadedUtc
        {
            get { return _loadedUtc; }
            set
            {
                _loadedUtc = value;
                _loadedLocal = ToLocalTime(_loadedLocal);
                DataLake.LocalTime = _loadedLocal;
            }
        }

        /// <summary>
        /// The date when App was initiated. Can be changed.
        /// If AppSetting 'TimeZoneId' is not set, then this time is UTC.
        /// The date used in dataLake to make a folder path
        /// </summary>
        public DateTime LoadedLocal
        {
            get { return _loadedLocal; }
            set
            {
                _loadedLocal = value;
                _loadedUtc = _loadedLocal.ToUniversalTime();
                DataLake.LocalTime = _loadedLocal;
            }
        }

        /// <summary>
        /// The log
        /// </summary>
        public Log Log { get; }

        /// <summary>
        /// The name of the module
        /// </summary>
        public string ModuleName
        {
            //The name is used for containername, functionAppName, schemaName
            get
            {
                if (_moduleName == null)
                {
                    _moduleName = Config["ModuleName"];
                    if (_moduleName == null)
                        throw new ArgumentNullException("'ModuleName' is not in appSettings. The module has been stopped.");

                    if (!_moduleName.All(o => char.IsLetterOrDigit(o)))
                        throw new Exception("The appSetting 'ModuleName', must only contain letters and numbers.");

                    if (_moduleName.Length < 3 || _moduleName.Length > 24)
                        throw new Exception("The appSetting 'ModuleName', must be between 3 and 24 letters or numbers.");
                }
                return _moduleName;
            }
        }

        /// <summary>
        /// Link to Bygdrift database tools for reading and writing. 
        /// </summary>
        public Mssql Mssql
        {
            get
            {
                _mssql ??= new Mssql(MssqlConnectionString, ModuleName, Log);
                return _mssql;
            }
        }

        /// <summary>
        /// The connectionstring for the database. 
        /// </summary>
        public string MssqlConnectionString
        {
            get
            {
                if (_mssqlConnectionString == null)
                {
                    _mssqlConnectionString = KeyVault.GetSecret("Secret--MssqlConnectionString");
                    if (string.IsNullOrEmpty(_mssqlConnectionString))
                        throw new Exception("'MssqlConnectionString' has not been set in " + (IsRunningLocal ? "settings.json." : "Key vault in Azure"));
                }
                return _mssqlConnectionString;
            }
        }

        /// <summary>
        /// Like: 'Romance Standard Time'. Get timeZoneId from here: https://raw.githubusercontent.com/Bygdrift/Warehouse/master/Docs/TimeZoneIds.csv
        /// If AppSetting "TimeZoneId" is not set, then standard is set to 'GMT Standard Time'.
        /// </summary>
        public TimeZoneInfo TimeZoneInfo
        {
            get
            {
                if (_timeZoneInfo == null)
                {
                    var timeZoneId = Config["TimeZoneId"];
                    if (timeZoneId != null)
                        _timeZoneInfo = TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
                    else
                        _timeZoneInfo = TimeZoneInfo.FindSystemTimeZoneById("GMT Standard Time");

                }
                return _timeZoneInfo;
            }
            set { _timeZoneInfo = value; }
        }

        /// <summary>
        /// Converts a date to localTime if the AppSetting "TimeZoneId" is set. If not, then universalTime will be returned.
        /// Se all TimeZoneId's here: http://www.xiirus.net/articles/article-_net-convert-datetime-from-one-timezone-to-another-7e44y.aspx
        /// </summary>
        public DateTime ToLocalTime(DateTime dateTime)
        {
            var utc = dateTime.ToUniversalTime();
            var timeZoneId = Config["TimeZoneId"];
            if (timeZoneId != null)
            {
                try
                {
                    var timeZone = TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
                    return TimeZoneInfo.ConvertTimeFromUtc(utc, timeZone);
                }
                catch (Exception)
                {
                    return utc;
                }

            }
            return utc;
        }
    }
}