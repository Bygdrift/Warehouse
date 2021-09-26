using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Web;
using Bygdrift.Warehouse.Modules;
using Bygdrift.Warehouse.DataLake.DataLakeTools;
using Bygdrift.Warehouse.DataLake.CsvTools;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

[assembly: InternalsVisibleTo("Warehouse.Common.Tests")]
namespace Bygdrift.Warehouse.DataLake
{
    class CommonDataModel
    {
        private readonly DataLakeTools.DataLake dataLake;

        public List<RefineBase> Refines { get; }
        public JObject Model { get; }

        /// <param name="baseDirectory">såsom "DaluxFM"</param>
        /// <param name="subDirectory">Såsom "current"</param>
        /// <param name="saveInCurrent">Saves model in data lake, in the folder "Curent"</param>
        public CommonDataModel(string connectionString, string container, string module, IEnumerable<RefineBase> refines, RefineBase logRefine, ILogger log, bool uploadToDataLake)
        {
            dataLake = new DataLakeTools.DataLake(connectionString, container, module);
            Refines = refines != null ? refines.Where(o => o.CsvAddToCommonDataModel).ToList() : default;
            Refines.Add(logRefine);

            var duplicates = Refines?.GroupBy(o => o.TableName).Where(o => o.Count() > 1).Select(o => o.Key).ToArray();
            if (duplicates?.Length > 0)
            {
                var items = string.Join(',', duplicates);
                throw new Exception($"model.json cannot be build. There are more than one entity called '{items}'.");
            }

            Model = CreateModel();

            if (uploadToDataLake)
                SaveToDataLake();
        }

        internal void SaveToDataLake()
        {
            string dataAsString = JsonConvert.SerializeObject(Model, Formatting.Indented);
            dataLake.SaveString(null, "model.json", dataAsString);
        }

        private JObject CreateModel()
        {
            var res = new JObject
            {
                new JProperty("$schema", "https://raw.githubusercontent.com/microsoft/CDM/master/docs/schema/modeljsonschema.json"),
                new JProperty("application", "appName"),
                new JProperty("name", "OrdersProducts"),
                new JProperty("description", "Model containing data for Order and Products."),
                new JProperty("version", "1.0"),
                //new JProperty("culture", "da-DK"),  //Do not use this: Excluded to follow internatinal rule of dot as seperator in decimal numbers and the use ISO 8601 as time standards
                new JProperty("modifiedTime", DateTime.UtcNow.ToString("s"))
            };

            if (Refines != null)
            {
                Validate();
                var entities = new JArray();
                foreach (var refine in Refines)
                    if (refine.CsvAddToCommonDataModel)
                        CreateEntity(ref entities, refine.TableName, refine.CsvSet, refine.CsvBasePath);

                res.Add(new JProperty("entities", entities));
            }

            return res;
        }

        private void Validate()
        {
            foreach (var item in Refines)
            {
                if (Refines.Count(o => o.TableName == item.TableName) > 1)
                    throw new Exception($"model.json cannot be build. There are more than one entity called '{item.TableName}'.");

                foreach (var header in item.CsvSet.Headers)
                    if (item.CsvSet.Headers.Count(o => o.Value == header.Value) > 1)
                        throw new Exception($"model.json cannot be build. There are more than one attribute called '{header.Value}', in entity '{item.TableName}'.");
            }
        }

        internal void CreateEntity(ref JArray entities, string tableName, CsvSet csv, string path)
        {
            var entity = new JObject
            {
                new JProperty("$type", "LocalEntity"),
                new JProperty("name", tableName)
            };

            var attributes = new JArray();
            foreach (var item in csv.Headers)
            {
                var attribute = new JObject
                {
                    new JProperty("name", item.Value.ToString()),
                    new JProperty("dataType", GetCommonDataModelType(csv.ColTypes[item.Key]))
                };
                attributes.Add(attribute);
            }
            entity.Add(new JProperty("attributes", attributes));

            var partitions = new JArray();
            var partition = new JObject
            {
                new JProperty("name", "CurrentPartition"),
                new JProperty("refreshTime", DateTime.UtcNow.ToString("s"))
            };

            var fullPath = string.Join('/', dataLake.ServiceUri.ToString().Replace(".dfs.", ".blob.").TrimEnd('/'), dataLake.Container, dataLake.Module, path, tableName + "csv");
            var urlPath = HttpUtility.UrlPathEncode(fullPath);
            partition.Add(new JProperty("location", urlPath));
            var fileFormatSettings = new JObject
            {
                new JProperty("$type", "CsvFormatSettings"),
                new JProperty("columnHeaders", true)
            };
            partition.Add("fileFormatSettings", fileFormatSettings);

            partitions.Add(partition);
            entity.Add(new JProperty("partitions", partitions));

            entities.Add(entity);
        }

        private string GetCommonDataModelType(Type type)
        {
            return Type.GetTypeCode(type) switch
            {
                TypeCode.Int64 => "int64",
                TypeCode.Object => "string",
                TypeCode.Boolean => "boolean",
                TypeCode.Char => "string",
                TypeCode.SByte => "int64",
                TypeCode.Byte => "int64",
                TypeCode.Int16 => "int64",
                TypeCode.UInt16 => "int64",
                TypeCode.Int32 => "int64",
                TypeCode.UInt32 => "int64",
                TypeCode.UInt64 => "int64",
                TypeCode.Single => "double",
                TypeCode.Double => "double",
                TypeCode.Decimal => "decimal",
                TypeCode.DateTime => "dateTime",
                TypeCode.String => "string",
                TypeCode.Empty => "string",
                TypeCode.DBNull => "string",
                _ => type.ToString().ToLower(),
            };
        }
    }
}
