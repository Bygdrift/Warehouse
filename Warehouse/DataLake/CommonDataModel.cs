using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Web;
using Bygdrift.Warehouse.Modules;

[assembly: InternalsVisibleTo("Warehouse.Common.Tests")]
namespace Bygdrift.Warehouse.DataLake
{
    class CommonDataModel
    {
        private readonly DataLake dataLake;

        public RefineBase[] Refines { get; }
        public JObject Model { get; }

        /// <param name="baseDirectory">såsom "DaluxFM"</param>
        /// <param name="subDirectory">Såsom "current"</param>
        /// <param name="saveInCurrent">Saves model in data lake, in the folder "Curent"</param>
        public CommonDataModel(IConfigurationRoot config, string module, RefineBase[] refines, bool uploadToDataLake)
        {
            dataLake = new DataLake(config, module, null);
            Refines = refines;
            Model = CreateModel();

            if (uploadToDataLake)
                SaveToDataLake();
        }

        internal void SaveToDataLake()
        {
            string dataAsString = JsonConvert.SerializeObject(Model, Formatting.Indented);
            dataLake.SaveStringToDataLake("model.json", dataAsString);
        }

        private JObject CreateModel()
        {
            Validate();
            var res = new JObject
            {
                new JProperty("$schema", "https://raw.githubusercontent.com/microsoft/CDM/master/docs/schema/modeljsonschema.json"),
                new JProperty("application", "appName"),
                new JProperty("name", "OrdersProducts"),
                new JProperty("description", "Model containing data for Order and Products."),
                new JProperty("version", "1.0"),
                //new JProperty("culture", "da-DK"),  //Excluded to follow internatinal rule of dot as seperator in decimal numbers and the use os ISO 8601 for dettime standards
                new JProperty("modifiedTime", DateTime.UtcNow.ToString("s"))
            };

            var entities = new JArray();
            foreach (var refine in Refines)
                CreateEntity(ref entities, refine);

            res.Add(new JProperty("entities", entities));

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

        internal void CreateEntity(ref JArray entities, RefineBase refine)
        {
            var entity = new JObject
            {
                new JProperty("$type", "LocalEntity"),
                new JProperty("name", refine.TableName)
            };

            var attributes = new JArray();
            foreach (var item in refine.CsvSet.Headers)
            {
                var attribute = new JObject
                {
                    new JProperty("name", item.Value.ToString()),
                    new JProperty("dataType", GetCommonDataModelType(refine.CsvSet.ColTypes[item.Key]))
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

            var path = string.Join('/', dataLake.ServiceUri.ToString().TrimEnd('/'), dataLake.BasePath, dataLake.BaseDirectory, refine.UploadAsDecodedPath);
            var urlPath = HttpUtility.UrlPathEncode(path);
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
