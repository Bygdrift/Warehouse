using Bygdrift.Tools.CsvTool;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Bygdrift.Warehouse.Attributes
{
    /// <summary>
    /// Loads data from AppSettings with the same name as the propertis in thes class. Case is ignored, so it can be written in camelCase and PascalCase.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ConfigSetting : Attribute
    {
        /// <summary>
        /// Loads data from AppSettings with the same name as the propertis in thes class. Case is ignored, so it can be written in camelCase and PascalCase.
        /// </summary>
        /// <param name="propertyName">The proertyName. Can be overwritten or is equal the current property</param>
        public ConfigSetting([CallerMemberName] string propertyName = "")
        {
            PropertyName = propertyName;
            NotSet = NotSet.DoNothing;
        }

        /// <summary>
        /// If there is no setting, then use this default value
        /// </summary>
        public object Default { get; set; }

        /// <summary>
        /// There will come a standard message that explains what happend and it can be combined can be combined with this message.
        /// </summary>
        public string ErrorMessage { get; set; }

        /// <summary>
        /// If true, it will read the text as json and save it as the property
        /// </summary>
        public bool IsJson { get; set; }

        /// <summary>
        /// Throws an error and a logError if the property is missing
        /// </summary>
        public NotSet NotSet { get; set; }

        /// <summary>
        ///  Gets or sets the name of the property.
        /// </summary>
        public string PropertyName { get; set; }

        internal void GetData<TSettings>(AppBase app, PropertyInfo prop, TSettings settings)
        {
            //if (prop.PropertyType.IsArray)
            //{
            //    if (prop.PropertyType.GetElementType() == typeof(string))
            //    {
            //        var res = app.Config.GetSection(PropertyName).GetChildren().Select(o => o.Value).ToArray();
            //        if(res.Length > 0)
            //            prop.SetValue(settings, res);
            //        else
            //            PropertIsNull(app, prop, settings);
            //    }
            //    else
            //        throw new InvalidOperationException($"The setting {prop.Name} is an array. It can only be a string[] in this version of Warehouse.");
            //}

            var value = app.Config.GetValue(IsJson ? typeof(string) : prop.PropertyType, PropertyName);

            if (value == null)
                value = PropertIsNull(app, prop, settings);

            if (IsJson && value != null)
                value = JsonConvert.DeserializeObject((string)value, prop.PropertyType);

            prop.SetValue(settings, value);
        }

        private object PropertIsNull<TSettings>(AppBase app, PropertyInfo property, TSettings settings)
        {
            //This warning can be removed June 2022:
            var settingValue = app.Config.GetValue(property.PropertyType, "Setting--" + PropertyName);
            if (settingValue != null)
            {
                app.Log.LogError($"With the new update of Bygdrift Warehouse, you have to remove all prefixes of 'Setting--' in the current modules cofiguration'. The module will continue to function until June 2022, where this error will stop the execution.");
                property.SetValue(settings, settingValue);
                return null;
            }

            if (NotSet == NotSet.ThrowError)
                throw new Exception($"App setting '{property.Name}' has not been set and is vital to continue. {ErrorMessage}");

            if (NotSet == NotSet.ShowLogError)
                app.Log.LogError($"App setting '{property.Name}' has not been set. {ErrorMessage}");

            if (NotSet == NotSet.ShowLogInfo)
                app.Log.LogInformation($"App setting '{property.Name}' has not been set. {ErrorMessage}");

            if (NotSet == NotSet.ShowLogWarning)
                app.Log.LogWarning($"App setting '{property.Name}' has not been set. {ErrorMessage}");

            if (Default != null && new Csv().RecordToType(IsJson ? typeof(string) : property.PropertyType, Default, out object res))
                return res;

            return null;
        }
    }
}
