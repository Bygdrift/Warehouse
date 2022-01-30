﻿using Microsoft.Extensions.Configuration;
using System;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Bygdrift.Warehouse.Helpers.Attributes
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
        /// Throws an error and a logError if the property is missing
        /// </summary>
        public NotSet NotSet { get; set; }

        /// <summary>
        ///  Gets or sets the name of the property.
        /// </summary>
        public string PropertyName { get; set; }

        internal void GetData<TSettings>(AppBase app, PropertyInfo prop, TSettings settings)
        {
            var value = app.Config.GetValue<object>("setting--" + PropertyName);
            if (value == null)
            {
                if (NotSet == NotSet.ThrowError)
                    throw new Exception($"App setting 'settings--{prop.Name}' has not been set and is vital to continue. {ErrorMessage}");

                if (NotSet == NotSet.ShowLogError)
                    app.Log.LogError($"App setting 'settings--{prop.Name}' has not been set. {ErrorMessage}");

                if (NotSet == NotSet.ShowLogInfo)
                    app.Log.LogInformation($"App setting 'settings--{prop.Name}' has not been set. {ErrorMessage}");

                if (NotSet == NotSet.ShowLogWarning)
                    app.Log.LogWarning($"App setting 'settings--{prop.Name}' has not been set. {ErrorMessage}");

                if (Default != null && CsvTools.Csv.RecordToType(prop.PropertyType, Default, out object res))
                    prop.SetValue(settings, res);
            }
            else if (CsvTools.Csv.RecordToType(prop.PropertyType, value, out object res))
                prop.SetValue(settings, res);
        }
    }
}
