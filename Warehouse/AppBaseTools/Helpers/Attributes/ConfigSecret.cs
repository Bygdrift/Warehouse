using Microsoft.Extensions.Configuration;
using System;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Bygdrift.Warehouse.Helpers.Attributes
{
    /// <summary>
    /// Loads data from AppSettings with the same name as the propertis in thes class. Case is ignored, so it can be written in camelCase and PascalCase.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ConfigSecret : Attribute
    {
        /// <summary>
        /// Loads data from AppSettings with the same name as the propertis in thes class. Case is ignored, so it can be written in camelCase and PascalCase.
        /// </summary>
        /// <param name="propertyName">The proertyName. Can be overwritten or is equal the current property</param>
        public ConfigSecret([CallerMemberName] string propertyName = "")
        {
            PropertyName = propertyName;
            if (!PropertyName.All(o => char.IsLetterOrDigit(o)) || propertyName.Length > 93)  //max Characters: 127 max - 24 in modulename - 4 dashes = 93.
                throw new Exception($"The name on the Secret setting '{propertyName}', must only contain letters and numbers and be no longer than 93 characters to be saved to a key vault.");

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
            var secretName = $"Secret--{app.ModuleName}--{prop.Name}";
            var secret = app.KeyVault.GetSecret(secretName);
            if (secret == null)
            {
                if (NotSet == NotSet.ThrowError)
                    throw new Exception($"Key vault secret '{secretName}' has not been set and is vital to continue. {ErrorMessage}");

                if (NotSet == NotSet.ShowLogError)
                    app.Log.LogError($"Key vault secret '{secretName}' has not been set. {ErrorMessage}");

                if (NotSet == NotSet.ShowLogInfo)
                    app.Log.LogInformation($"Key vault secret '{secretName}' has not been set. {ErrorMessage}");

                if (NotSet == NotSet.ShowLogWarning)
                    app.Log.LogWarning($"Key vault secret '{secretName}' has not been set. {ErrorMessage}");

                if (Default != null && Bygdrift.CsvTools.Csv.RecordToType(prop.PropertyType, Default, out object res))
                    prop.SetValue(settings, res);
            }
            else if (Bygdrift.CsvTools.Csv.RecordToType(prop.PropertyType, secret, out object res))
                prop.SetValue(settings, res);
        }
    }
}
