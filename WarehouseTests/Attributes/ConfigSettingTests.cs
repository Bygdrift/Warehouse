using Bygdrift.Warehouse;
using Bygdrift.Warehouse.Attributes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace WarehouseTests.Attributes
{
    [TestClass]
    public class ConfigSettingTests
    {
        [TestMethod]
        public void MissingVitalSetting()
        {
            try
            {
                var app = new AppBase<Settings_ThrowError>();
                Assert.Fail("No exception thrown");
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "App setting 'IsMissing_HasMissingAttribute' has not been set and is vital to continue. Thats not good");
            }
        }

        [TestMethod]
        public void MissingNonVitalSetting()
        {
            var app = new AppBase<Settings_ThrowLogError>();
            var errors = app.Log.GetErrorsAndCriticals().ToList();
            Assert.AreEqual(1, errors.Count);
            Assert.AreEqual(errors.First(), "App setting 'IsMissing_HasNoMissingAttribute' has not been set. Thats not good");
            Assert.AreEqual(7, app.Settings.IsMissing_HasNoMissingAttribute);  //The default value on '7' is used
        }

        [TestMethod]
        public void ReadSecret()
        {
            var app = new AppBase<Settings_ReadSecret>();
            var a = app.Settings.SecretTest;
            var errors = app.Log.GetErrorsAndCriticals().ToList();
            //Assert.AreEqual(1, errors.Count);
            //Assert.AreEqual(errors.First(), "App setting 'IsMissing_HasNoMissingAttribute' has not been set. Thats not good");
            Assert.AreEqual(8, app.Settings.SecretTest);
        }

        [TestMethod]
        public void ReadJson()
        {
            var app = new AppBase<Settings_ReadJson>();
            var res = app.Settings.TestOperators;
            Assert.IsTrue(res.Count == 2);
        }
    }

    public class Settings_ReadJson
    {
        [ConfigSetting(IsJson = true, Default = "[{\"Id\":1209, \"AreaNumbers\":[3400,2,3]},{\"Id\":1209, \"AreaNumbers\":[3400,2,3]}]")]
        public List<JsonModel> TestOperators { get; set; }
    }

    public class Settings_ThrowLogError
    {
        [ConfigSetting(NotSet = NotSet.ShowLogError, ErrorMessage = "Thats not good", Default = "7")]
        public int IsMissing_HasNoMissingAttribute { get; set; }
    }

    public class Settings_ThrowError
    {
        [ConfigSetting(NotSet = NotSet.ThrowError, ErrorMessage = "Thats not good")]
        public string IsMissing_HasMissingAttribute { get; set; }
    }

    public class Settings_ReadSecret
    {
        [ConfigSecret(NotSet = NotSet.DoNothing, Default = "7")]
        public int SecretTest { get; set; }
    }

    public class JsonModel
    {
        public int Id { get; set; }

        public int[] AreaNumbers { get; set; }
    }
}