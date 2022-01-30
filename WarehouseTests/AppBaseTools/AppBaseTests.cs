using Bygdrift.Warehouse;
using Bygdrift.Warehouse.Helpers.Attributes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace WarehouseTests
{
    [TestClass]
    public class AppBaseTests
    {
        [TestMethod]
        public void LoadTestConfigSecretGet()
        {
            var app = new AppBase<TestConfigSecretGet>();
            var errors = app.Log.GetErrorsAndCriticals().ToList();
            Assert.IsTrue(!string.IsNullOrEmpty(app.Settings.MssqlConnectionString));
            Assert.IsTrue(!errors.Any());
        }

        [TestMethod]
        public void LoadSettings_MissingVitalSetting()
        {
            try
            {
                var app = new AppBase<TestConfigSettingsThrowError>();
                Assert.Fail("No exception thrown");
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "App setting 'Warehouse:Settings:IsMissing_HasMissingAttribute' has not been set and is vital to continue. Thats not good");
            }
        }

        [TestMethod]
        public void LoadSettings_MissingNonVitalSetting()
        {
            var app = new AppBase<TestConfigSettingsThrowLogError>();
            var errors = app.Log.GetErrorsAndCriticals().ToList();
            Assert.AreEqual(1, errors.Count);
            Assert.AreEqual(errors.First(), "App setting 'Warehouse:Settings:IsMissing_HasNoMissingAttribute' has not been set. Thats not good");
            Assert.AreEqual(7, app.Settings.IsMissing_HasNoMissingAttribute);  //The default value on '7' is used
        }
    }

    public class TestConfigSecretGet
    {
        [ConfigSecret(NotSet = NotSet.ShowLogError)]
        public string MssqlConnectionString { get; set; }
    }

    public class TestConfigSettingsThrowLogError
    {
        [ConfigSetting(NotSet = NotSet.ShowLogError, ErrorMessage = "Thats not good", Default = "7")]
        public int IsMissing_HasNoMissingAttribute { get; set; }
    }

    public class TestConfigSettingsThrowError
    {
        [ConfigSetting(NotSet = NotSet.ThrowError, ErrorMessage = "Thats not good")]
        public string IsMissing_HasMissingAttribute { get; set; }
    }
}