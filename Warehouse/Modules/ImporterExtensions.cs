namespace Bygdrift.Warehouse.Modules
{
    public static class ImporterExtensions
    {
        public static ImportResult Run(this IImporter exporter, bool uploadToDataLake)
        {
            var result = new ImportResult();
            result.AppSettingsOk = exporter.VerifyAppSettings();

            if (result.AppSettingsOk)
            {
                var refines = exporter.Import(uploadToDataLake);
                if (refines != null)
                {
                    result.Refines.AddRange(refines);
                    result.CMDModel = exporter.CreateCommonDataModel(result.Refines, uploadToDataLake);
                    result.ImportLog = exporter.CreateImportLog(result.Refines, uploadToDataLake);
                }
            }
            return result;
        }
    }
}