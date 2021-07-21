namespace Bygdrift.Warehouse.Modules
{
    public static class ExporterExtensions
    {
        public static ExportResult Run(this IExporter exporter, bool uploadToDataLake)
        {
            var result = new ExportResult();
            result.AppSettingsOk = exporter.VerifyAppSettings();

            if (result.AppSettingsOk)
            {
                var refines = exporter.Export(uploadToDataLake);
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