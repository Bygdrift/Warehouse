using System.Collections.Generic;

namespace Bygdrift.Warehouse.DataLake.CsvTools
{
    public class UpdateMethod
    {
        public CsvSet CsvToUpdate { get; private set; }
        private Dictionary<string, int> origKeys;
        private readonly string[] headers;

        public UpdateMethod(CsvSet csvToUpdate, params string[] headers)
        {
            CsvToUpdate = csvToUpdate;
            this.headers = headers;
            origKeys = CsvToUpdate.GetCompositeKeys(headers);
        }

        public void Merge(CsvSet csvToMerge)
        {
            if (csvToMerge == null)
                return;

            if(CsvToUpdate == null)
            {
                CsvToUpdate = csvToMerge;
                origKeys = CsvToUpdate.GetCompositeKeys(headers);
                return;
            }

            var mergeKeys = csvToMerge.GetCompositeKeys(headers);
            foreach (var item in mergeKeys)
                if (!origKeys.ContainsKey(item.Key))
                {
                    var row = csvToMerge.GetRecordRow(item.Value);
                    CsvToUpdate.AddRow(row);
                }
        }
    }
}
