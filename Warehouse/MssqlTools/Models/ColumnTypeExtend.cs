using System;

namespace Bygdrift.Warehouse.MssqlTools.Models
{
    /// <summary>
    /// 
    /// </summary>
    public class ColumnTypeExtend : ColumnType
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnType"></param>
        public ColumnTypeExtend(ColumnType columnType)
        {
            Name = columnType.Name;
            SqlIsSet = true;
            SqlTypeName = columnType.SqlTypeName;  //Must be set before MaxLength
            SqlMaxLength = columnType.SqlMaxLength;
            SqlIsPrimaryKey = columnType.SqlIsPrimaryKey;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="csvType"></param>
        /// <param name="csvMaxLength"></param>
        /// <param name="csvIsPrimaryKey"></param>
        public ColumnTypeExtend(string name, Type csvType, int csvMaxLength, bool csvIsPrimaryKey)
        {
            Name = name;
            CsvIsSet = true;
            CsvTypeName = GetCsvToSqlTypeName(csvType);
            CsvMaxLength = csvMaxLength;
            CsvIsPrimaryKey = csvIsPrimaryKey;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnType"></param>
        /// <param name="csvType"></param>
        /// <param name="csvMaxLength"></param>
        /// <param name="csvIsPrimaryKey"></param>
        public ColumnTypeExtend(ColumnType columnType, Type csvType, int csvMaxLength, bool csvIsPrimaryKey)
        {
            Name = columnType.Name;
            SqlIsSet = true;
            SqlTypeName = columnType.SqlTypeName;  //Must be set before MaxLength
            SqlMaxLength = columnType.SqlMaxLength;
            SqlIsPrimaryKey = columnType.SqlIsPrimaryKey;
            CsvIsSet = true;
            CsvTypeName = GetCsvToSqlTypeName(csvType);
            CsvMaxLength = csvMaxLength;
            CsvIsPrimaryKey = csvIsPrimaryKey;
        }

        ///// <summary>
        ///// The name of the column
        ///// </summary>
        //public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool CsvIsSet { get; }

        /// <summary>
        /// like 'varchar' of 'varchar(8) 
        /// </summary>
        public SqlType CsvTypeName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int? CsvMaxLength { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool CsvIsPrimaryKey { get; private set; }

        /// <summary>
        /// Like '(8)' of 'varchar(8) 
        /// </summary>
        public string CsvTypeExtension { get { return GetTypeExtension(CsvTypeName, CsvMaxLength); } }

        /// <summary>
        /// Like 'varchar(8)'
        /// </summary>
        public string CsvTypeExpression { get { return CsvTypeName + CsvTypeExtension; } }

        /// <summary>
        /// Wether the TypeExpression should be updated. If so, typeExpression will retrun the result
        /// </summary>
        /// <param name="typeExpression"></param>
        public bool TryGetUpdatedChangedType(out string typeExpression)
        {
            typeExpression = SqlTypeExpression;
            var isChanged = false;

            if (!(CsvIsSet && SqlIsSet) || CsvTypeExpression.Equals(SqlTypeExpression))  //If not update or no changes
                return false;
            
            if (SqlTypeName == SqlType.bit)
            {
                if (TryGetTypeExpression(SqlType.bit, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.binary, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.smallint, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@int, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.bigint, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.real, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@float, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@decimal, true, ref typeExpression, ref isChanged)) return isChanged;
            }
            if (SqlTypeName == SqlType.smallint)
            {
                if (TryGetTypeExpression(SqlType.bit, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.binary, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.smallint, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@int, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.bigint, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.real, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@float, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@decimal, true, ref typeExpression, ref isChanged)) return isChanged;
            }
            if (SqlTypeName == SqlType.@int)
            {
                if (TryGetTypeExpression(SqlType.bit, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.binary, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.smallint, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@int, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.bigint, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.real, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@float, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@decimal, true, ref typeExpression, ref isChanged)) return isChanged;
            }
            if (SqlTypeName == SqlType.bigint)
            {
                if (TryGetTypeExpression(SqlType.bit, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.binary, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.smallint, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@int, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.bigint, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.real, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@float, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@decimal, true, ref typeExpression, ref isChanged)) return isChanged;
            }
            if (SqlTypeName == SqlType.real)
            {
                if (TryGetTypeExpression(SqlType.bit, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.binary, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.smallint, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@int, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.bigint, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.real, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@float, true, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@decimal, true, ref typeExpression, ref isChanged)) return isChanged;
            }
            if (SqlTypeName == SqlType.@float)
            {
                if (TryGetTypeExpression(SqlType.bit, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.binary, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.smallint, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@int, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.bigint, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.real, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@float, false, ref typeExpression, ref isChanged)) return isChanged;
                if (TryGetTypeExpression(SqlType.@decimal, true, ref typeExpression, ref isChanged)) return isChanged;
            }
            if (SqlTypeName == SqlType.datetime)
            {
                if (TryGetTypeExpression(SqlType.datetime, false, ref typeExpression, ref isChanged)) return isChanged;
            }

            var maxLength = CsvMaxLength > SqlMaxLength || SqlMaxLength == null ? CsvMaxLength : SqlMaxLength;
            typeExpression = GetTypeExtension(SqlType.varchar, maxLength);
            return true;
        }

        private bool TryGetTypeExpression(SqlType csvTypeName, bool doChange, ref string typeExpression, ref bool isChanged)
        {
            if (CsvTypeName != csvTypeName)
                return false;

            if (CsvTypeName == csvTypeName && !doChange)
            {
                isChanged = false;
                return true;
            }

            isChanged = true;
            var maxLength = CsvMaxLength > SqlMaxLength || SqlMaxLength == null ? CsvMaxLength : SqlMaxLength;
            typeExpression = CsvTypeName.ToString() + GetTypeExtension(csvTypeName, maxLength);
            return true;
        }

        private SqlType GetCsvToSqlTypeName(Type type)
        {
            return Type.GetTypeCode(type) switch  //Remember to keep theses returns lowercase
            {
                TypeCode.Int64 => SqlType.bigint,
                TypeCode.Object => SqlType.varchar,
                TypeCode.Boolean => SqlType.bit,
                TypeCode.Char => SqlType.varchar,
                TypeCode.SByte => SqlType.binary,
                TypeCode.Byte => SqlType.binary,
                TypeCode.Int16 => SqlType.smallint,
                TypeCode.UInt16 => SqlType.smallint,
                TypeCode.Int32 => SqlType.@int,
                TypeCode.UInt32 => SqlType.@int,
                TypeCode.UInt64 => SqlType.bigint,
                TypeCode.Single => SqlType.real,
                TypeCode.Double => SqlType.@float,
                TypeCode.Decimal => SqlType.@decimal,
                TypeCode.DateTime => SqlType.datetime,
                TypeCode.String => SqlType.varchar,
                TypeCode.Empty => SqlType.varchar,
                TypeCode.DBNull => SqlType.varchar,
                _=> throw new NotImplementedException()
            };
        }

        private string GetTypeExtension(SqlType type, int? maxLength)
        {
            return type switch
            {
                SqlType.@decimal => "(18,12)",
                SqlType.varchar => CsvMaxLength > 8000 ? $"(MAX)" : $"({(maxLength == 0 ? 1 : maxLength)})",
                _ => ""
            };
        }
    }
}
