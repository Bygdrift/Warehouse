using Bygdrift.CsvTools;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Bygdrift.Warehouse.MssqlTools.Models
{
    /// <summary></summary>
    public class ColumnType
    {
        /// <summary></summary>
        public ColumnType(string name)
        {
            Name = name;
        }

        /// <summary> </summary>
        public bool IsPrimaryKeyCsv { get; private set; }

        /// <summary> </summary>
        public bool IsPrimaryKeySql { get; private set; }

        /// <summary> </summary>
        public bool IsSetForCsv { get; set; }

        /// <summary> </summary>
        public bool IsSetForSql { get; internal set; }

        /// <summary> </summary>
        public int MaxLengthCsv { get; set; }

        /// <summary>
        /// The max length
        /// </summary>
        public int? MaxLengthSql { get; set; }

        /// <summary>
        /// The name of the column
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// like 'varchar' of 'varchar(8) 
        /// </summary>
        public SqlType? TypeNameCsv { get; set; }

        /// <summary>
        /// like 'varchar' of 'varchar(8) 
        /// </summary>
        public SqlType? TypeNameSql { get; set; }

        /// <summary>
        /// Add a ColumnType from the database
        /// </summary>
        /// <param name="sqlTypeName"></param>
        /// <param name="sqlMaxLength"></param>
        /// <param name="sqlIsPrimaryKey"></param>
        /// <returns></returns>
        public ColumnType AddSql(string sqlTypeName, int? sqlMaxLength, bool sqlIsPrimaryKey)
        {
            IsSetForSql = true;
            TypeNameSql = GetSqlTypeName(sqlTypeName);  //Must be set before MaxLength
            MaxLengthSql = sqlMaxLength;
            IsPrimaryKeySql = sqlIsPrimaryKey;
            return this;
        }

        /// <summary>
        /// Add the columntype from the CSV
        /// </summary>
        /// <param name="csvType"></param>
        /// <param name="csvMaxLength"></param>
        /// <param name="csvIsPrimaryKey"></param>
        /// <returns></returns>
        public ColumnType AddCsv(Type csvType, int csvMaxLength, bool csvIsPrimaryKey)
        {
            IsSetForCsv = true;
            TypeNameCsv = GetCsvToSqlTypeName(csvType);
            MaxLengthCsv = csvMaxLength;
            IsPrimaryKeyCsv = csvIsPrimaryKey;
            return this;
        }

        private SqlType GetSqlTypeName(string type)
        {
            if (Enum.TryParse(typeof(SqlType), type.ToLower(), out object res))
                return (SqlType)res;

            throw new NotImplementedException();
        }

        /// <summary>
        /// Wether the TypeExpression should be updated. If so, typeExpression will retrun the result
        /// </summary>
        /// <param name="typeExpression"></param>
        public bool TryGetUpdatedChangedType(out string typeExpression)
        {
            var TypeExpressionCsv = IsSetForCsv ? TypeNameCsv + GetTypeExtension((SqlType)TypeNameCsv, MaxLengthCsv) : null;
            var TypeExpressionSql = IsSetForSql ? TypeNameSql + GetTypeExtension((SqlType)TypeNameSql, MaxLengthSql) : null;
            typeExpression = TypeExpressionSql ?? TypeExpressionCsv;
            var isChanged = false;

            if (IsSetForCsv != IsSetForSql || TypeExpressionCsv.Equals(TypeExpressionSql))  //If not update or no changes
                return false;

            if (TypeNameSql == SqlType.bit)
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
            if (TypeNameSql == SqlType.smallint)
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
            if (TypeNameSql == SqlType.@int)
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
            if (TypeNameSql == SqlType.bigint)
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
            if (TypeNameSql == SqlType.real)
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
            if (TypeNameSql == SqlType.@float)
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
            if (TypeNameSql == SqlType.datetime)
            {
                if (TryGetTypeExpression(SqlType.datetime, false, ref typeExpression, ref isChanged)) return isChanged;
            }

            var maxLength = MaxLengthCsv > MaxLengthSql || MaxLengthSql == null ? MaxLengthCsv : MaxLengthSql;
            typeExpression = "varchar" + GetTypeExtension(SqlType.varchar, maxLength);
            return true;
        }

        internal string GetTypeExtension(SqlType type, int? maxLength)
        {
            return type switch
            {
                SqlType.@decimal => "(18,12)",
                SqlType.varchar => maxLength > 8000 ? $"(MAX)" : $"({(maxLength == 0 ? 1 : maxLength)})",
                _ => ""
            };
        }

        private bool TryGetTypeExpression(SqlType csvTypeName, bool doChange, ref string typeExpression, ref bool isChanged)
        {
            if (TypeNameCsv != csvTypeName)
                return false;

            if (TypeNameCsv == csvTypeName && !doChange)
            {
                isChanged = false;
                return true;
            }

            isChanged = true;
            var maxLength = MaxLengthCsv > MaxLengthSql || MaxLengthSql == null ? MaxLengthCsv : MaxLengthSql;
            typeExpression = TypeNameCsv + GetTypeExtension(csvTypeName, maxLength);
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
                _ => throw new NotImplementedException()
            };
        }
    }
}
