using System;

namespace Bygdrift.Warehouse.MssqlTools.Models
{
    /// <summary></summary>
    public class ColumnType
    {
        /// <summary></summary>
        public ColumnType() { }

        /// <summary></summary>
        public ColumnType(string name, string sqlTypeName, int? sqlMaxLength, bool sqlIsPrimaryKey)
        {
            Name = name;
            SqlIsSet = true;
            SqlTypeName = GetSqlTypeName(sqlTypeName);  //Must be set before MaxLength
            SqlMaxLength = sqlMaxLength;
            SqlIsPrimaryKey = sqlIsPrimaryKey;
        }

        /// <summary>
        /// The name of the column
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool SqlIsSet { get; internal set; }

        /// <summary>
        /// like 'varchar' of 'varchar(8) 
        /// </summary>
        public SqlType SqlTypeName { get; set; }

        /// <summary>
        /// The max length
        /// </summary>
        public int? SqlMaxLength { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool SqlIsPrimaryKey { get; internal set; }

        /// <summary>
        /// Like '(8)' of 'varchar(8) 
        /// </summary>
        public string SqlTypeExtension { get { return GetTypeExtension(SqlTypeName, SqlMaxLength); } }

        /// <summary>
        /// Like 'varchar(8)'
        /// </summary>
        public string SqlTypeExpression { get { return SqlTypeName + SqlTypeExtension; } }

        private SqlType GetSqlTypeName(string type)
        {
            if (Enum.TryParse(typeof(SqlType), type.ToLower(), out object res))
                return (SqlType)res;

            throw new NotImplementedException();
        }

        private string GetTypeExtension(SqlType type, int? maxLength)
        {
            return type switch
            {
                SqlType.@decimal => "(18,12)",
                SqlType.varchar => SqlMaxLength > 8000 ? $"(MAX)" : $"({maxLength})",
                _ => ""
            };
        }
    }
}
