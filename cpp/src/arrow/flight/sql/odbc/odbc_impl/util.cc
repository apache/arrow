// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/flight/sql/odbc/odbc_impl/util.h"

#include "arrow/flight/sql/odbc/odbc_impl/calendar_utils.h"
#include "arrow/flight/sql/odbc/odbc_impl/encoding.h"
#include "arrow/flight/sql/odbc/odbc_impl/platform.h"
#include "arrow/flight/sql/odbc/odbc_impl/types.h"

#include "arrow/builder.h"
#include "arrow/compute/api.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

#include "arrow/flight/sql/odbc/odbc_impl/json_converter.h"

#include <boost/tokenizer.hpp>

#include <ctime>
#include <sstream>

namespace arrow::flight::sql::odbc {
namespace util {
namespace {
bool IsComplexType(Type::type type_id) {
  switch (type_id) {
    case Type::LIST:
    case Type::LARGE_LIST:
    case Type::FIXED_SIZE_LIST:
    case Type::MAP:
    case Type::STRUCT:
      return true;
    default:
      return false;
  }
}

SqlDataType GetDefaultSqlCharType(bool use_wide_char) {
  return use_wide_char ? SqlDataType_WCHAR : SqlDataType_CHAR;
}
SqlDataType GetDefaultSqlVarcharType(bool use_wide_char) {
  return use_wide_char ? SqlDataType_WVARCHAR : SqlDataType_VARCHAR;
}
SqlDataType GetDefaultSqlLongVarcharType(bool use_wide_char) {
  return use_wide_char ? SqlDataType_WLONGVARCHAR : SqlDataType_LONGVARCHAR;
}
CDataType GetDefaultCCharType(bool use_wide_char) {
  return use_wide_char ? CDataType_WCHAR : CDataType_CHAR;
}

}  // namespace

using std::make_optional;
using std::nullopt;

/// \brief Returns the mapping from Arrow type to SqlDataType
/// \param field the field to return the SqlDataType for
/// \return the concise SqlDataType for the field.
/// \note use GetNonConciseDataType on the output to get the verbose type
/// \note the concise and verbose types are the same for all but types relating to times
/// and intervals
SqlDataType GetDataTypeFromArrowFieldV3(const std::shared_ptr<Field>& field,
                                        bool use_wide_char) {
  const std::shared_ptr<DataType>& type = field->type();

  switch (type->id()) {
    case Type::BOOL:
      return SqlDataType_BIT;
    case Type::UINT8:
    case Type::INT8:
      return SqlDataType_TINYINT;
    case Type::UINT16:
    case Type::INT16:
      return SqlDataType_SMALLINT;
    case Type::UINT32:
    case Type::INT32:
      return SqlDataType_INTEGER;
    case Type::UINT64:
    case Type::INT64:
      return SqlDataType_BIGINT;
    case Type::HALF_FLOAT:
    case Type::FLOAT:
      return SqlDataType_FLOAT;
    case Type::DOUBLE:
      return SqlDataType_DOUBLE;
    case Type::BINARY:
    case Type::FIXED_SIZE_BINARY:
    case Type::LARGE_BINARY:
      return SqlDataType_BINARY;
    case Type::STRING:
    case Type::LARGE_STRING:
      return GetDefaultSqlVarcharType(use_wide_char);
    case Type::DATE32:
    case Type::DATE64:
      return SqlDataType_TYPE_DATE;
    case Type::TIMESTAMP:
      return SqlDataType_TYPE_TIMESTAMP;
    case Type::DECIMAL128:
      return SqlDataType_DECIMAL;
    case Type::TIME32:
    case Type::TIME64:
      return SqlDataType_TYPE_TIME;
    case Type::INTERVAL_MONTHS:
      return SqlDataType_INTERVAL_MONTH;  // GH-47873 TODO: check and update to
                                          // SqlDataType_INTERVAL_YEAR_TO_MONTH if it is
                                          // more appropriate
    case Type::INTERVAL_DAY_TIME:
      return SqlDataType_INTERVAL_DAY;

    // GH-47873 TODO: Handle remaining types.
    case Type::INTERVAL_MONTH_DAY_NANO:
    case Type::LIST:
    case Type::STRUCT:
    case Type::SPARSE_UNION:
    case Type::DENSE_UNION:
    case Type::DICTIONARY:
    case Type::MAP:
    case Type::EXTENSION:
    case Type::FIXED_SIZE_LIST:
    case Type::DURATION:
    case Type::LARGE_LIST:
    case Type::MAX_ID:
    case Type::NA:
      break;
  }

  return GetDefaultSqlVarcharType(use_wide_char);
}

SqlDataType EnsureRightSqlCharType(SqlDataType data_type, bool use_wide_char) {
  switch (data_type) {
    case SqlDataType_CHAR:
    case SqlDataType_WCHAR:
      return GetDefaultSqlCharType(use_wide_char);
    case SqlDataType_VARCHAR:
    case SqlDataType_WVARCHAR:
      return GetDefaultSqlVarcharType(use_wide_char);
    case SqlDataType_LONGVARCHAR:
    case SqlDataType_WLONGVARCHAR:
      return GetDefaultSqlLongVarcharType(use_wide_char);
    default:
      return data_type;
  }
}

int16_t ConvertSqlDataTypeFromV3ToV2(int16_t data_type_v3) {
  switch (data_type_v3) {
    case SqlDataType_TYPE_DATE:
      return 9;  // Same as SQL_DATE from sqlext.h
    case SqlDataType_TYPE_TIME:
      return 10;  // Same as SQL_TIME from sqlext.h
    case SqlDataType_TYPE_TIMESTAMP:
      return 11;  // Same as SQL_TIMESTAMP from sqlext.h
    default:
      return data_type_v3;
  }
}

CDataType ConvertCDataTypeFromV2ToV3(int16_t data_type_v2) {
  switch (data_type_v2) {
    case -6:  // Same as SQL_C_TINYINT from sqlext.h
      return CDataType_STINYINT;
    case 4:  // Same as SQL_C_LONG from sqlext.h
      return CDataType_SLONG;
    case 5:  // Same as SQL_C_SHORT from sqlext.h
      return CDataType_SSHORT;
    case 7:  // Same as SQL_C_FLOAT from sqlext.h
      return CDataType_FLOAT;
    case 8:  // Same as SQL_C_DOUBLE from sqlext.h
      return CDataType_DOUBLE;
    case 9:  // Same as SQL_C_DATE from sqlext.h
      return CDataType_DATE;
    case 10:  // Same as SQL_C_TIME from sqlext.h
      return CDataType_TIME;
    case 11:  // Same as SQL_C_TIMESTAMP from sqlext.h
      return CDataType_TIMESTAMP;
    default:
      return static_cast<CDataType>(data_type_v2);
  }
}

std::string GetTypeNameFromSqlDataType(int16_t data_type) {
  switch (data_type) {
    case SqlDataType_CHAR:
      return "CHAR";
    case SqlDataType_VARCHAR:
      return "VARCHAR";
    case SqlDataType_LONGVARCHAR:
      return "LONGVARCHAR";
    case SqlDataType_WCHAR:
      return "WCHAR";
    case SqlDataType_WVARCHAR:
      return "WVARCHAR";
    case SqlDataType_WLONGVARCHAR:
      return "WLONGVARCHAR";
    case SqlDataType_DECIMAL:
      return "DECIMAL";
    case SqlDataType_NUMERIC:
      return "NUMERIC";
    case SqlDataType_SMALLINT:
      return "SMALLINT";
    case SqlDataType_INTEGER:
      return "INTEGER";
    case SqlDataType_REAL:
      return "REAL";
    case SqlDataType_FLOAT:
      return "FLOAT";
    case SqlDataType_DOUBLE:
      return "DOUBLE";
    case SqlDataType_BIT:
      return "BIT";
    case SqlDataType_TINYINT:
      return "TINYINT";
    case SqlDataType_BIGINT:
      return "BIGINT";
    case SqlDataType_BINARY:
      return "BINARY";
    case SqlDataType_VARBINARY:
      return "VARBINARY";
    case SqlDataType_LONGVARBINARY:
      return "LONGVARBINARY";
    case SqlDataType_TYPE_DATE:
    case 9:
      return "DATE";
    case SqlDataType_TYPE_TIME:
    case 10:
      return "TIME";
    case SqlDataType_TYPE_TIMESTAMP:
    case 11:
      return "TIMESTAMP";
    case SqlDataType_INTERVAL_MONTH:
      return "INTERVAL_MONTH";
    case SqlDataType_INTERVAL_YEAR:
      return "INTERVAL_YEAR";
    case SqlDataType_INTERVAL_YEAR_TO_MONTH:
      return "INTERVAL_YEAR_TO_MONTH";
    case SqlDataType_INTERVAL_DAY:
      return "INTERVAL_DAY";
    case SqlDataType_INTERVAL_HOUR:
      return "INTERVAL_HOUR";
    case SqlDataType_INTERVAL_MINUTE:
      return "INTERVAL_MINUTE";
    case SqlDataType_INTERVAL_SECOND:
      return "INTERVAL_SECOND";
    case SqlDataType_INTERVAL_DAY_TO_HOUR:
      return "INTERVAL_DAY_TO_HOUR";
    case SqlDataType_INTERVAL_DAY_TO_MINUTE:
      return "INTERVAL_DAY_TO_MINUTE";
    case SqlDataType_INTERVAL_DAY_TO_SECOND:
      return "INTERVAL_DAY_TO_SECOND";
    case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
      return "INTERVAL_HOUR_TO_MINUTE";
    case SqlDataType_INTERVAL_HOUR_TO_SECOND:
      return "INTERVAL_HOUR_TO_SECOND";
    case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return "INTERVAL_MINUTE_TO_SECOND";
    case SqlDataType_GUID:
      return "GUID";
  }

  throw DriverException("Unsupported data type: " + std::to_string(data_type));
}

optional<int16_t> GetRadixFromSqlDataType(SqlDataType data_type) {
  switch (data_type) {
    case SqlDataType_DECIMAL:
    case SqlDataType_NUMERIC:
    case SqlDataType_SMALLINT:
    case SqlDataType_TINYINT:
    case SqlDataType_INTEGER:
    case SqlDataType_BIGINT:
      return 10;
    case SqlDataType_REAL:
    case SqlDataType_FLOAT:
    case SqlDataType_DOUBLE:
      return 2;
    default:
      return std::nullopt;
  }
}

int16_t GetNonConciseDataType(SqlDataType data_type) {
  switch (data_type) {
    case SqlDataType_TYPE_DATE:
    case SqlDataType_TYPE_TIME:
    case SqlDataType_TYPE_TIMESTAMP:
      return 9;  // Same as SQL_DATETIME on sql.h
    case SqlDataType_INTERVAL_YEAR:
    case SqlDataType_INTERVAL_MONTH:
    case SqlDataType_INTERVAL_DAY:
    case SqlDataType_INTERVAL_HOUR:
    case SqlDataType_INTERVAL_MINUTE:
    case SqlDataType_INTERVAL_SECOND:
    case SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case SqlDataType_INTERVAL_DAY_TO_HOUR:
    case SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case SqlDataType_INTERVAL_DAY_TO_SECOND:
    case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return 10;  // Same as SQL_INTERVAL on sqlext.h
    default:
      return data_type;
  }
}

optional<int16_t> GetSqlDateTimeSubCode(SqlDataType data_type) {
  switch (data_type) {
    case SqlDataType_TYPE_DATE:
      return SqlDateTimeSubCode_DATE;
    case SqlDataType_TYPE_TIME:
      return SqlDateTimeSubCode_TIME;
    case SqlDataType_TYPE_TIMESTAMP:
      return SqlDateTimeSubCode_TIMESTAMP;
    case SqlDataType_INTERVAL_YEAR:
      return SqlDateTimeSubCode_YEAR;
    case SqlDataType_INTERVAL_MONTH:
      return SqlDateTimeSubCode_MONTH;
    case SqlDataType_INTERVAL_DAY:
      return SqlDateTimeSubCode_DAY;
    case SqlDataType_INTERVAL_HOUR:
      return SqlDateTimeSubCode_HOUR;
    case SqlDataType_INTERVAL_MINUTE:
      return SqlDateTimeSubCode_MINUTE;
    case SqlDataType_INTERVAL_SECOND:
      return SqlDateTimeSubCode_SECOND;
    case SqlDataType_INTERVAL_YEAR_TO_MONTH:
      return SqlDateTimeSubCode_YEAR_TO_MONTH;
    case SqlDataType_INTERVAL_DAY_TO_HOUR:
      return SqlDateTimeSubCode_DAY_TO_HOUR;
    case SqlDataType_INTERVAL_DAY_TO_MINUTE:
      return SqlDateTimeSubCode_DAY_TO_MINUTE;
    case SqlDataType_INTERVAL_DAY_TO_SECOND:
      return SqlDateTimeSubCode_DAY_TO_SECOND;
    case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
      return SqlDateTimeSubCode_HOUR_TO_MINUTE;
    case SqlDataType_INTERVAL_HOUR_TO_SECOND:
      return SqlDateTimeSubCode_HOUR_TO_SECOND;
    case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return SqlDateTimeSubCode_MINUTE_TO_SECOND;
    default:
      return std::nullopt;
  }
}

optional<int32_t> GetCharOctetLength(SqlDataType data_type,
                                     const arrow::Result<int32_t>& column_size,
                                     const int32_t decimal_precison) {
  switch (data_type) {
    case SqlDataType_BINARY:
    case SqlDataType_VARBINARY:
    case SqlDataType_LONGVARBINARY:
    case SqlDataType_CHAR:
    case SqlDataType_VARCHAR:
    case SqlDataType_LONGVARCHAR:
      if (column_size.ok()) {
        return column_size.ValueOrDie();
      } else {
        return std::nullopt;
      }
    case SqlDataType_WCHAR:
    case SqlDataType_WVARCHAR:
    case SqlDataType_WLONGVARCHAR:
      if (column_size.ok()) {
        return column_size.ValueOrDie() * GetSqlWCharSize();
      } else {
        return std::nullopt;
      }
    case SqlDataType_TINYINT:
    case SqlDataType_BIT:
      return 1;  // The same as sizeof(SQL_C_BIT)
    case SqlDataType_SMALLINT:
      return 2;  // The same as sizeof(SQL_C_SMALLINT)
    case SqlDataType_INTEGER:
      return 4;  // The same as sizeof(SQL_C_INTEGER)
    case SqlDataType_BIGINT:
    case SqlDataType_FLOAT:
    case SqlDataType_DOUBLE:
      return 8;  // The same as sizeof(SQL_C_DOUBLE)
    case SqlDataType_DECIMAL:
    case SqlDataType_NUMERIC:
      return decimal_precison + 2;  // One char for each digit and two extra chars for a
                                    // sign and a decimal point
    case SqlDataType_TYPE_DATE:
    case SqlDataType_TYPE_TIME:
      return 6;  // The same as sizeof(SQL_TIME_STRUCT)
    case SqlDataType_TYPE_TIMESTAMP:
      return 16;  // The same as sizeof(SQL_TIMESTAMP_STRUCT)
    case SqlDataType_INTERVAL_MONTH:
    case SqlDataType_INTERVAL_YEAR:
    case SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case SqlDataType_INTERVAL_DAY:
    case SqlDataType_INTERVAL_HOUR:
    case SqlDataType_INTERVAL_MINUTE:
    case SqlDataType_INTERVAL_SECOND:
    case SqlDataType_INTERVAL_DAY_TO_HOUR:
    case SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case SqlDataType_INTERVAL_DAY_TO_SECOND:
    case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return 34;  // The same as sizeof(SQL_INTERVAL_STRUCT)
    case SqlDataType_GUID:
      return 16;
    default:
      return std::nullopt;
  }
}
optional<int32_t> GetTypeScale(SqlDataType data_type,
                               const optional<int32_t>& type_scale) {
  switch (data_type) {
    case SqlDataType_TYPE_TIMESTAMP:
    case SqlDataType_TYPE_TIME:
      return 3;
    case SqlDataType_DECIMAL:
      return type_scale;
    case SqlDataType_NUMERIC:
      return type_scale;
    case SqlDataType_TINYINT:
    case SqlDataType_SMALLINT:
    case SqlDataType_INTEGER:
    case SqlDataType_BIGINT:
      return 0;
    default:
      return std::nullopt;
  }
}
optional<int32_t> GetColumnSize(SqlDataType data_type,
                                const optional<int32_t>& column_size) {
  switch (data_type) {
    case SqlDataType_CHAR:
    case SqlDataType_VARCHAR:
    case SqlDataType_LONGVARCHAR:
      return column_size;
    case SqlDataType_WCHAR:
    case SqlDataType_WVARCHAR:
    case SqlDataType_WLONGVARCHAR:
      return column_size.has_value()
                 ? std::make_optional(column_size.value() * GetSqlWCharSize())
                 : std::nullopt;
    case SqlDataType_BINARY:
    case SqlDataType_VARBINARY:
    case SqlDataType_LONGVARBINARY:
      return column_size;
    case SqlDataType_DECIMAL:
      return 19;  // The same as sizeof(SQL_NUMERIC_STRUCT)
    case SqlDataType_NUMERIC:
      return 19;  // The same as sizeof(SQL_NUMERIC_STRUCT)
    case SqlDataType_BIT:
    case SqlDataType_TINYINT:
      return 1;
    case SqlDataType_SMALLINT:
      return 2;
    case SqlDataType_INTEGER:
      return 4;
    case SqlDataType_BIGINT:
      return 8;
    case SqlDataType_REAL:
      return 4;
    case SqlDataType_FLOAT:
    case SqlDataType_DOUBLE:
      return 8;
    case SqlDataType_TYPE_DATE:
      return 10;  // The same as sizeof(SQL_DATE_STRUCT)
    case SqlDataType_TYPE_TIME:
      return 12;  // The same as sizeof(SQL_TIME_STRUCT)
    case SqlDataType_TYPE_TIMESTAMP:
      return 23;  // The same as sizeof(SQL_TIME_STRUCT)
    case SqlDataType_INTERVAL_MONTH:
    case SqlDataType_INTERVAL_YEAR:
    case SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case SqlDataType_INTERVAL_DAY:
    case SqlDataType_INTERVAL_HOUR:
    case SqlDataType_INTERVAL_MINUTE:
    case SqlDataType_INTERVAL_SECOND:
    case SqlDataType_INTERVAL_DAY_TO_HOUR:
    case SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case SqlDataType_INTERVAL_DAY_TO_SECOND:
    case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return 28;  // The same as sizeof(SQL_INTERVAL_STRUCT)
    case SqlDataType_GUID:
      return 16;
    default:
      return std::nullopt;
  }
}

optional<int32_t> GetBufferLength(SqlDataType data_type,
                                  const optional<int32_t>& column_size) {
  switch (data_type) {
    case SqlDataType_CHAR:
    case SqlDataType_VARCHAR:
    case SqlDataType_LONGVARCHAR:
      return column_size;
    case SqlDataType_WCHAR:
    case SqlDataType_WVARCHAR:
    case SqlDataType_WLONGVARCHAR:
      return column_size.has_value()
                 ? std::make_optional(column_size.value() * GetSqlWCharSize())
                 : std::nullopt;
    case SqlDataType_BINARY:
    case SqlDataType_VARBINARY:
    case SqlDataType_LONGVARBINARY:
      return column_size;
    case SqlDataType_DECIMAL:
    case SqlDataType_NUMERIC:
      return 19;  // The same as sizeof(SQL_NUMERIC_STRUCT)
    case SqlDataType_BIT:
    case SqlDataType_TINYINT:
      return 1;
    case SqlDataType_SMALLINT:
      return 2;
    case SqlDataType_INTEGER:
      return 4;
    case SqlDataType_BIGINT:
      return 8;
    case SqlDataType_REAL:
      return 4;
    case SqlDataType_FLOAT:
    case SqlDataType_DOUBLE:
      return 8;
    case SqlDataType_TYPE_DATE:
      return 10;  // The same as sizeof(SQL_DATE_STRUCT)
    case SqlDataType_TYPE_TIME:
      return 12;  // The same as sizeof(SQL_TIME_STRUCT)
    case SqlDataType_TYPE_TIMESTAMP:
      return 23;  // The same as sizeof(SQL_TIME_STRUCT)
    case SqlDataType_INTERVAL_MONTH:
    case SqlDataType_INTERVAL_YEAR:
    case SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case SqlDataType_INTERVAL_DAY:
    case SqlDataType_INTERVAL_HOUR:
    case SqlDataType_INTERVAL_MINUTE:
    case SqlDataType_INTERVAL_SECOND:
    case SqlDataType_INTERVAL_DAY_TO_HOUR:
    case SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case SqlDataType_INTERVAL_DAY_TO_SECOND:
    case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return 28;  // The same as sizeof(SQL_INTERVAL_STRUCT)
    case SqlDataType_GUID:
      return 16;
    default:
      return std::nullopt;
  }
}

optional<int32_t> GetLength(SqlDataType data_type, const optional<int32_t>& column_size) {
  switch (data_type) {
    case SqlDataType_CHAR:
    case SqlDataType_VARCHAR:
    case SqlDataType_LONGVARCHAR:
    case SqlDataType_WCHAR:
    case SqlDataType_WVARCHAR:
    case SqlDataType_WLONGVARCHAR:
    case SqlDataType_BINARY:
    case SqlDataType_VARBINARY:
    case SqlDataType_LONGVARBINARY:
      return column_size;
    case SqlDataType_DECIMAL:
    case SqlDataType_NUMERIC:
      return 19;  // The same as sizeof(SQL_NUMERIC_STRUCT)
    case SqlDataType_BIT:
    case SqlDataType_TINYINT:
      return 1;
    case SqlDataType_SMALLINT:
      return 2;
    case SqlDataType_INTEGER:
      return 4;
    case SqlDataType_BIGINT:
      return 8;
    case SqlDataType_REAL:
      return 4;
    case SqlDataType_FLOAT:
    case SqlDataType_DOUBLE:
      return 8;
    case SqlDataType_TYPE_DATE:
      return 10;  // The same as sizeof(SQL_DATE_STRUCT)
    case SqlDataType_TYPE_TIME:
      return 12;  // The same as sizeof(SQL_TIME_STRUCT)
    case SqlDataType_TYPE_TIMESTAMP:
      return 23;  // The same as sizeof(SQL_TIME_STRUCT)
    case SqlDataType_INTERVAL_MONTH:
    case SqlDataType_INTERVAL_YEAR:
    case SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case SqlDataType_INTERVAL_DAY:
    case SqlDataType_INTERVAL_HOUR:
    case SqlDataType_INTERVAL_MINUTE:
    case SqlDataType_INTERVAL_SECOND:
    case SqlDataType_INTERVAL_DAY_TO_HOUR:
    case SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case SqlDataType_INTERVAL_DAY_TO_SECOND:
    case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return 28;  // The same as sizeof(SQL_INTERVAL_STRUCT)
    case SqlDataType_GUID:
      return 16;
    default:
      return std::nullopt;
  }
}

optional<int32_t> GetDisplaySize(SqlDataType data_type,
                                 const optional<int32_t>& column_size) {
  switch (data_type) {
    case SqlDataType_CHAR:
    case SqlDataType_VARCHAR:
    case SqlDataType_LONGVARCHAR:
    case SqlDataType_WCHAR:
    case SqlDataType_WVARCHAR:
    case SqlDataType_WLONGVARCHAR:
      return column_size;
    case SqlDataType_BINARY:
    case SqlDataType_VARBINARY:
    case SqlDataType_LONGVARBINARY:
      return column_size ? make_optional(*column_size * 2) : nullopt;
    case SqlDataType_DECIMAL:
    case SqlDataType_NUMERIC:
      return column_size ? make_optional(*column_size + 2) : nullopt;
    case SqlDataType_BIT:
      return 1;
    case SqlDataType_TINYINT:
      return 4;
    case SqlDataType_SMALLINT:
      return 6;
    case SqlDataType_INTEGER:
      return 11;
    case SqlDataType_BIGINT:
      return 20;
    case SqlDataType_REAL:
      return 14;
    case SqlDataType_FLOAT:
    case SqlDataType_DOUBLE:
      return 24;
    case SqlDataType_TYPE_DATE:
      return 10;
    case SqlDataType_TYPE_TIME:
      return 12;  // Assuming format "hh:mm:ss.fff"
    case SqlDataType_TYPE_TIMESTAMP:
      return 23;  // Assuming format "yyyy-mm-dd hh:mm:ss.fff"
    case SqlDataType_INTERVAL_MONTH:
    case SqlDataType_INTERVAL_YEAR:
    case SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case SqlDataType_INTERVAL_DAY:
    case SqlDataType_INTERVAL_HOUR:
    case SqlDataType_INTERVAL_MINUTE:
    case SqlDataType_INTERVAL_SECOND:
    case SqlDataType_INTERVAL_DAY_TO_HOUR:
    case SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case SqlDataType_INTERVAL_DAY_TO_SECOND:
    case SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return nullopt;  // GH-47874 TODO: Implement for INTERVAL types
    case SqlDataType_GUID:
      return 36;
    default:
      return nullopt;
  }
}

std::string ConvertSqlPatternToRegexString(const std::string& pattern) {
  static const std::string specials = "[]()|^-+*?{}$\\.";

  std::string regex_str;
  bool escape = false;
  for (const auto& c : pattern) {
    if (escape) {
      regex_str += c;
      escape = false;
      continue;
    }

    switch (c) {
      case '\\':
        escape = true;
        break;
      case '_':
        regex_str += '.';
        break;
      case '%':
        regex_str += ".*";
        break;
      default:
        if (specials.find(c) != std::string::npos) {
          regex_str += '\\';
        }
        regex_str += c;
        break;
    }
  }
  return regex_str;
}

boost::xpressive::sregex ConvertSqlPatternToRegex(const std::string& pattern) {
  const std::string& regex_str = ConvertSqlPatternToRegexString(pattern);
  return boost::xpressive::sregex(boost::xpressive::sregex::compile(regex_str));
}

bool NeedArrayConversion(Type::type original_type_id, CDataType data_type) {
  switch (original_type_id) {
    case Type::DATE32:
    case Type::DATE64:
      return data_type != CDataType_DATE;
    case Type::TIME32:
    case Type::TIME64:
      return data_type != CDataType_TIME;
    case Type::TIMESTAMP:
      return data_type != CDataType_TIMESTAMP;
    case Type::STRING:
      return data_type != CDataType_CHAR && data_type != CDataType_WCHAR;
    case Type::INT16:
      return data_type != CDataType_SSHORT;
    case Type::UINT16:
      return data_type != CDataType_USHORT;
    case Type::INT32:
      return data_type != CDataType_SLONG;
    case Type::UINT32:
      return data_type != CDataType_ULONG;
    case Type::FLOAT:
      return data_type != CDataType_FLOAT;
    case Type::DOUBLE:
      return data_type != CDataType_DOUBLE;
    case Type::BOOL:
      return data_type != CDataType_BIT;
    case Type::INT8:
      return data_type != CDataType_STINYINT;
    case Type::UINT8:
      return data_type != CDataType_UTINYINT;
    case Type::INT64:
      return data_type != CDataType_SBIGINT;
    case Type::UINT64:
      return data_type != CDataType_UBIGINT;
    case Type::BINARY:
      return data_type != CDataType_BINARY;
    case Type::DECIMAL128:
      return data_type != CDataType_NUMERIC;
    case Type::DURATION:
    case Type::LIST:
    case Type::LARGE_LIST:
    case Type::FIXED_SIZE_LIST:
    case Type::MAP:
    case Type::STRING_VIEW:
    case Type::STRUCT:
      return data_type == CDataType_CHAR || data_type == CDataType_WCHAR;
    default:
      throw DriverException(std::string("Invalid conversion"));
  }
}

std::shared_ptr<DataType> GetDefaultDataTypeForTypeId(Type::type type_id) {
  switch (type_id) {
    case Type::STRING:
      return arrow::utf8();
    case Type::INT16:
      return arrow::int16();
    case Type::UINT16:
      return arrow::uint16();
    case Type::INT32:
      return arrow::int32();
    case Type::UINT32:
      return arrow::uint32();
    case Type::FLOAT:
      return arrow::float32();
    case Type::DOUBLE:
      return arrow::float64();
    case Type::BOOL:
      return arrow::boolean();
    case Type::INT8:
      return arrow::int8();
    case Type::UINT8:
      return arrow::uint8();
    case Type::INT64:
      return arrow::int64();
    case Type::UINT64:
      return arrow::uint64();
    case Type::BINARY:
      return arrow::binary();
    case Type::DECIMAL128:
      return arrow::decimal128(Decimal128Type::kMaxPrecision, 0);
    case Type::DATE64:
      return arrow::date64();
    case Type::TIME64:
      return arrow::time64(TimeUnit::MICRO);
    case Type::TIMESTAMP:
      return arrow::timestamp(TimeUnit::SECOND);
  }

  throw DriverException(std::string("Invalid type id: ") + std::to_string(type_id));
}

Type::type ConvertCToArrowType(CDataType data_type) {
  switch (data_type) {
    case CDataType_CHAR:
    case CDataType_WCHAR:
      return Type::STRING;
    case CDataType_SSHORT:
      return Type::INT16;
    case CDataType_USHORT:
      return Type::UINT16;
    case CDataType_SLONG:
      return Type::INT32;
    case CDataType_ULONG:
      return Type::UINT32;
    case CDataType_FLOAT:
      return Type::FLOAT;
    case CDataType_DOUBLE:
      return Type::DOUBLE;
    case CDataType_BIT:
      return Type::BOOL;
    case CDataType_STINYINT:
      return Type::INT8;
    case CDataType_UTINYINT:
      return Type::UINT8;
    case CDataType_SBIGINT:
      return Type::INT64;
    case CDataType_UBIGINT:
      return Type::UINT64;
    case CDataType_BINARY:
      return Type::BINARY;
    case CDataType_NUMERIC:
      return Type::DECIMAL128;
    case CDataType_TIMESTAMP:
      return Type::TIMESTAMP;
    case CDataType_TIME:
      return Type::TIME64;
    case CDataType_DATE:
      return Type::DATE64;
    default:
      throw DriverException(std::string("Invalid target type: ") +
                            std::to_string(data_type));
  }
}

CDataType ConvertArrowTypeToC(Type::type type_id, bool use_wide_char) {
  switch (type_id) {
    case Type::STRING:
      return GetDefaultCCharType(use_wide_char);
    case Type::INT16:
      return CDataType_SSHORT;
    case Type::UINT16:
      return CDataType_USHORT;
    case Type::INT32:
      return CDataType_SLONG;
    case Type::UINT32:
      return CDataType_ULONG;
    case Type::FLOAT:
      return CDataType_FLOAT;
    case Type::DOUBLE:
      return CDataType_DOUBLE;
    case Type::BOOL:
      return CDataType_BIT;
    case Type::INT8:
      return CDataType_STINYINT;
    case Type::UINT8:
      return CDataType_UTINYINT;
    case Type::INT64:
      return CDataType_SBIGINT;
    case Type::UINT64:
      return CDataType_UBIGINT;
    case Type::BINARY:
      return CDataType_BINARY;
    case Type::DECIMAL128:
      return CDataType_NUMERIC;
    case Type::DATE64:
    case Type::DATE32:
      return CDataType_DATE;
    case Type::TIME64:
    case Type::TIME32:
      return CDataType_TIME;
    case Type::TIMESTAMP:
      return CDataType_TIMESTAMP;
    default:
      throw DriverException(std::string("Invalid type id: ") + std::to_string(type_id));
  }
}

std::shared_ptr<Array> CheckConversion(const arrow::Result<Datum>& result) {
  if (result.ok()) {
    const Datum& datum = result.ValueOrDie();
    return datum.make_array();
  } else {
    throw DriverException(result.status().message());
  }
}

ArrayConvertTask GetConverter(Type::type original_type_id, CDataType target_type) {
  // The else statement has a convert the works for the most case of array
  // conversion. In case, we find conversion that the default one can't handle
  // we can include some additional if-else statement with the logic to handle
  // it
  if (original_type_id == Type::STRING && target_type == CDataType_TIME) {
    return [=](const std::shared_ptr<Array>& original_array) {
      arrow::compute::StrptimeOptions options("%H:%M", TimeUnit::MICRO, false);

      auto converted_result = arrow::compute::Strptime({original_array}, options);
      auto first_converted_array = CheckConversion(converted_result);

      arrow::compute::CastOptions cast_options;
      cast_options.to_type = time64(TimeUnit::MICRO);
      return CheckConversion(
          arrow::compute::CallFunction("cast", {first_converted_array}, &cast_options));
    };
  } else if (original_type_id == Type::TIME32 && target_type == CDataType_TIMESTAMP) {
    return [=](const std::shared_ptr<Array>& original_array) {
      arrow::compute::CastOptions cast_options;
      cast_options.to_type = arrow::int32();

      auto first_converted_array =
          CheckConversion(arrow::compute::Cast(original_array, cast_options));

      cast_options.to_type = arrow::int64();

      auto second_converted_array =
          CheckConversion(arrow::compute::Cast(first_converted_array, cast_options));

      auto seconds_from_epoch = GetTodayTimeFromEpoch();

      auto third_converted_array = CheckConversion(arrow::compute::Add(
          second_converted_array,
          std::make_shared<arrow::Int64Scalar>(seconds_from_epoch * 1000)));

      arrow::compute::CastOptions cast_options_2;
      cast_options_2.to_type = arrow::timestamp(TimeUnit::MILLI);

      return CheckConversion(arrow::compute::Cast(third_converted_array, cast_options_2));
    };
  } else if (original_type_id == Type::TIME64 && target_type == CDataType_TIMESTAMP) {
    return [=](const std::shared_ptr<Array>& original_array) {
      arrow::compute::CastOptions cast_options;
      cast_options.to_type = arrow::int64();

      auto first_converted_array =
          CheckConversion(arrow::compute::Cast(original_array, cast_options));

      auto seconds_from_epoch = GetTodayTimeFromEpoch();

      auto second_converted_array = CheckConversion(arrow::compute::Add(
          first_converted_array,
          std::make_shared<arrow::Int64Scalar>(seconds_from_epoch * 1000000000)));

      arrow::compute::CastOptions cast_options_2;
      cast_options_2.to_type = arrow::timestamp(TimeUnit::NANO);

      return CheckConversion(
          arrow::compute::Cast(second_converted_array, cast_options_2));
    };
  } else if (original_type_id == Type::STRING && target_type == CDataType_DATE) {
    return [=](const std::shared_ptr<Array>& original_array) {
      // The Strptime requires a date format. Using the ISO 8601 format
      arrow::compute::StrptimeOptions options("%Y-%m-%d", TimeUnit::SECOND, false);

      auto converted_result = arrow::compute::Strptime({original_array}, options);

      auto first_converted_array = CheckConversion(converted_result);
      arrow::compute::CastOptions cast_options;
      cast_options.to_type = arrow::date64();
      return CheckConversion(
          arrow::compute::CallFunction("cast", {first_converted_array}, &cast_options));
    };
  } else if (original_type_id == Type::DECIMAL128 &&
             (target_type == CDataType_CHAR || target_type == CDataType_WCHAR)) {
    return [=](const std::shared_ptr<Array>& original_array) {
      arrow::StringBuilder builder;
      int64_t length = original_array->length();
      ThrowIfNotOK(builder.ReserveData(length));

      for (int64_t i = 0; i < length; ++i) {
        if (original_array->IsNull(i)) {
          ThrowIfNotOK(builder.AppendNull());
        } else {
          auto result = original_array->GetScalar(i);
          auto scalar = result.ValueOrDie();
          ThrowIfNotOK(builder.Append(scalar->ToString()));
        }
      }

      auto finish = builder.Finish();

      return finish.ValueOrDie();
    };
  } else if (IsComplexType(original_type_id) &&
             (target_type == CDataType_CHAR || target_type == CDataType_WCHAR)) {
    return [=](const std::shared_ptr<Array>& original_array) {
      const auto& json_conversion_result = ConvertToJson(original_array);
      ThrowIfNotOK(json_conversion_result.status());
      return json_conversion_result.ValueOrDie();
    };
  } else {
    // Default converter
    return [=](const std::shared_ptr<Array>& original_array) {
      const Type::type& target_arrow_type_id = ConvertCToArrowType(target_type);
      arrow::compute::CastOptions cast_options;
      cast_options.to_type = GetDefaultDataTypeForTypeId(target_arrow_type_id);

      return CheckConversion(
          arrow::compute::CallFunction("cast", {original_array}, &cast_options));
    };
  }
}
std::string ConvertToDBMSVer(const std::string& str) {
  boost::char_separator<char> separator(".");
  boost::tokenizer<boost::char_separator<char> > tokenizer(str, separator);
  std::string result;
  // The permitted ODBC format is ##.##.####<custom-server-information>
  // If any of the first 3 tokens are not numbers or are greater than the permitted
  // digits, assume we hit the custom-server-information early and assume the remaining
  // version digits are zero.
  size_t position = 0;
  bool is_showing_custom_data = false;
  auto pad_remaining_tokens = [&](size_t pos) -> std::string {
    std::string padded_str;
    if (pos == 0) {
      padded_str += "00";
    }
    if (pos <= 1) {
      padded_str += ".00";
    }
    if (pos <= 2) {
      padded_str += ".0000";
    }
    return padded_str;
  };

  for (auto token : tokenizer) {
    if (token.empty()) {
      continue;
    }

    if (!is_showing_custom_data && position < 3) {
      std::string suffix;
      try {
        size_t next_pos = 0;
        int version = stoi(token, &next_pos);
        if (next_pos != token.size()) {
          suffix = &token[0];
        }
        if (version < 0 || (position < 2 && (version > 99)) ||
            (position == 2 && version > 9999)) {
          is_showing_custom_data = true;
        } else {
          std::stringstream strstream;
          if (position == 2) {
            strstream << std::setfill('0') << std::setw(4);
          } else {
            strstream << std::setfill('0') << std::setw(2);
          }
          strstream << version;

          if (position != 0) {
            result += ".";
          }
          result += strstream.str();
          if (next_pos != token.size()) {
            suffix = &token[next_pos];
            result += pad_remaining_tokens(++position) + suffix;
            position = 4;  // Prevent additional padding.
            is_showing_custom_data = true;
            continue;
          }
          ++position;
          continue;
        }
      } catch (std::logic_error&) {
        is_showing_custom_data = true;
      }

      result += pad_remaining_tokens(position) + suffix;
      ++position;
    }

    result += "." + token;
    ++position;
  }

  result += pad_remaining_tokens(position);
  return result;
}

int32_t GetDecimalTypeScale(const std::shared_ptr<DataType>& decimal_type) {
  auto decimal128_type = std::dynamic_pointer_cast<Decimal128Type>(decimal_type);
  return decimal128_type->scale();
}

int32_t GetDecimalTypePrecision(const std::shared_ptr<DataType>& decimal_type) {
  auto decimal128_type = std::dynamic_pointer_cast<Decimal128Type>(decimal_type);
  return decimal128_type->precision();
}

std::optional<bool> AsBool(const std::string& value) {
  if (boost::iequals(value, "true") || boost::iequals(value, "1")) {
    return true;
  } else if (boost::iequals(value, "false") || boost::iequals(value, "0")) {
    return false;
  } else {
    return std::nullopt;
  }
}

std::optional<bool> AsBool(const Connection::ConnPropertyMap& conn_property_map,
                           const std::string_view& property_name) {
  auto extracted_property = conn_property_map.find(std::string(property_name));

  if (extracted_property != conn_property_map.end()) {
    return AsBool(extracted_property->second);
  }

  return std::nullopt;
}

std::optional<int32_t> AsInt32(int32_t min_value,
                               const Connection::ConnPropertyMap& conn_property_map,
                               const std::string_view& property_name) {
  auto extracted_property = conn_property_map.find(std::string(property_name));

  if (extracted_property != conn_property_map.end()) {
    const int32_t string_column_length = std::stoi(extracted_property->second);

    if (string_column_length >= min_value && string_column_length <= INT32_MAX) {
      return string_column_length;
    }
  }
  return std::nullopt;
}

}  // namespace util
}  // namespace arrow::flight::sql::odbc
