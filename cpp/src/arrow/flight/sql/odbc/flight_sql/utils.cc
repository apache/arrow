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

#include "arrow/flight/sql/odbc/flight_sql/utils.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/calendar_utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/encoding.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"

#include "arrow/builder.h"
#include "arrow/compute/api.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

#include "arrow/flight/sql/odbc/flight_sql/json_converter.h"

#include <boost/tokenizer.hpp>

#include <ctime>
#include <sstream>

namespace driver {
namespace flight_sql {

namespace {
bool IsComplexType(arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::MAP:
    case arrow::Type::STRUCT:
      return true;
    default:
      return false;
  }
}

odbcabstraction::SqlDataType GetDefaultSqlCharType(bool useWideChar) {
  return useWideChar ? odbcabstraction::SqlDataType_WCHAR
                     : odbcabstraction::SqlDataType_CHAR;
}
odbcabstraction::SqlDataType GetDefaultSqlVarcharType(bool useWideChar) {
  return useWideChar ? odbcabstraction::SqlDataType_WVARCHAR
                     : odbcabstraction::SqlDataType_VARCHAR;
}
odbcabstraction::CDataType GetDefaultCCharType(bool useWideChar) {
  return useWideChar ? odbcabstraction::CDataType_WCHAR : odbcabstraction::CDataType_CHAR;
}

}  // namespace

using odbcabstraction::CDataType;
using odbcabstraction::GetSqlWCharSize;
using odbcabstraction::GetTodayTimeFromEpoch;
using odbcabstraction::SqlDataType;

using std::make_optional;
using std::nullopt;

/// \brief Returns the mapping from Arrow type to SqlDataType
/// \param field the field to return the SqlDataType for
/// \return the concise SqlDataType for the field.
/// \note use GetNonConciseDataType on the output to get the verbose type
/// \note the concise and verbose types are the same for all but types relating to times
/// and intervals
SqlDataType GetDataTypeFromArrowField_V3(const std::shared_ptr<arrow::Field>& field,
                                         bool useWideChar) {
  const std::shared_ptr<arrow::DataType>& type = field->type();

  switch (type->id()) {
    case arrow::Type::BOOL:
      return odbcabstraction::SqlDataType_BIT;
    case arrow::Type::UINT8:
    case arrow::Type::INT8:
      return odbcabstraction::SqlDataType_TINYINT;
    case arrow::Type::UINT16:
    case arrow::Type::INT16:
      return odbcabstraction::SqlDataType_SMALLINT;
    case arrow::Type::UINT32:
    case arrow::Type::INT32:
      return odbcabstraction::SqlDataType_INTEGER;
    case arrow::Type::UINT64:
    case arrow::Type::INT64:
      return odbcabstraction::SqlDataType_BIGINT;
    case arrow::Type::HALF_FLOAT:
    case arrow::Type::FLOAT:
      return odbcabstraction::SqlDataType_FLOAT;
    case arrow::Type::DOUBLE:
      return odbcabstraction::SqlDataType_DOUBLE;
    case arrow::Type::BINARY:
    case arrow::Type::FIXED_SIZE_BINARY:
    case arrow::Type::LARGE_BINARY:
      return odbcabstraction::SqlDataType_BINARY;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      return GetDefaultSqlVarcharType(useWideChar);
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
      return odbcabstraction::SqlDataType_TYPE_DATE;
    case arrow::Type::TIMESTAMP:
      return odbcabstraction::SqlDataType_TYPE_TIMESTAMP;
    case arrow::Type::DECIMAL128:
      return odbcabstraction::SqlDataType_DECIMAL;
    case arrow::Type::TIME32:
    case arrow::Type::TIME64:
      return odbcabstraction::SqlDataType_TYPE_TIME;
    case arrow::Type::INTERVAL_MONTHS:
      return odbcabstraction::
          SqlDataType_INTERVAL_MONTH;  // TODO: maybe
                                       // odbcabstraction::SqlDataType_INTERVAL_YEAR_TO_MONTH
    case arrow::Type::INTERVAL_DAY_TIME:
      return odbcabstraction::SqlDataType_INTERVAL_DAY;

    // TODO: Handle remaining types.
    case arrow::Type::INTERVAL_MONTH_DAY_NANO:
    case arrow::Type::LIST:
    case arrow::Type::STRUCT:
    case arrow::Type::SPARSE_UNION:
    case arrow::Type::DENSE_UNION:
    case arrow::Type::DICTIONARY:
    case arrow::Type::MAP:
    case arrow::Type::EXTENSION:
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::DURATION:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::MAX_ID:
    case arrow::Type::NA:
      break;
  }

  return GetDefaultSqlVarcharType(useWideChar);
}

SqlDataType EnsureRightSqlCharType(SqlDataType data_type, bool useWideChar) {
  switch (data_type) {
    case odbcabstraction::SqlDataType_CHAR:
    case odbcabstraction::SqlDataType_WCHAR:
      return GetDefaultSqlCharType(useWideChar);
    case odbcabstraction::SqlDataType_VARCHAR:
    case odbcabstraction::SqlDataType_WVARCHAR:
      return GetDefaultSqlVarcharType(useWideChar);
    default:
      return data_type;
  }
}

int16_t ConvertSqlDataTypeFromV3ToV2(int16_t data_type_v3) {
  switch (data_type_v3) {
    case odbcabstraction::SqlDataType_TYPE_DATE:
      return 9;  // Same as SQL_DATE from sqlext.h
    case odbcabstraction::SqlDataType_TYPE_TIME:
      return 10;  // Same as SQL_TIME from sqlext.h
    case odbcabstraction::SqlDataType_TYPE_TIMESTAMP:
      return 11;  // Same as SQL_TIMESTAMP from sqlext.h
    default:
      return data_type_v3;
  }
}

CDataType ConvertCDataTypeFromV2ToV3(int16_t data_type_v2) {
  switch (data_type_v2) {
    case -6:  // Same as SQL_C_TINYINT from sqlext.h
      return odbcabstraction::CDataType_STINYINT;
    case 4:  // Same as SQL_C_LONG from sqlext.h
      return odbcabstraction::CDataType_SLONG;
    case 5:  // Same as SQL_C_SHORT from sqlext.h
      return odbcabstraction::CDataType_SSHORT;
    case 7:  // Same as SQL_C_FLOAT from sqlext.h
      return odbcabstraction::CDataType_FLOAT;
    case 8:  // Same as SQL_C_DOUBLE from sqlext.h
      return odbcabstraction::CDataType_DOUBLE;
    case 9:  // Same as SQL_C_DATE from sqlext.h
      return odbcabstraction::CDataType_DATE;
    case 10:  // Same as SQL_C_TIME from sqlext.h
      return odbcabstraction::CDataType_TIME;
    case 11:  // Same as SQL_C_TIMESTAMP from sqlext.h
      return odbcabstraction::CDataType_TIMESTAMP;
    default:
      return static_cast<CDataType>(data_type_v2);
  }
}

std::string GetTypeNameFromSqlDataType(int16_t data_type) {
  switch (data_type) {
    case odbcabstraction::SqlDataType_CHAR:
      return "CHAR";
    case odbcabstraction::SqlDataType_VARCHAR:
      return "VARCHAR";
    case odbcabstraction::SqlDataType_LONGVARCHAR:
      return "LONGVARCHAR";
    case odbcabstraction::SqlDataType_WCHAR:
      return "WCHAR";
    case odbcabstraction::SqlDataType_WVARCHAR:
      return "WVARCHAR";
    case odbcabstraction::SqlDataType_WLONGVARCHAR:
      return "WLONGVARCHAR";
    case odbcabstraction::SqlDataType_DECIMAL:
      return "DECIMAL";
    case odbcabstraction::SqlDataType_NUMERIC:
      return "NUMERIC";
    case odbcabstraction::SqlDataType_SMALLINT:
      return "SMALLINT";
    case odbcabstraction::SqlDataType_INTEGER:
      return "INTEGER";
    case odbcabstraction::SqlDataType_REAL:
      return "REAL";
    case odbcabstraction::SqlDataType_FLOAT:
      return "FLOAT";
    case odbcabstraction::SqlDataType_DOUBLE:
      return "DOUBLE";
    case odbcabstraction::SqlDataType_BIT:
      return "BIT";
    case odbcabstraction::SqlDataType_TINYINT:
      return "TINYINT";
    case odbcabstraction::SqlDataType_BIGINT:
      return "BIGINT";
    case odbcabstraction::SqlDataType_BINARY:
      return "BINARY";
    case odbcabstraction::SqlDataType_VARBINARY:
      return "VARBINARY";
    case odbcabstraction::SqlDataType_LONGVARBINARY:
      return "LONGVARBINARY";
    case odbcabstraction::SqlDataType_TYPE_DATE:
    case 9:
      return "DATE";
    case odbcabstraction::SqlDataType_TYPE_TIME:
    case 10:
      return "TIME";
    case odbcabstraction::SqlDataType_TYPE_TIMESTAMP:
    case 11:
      return "TIMESTAMP";
    case odbcabstraction::SqlDataType_INTERVAL_MONTH:
      return "INTERVAL_MONTH";
    case odbcabstraction::SqlDataType_INTERVAL_YEAR:
      return "INTERVAL_YEAR";
    case odbcabstraction::SqlDataType_INTERVAL_YEAR_TO_MONTH:
      return "INTERVAL_YEAR_TO_MONTH";
    case odbcabstraction::SqlDataType_INTERVAL_DAY:
      return "INTERVAL_DAY";
    case odbcabstraction::SqlDataType_INTERVAL_HOUR:
      return "INTERVAL_HOUR";
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE:
      return "INTERVAL_MINUTE";
    case odbcabstraction::SqlDataType_INTERVAL_SECOND:
      return "INTERVAL_SECOND";
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_HOUR:
      return "INTERVAL_DAY_TO_HOUR";
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_MINUTE:
      return "INTERVAL_DAY_TO_MINUTE";
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_SECOND:
      return "INTERVAL_DAY_TO_SECOND";
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_MINUTE:
      return "INTERVAL_HOUR_TO_MINUTE";
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_SECOND:
      return "INTERVAL_HOUR_TO_SECOND";
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return "INTERVAL_MINUTE_TO_SECOND";
    case odbcabstraction::SqlDataType_GUID:
      return "GUID";
  }

  throw driver::odbcabstraction::DriverException("Unsupported data type: " +
                                                 std::to_string(data_type));
}

optional<int16_t> GetRadixFromSqlDataType(odbcabstraction::SqlDataType data_type) {
  switch (data_type) {
    case odbcabstraction::SqlDataType_DECIMAL:
    case odbcabstraction::SqlDataType_NUMERIC:
    case odbcabstraction::SqlDataType_SMALLINT:
    case odbcabstraction::SqlDataType_TINYINT:
    case odbcabstraction::SqlDataType_INTEGER:
    case odbcabstraction::SqlDataType_BIGINT:
      return 10;
    case odbcabstraction::SqlDataType_REAL:
    case odbcabstraction::SqlDataType_FLOAT:
    case odbcabstraction::SqlDataType_DOUBLE:
      return 2;
    default:
      return std::nullopt;
  }
}

int16_t GetNonConciseDataType(odbcabstraction::SqlDataType data_type) {
  switch (data_type) {
    case odbcabstraction::SqlDataType_TYPE_DATE:
    case odbcabstraction::SqlDataType_TYPE_TIME:
    case odbcabstraction::SqlDataType_TYPE_TIMESTAMP:
      return 9;  // Same as SQL_DATETIME on sql.h
    case odbcabstraction::SqlDataType_INTERVAL_YEAR:
    case odbcabstraction::SqlDataType_INTERVAL_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_DAY:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return 10;  // Same as SQL_INTERVAL on sqlext.h
    default:
      return data_type;
  }
}

optional<int16_t> GetSqlDateTimeSubCode(SqlDataType data_type) {
  switch (data_type) {
    case odbcabstraction::SqlDataType_TYPE_DATE:
      return odbcabstraction::SqlDateTimeSubCode_DATE;
    case odbcabstraction::SqlDataType_TYPE_TIME:
      return odbcabstraction::SqlDateTimeSubCode_TIME;
    case odbcabstraction::SqlDataType_TYPE_TIMESTAMP:
      return odbcabstraction::SqlDateTimeSubCode_TIMESTAMP;
    case odbcabstraction::SqlDataType_INTERVAL_YEAR:
      return odbcabstraction::SqlDateTimeSubCode_YEAR;
    case odbcabstraction::SqlDataType_INTERVAL_MONTH:
      return odbcabstraction::SqlDateTimeSubCode_MONTH;
    case odbcabstraction::SqlDataType_INTERVAL_DAY:
      return odbcabstraction::SqlDateTimeSubCode_DAY;
    case odbcabstraction::SqlDataType_INTERVAL_HOUR:
      return odbcabstraction::SqlDateTimeSubCode_HOUR;
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE:
      return odbcabstraction::SqlDateTimeSubCode_MINUTE;
    case odbcabstraction::SqlDataType_INTERVAL_SECOND:
      return odbcabstraction::SqlDateTimeSubCode_SECOND;
    case odbcabstraction::SqlDataType_INTERVAL_YEAR_TO_MONTH:
      return odbcabstraction::SqlDateTimeSubCode_YEAR_TO_MONTH;
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_HOUR:
      return odbcabstraction::SqlDateTimeSubCode_DAY_TO_HOUR;
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_MINUTE:
      return odbcabstraction::SqlDateTimeSubCode_DAY_TO_MINUTE;
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_SECOND:
      return odbcabstraction::SqlDateTimeSubCode_DAY_TO_SECOND;
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_MINUTE:
      return odbcabstraction::SqlDateTimeSubCode_HOUR_TO_MINUTE;
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_SECOND:
      return odbcabstraction::SqlDateTimeSubCode_HOUR_TO_SECOND;
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return odbcabstraction::SqlDateTimeSubCode_MINUTE_TO_SECOND;
    default:
      return std::nullopt;
  }
}

optional<int32_t> GetCharOctetLength(SqlDataType data_type,
                                     const arrow::Result<int32_t>& column_size,
                                     const int32_t decimal_precison) {
  switch (data_type) {
    case odbcabstraction::SqlDataType_BINARY:
    case odbcabstraction::SqlDataType_VARBINARY:
    case odbcabstraction::SqlDataType_LONGVARBINARY:
    case odbcabstraction::SqlDataType_CHAR:
    case odbcabstraction::SqlDataType_VARCHAR:
    case odbcabstraction::SqlDataType_LONGVARCHAR:
      if (column_size.ok()) {
        return column_size.ValueOrDie();
      } else {
        return std::nullopt;
      }
    case odbcabstraction::SqlDataType_WCHAR:
    case odbcabstraction::SqlDataType_WVARCHAR:
    case odbcabstraction::SqlDataType_WLONGVARCHAR:
      if (column_size.ok()) {
        return column_size.ValueOrDie() * GetSqlWCharSize();
      } else {
        return std::nullopt;
      }
    case odbcabstraction::SqlDataType_TINYINT:
    case odbcabstraction::SqlDataType_BIT:
      return 1;  // The same as sizeof(SQL_C_BIT)
    case odbcabstraction::SqlDataType_SMALLINT:
      return 2;  // The same as sizeof(SQL_C_SMALLINT)
    case odbcabstraction::SqlDataType_INTEGER:
      return 4;  // The same as sizeof(SQL_C_INTEGER)
    case odbcabstraction::SqlDataType_BIGINT:
    case odbcabstraction::SqlDataType_FLOAT:
    case odbcabstraction::SqlDataType_DOUBLE:
      return 8;  // The same as sizeof(SQL_C_DOUBLE)
    case odbcabstraction::SqlDataType_DECIMAL:
    case odbcabstraction::SqlDataType_NUMERIC:
      return decimal_precison + 2;  // One char for each digit and two extra chars for a
                                    // sign and a decimal point
    case odbcabstraction::SqlDataType_TYPE_DATE:
    case odbcabstraction::SqlDataType_TYPE_TIME:
      return 6;  // The same as sizeof(SQL_TIME_STRUCT)
    case odbcabstraction::SqlDataType_TYPE_TIMESTAMP:
      return 16;  // The same as sizeof(SQL_TIMESTAMP_STRUCT)
    case odbcabstraction::SqlDataType_INTERVAL_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_YEAR:
    case odbcabstraction::SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_DAY:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return 34;  // The same as sizeof(SQL_INTERVAL_STRUCT)
    case odbcabstraction::SqlDataType_GUID:
      return 16;
    default:
      return std::nullopt;
  }
}
optional<int32_t> GetTypeScale(SqlDataType data_type,
                               const optional<int32_t>& type_scale) {
  switch (data_type) {
    case odbcabstraction::SqlDataType_TYPE_TIMESTAMP:
    case odbcabstraction::SqlDataType_TYPE_TIME:
      return 3;
    case odbcabstraction::SqlDataType_DECIMAL:
      return type_scale;
    case odbcabstraction::SqlDataType_NUMERIC:
      return type_scale;
    case odbcabstraction::SqlDataType_TINYINT:
    case odbcabstraction::SqlDataType_SMALLINT:
    case odbcabstraction::SqlDataType_INTEGER:
    case odbcabstraction::SqlDataType_BIGINT:
      return 0;
    default:
      return std::nullopt;
  }
}
optional<int32_t> GetColumnSize(SqlDataType data_type,
                                const optional<int32_t>& column_size) {
  switch (data_type) {
    case odbcabstraction::SqlDataType_CHAR:
    case odbcabstraction::SqlDataType_VARCHAR:
    case odbcabstraction::SqlDataType_LONGVARCHAR:
      return column_size;
    case odbcabstraction::SqlDataType_WCHAR:
    case odbcabstraction::SqlDataType_WVARCHAR:
    case odbcabstraction::SqlDataType_WLONGVARCHAR:
      return column_size.has_value()
                 ? std::make_optional(column_size.value() * GetSqlWCharSize())
                 : std::nullopt;
    case odbcabstraction::SqlDataType_BINARY:
    case odbcabstraction::SqlDataType_VARBINARY:
    case odbcabstraction::SqlDataType_LONGVARBINARY:
      return column_size;
    case odbcabstraction::SqlDataType_DECIMAL:
      return 19;  // The same as sizeof(SQL_NUMERIC_STRUCT)
    case odbcabstraction::SqlDataType_NUMERIC:
      return 19;  // The same as sizeof(SQL_NUMERIC_STRUCT)
    case odbcabstraction::SqlDataType_BIT:
    case odbcabstraction::SqlDataType_TINYINT:
      return 1;
    case odbcabstraction::SqlDataType_SMALLINT:
      return 2;
    case odbcabstraction::SqlDataType_INTEGER:
      return 4;
    case odbcabstraction::SqlDataType_BIGINT:
      return 8;
    case odbcabstraction::SqlDataType_REAL:
      return 4;
    case odbcabstraction::SqlDataType_FLOAT:
    case odbcabstraction::SqlDataType_DOUBLE:
      return 8;
    case odbcabstraction::SqlDataType_TYPE_DATE:
      return 10;  // The same as sizeof(SQL_DATE_STRUCT)
    case odbcabstraction::SqlDataType_TYPE_TIME:
      return 12;  // The same as sizeof(SQL_TIME_STRUCT)
    case odbcabstraction::SqlDataType_TYPE_TIMESTAMP:
      return 23;  // The same as sizeof(SQL_TIME_STRUCT)
    case odbcabstraction::SqlDataType_INTERVAL_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_YEAR:
    case odbcabstraction::SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_DAY:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return 28;  // The same as sizeof(SQL_INTERVAL_STRUCT)
    case odbcabstraction::SqlDataType_GUID:
      return 16;
    default:
      return std::nullopt;
  }
}

optional<int32_t> GetBufferLength(SqlDataType data_type,
                                  const optional<int32_t>& column_size) {
  switch (data_type) {
    case odbcabstraction::SqlDataType_CHAR:
    case odbcabstraction::SqlDataType_VARCHAR:
    case odbcabstraction::SqlDataType_LONGVARCHAR:
      return column_size;
    case odbcabstraction::SqlDataType_WCHAR:
    case odbcabstraction::SqlDataType_WVARCHAR:
    case odbcabstraction::SqlDataType_WLONGVARCHAR:
      return column_size.has_value()
                 ? std::make_optional(column_size.value() * GetSqlWCharSize())
                 : std::nullopt;
    case odbcabstraction::SqlDataType_BINARY:
    case odbcabstraction::SqlDataType_VARBINARY:
    case odbcabstraction::SqlDataType_LONGVARBINARY:
      return column_size;
    case odbcabstraction::SqlDataType_DECIMAL:
    case odbcabstraction::SqlDataType_NUMERIC:
      return 19;  // The same as sizeof(SQL_NUMERIC_STRUCT)
    case odbcabstraction::SqlDataType_BIT:
    case odbcabstraction::SqlDataType_TINYINT:
      return 1;
    case odbcabstraction::SqlDataType_SMALLINT:
      return 2;
    case odbcabstraction::SqlDataType_INTEGER:
      return 4;
    case odbcabstraction::SqlDataType_BIGINT:
      return 8;
    case odbcabstraction::SqlDataType_REAL:
      return 4;
    case odbcabstraction::SqlDataType_FLOAT:
    case odbcabstraction::SqlDataType_DOUBLE:
      return 8;
    case odbcabstraction::SqlDataType_TYPE_DATE:
      return 10;  // The same as sizeof(SQL_DATE_STRUCT)
    case odbcabstraction::SqlDataType_TYPE_TIME:
      return 12;  // The same as sizeof(SQL_TIME_STRUCT)
    case odbcabstraction::SqlDataType_TYPE_TIMESTAMP:
      return 23;  // The same as sizeof(SQL_TIME_STRUCT)
    case odbcabstraction::SqlDataType_INTERVAL_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_YEAR:
    case odbcabstraction::SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_DAY:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return 28;  // The same as sizeof(SQL_INTERVAL_STRUCT)
    case odbcabstraction::SqlDataType_GUID:
      return 16;
    default:
      return std::nullopt;
  }
}

optional<int32_t> GetLength(SqlDataType data_type, const optional<int32_t>& column_size) {
  switch (data_type) {
    case odbcabstraction::SqlDataType_CHAR:
    case odbcabstraction::SqlDataType_VARCHAR:
    case odbcabstraction::SqlDataType_LONGVARCHAR:
    case odbcabstraction::SqlDataType_WCHAR:
    case odbcabstraction::SqlDataType_WVARCHAR:
    case odbcabstraction::SqlDataType_WLONGVARCHAR:
    case odbcabstraction::SqlDataType_BINARY:
    case odbcabstraction::SqlDataType_VARBINARY:
    case odbcabstraction::SqlDataType_LONGVARBINARY:
      return column_size;
    case odbcabstraction::SqlDataType_DECIMAL:
    case odbcabstraction::SqlDataType_NUMERIC:
      return 19;  // The same as sizeof(SQL_NUMERIC_STRUCT)
    case odbcabstraction::SqlDataType_BIT:
    case odbcabstraction::SqlDataType_TINYINT:
      return 1;
    case odbcabstraction::SqlDataType_SMALLINT:
      return 2;
    case odbcabstraction::SqlDataType_INTEGER:
      return 4;
    case odbcabstraction::SqlDataType_BIGINT:
      return 8;
    case odbcabstraction::SqlDataType_REAL:
      return 4;
    case odbcabstraction::SqlDataType_FLOAT:
    case odbcabstraction::SqlDataType_DOUBLE:
      return 8;
    case odbcabstraction::SqlDataType_TYPE_DATE:
      return 10;  // The same as sizeof(SQL_DATE_STRUCT)
    case odbcabstraction::SqlDataType_TYPE_TIME:
      return 12;  // The same as sizeof(SQL_TIME_STRUCT)
    case odbcabstraction::SqlDataType_TYPE_TIMESTAMP:
      return 23;  // The same as sizeof(SQL_TIME_STRUCT)
    case odbcabstraction::SqlDataType_INTERVAL_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_YEAR:
    case odbcabstraction::SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_DAY:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return 28;  // The same as sizeof(SQL_INTERVAL_STRUCT)
    case odbcabstraction::SqlDataType_GUID:
      return 16;
    default:
      return std::nullopt;
  }
}

optional<int32_t> GetDisplaySize(SqlDataType data_type,
                                 const optional<int32_t>& column_size) {
  switch (data_type) {
    case odbcabstraction::SqlDataType_CHAR:
    case odbcabstraction::SqlDataType_VARCHAR:
    case odbcabstraction::SqlDataType_LONGVARCHAR:
    case odbcabstraction::SqlDataType_WCHAR:
    case odbcabstraction::SqlDataType_WVARCHAR:
    case odbcabstraction::SqlDataType_WLONGVARCHAR:
      return column_size;
    case odbcabstraction::SqlDataType_BINARY:
    case odbcabstraction::SqlDataType_VARBINARY:
    case odbcabstraction::SqlDataType_LONGVARBINARY:
      return column_size ? make_optional(*column_size * 2) : nullopt;
    case odbcabstraction::SqlDataType_DECIMAL:
    case odbcabstraction::SqlDataType_NUMERIC:
      return column_size ? make_optional(*column_size + 2) : nullopt;
    case odbcabstraction::SqlDataType_BIT:
      return 1;
    case odbcabstraction::SqlDataType_TINYINT:
      return 4;
    case odbcabstraction::SqlDataType_SMALLINT:
      return 6;
    case odbcabstraction::SqlDataType_INTEGER:
      return 11;
    case odbcabstraction::SqlDataType_BIGINT:
      return 20;
    case odbcabstraction::SqlDataType_REAL:
      return 14;
    case odbcabstraction::SqlDataType_FLOAT:
    case odbcabstraction::SqlDataType_DOUBLE:
      return 24;
    case odbcabstraction::SqlDataType_TYPE_DATE:
      return 10;
    case odbcabstraction::SqlDataType_TYPE_TIME:
      return 12;  // Assuming format "hh:mm:ss.fff"
    case odbcabstraction::SqlDataType_TYPE_TIMESTAMP:
      return 23;  // Assuming format "yyyy-mm-dd hh:mm:ss.fff"
    case odbcabstraction::SqlDataType_INTERVAL_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_YEAR:
    case odbcabstraction::SqlDataType_INTERVAL_YEAR_TO_MONTH:
    case odbcabstraction::SqlDataType_INTERVAL_DAY:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_HOUR:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_DAY_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_MINUTE:
    case odbcabstraction::SqlDataType_INTERVAL_HOUR_TO_SECOND:
    case odbcabstraction::SqlDataType_INTERVAL_MINUTE_TO_SECOND:
      return nullopt;  // TODO: Implement for INTERVAL types
    case odbcabstraction::SqlDataType_GUID:
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

bool NeedArrayConversion(arrow::Type::type original_type_id,
                         odbcabstraction::CDataType data_type) {
  switch (original_type_id) {
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
      return data_type != odbcabstraction::CDataType_DATE;
    case arrow::Type::TIME32:
    case arrow::Type::TIME64:
      return data_type != odbcabstraction::CDataType_TIME;
    case arrow::Type::TIMESTAMP:
      return data_type != odbcabstraction::CDataType_TIMESTAMP;
    case arrow::Type::STRING:
      return data_type != odbcabstraction::CDataType_CHAR &&
             data_type != odbcabstraction::CDataType_WCHAR;
    case arrow::Type::INT16:
      return data_type != odbcabstraction::CDataType_SSHORT;
    case arrow::Type::UINT16:
      return data_type != odbcabstraction::CDataType_USHORT;
    case arrow::Type::INT32:
      return data_type != odbcabstraction::CDataType_SLONG;
    case arrow::Type::UINT32:
      return data_type != odbcabstraction::CDataType_ULONG;
    case arrow::Type::FLOAT:
      return data_type != odbcabstraction::CDataType_FLOAT;
    case arrow::Type::DOUBLE:
      return data_type != odbcabstraction::CDataType_DOUBLE;
    case arrow::Type::BOOL:
      return data_type != odbcabstraction::CDataType_BIT;
    case arrow::Type::INT8:
      return data_type != odbcabstraction::CDataType_STINYINT;
    case arrow::Type::UINT8:
      return data_type != odbcabstraction::CDataType_UTINYINT;
    case arrow::Type::INT64:
      return data_type != odbcabstraction::CDataType_SBIGINT;
    case arrow::Type::UINT64:
      return data_type != odbcabstraction::CDataType_UBIGINT;
    case arrow::Type::BINARY:
      return data_type != odbcabstraction::CDataType_BINARY;
    case arrow::Type::DECIMAL128:
      return data_type != odbcabstraction::CDataType_NUMERIC;
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::MAP:
    case arrow::Type::STRUCT:
      return data_type == odbcabstraction::CDataType_CHAR ||
             data_type == odbcabstraction::CDataType_WCHAR;
    default:
      throw odbcabstraction::DriverException(std::string("Invalid conversion"));
  }
}

std::shared_ptr<arrow::DataType> GetDefaultDataTypeForTypeId(arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::STRING:
      return arrow::utf8();
    case arrow::Type::INT16:
      return arrow::int16();
    case arrow::Type::UINT16:
      return arrow::uint16();
    case arrow::Type::INT32:
      return arrow::int32();
    case arrow::Type::UINT32:
      return arrow::uint32();
    case arrow::Type::FLOAT:
      return arrow::float32();
    case arrow::Type::DOUBLE:
      return arrow::float64();
    case arrow::Type::BOOL:
      return arrow::boolean();
    case arrow::Type::INT8:
      return arrow::int8();
    case arrow::Type::UINT8:
      return arrow::uint8();
    case arrow::Type::INT64:
      return arrow::int64();
    case arrow::Type::UINT64:
      return arrow::uint64();
    case arrow::Type::BINARY:
      return arrow::binary();
    case arrow::Type::DECIMAL128:
      return arrow::decimal128(arrow::Decimal128Type::kMaxPrecision, 0);
    case arrow::Type::DATE64:
      return arrow::date64();
    case arrow::Type::TIME64:
      return arrow::time64(arrow::TimeUnit::MICRO);
    case arrow::Type::TIMESTAMP:
      return arrow::timestamp(arrow::TimeUnit::SECOND);
  }

  throw odbcabstraction::DriverException(std::string("Invalid type id: ") +
                                         std::to_string(type_id));
}

arrow::Type::type ConvertCToArrowType(odbcabstraction::CDataType data_type) {
  switch (data_type) {
    case odbcabstraction::CDataType_CHAR:
    case odbcabstraction::CDataType_WCHAR:
      return arrow::Type::STRING;
    case odbcabstraction::CDataType_SSHORT:
      return arrow::Type::INT16;
    case odbcabstraction::CDataType_USHORT:
      return arrow::Type::UINT16;
    case odbcabstraction::CDataType_SLONG:
      return arrow::Type::INT32;
    case odbcabstraction::CDataType_ULONG:
      return arrow::Type::UINT32;
    case odbcabstraction::CDataType_FLOAT:
      return arrow::Type::FLOAT;
    case odbcabstraction::CDataType_DOUBLE:
      return arrow::Type::DOUBLE;
    case odbcabstraction::CDataType_BIT:
      return arrow::Type::BOOL;
    case odbcabstraction::CDataType_STINYINT:
      return arrow::Type::INT8;
    case odbcabstraction::CDataType_UTINYINT:
      return arrow::Type::UINT8;
    case odbcabstraction::CDataType_SBIGINT:
      return arrow::Type::INT64;
    case odbcabstraction::CDataType_UBIGINT:
      return arrow::Type::UINT64;
    case odbcabstraction::CDataType_BINARY:
      return arrow::Type::BINARY;
    case odbcabstraction::CDataType_NUMERIC:
      return arrow::Type::DECIMAL128;
    case odbcabstraction::CDataType_TIMESTAMP:
      return arrow::Type::TIMESTAMP;
    case odbcabstraction::CDataType_TIME:
      return arrow::Type::TIME64;
    case odbcabstraction::CDataType_DATE:
      return arrow::Type::DATE64;
    default:
      throw odbcabstraction::DriverException(std::string("Invalid target type: ") +
                                             std::to_string(data_type));
  }
}

odbcabstraction::CDataType ConvertArrowTypeToC(arrow::Type::type type_id,
                                               bool useWideChar) {
  switch (type_id) {
    case arrow::Type::STRING:
      return GetDefaultCCharType(useWideChar);
    case arrow::Type::INT16:
      return odbcabstraction::CDataType_SSHORT;
    case arrow::Type::UINT16:
      return odbcabstraction::CDataType_USHORT;
    case arrow::Type::INT32:
      return odbcabstraction::CDataType_SLONG;
    case arrow::Type::UINT32:
      return odbcabstraction::CDataType_ULONG;
    case arrow::Type::FLOAT:
      return odbcabstraction::CDataType_FLOAT;
    case arrow::Type::DOUBLE:
      return odbcabstraction::CDataType_DOUBLE;
    case arrow::Type::BOOL:
      return odbcabstraction::CDataType_BIT;
    case arrow::Type::INT8:
      return odbcabstraction::CDataType_STINYINT;
    case arrow::Type::UINT8:
      return odbcabstraction::CDataType_UTINYINT;
    case arrow::Type::INT64:
      return odbcabstraction::CDataType_SBIGINT;
    case arrow::Type::UINT64:
      return odbcabstraction::CDataType_UBIGINT;
    case arrow::Type::BINARY:
      return odbcabstraction::CDataType_BINARY;
    case arrow::Type::DECIMAL128:
      return odbcabstraction::CDataType_NUMERIC;
    case arrow::Type::DATE64:
    case arrow::Type::DATE32:
      return odbcabstraction::CDataType_DATE;
    case arrow::Type::TIME64:
    case arrow::Type::TIME32:
      return odbcabstraction::CDataType_TIME;
    case arrow::Type::TIMESTAMP:
      return odbcabstraction::CDataType_TIMESTAMP;
    default:
      throw odbcabstraction::DriverException(std::string("Invalid type id: ") +
                                             std::to_string(type_id));
  }
}

std::shared_ptr<arrow::Array> CheckConversion(const arrow::Result<arrow::Datum>& result) {
  if (result.ok()) {
    const arrow::Datum& datum = result.ValueOrDie();
    return datum.make_array();
  } else {
    throw odbcabstraction::DriverException(result.status().message());
  }
}

ArrayConvertTask GetConverter(arrow::Type::type original_type_id,
                              odbcabstraction::CDataType target_type) {
  // The else statement has a convert the works for the most case of array
  // conversion. In case, we find conversion that the default one can't handle
  // we can include some additional if-else statement with the logic to handle
  // it
  if (original_type_id == arrow::Type::STRING &&
      target_type == odbcabstraction::CDataType_TIME) {
    return [=](const std::shared_ptr<arrow::Array>& original_array) {
      arrow::compute::StrptimeOptions options("%H:%M", arrow::TimeUnit::MICRO, false);

      auto converted_result = arrow::compute::Strptime({original_array}, options);
      auto first_converted_array = CheckConversion(converted_result);

      arrow::compute::CastOptions cast_options;
      cast_options.to_type = time64(arrow::TimeUnit::MICRO);
      return CheckConversion(
          arrow::compute::CallFunction("cast", {first_converted_array}, &cast_options));
    };
  } else if (original_type_id == arrow::Type::TIME32 &&
             target_type == odbcabstraction::CDataType_TIMESTAMP) {
    return [=](const std::shared_ptr<arrow::Array>& original_array) {
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
      cast_options_2.to_type = arrow::timestamp(arrow::TimeUnit::MILLI);

      return CheckConversion(arrow::compute::Cast(third_converted_array, cast_options_2));
    };
  } else if (original_type_id == arrow::Type::TIME64 &&
             target_type == odbcabstraction::CDataType_TIMESTAMP) {
    return [=](const std::shared_ptr<arrow::Array>& original_array) {
      arrow::compute::CastOptions cast_options;
      cast_options.to_type = arrow::int64();

      auto first_converted_array =
          CheckConversion(arrow::compute::Cast(original_array, cast_options));

      auto seconds_from_epoch = GetTodayTimeFromEpoch();

      auto second_converted_array = CheckConversion(arrow::compute::Add(
          first_converted_array,
          std::make_shared<arrow::Int64Scalar>(seconds_from_epoch * 1000000000)));

      arrow::compute::CastOptions cast_options_2;
      cast_options_2.to_type = arrow::timestamp(arrow::TimeUnit::NANO);

      return CheckConversion(
          arrow::compute::Cast(second_converted_array, cast_options_2));
    };
  } else if (original_type_id == arrow::Type::STRING &&
             target_type == odbcabstraction::CDataType_DATE) {
    return [=](const std::shared_ptr<arrow::Array>& original_array) {
      // The Strptime requires a date format. Using the ISO 8601 format
      arrow::compute::StrptimeOptions options("%Y-%m-%d", arrow::TimeUnit::SECOND, false);

      auto converted_result = arrow::compute::Strptime({original_array}, options);

      auto first_converted_array = CheckConversion(converted_result);
      arrow::compute::CastOptions cast_options;
      cast_options.to_type = arrow::date64();
      return CheckConversion(
          arrow::compute::CallFunction("cast", {first_converted_array}, &cast_options));
    };
  } else if (original_type_id == arrow::Type::DECIMAL128 &&
             (target_type == odbcabstraction::CDataType_CHAR ||
              target_type == odbcabstraction::CDataType_WCHAR)) {
    return [=](const std::shared_ptr<arrow::Array>& original_array) {
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
             (target_type == odbcabstraction::CDataType_CHAR ||
              target_type == odbcabstraction::CDataType_WCHAR)) {
    return [=](const std::shared_ptr<arrow::Array>& original_array) {
      const auto& json_conversion_result = ConvertToJson(original_array);
      ThrowIfNotOK(json_conversion_result.status());
      return json_conversion_result.ValueOrDie();
    };
  } else {
    // Default converter
    return [=](const std::shared_ptr<arrow::Array>& original_array) {
      const arrow::Type::type& target_arrow_type_id = ConvertCToArrowType(target_type);
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

int32_t GetDecimalTypeScale(const std::shared_ptr<arrow::DataType>& decimalType) {
  auto decimal128Type = std::dynamic_pointer_cast<arrow::Decimal128Type>(decimalType);
  return decimal128Type->scale();
}

int32_t GetDecimalTypePrecision(const std::shared_ptr<arrow::DataType>& decimalType) {
  auto decimal128Type = std::dynamic_pointer_cast<arrow::Decimal128Type>(decimalType);
  return decimal128Type->precision();
}

}  // namespace flight_sql
}  // namespace driver
