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

#pragma once

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h>
#include <boost/optional.hpp>

namespace driver {
namespace odbcabstraction {

/// \brief Supported ODBC versions.
enum OdbcVersion { V_2, V_3, V_4 };

// Based on ODBC sql.h and sqlext.h definitions.
enum SqlDataType : int16_t {
  SqlDataType_CHAR = 1,
  SqlDataType_VARCHAR = 12,
  SqlDataType_LONGVARCHAR = (-1),
  SqlDataType_WCHAR = (-8),
  SqlDataType_WVARCHAR = (-9),
  SqlDataType_WLONGVARCHAR = (-10),
  SqlDataType_DECIMAL = 3,
  SqlDataType_NUMERIC = 2,
  SqlDataType_SMALLINT = 5,
  SqlDataType_INTEGER = 4,
  SqlDataType_REAL = 7,
  SqlDataType_FLOAT = 6,
  SqlDataType_DOUBLE = 8,
  SqlDataType_BIT = (-7),
  SqlDataType_TINYINT = (-6),
  SqlDataType_BIGINT = (-5),
  SqlDataType_BINARY = (-2),
  SqlDataType_VARBINARY = (-3),
  SqlDataType_LONGVARBINARY = (-4),
  SqlDataType_TYPE_DATE = 91,
  SqlDataType_TYPE_TIME = 92,
  SqlDataType_TYPE_TIMESTAMP = 93,
  SqlDataType_INTERVAL_MONTH = (100 + 2),
  SqlDataType_INTERVAL_YEAR = (100 + 1),
  SqlDataType_INTERVAL_YEAR_TO_MONTH = (100 + 7),
  SqlDataType_INTERVAL_DAY = (100 + 3),
  SqlDataType_INTERVAL_HOUR = (100 + 4),
  SqlDataType_INTERVAL_MINUTE = (100 + 5),
  SqlDataType_INTERVAL_SECOND = (100 + 6),
  SqlDataType_INTERVAL_DAY_TO_HOUR = (100 + 8),
  SqlDataType_INTERVAL_DAY_TO_MINUTE = (100 + 9),
  SqlDataType_INTERVAL_DAY_TO_SECOND = (100 + 10),
  SqlDataType_INTERVAL_HOUR_TO_MINUTE = (100 + 11),
  SqlDataType_INTERVAL_HOUR_TO_SECOND = (100 + 12),
  SqlDataType_INTERVAL_MINUTE_TO_SECOND = (100 + 13),
  SqlDataType_GUID = (-11),
};

enum SqlDateTimeSubCode : int16_t {
  SqlDateTimeSubCode_DATE = 1,
  SqlDateTimeSubCode_TIME = 2,
  SqlDateTimeSubCode_TIMESTAMP = 3,
  SqlDateTimeSubCode_YEAR = 1,
  SqlDateTimeSubCode_MONTH = 2,
  SqlDateTimeSubCode_DAY = 3,
  SqlDateTimeSubCode_HOUR = 4,
  SqlDateTimeSubCode_MINUTE = 5,
  SqlDateTimeSubCode_SECOND = 6,
  SqlDateTimeSubCode_YEAR_TO_MONTH = 7,
  SqlDateTimeSubCode_DAY_TO_HOUR = 8,
  SqlDateTimeSubCode_DAY_TO_MINUTE = 9,
  SqlDateTimeSubCode_DAY_TO_SECOND = 10,
  SqlDateTimeSubCode_HOUR_TO_MINUTE = 11,
  SqlDateTimeSubCode_HOUR_TO_SECOND = 12,
  SqlDateTimeSubCode_MINUTE_TO_SECOND = 13,
};

// Based on ODBC sql.h and sqlext.h definitions.
enum CDataType {
  CDataType_CHAR = 1,
  CDataType_WCHAR = -8,
  CDataType_SSHORT = (5 + (-20)),
  CDataType_USHORT = (5 + (-22)),
  CDataType_SLONG = (4 + (-20)),
  CDataType_ULONG = (4 + (-22)),
  CDataType_FLOAT = 7,
  CDataType_DOUBLE = 8,
  CDataType_BIT = -7,
  CDataType_DATE = 91,
  CDataType_TIME = 92,
  CDataType_TIMESTAMP = 93,
  CDataType_STINYINT = ((-6) + (-20)),
  CDataType_UTINYINT = ((-6) + (-22)),
  CDataType_SBIGINT = ((-5) + (-20)),
  CDataType_UBIGINT = ((-5) + (-22)),
  CDataType_BINARY = (-2),
  CDataType_NUMERIC = 2,
  CDataType_DEFAULT = 99,
};

enum Nullability {
  NULLABILITY_NO_NULLS = 0,
  NULLABILITY_NULLABLE = 1,
  NULLABILITY_UNKNOWN = 2,
};

enum Searchability {
  SEARCHABILITY_NONE = 0,
  SEARCHABILITY_LIKE_ONLY = 1,
  SEARCHABILITY_ALL_EXPECT_LIKE = 2,
  SEARCHABILITY_ALL = 3,
};

enum Updatability {
  UPDATABILITY_READONLY = 0,
  UPDATABILITY_WRITE = 1,
  UPDATABILITY_READWRITE_UNKNOWN = 2,
};

constexpr ssize_t NULL_DATA = -1;
constexpr ssize_t NO_TOTAL = -4;
constexpr ssize_t ALL_TYPES = 0;
constexpr ssize_t DAYS_TO_SECONDS_MULTIPLIER = 86400;
constexpr ssize_t MILLI_TO_SECONDS_DIVISOR = 1000;
constexpr ssize_t MICRO_TO_SECONDS_DIVISOR = 1000000;
constexpr ssize_t NANO_TO_SECONDS_DIVISOR = 1000000000;

typedef struct tagDATE_STRUCT {
  int16_t year;
  uint16_t month;
  uint16_t day;
} DATE_STRUCT;

typedef struct tagTIME_STRUCT {
  uint16_t hour;
  uint16_t minute;
  uint16_t second;
} TIME_STRUCT;

typedef struct tagTIMESTAMP_STRUCT {
  int16_t year;
  uint16_t month;
  uint16_t day;
  uint16_t hour;
  uint16_t minute;
  uint16_t second;
  uint32_t fraction;
} TIMESTAMP_STRUCT;

typedef struct tagNUMERIC_STRUCT {
  uint8_t precision;
  int8_t scale;
  uint8_t sign;     // The sign field is 1 if positive, 0 if negative.
  uint8_t val[16];  //[e], [f]
} NUMERIC_STRUCT;

enum RowStatus : uint16_t {
  RowStatus_SUCCESS = 0,            // Same as SQL_ROW_SUCCESS
  RowStatus_SUCCESS_WITH_INFO = 6,  // Same as SQL_ROW_SUCCESS_WITH_INFO
  RowStatus_ERROR = 5,              // Same as SQL_ROW_ERROR
  RowStatus_NOROW = 3               // Same as SQL_ROW_NOROW
};

struct MetadataSettings {
  boost::optional<int32_t> string_column_length_{boost::none};
  size_t chunk_buffer_capacity_;
  bool use_wide_char_;
};

}  // namespace odbcabstraction
}  // namespace driver
