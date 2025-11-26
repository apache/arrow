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

#include <boost/variant.hpp>
#include <boost/xpressive/xpressive.hpp>

#include <codecvt>
#include <functional>
#include <optional>
#include "arrow/flight/sql/odbc/odbc_impl/exceptions.h"
#include "arrow/flight/sql/odbc/odbc_impl/spi/connection.h"
#include "arrow/flight/sql/odbc/odbc_impl/types.h"
#include "arrow/flight/types.h"
#include "arrow/util/utf8.h"

#define CONVERT_WIDE_STR(wstring_var, utf8_target)                                \
  wstring_var = [&] {                                                             \
    arrow::Result<std::wstring> res = arrow::util::UTF8ToWideString(utf8_target); \
    arrow::flight::sql::odbc::util::ThrowIfNotOK(res.status());                   \
    return res.ValueOrDie();                                                      \
  }()

#define CONVERT_UTF8_STR(string_var, wide_str_target)                                \
  string_var = [&] {                                                                 \
    arrow::Result<std::string> res = arrow::util::WideStringToUTF8(wide_str_target); \
    arrow::flight::sql::odbc::util::ThrowIfNotOK(res.status());                      \
    return res.ValueOrDie();                                                         \
  }()

#define CONVERT_WIDE_STR(wstring_var, utf8_target)                                \
  wstring_var = [&] {                                                             \
    arrow::Result<std::wstring> res = arrow::util::UTF8ToWideString(utf8_target); \
    arrow::flight::sql::odbc::util::ThrowIfNotOK(res.status());                   \
    return res.ValueOrDie();                                                      \
  }()

#define CONVERT_UTF8_STR(string_var, wide_str_target)                                \
  string_var = [&] {                                                                 \
    arrow::Result<std::string> res = arrow::util::WideStringToUTF8(wide_str_target); \
    arrow::flight::sql::odbc::util::ThrowIfNotOK(res.status());                      \
    return res.ValueOrDie();                                                         \
  }()

namespace arrow::flight::sql::odbc {
namespace util {

typedef std::function<std::shared_ptr<Array>(const std::shared_ptr<Array>&)>
    ArrayConvertTask;

using std::optional;

inline void ThrowIfNotOK(const Status& status) {
  if (!status.ok()) {
    throw DriverException(status.message());
  }
}

template <typename T, typename AttributeTypeT>
inline bool CheckIfSetToOnlyValidValue(const AttributeTypeT& value, T allowed_value) {
  return std::get<T>(value) == allowed_value;
}

template <typename BUILDER, typename T>
Status AppendToBuilder(BUILDER& builder, optional<T> opt_value) {
  if (opt_value) {
    return builder.Append(*opt_value);
  } else {
    return builder.AppendNull();
  }
}

template <typename BUILDER, typename T>
Status AppendToBuilder(BUILDER& builder, T value) {
  return builder.Append(value);
}

SqlDataType GetDataTypeFromArrowFieldV3(const std::shared_ptr<Field>& field,
                                        bool use_wide_char);

SqlDataType EnsureRightSqlCharType(SqlDataType data_type, bool use_wide_char);

int16_t ConvertSqlDataTypeFromV3ToV2(int16_t data_type_v3);

CDataType ConvertCDataTypeFromV2ToV3(int16_t data_type_v2);

std::string GetTypeNameFromSqlDataType(int16_t data_type);

optional<int16_t> GetRadixFromSqlDataType(SqlDataType data_type);

int16_t GetNonConciseDataType(SqlDataType data_type);

optional<int16_t> GetSqlDateTimeSubCode(SqlDataType data_type);

optional<int32_t> GetCharOctetLength(SqlDataType data_type,
                                     const arrow::Result<int32_t>& column_size,
                                     const int32_t decimal_precison = 0);

optional<int32_t> GetBufferLength(SqlDataType data_type,
                                  const optional<int32_t>& column_size);

optional<int32_t> GetLength(SqlDataType data_type, const optional<int32_t>& column_size);

optional<int32_t> GetTypeScale(SqlDataType data_type,
                               const optional<int32_t>& type_scale);

optional<int32_t> GetColumnSize(SqlDataType data_type,
                                const optional<int32_t>& column_size);

optional<int32_t> GetDisplaySize(SqlDataType data_type,
                                 const optional<int32_t>& column_size);

std::string ConvertSqlPatternToRegexString(const std::string& pattern);

boost::xpressive::sregex ConvertSqlPatternToRegex(const std::string& pattern);

bool NeedArrayConversion(Type::type original_type_id, CDataType data_type);

std::shared_ptr<DataType> GetDefaultDataTypeForTypeId(Type::type type_id);

Type::type ConvertCToArrowType(CDataType data_type);

CDataType ConvertArrowTypeToC(Type::type type_id, bool use_wide_char);

std::shared_ptr<Array> CheckConversion(const arrow::Result<Datum>& result);

ArrayConvertTask GetConverter(Type::type original_type_id, CDataType target_type);

std::string ConvertToDBMSVer(const std::string& str);

int32_t GetDecimalTypeScale(const std::shared_ptr<DataType>& decimal_type);

int32_t GetDecimalTypePrecision(const std::shared_ptr<DataType>& decimal_type);

/// Parse a string value to a boolean.
/// \param value            the value to be parsed.
/// \return                 the parsed valued.
std::optional<bool> AsBool(const std::string& value);

/// Looks up for a value inside the ConnPropertyMap and then try to parse it.
/// In case it does not find or it cannot parse, the default value will be returned.
/// \param conn_property_map    the map with the connection properties.
/// \param property_name      the name of the property that will be looked up.
/// \return                   the parsed valued.
std::optional<bool> AsBool(const Connection::ConnPropertyMap& conn_property_map,
                           std::string_view property_name);

/// Looks up for a value inside the ConnPropertyMap and then try to parse it.
/// In case it does not find or it cannot parse, the default value will be returned.
/// \param min_value                    the minimum value to be parsed, else the default
/// value is returned. \param conn_property_map              the map with the connection
/// properties. \param property_name                the name of the property that will be
/// looked up. \return                             the parsed valued. \exception
/// std::invalid_argument    exception from std::stoi \exception
/// std::out_of_range        exception from std::stoi
std::optional<int32_t> AsInt32(int32_t min_value,
                               const Connection::ConnPropertyMap& conn_property_map,
                               std::string_view property_name);

}  // namespace util
}  // namespace arrow::flight::sql::odbc
