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

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h>
#include <arrow/flight/types.h>
#include <boost/xpressive/xpressive.hpp>
#include <codecvt>
#include <functional>
#include <optional>

namespace driver {
namespace flight_sql {

typedef std::function<std::shared_ptr<arrow::Array>(const std::shared_ptr<arrow::Array>&)>
    ArrayConvertTask;

using std::optional;

inline void ThrowIfNotOK(const arrow::Status& status) {
  if (!status.ok()) {
    throw odbcabstraction::DriverException(status.message());
  }
}

template <typename T, typename AttributeTypeT>
inline bool CheckIfSetToOnlyValidValue(const AttributeTypeT& value, T allowed_value) {
  return boost::get<T>(value) == allowed_value;
}

template <typename BUILDER, typename T>
arrow::Status AppendToBuilder(BUILDER& builder, optional<T> opt_value) {
  if (opt_value) {
    return builder.Append(*opt_value);
  } else {
    return builder.AppendNull();
  }
}

template <typename BUILDER, typename T>
arrow::Status AppendToBuilder(BUILDER& builder, T value) {
  return builder.Append(value);
}

odbcabstraction::SqlDataType GetDataTypeFromArrowField_V3(
    const std::shared_ptr<arrow::Field>& field, bool useWideChar);

odbcabstraction::SqlDataType EnsureRightSqlCharType(
    odbcabstraction::SqlDataType data_type, bool useWideChar);

int16_t ConvertSqlDataTypeFromV3ToV2(int16_t data_type_v3);

odbcabstraction::CDataType ConvertCDataTypeFromV2ToV3(int16_t data_type_v2);

std::string GetTypeNameFromSqlDataType(int16_t data_type);

optional<int16_t> GetRadixFromSqlDataType(odbcabstraction::SqlDataType data_type);

int16_t GetNonConciseDataType(odbcabstraction::SqlDataType data_type);

optional<int16_t> GetSqlDateTimeSubCode(odbcabstraction::SqlDataType data_type);

optional<int32_t> GetCharOctetLength(odbcabstraction::SqlDataType data_type,
                                     const arrow::Result<int32_t>& column_size,
                                     const int32_t decimal_precison = 0);

optional<int32_t> GetBufferLength(odbcabstraction::SqlDataType data_type,
                                  const optional<int32_t>& column_size);

optional<int32_t> GetLength(odbcabstraction::SqlDataType data_type,
                            const optional<int32_t>& column_size);

optional<int32_t> GetTypeScale(odbcabstraction::SqlDataType data_type,
                               const optional<int32_t>& type_scale);

optional<int32_t> GetColumnSize(odbcabstraction::SqlDataType data_type,
                                const optional<int32_t>& column_size);

optional<int32_t> GetDisplaySize(odbcabstraction::SqlDataType data_type,
                                 const optional<int32_t>& column_size);

std::string ConvertSqlPatternToRegexString(const std::string& pattern);

boost::xpressive::sregex ConvertSqlPatternToRegex(const std::string& pattern);

bool NeedArrayConversion(arrow::Type::type original_type_id,
                         odbcabstraction::CDataType data_type);

std::shared_ptr<arrow::DataType> GetDefaultDataTypeForTypeId(arrow::Type::type type_id);

arrow::Type::type ConvertCToArrowType(odbcabstraction::CDataType data_type);

odbcabstraction::CDataType ConvertArrowTypeToC(arrow::Type::type type_id,
                                               bool useWideChar);

std::shared_ptr<arrow::Array> CheckConversion(const arrow::Result<arrow::Datum>& result);

ArrayConvertTask GetConverter(arrow::Type::type original_type_id,
                              odbcabstraction::CDataType target_type);

std::string ConvertToDBMSVer(const std::string& str);

int32_t GetDecimalTypeScale(const std::shared_ptr<arrow::DataType>& decimalType);

int32_t GetDecimalTypePrecision(const std::shared_ptr<arrow::DataType>& decimalType);

}  // namespace flight_sql
}  // namespace driver
