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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_result_set_metadata.h"
#include "arrow/flight/sql/column_metadata.h"
#include "arrow/flight/sql/odbc/flight_sql/util.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/util/key_value_metadata.h"

#include <utility>
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"

namespace arrow::flight::sql::odbc {

using arrow::DataType;
using arrow::Field;
using util::GetCharOctetLength;
using util::GetDataTypeFromArrowFieldV3;

using std::make_optional;
using std::nullopt;

constexpr int32_t DefaultDecimalPrecision = 38;

// This indicates the column length used when the both property StringColumnLength is not
// specified and the server does not provide a length on column metadata.
constexpr int32_t DefaultLengthForVariableLengthColumns = 1024;

namespace {
inline arrow::flight::sql::ColumnMetadata GetMetadata(
    const std::shared_ptr<Field>& field) {
  arrow::flight::sql::ColumnMetadata metadata(field->metadata());
  return metadata;
}

arrow::Result<int32_t> GetFieldPrecision(const std::shared_ptr<Field>& field) {
  return GetMetadata(field).GetPrecision();
}
}  // namespace

size_t FlightSqlResultSetMetadata::GetColumnCount() { return schema_->num_fields(); }

std::string FlightSqlResultSetMetadata::GetColumnName(int column_position) {
  return schema_->field(column_position - 1)->name();
}

std::string FlightSqlResultSetMetadata::GetName(int column_position) {
  return schema_->field(column_position - 1)->name();
}

size_t FlightSqlResultSetMetadata::GetPrecision(int column_position) {
  const std::shared_ptr<Field>& field = schema_->field(column_position - 1);

  int32_t column_size = GetFieldPrecision(field).ValueOrElse([] { return 0; });
  SqlDataType data_type_v3 =
      GetDataTypeFromArrowFieldV3(field, metadata_settings_.use_wide_char);

  return util::GetColumnSize(data_type_v3, column_size).value_or(0);
}

size_t FlightSqlResultSetMetadata::GetScale(int column_position) {
  const std::shared_ptr<Field>& field = schema_->field(column_position - 1);
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(field);

  int32_t type_scale = metadata.GetScale().ValueOrElse([] { return 0; });
  SqlDataType data_type_v3 =
      GetDataTypeFromArrowFieldV3(field, metadata_settings_.use_wide_char);

  return util::GetTypeScale(data_type_v3, type_scale).value_or(0);
}

uint16_t FlightSqlResultSetMetadata::GetDataType(int column_position) {
  const std::shared_ptr<Field>& field = schema_->field(column_position - 1);
  const SqlDataType concise_type =
      GetDataTypeFromArrowFieldV3(field, metadata_settings_.use_wide_char);
  return util::GetNonConciseDataType(concise_type);
}

arrow::flight::sql::odbc::Nullability FlightSqlResultSetMetadata::IsNullable(
    int column_position) {
  const std::shared_ptr<Field>& field = schema_->field(column_position - 1);
  return field->nullable() ? NULLABILITY_NULLABLE : NULLABILITY_NO_NULLS;
}

std::string FlightSqlResultSetMetadata::GetSchemaName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata =
      GetMetadata(schema_->field(column_position - 1));

  return metadata.GetSchemaName().ValueOrElse([] { return ""; });
}

std::string FlightSqlResultSetMetadata::GetCatalogName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata =
      GetMetadata(schema_->field(column_position - 1));

  return metadata.GetCatalogName().ValueOrElse([] { return ""; });
}

std::string FlightSqlResultSetMetadata::GetTableName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata =
      GetMetadata(schema_->field(column_position - 1));

  return metadata.GetTableName().ValueOrElse([] { return ""; });
}

std::string FlightSqlResultSetMetadata::GetColumnLabel(int column_position) {
  return schema_->field(column_position - 1)->name();
}

size_t FlightSqlResultSetMetadata::GetColumnDisplaySize(int column_position) {
  const std::shared_ptr<Field>& field = schema_->field(column_position - 1);

  int32_t column_size = metadata_settings_.string_column_length.value_or(
      GetFieldPrecision(field).ValueOr(DefaultLengthForVariableLengthColumns));
  SqlDataType data_type_v3 =
      GetDataTypeFromArrowFieldV3(field, metadata_settings_.use_wide_char);

  return util::GetDisplaySize(data_type_v3, column_size).value_or(NO_TOTAL);
}

std::string FlightSqlResultSetMetadata::GetBaseColumnName(int column_position) {
  return schema_->field(column_position - 1)->name();
}

std::string FlightSqlResultSetMetadata::GetBaseTableName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata =
      GetMetadata(schema_->field(column_position - 1));
  return metadata.GetTableName().ValueOrElse([] { return ""; });
}

uint16_t FlightSqlResultSetMetadata::GetConciseType(int column_position) {
  const std::shared_ptr<Field>& field = schema_->field(column_position - 1);

  const SqlDataType sqlColumnType =
      GetDataTypeFromArrowFieldV3(field, metadata_settings_.use_wide_char);
  return sqlColumnType;
}

size_t FlightSqlResultSetMetadata::GetLength(int column_position) {
  const std::shared_ptr<Field>& field = schema_->field(column_position - 1);

  int32_t column_size = metadata_settings_.string_column_length.value_or(
      GetFieldPrecision(field).ValueOr(DefaultLengthForVariableLengthColumns));
  SqlDataType data_type_v3 =
      GetDataTypeFromArrowFieldV3(field, metadata_settings_.use_wide_char);

  return util::GetLength(data_type_v3, column_size)
      .value_or(DefaultLengthForVariableLengthColumns);
}

std::string FlightSqlResultSetMetadata::GetLiteralPrefix(int column_position) {
  // TODO: Flight SQL column metadata does not have this, should we add to the spec?
  return "";
}

std::string FlightSqlResultSetMetadata::GetLiteralSuffix(int column_position) {
  // TODO: Flight SQL column metadata does not have this, should we add to the spec?
  return "";
}

std::string FlightSqlResultSetMetadata::GetLocalTypeName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata =
      GetMetadata(schema_->field(column_position - 1));

  // TODO: Is local type name the same as type name?
  return metadata.GetTypeName().ValueOrElse([] { return ""; });
}

size_t FlightSqlResultSetMetadata::GetNumPrecRadix(int column_position) {
  const std::shared_ptr<Field>& field = schema_->field(column_position - 1);
  SqlDataType data_type_v3 =
      GetDataTypeFromArrowFieldV3(field, metadata_settings_.use_wide_char);

  return util::GetRadixFromSqlDataType(data_type_v3).value_or(NO_TOTAL);
}

size_t FlightSqlResultSetMetadata::GetOctetLength(int column_position) {
  const std::shared_ptr<Field>& field = schema_->field(column_position - 1);
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(field);

  int32_t column_size = metadata_settings_.string_column_length.value_or(
      GetFieldPrecision(field).ValueOr(DefaultLengthForVariableLengthColumns));
  SqlDataType data_type_v3 =
      GetDataTypeFromArrowFieldV3(field, metadata_settings_.use_wide_char);

  // Workaround to get the precision for Decimal and Numeric types, since server doesn't
  // return it currently.
  // TODO: Use the server precision when its fixed.
  std::shared_ptr<DataType> arrow_type = field->type();
  if (arrow_type->id() == arrow::Type::DECIMAL128) {
    int32_t precision = util::GetDecimalTypePrecision(arrow_type);
    return GetCharOctetLength(data_type_v3, column_size, precision)
        .value_or(DefaultDecimalPrecision + 2);
  }

  return GetCharOctetLength(data_type_v3, column_size)
      .value_or(DefaultLengthForVariableLengthColumns);
}

std::string FlightSqlResultSetMetadata::GetTypeName(int column_position, int data_type) {
  arrow::flight::sql::ColumnMetadata metadata =
      GetMetadata(schema_->field(column_position - 1));

  return metadata.GetTypeName().ValueOrElse([data_type] {
    // If we get an empty type name, figure out the type name from the data_type.
    return util::GetTypeNameFromSqlDataType(data_type);
  });
}

arrow::flight::sql::odbc::Updatability FlightSqlResultSetMetadata::GetUpdatable(
    int column_position) {
  return UPDATABILITY_READWRITE_UNKNOWN;
}

bool FlightSqlResultSetMetadata::IsAutoUnique(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata =
      GetMetadata(schema_->field(column_position - 1));

  // TODO: Is AutoUnique equivalent to AutoIncrement?
  return metadata.GetIsAutoIncrement().ValueOrElse([] { return false; });
}

bool FlightSqlResultSetMetadata::IsCaseSensitive(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata =
      GetMetadata(schema_->field(column_position - 1));

  return metadata.GetIsCaseSensitive().ValueOrElse([] { return false; });
}

arrow::flight::sql::odbc::Searchability FlightSqlResultSetMetadata::IsSearchable(
    int column_position) {
  arrow::flight::sql::ColumnMetadata metadata =
      GetMetadata(schema_->field(column_position - 1));

  bool is_searchable = metadata.GetIsSearchable().ValueOrElse([] { return false; });
  return is_searchable ? SEARCHABILITY_ALL : SEARCHABILITY_NONE;
}

bool FlightSqlResultSetMetadata::IsUnsigned(int column_position) {
  const std::shared_ptr<Field>& field = schema_->field(column_position - 1);

  switch (field->type()->id()) {
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
    case arrow::Type::INT64:
    case arrow::Type::DOUBLE:
    case arrow::Type::FLOAT:
    case arrow::Type::HALF_FLOAT:
    case arrow::Type::DECIMAL32:
    case arrow::Type::DECIMAL64:
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256:
      return false;
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
    default:
      return true;
  }
}

bool FlightSqlResultSetMetadata::IsFixedPrecScale(int column_position) {
  // Precision for Arrow data types are modifiable by the user
  return false;
}

FlightSqlResultSetMetadata::FlightSqlResultSetMetadata(
    std::shared_ptr<arrow::Schema> schema, const MetadataSettings& metadata_settings)
    : metadata_settings_(metadata_settings), schema_(std::move(schema)) {}

FlightSqlResultSetMetadata::FlightSqlResultSetMetadata(
    const std::shared_ptr<arrow::flight::FlightInfo>& flight_info,
    const MetadataSettings& metadata_settings)
    : metadata_settings_(metadata_settings) {
  arrow::ipc::DictionaryMemo dict_memo;

  util::ThrowIfNotOK(flight_info->GetSchema(&dict_memo).Value(&schema_));
}

}  // namespace arrow::flight::sql::odbc
