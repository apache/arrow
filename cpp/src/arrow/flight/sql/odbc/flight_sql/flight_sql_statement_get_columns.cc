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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_statement_get_columns.h"
#include "arrow/flight/sql/column_metadata.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_get_tables_reader.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"

namespace driver {
namespace flight_sql {

using arrow::Schema;
using arrow::flight::sql::ColumnMetadata;
using std::make_optional;
using std::nullopt;
using std::optional;

namespace {
std::shared_ptr<Schema> GetColumns_V3_Schema() {
  return arrow::schema({
      field("TABLE_CAT", arrow::utf8()),
      field("TABLE_SCHEM", arrow::utf8()),
      field("TABLE_NAME", arrow::utf8()),
      field("COLUMN_NAME", arrow::utf8()),
      field("DATA_TYPE", arrow::int16()),
      field("TYPE_NAME", arrow::utf8()),
      field("COLUMN_SIZE", arrow::int32()),
      field("BUFFER_LENGTH", arrow::int32()),
      field("DECIMAL_DIGITS", arrow::int16()),
      field("NUM_PREC_RADIX", arrow::int16()),
      field("NULLABLE", arrow::int16()),
      field("REMARKS", arrow::utf8()),
      field("COLUMN_DEF", arrow::utf8()),
      field("SQL_DATA_TYPE", arrow::int16()),
      field("SQL_DATETIME_SUB", arrow::int16()),
      field("CHAR_OCTET_LENGTH", arrow::int32()),
      field("ORDINAL_POSITION", arrow::int32()),
      field("IS_NULLABLE", arrow::utf8()),
  });
}

std::shared_ptr<Schema> GetColumns_V2_Schema() {
  return arrow::schema({
      field("TABLE_QUALIFIER", arrow::utf8()),
      field("TABLE_OWNER", arrow::utf8()),
      field("TABLE_NAME", arrow::utf8()),
      field("COLUMN_NAME", arrow::utf8()),
      field("DATA_TYPE", arrow::int16()),
      field("TYPE_NAME", arrow::utf8()),
      field("PRECISION", arrow::int32()),
      field("LENGTH", arrow::int32()),
      field("SCALE", arrow::int16()),
      field("RADIX", arrow::int16()),
      field("NULLABLE", arrow::int16()),
      field("REMARKS", arrow::utf8()),
      field("COLUMN_DEF", arrow::utf8()),
      field("SQL_DATA_TYPE", arrow::int16()),
      field("SQL_DATETIME_SUB", arrow::int16()),
      field("CHAR_OCTET_LENGTH", arrow::int32()),
      field("ORDINAL_POSITION", arrow::int32()),
      field("IS_NULLABLE", arrow::utf8()),
  });
}

Result<std::shared_ptr<RecordBatch>> Transform_inner(
    const odbcabstraction::OdbcVersion odbc_version,
    const std::shared_ptr<RecordBatch>& original,
    const optional<std::string>& column_name_pattern,
    const MetadataSettings& metadata_settings) {
  GetColumns_RecordBatchBuilder builder(odbc_version);
  GetColumns_RecordBatchBuilder::Data data;

  GetTablesReader reader(original);

  optional<boost::xpressive::sregex> column_name_regex =
      column_name_pattern ? make_optional(ConvertSqlPatternToRegex(*column_name_pattern))
                          : nullopt;

  while (reader.Next()) {
    const auto& table_catalog = reader.GetCatalogName();
    const auto& table_schema = reader.GetDbSchemaName();
    const auto& table_name = reader.GetTableName();
    const std::shared_ptr<Schema>& schema = reader.GetSchema();
    if (schema == nullptr) {
      // TODO: Remove this if after fixing TODO on GetTablesReader::GetSchema()
      // This is because of a problem on Dremio server, where complex types columns
      // are being returned without the children types, so we are simply ignoring
      // it by now.
      continue;
    }
    for (int i = 0; i < schema->num_fields(); ++i) {
      const std::shared_ptr<Field>& field = schema->field(i);

      if (column_name_regex &&
          !boost::xpressive::regex_match(field->name(), *column_name_regex)) {
        continue;
      }

      odbcabstraction::SqlDataType data_type_v3 =
          GetDataTypeFromArrowField_V3(field, metadata_settings.use_wide_char_);

      ColumnMetadata metadata(field->metadata());

      data.table_cat = table_catalog;
      data.table_schem = table_schema;
      data.table_name = table_name;
      data.column_name = field->name();
      data.data_type = odbc_version == odbcabstraction::V_3
                           ? data_type_v3
                           : ConvertSqlDataTypeFromV3ToV2(data_type_v3);

      // TODO: Use `metadata.GetTypeName()` when ARROW-16064 is merged.
      const auto& type_name_result = field->metadata()->Get("ARROW:FLIGHT:SQL:TYPE_NAME");
      data.type_name = type_name_result.ok() ? type_name_result.ValueOrDie()
                                             : GetTypeNameFromSqlDataType(data_type_v3);

      const Result<int32_t>& precision_result = metadata.GetPrecision();
      data.column_size =
          precision_result.ok() ? make_optional(precision_result.ValueOrDie()) : nullopt;
      data.char_octet_length = GetCharOctetLength(data_type_v3, precision_result);

      data.buffer_length = GetBufferLength(data_type_v3, data.column_size);

      const Result<int32_t>& scale_result = metadata.GetScale();
      data.decimal_digits =
          scale_result.ok() ? make_optional(scale_result.ValueOrDie()) : nullopt;
      data.num_prec_radix = GetRadixFromSqlDataType(data_type_v3);
      data.nullable = field->nullable();
      data.remarks = nullopt;
      data.column_def = nullopt;
      data.sql_data_type = GetNonConciseDataType(data_type_v3);
      data.sql_datetime_sub = GetSqlDateTimeSubCode(data_type_v3);
      data.ordinal_position = i + 1;
      data.is_nullable = field->nullable() ? "YES" : "NO";

      ARROW_RETURN_NOT_OK(builder.Append(data));
    }
  }

  return builder.Build();
}
}  // namespace

GetColumns_RecordBatchBuilder::GetColumns_RecordBatchBuilder(
    odbcabstraction::OdbcVersion odbc_version)
    : odbc_version_(odbc_version) {}

Result<std::shared_ptr<RecordBatch>> GetColumns_RecordBatchBuilder::Build() {
  ARROW_ASSIGN_OR_RAISE(auto TABLE_CAT_Array, TABLE_CAT_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TABLE_SCHEM_Array, TABLE_SCHEM_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TABLE_NAME_Array, TABLE_NAME_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_NAME_Array, COLUMN_NAME_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto DATA_TYPE_Array, DATA_TYPE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto TYPE_NAME_Array, TYPE_NAME_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_SIZE_Array, COLUMN_SIZE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto BUFFER_LENGTH_Array, BUFFER_LENGTH_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto DECIMAL_DIGITS_Array, DECIMAL_DIGITS_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto NUM_PREC_RADIX_Array, NUM_PREC_RADIX_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto NULLABLE_Array, NULLABLE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto REMARKS_Array, REMARKS_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_DEF_Array, COLUMN_DEF_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SQL_DATA_TYPE_Array, SQL_DATA_TYPE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SQL_DATETIME_SUB_Array, SQL_DATETIME_SUB_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto CHAR_OCTET_LENGTH_Array, CHAR_OCTET_LENGTH_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto ORDINAL_POSITION_Array, ORDINAL_POSITION_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto IS_NULLABLE_Array, IS_NULLABLE_Builder_.Finish())

  std::vector<std::shared_ptr<Array>> arrays = {
      TABLE_CAT_Array,         TABLE_SCHEM_Array,      TABLE_NAME_Array,
      COLUMN_NAME_Array,       DATA_TYPE_Array,        TYPE_NAME_Array,
      COLUMN_SIZE_Array,       BUFFER_LENGTH_Array,    DECIMAL_DIGITS_Array,
      NUM_PREC_RADIX_Array,    NULLABLE_Array,         REMARKS_Array,
      COLUMN_DEF_Array,        SQL_DATA_TYPE_Array,    SQL_DATETIME_SUB_Array,
      CHAR_OCTET_LENGTH_Array, ORDINAL_POSITION_Array, IS_NULLABLE_Array};

  const std::shared_ptr<Schema>& schema = odbc_version_ == odbcabstraction::V_3
                                              ? GetColumns_V3_Schema()
                                              : GetColumns_V2_Schema();
  return RecordBatch::Make(schema, num_rows_, arrays);
}

Status GetColumns_RecordBatchBuilder::Append(
    const GetColumns_RecordBatchBuilder::Data& data) {
  ARROW_RETURN_NOT_OK(AppendToBuilder(TABLE_CAT_Builder_, data.table_cat));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TABLE_SCHEM_Builder_, data.table_schem));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TABLE_NAME_Builder_, data.table_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_NAME_Builder_, data.column_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(DATA_TYPE_Builder_, data.data_type));
  ARROW_RETURN_NOT_OK(AppendToBuilder(TYPE_NAME_Builder_, data.type_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_SIZE_Builder_, data.column_size));
  ARROW_RETURN_NOT_OK(AppendToBuilder(BUFFER_LENGTH_Builder_, data.buffer_length));
  ARROW_RETURN_NOT_OK(AppendToBuilder(DECIMAL_DIGITS_Builder_, data.decimal_digits));
  ARROW_RETURN_NOT_OK(AppendToBuilder(NUM_PREC_RADIX_Builder_, data.num_prec_radix));
  ARROW_RETURN_NOT_OK(AppendToBuilder(NULLABLE_Builder_, data.nullable));
  ARROW_RETURN_NOT_OK(AppendToBuilder(REMARKS_Builder_, data.remarks));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_DEF_Builder_, data.column_def));
  ARROW_RETURN_NOT_OK(AppendToBuilder(SQL_DATA_TYPE_Builder_, data.sql_data_type));
  ARROW_RETURN_NOT_OK(AppendToBuilder(SQL_DATETIME_SUB_Builder_, data.sql_datetime_sub));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(CHAR_OCTET_LENGTH_Builder_, data.char_octet_length));
  ARROW_RETURN_NOT_OK(AppendToBuilder(ORDINAL_POSITION_Builder_, data.ordinal_position));
  ARROW_RETURN_NOT_OK(AppendToBuilder(IS_NULLABLE_Builder_, data.is_nullable));
  num_rows_++;

  return Status::OK();
}

GetColumns_Transformer::GetColumns_Transformer(
    const MetadataSettings& metadata_settings,
    const odbcabstraction::OdbcVersion odbc_version,
    const std::string* column_name_pattern)
    : metadata_settings_(metadata_settings),
      odbc_version_(odbc_version),
      column_name_pattern_(column_name_pattern ? make_optional(*column_name_pattern)
                                               : nullopt) {}

std::shared_ptr<RecordBatch> GetColumns_Transformer::Transform(
    const std::shared_ptr<RecordBatch>& original) {
  const Result<std::shared_ptr<RecordBatch>>& result =
      Transform_inner(odbc_version_, original, column_name_pattern_, metadata_settings_);
  ThrowIfNotOK(result.status());

  return result.ValueOrDie();
}

std::shared_ptr<Schema> GetColumns_Transformer::GetTransformedSchema() {
  return odbc_version_ == odbcabstraction::V_3 ? GetColumns_V3_Schema()
                                               : GetColumns_V2_Schema();
}

}  // namespace flight_sql
}  // namespace driver
