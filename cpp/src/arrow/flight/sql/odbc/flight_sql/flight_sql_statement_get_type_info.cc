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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_statement_get_type_info.h"
#include <boost/algorithm/string/join.hpp>
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_get_type_info_reader.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"

namespace driver {
namespace flight_sql {

using std::make_optional;
using std::nullopt;
using std::optional;

namespace {
std::shared_ptr<Schema> GetTypeInfo_V3_Schema() {
  return arrow::schema({
      field("TYPE_NAME", arrow::utf8(), false),
      field("DATA_TYPE", arrow::int16(), false),
      field("COLUMN_SIZE", arrow::int32()),
      field("LITERAL_PREFIX", arrow::utf8()),
      field("LITERAL_SUFFIX", arrow::utf8()),
      field("CREATE_PARAMS", arrow::utf8()),
      field("NULLABLE", arrow::int16(), false),
      field("CASE_SENSITIVE", arrow::int16(), false),
      field("SEARCHABLE", arrow::int16(), false),
      field("UNSIGNED_ATTRIBUTE", arrow::int16()),
      field("FIXED_PREC_SCALE", arrow::int16(), false),
      field("AUTO_UNIQUE_VALUE", arrow::int16()),
      field("LOCAL_TYPE_NAME", arrow::utf8()),
      field("MINIMUM_SCALE", arrow::int16()),
      field("MAXIMUM_SCALE", arrow::int16()),
      field("SQL_DATA_TYPE", arrow::int16(), false),
      field("SQL_DATETIME_SUB", arrow::int16()),
      field("NUM_PREC_RADIX", arrow::int32()),
      field("INTERVAL_PRECISION", arrow::int16()),
  });
}

std::shared_ptr<Schema> GetTypeInfo_V2_Schema() {
  return arrow::schema({
      field("TYPE_NAME", arrow::utf8(), false),
      field("DATA_TYPE", arrow::int16(), false),
      field("PRECISION", arrow::int32()),
      field("LITERAL_PREFIX", arrow::utf8()),
      field("LITERAL_SUFFIX", arrow::utf8()),
      field("CREATE_PARAMS", arrow::utf8()),
      field("NULLABLE", arrow::int16(), false),
      field("CASE_SENSITIVE", arrow::int16(), false),
      field("SEARCHABLE", arrow::int16(), false),
      field("UNSIGNED_ATTRIBUTE", arrow::int16()),
      field("MONEY", arrow::int16(), false),
      field("AUTO_INCREMENT", arrow::int16()),
      field("LOCAL_TYPE_NAME", arrow::utf8()),
      field("MINIMUM_SCALE", arrow::int16()),
      field("MAXIMUM_SCALE", arrow::int16()),
      field("SQL_DATA_TYPE", arrow::int16(), false),
      field("SQL_DATETIME_SUB", arrow::int16()),
      field("NUM_PREC_RADIX", arrow::int32()),
      field("INTERVAL_PRECISION", arrow::int16()),
  });
}

Result<std::shared_ptr<RecordBatch>> Transform_inner(
    const odbcabstraction::OdbcVersion odbc_version,
    const std::shared_ptr<RecordBatch>& original, int data_type,
    const MetadataSettings& metadata_settings_) {
  GetTypeInfo_RecordBatchBuilder builder(odbc_version);
  GetTypeInfo_RecordBatchBuilder::Data data;

  GetTypeInfoReader reader(original);

  while (reader.Next()) {
    auto data_type_v3 = EnsureRightSqlCharType(
        static_cast<odbcabstraction::SqlDataType>(reader.GetDataType()),
        metadata_settings_.use_wide_char_);
    int16_t data_type_v2 = ConvertSqlDataTypeFromV3ToV2(data_type_v3);

    if (data_type != odbcabstraction::ALL_TYPES && data_type_v3 != data_type &&
        data_type_v2 != data_type) {
      continue;
    }

    data.data_type = odbc_version == odbcabstraction::V_3 ? data_type_v3 : data_type_v2;
    data.type_name = reader.GetTypeName();
    data.column_size = reader.GetColumnSize();
    data.literal_prefix = reader.GetLiteralPrefix();
    data.literal_suffix = reader.GetLiteralSuffix();

    const auto& create_params = reader.GetCreateParams();
    if (create_params) {
      data.create_params = boost::algorithm::join(*create_params, ",");
    } else {
      data.create_params = nullopt;
    }

    data.nullable = reader.GetNullable() ? odbcabstraction::NULLABILITY_NULLABLE
                                         : odbcabstraction::NULLABILITY_NO_NULLS;
    data.case_sensitive = reader.GetCaseSensitive();
    data.searchable = reader.GetSearchable() ? odbcabstraction::SEARCHABILITY_ALL
                                             : odbcabstraction::SEARCHABILITY_NONE;
    data.unsigned_attribute = reader.GetUnsignedAttribute();
    data.fixed_prec_scale = reader.GetFixedPrecScale();
    data.auto_unique_value = reader.GetAutoIncrement();
    data.local_type_name = reader.GetLocalTypeName();
    data.minimum_scale = reader.GetMinimumScale();
    data.maximum_scale = reader.GetMaximumScale();
    data.sql_data_type = EnsureRightSqlCharType(
        static_cast<odbcabstraction::SqlDataType>(reader.GetSqlDataType()),
        metadata_settings_.use_wide_char_);
    data.sql_datetime_sub =
        GetSqlDateTimeSubCode(static_cast<odbcabstraction::SqlDataType>(data.data_type));
    data.num_prec_radix = reader.GetNumPrecRadix();
    data.interval_precision = reader.GetIntervalPrecision();

    ARROW_RETURN_NOT_OK(builder.Append(data));
  }

  return builder.Build();
}
}  // namespace

GetTypeInfo_RecordBatchBuilder::GetTypeInfo_RecordBatchBuilder(
    odbcabstraction::OdbcVersion odbc_version)
    : odbc_version_(odbc_version) {}

Result<std::shared_ptr<RecordBatch>> GetTypeInfo_RecordBatchBuilder::Build() {
  ARROW_ASSIGN_OR_RAISE(auto TYPE_NAME_Array, TYPE_NAME_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto DATA_TYPE_Array, DATA_TYPE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto COLUMN_SIZE_Array, COLUMN_SIZE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto LITERAL_PREFIX_Array, LITERAL_PREFIX_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto LITERAL_SUFFIX_Array, LITERAL_SUFFIX_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto CREATE_PARAMS_Array, CREATE_PARAMS_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto NULLABLE_Array, NULLABLE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto CASE_SENSITIVE_Array, CASE_SENSITIVE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SEARCHABLE_Array, SEARCHABLE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto UNSIGNED_ATTRIBUTE_Array,
                        UNSIGNED_ATTRIBUTE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto FIXED_PREC_SCALE_Array, FIXED_PREC_SCALE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto AUTO_UNIQUE_VALUE_Array, AUTO_UNIQUE_VALUE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto LOCAL_TYPE_NAME_Array, LOCAL_TYPE_NAME_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto MINIMUM_SCALE_Array, MINIMUM_SCALE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto MAXIMUM_SCALE_Array, MAXIMUM_SCALE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SQL_DATA_TYPE_Array, SQL_DATA_TYPE_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto SQL_DATETIME_SUB_Array, SQL_DATETIME_SUB_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto NUM_PREC_RADIX_Array, NUM_PREC_RADIX_Builder_.Finish())
  ARROW_ASSIGN_OR_RAISE(auto INTERVAL_PRECISION_Array,
                        INTERVAL_PRECISION_Builder_.Finish())

  std::vector<std::shared_ptr<Array>> arrays = {
      TYPE_NAME_Array,          DATA_TYPE_Array,        COLUMN_SIZE_Array,
      LITERAL_PREFIX_Array,     LITERAL_SUFFIX_Array,   CREATE_PARAMS_Array,
      NULLABLE_Array,           CASE_SENSITIVE_Array,   SEARCHABLE_Array,
      UNSIGNED_ATTRIBUTE_Array, FIXED_PREC_SCALE_Array, AUTO_UNIQUE_VALUE_Array,
      LOCAL_TYPE_NAME_Array,    MINIMUM_SCALE_Array,    MAXIMUM_SCALE_Array,
      SQL_DATA_TYPE_Array,      SQL_DATETIME_SUB_Array, NUM_PREC_RADIX_Array,
      INTERVAL_PRECISION_Array};

  const std::shared_ptr<Schema>& schema = odbc_version_ == odbcabstraction::V_3
                                              ? GetTypeInfo_V3_Schema()
                                              : GetTypeInfo_V2_Schema();
  return RecordBatch::Make(schema, num_rows_, arrays);
}

Status GetTypeInfo_RecordBatchBuilder::Append(
    const GetTypeInfo_RecordBatchBuilder::Data& data) {
  ARROW_RETURN_NOT_OK(AppendToBuilder(TYPE_NAME_Builder_, data.type_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(DATA_TYPE_Builder_, data.data_type));
  ARROW_RETURN_NOT_OK(AppendToBuilder(COLUMN_SIZE_Builder_, data.column_size));
  ARROW_RETURN_NOT_OK(AppendToBuilder(LITERAL_PREFIX_Builder_, data.literal_prefix));
  ARROW_RETURN_NOT_OK(AppendToBuilder(LITERAL_SUFFIX_Builder_, data.literal_suffix));
  ARROW_RETURN_NOT_OK(AppendToBuilder(CREATE_PARAMS_Builder_, data.create_params));
  ARROW_RETURN_NOT_OK(AppendToBuilder(NULLABLE_Builder_, data.nullable));
  ARROW_RETURN_NOT_OK(AppendToBuilder(CASE_SENSITIVE_Builder_, data.case_sensitive));
  ARROW_RETURN_NOT_OK(AppendToBuilder(SEARCHABLE_Builder_, data.searchable));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(UNSIGNED_ATTRIBUTE_Builder_, data.unsigned_attribute));
  ARROW_RETURN_NOT_OK(AppendToBuilder(FIXED_PREC_SCALE_Builder_, data.fixed_prec_scale));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(AUTO_UNIQUE_VALUE_Builder_, data.auto_unique_value));
  ARROW_RETURN_NOT_OK(AppendToBuilder(LOCAL_TYPE_NAME_Builder_, data.local_type_name));
  ARROW_RETURN_NOT_OK(AppendToBuilder(MINIMUM_SCALE_Builder_, data.minimum_scale));
  ARROW_RETURN_NOT_OK(AppendToBuilder(MAXIMUM_SCALE_Builder_, data.maximum_scale));
  ARROW_RETURN_NOT_OK(AppendToBuilder(SQL_DATA_TYPE_Builder_, data.sql_data_type));
  ARROW_RETURN_NOT_OK(AppendToBuilder(SQL_DATETIME_SUB_Builder_, data.sql_datetime_sub));
  ARROW_RETURN_NOT_OK(AppendToBuilder(NUM_PREC_RADIX_Builder_, data.num_prec_radix));
  ARROW_RETURN_NOT_OK(
      AppendToBuilder(INTERVAL_PRECISION_Builder_, data.interval_precision));
  num_rows_++;

  return Status::OK();
}

GetTypeInfo_Transformer::GetTypeInfo_Transformer(
    const MetadataSettings& metadata_settings,
    const odbcabstraction::OdbcVersion odbc_version, int data_type)
    : metadata_settings_(metadata_settings),
      odbc_version_(odbc_version),
      data_type_(data_type) {}

std::shared_ptr<RecordBatch> GetTypeInfo_Transformer::Transform(
    const std::shared_ptr<RecordBatch>& original) {
  const Result<std::shared_ptr<RecordBatch>>& result =
      Transform_inner(odbc_version_, original, data_type_, metadata_settings_);
  ThrowIfNotOK(result.status());

  return result.ValueOrDie();
}

std::shared_ptr<Schema> GetTypeInfo_Transformer::GetTransformedSchema() {
  return odbc_version_ == odbcabstraction::V_3 ? GetTypeInfo_V3_Schema()
                                               : GetTypeInfo_V2_Schema();
}

}  // namespace flight_sql
}  // namespace driver
