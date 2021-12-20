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

#include "arrow/flight/sql/column_metadata.h"

#include <utility>

namespace {
  /// \brief Constant variable used to convert boolean true value
  ///        to a string.
  const char* BOOLEAN_TRUE_STR = "YES";
  /// \brief Constant variable used to convert boolean false value
  ///        to a string.
  const char* BOOLEAN_FALSE_STR = "NO";

  std::string BooleanToString(bool boolean_value) {
    return boolean_value ? BOOLEAN_TRUE_STR :
           BOOLEAN_FALSE_STR;
  }

  bool StringToBoolean(const std::string& string_value) {
    return string_value == BOOLEAN_TRUE_STR;
  }
}  // namespace

namespace arrow {
namespace flight {
namespace sql {

const char* ColumnMetadata::CATALOG_NAME = "CATALOG_NAME";
const char* ColumnMetadata::SCHEMA_NAME = "SCHEMA_NAME";
const char* ColumnMetadata::TABLE_NAME = "TABLE_NAME";
const char* ColumnMetadata::PRECISION = "PRECISION";
const char* ColumnMetadata::SCALE = "SCALE";
const char* ColumnMetadata::IS_AUTO_INCREMENT = "IS_AUTO_INCREMENT";
const char* ColumnMetadata::IS_CASE_SENSITIVE = "IS_CASE_SENSITIVE";
const char* ColumnMetadata::IS_READ_ONLY = "IS_READ_ONLY";
const char* ColumnMetadata::IS_SEARCHABLE = "IS_SEARCHABLE";

ColumnMetadata::ColumnMetadata(std::shared_ptr<arrow::KeyValueMetadata> metadata_map) :
  metadata_map_(std::move(metadata_map)) {
}

arrow::Result<std::string> ColumnMetadata::GetCatalogName() {
  return metadata_map_->Get(CATALOG_NAME);
}

arrow::Result<std::string> ColumnMetadata::GetSchemaName() {
  return metadata_map_->Get(SCHEMA_NAME);
}

arrow::Result<std::string> ColumnMetadata::GetTableName() {
  return metadata_map_->Get(TABLE_NAME);
}

arrow::Result<int32_t> ColumnMetadata::GetPrecision() {
  const Result <std::string> &result = metadata_map_->Get(PRECISION);
  std::string precision_string;
  ARROW_ASSIGN_OR_RAISE(precision_string, result);

  return std::stoi(precision_string);
}

arrow::Result<int32_t> ColumnMetadata::GetScale() {
  std::string scale_string;
  ARROW_ASSIGN_OR_RAISE(scale_string, metadata_map_->Get(SCALE));

  return std::stoi(scale_string);
}

arrow::Result<bool> ColumnMetadata::GetIsAutoIncrement() {
  std::string auto_increment_string;
  ARROW_ASSIGN_OR_RAISE(auto_increment_string, metadata_map_->Get(IS_AUTO_INCREMENT));
  return StringToBoolean(auto_increment_string);
}

arrow::Result<bool> ColumnMetadata::GetIsCaseSensitive() {
  std::string is_case_sensitive;
  ARROW_ASSIGN_OR_RAISE(is_case_sensitive, metadata_map_->Get(IS_AUTO_INCREMENT));
  return StringToBoolean(is_case_sensitive);
}

arrow::Result<bool> ColumnMetadata::GetIsReadOnly() {
  std::string is_read_only;
  ARROW_ASSIGN_OR_RAISE(is_read_only, metadata_map_->Get(IS_AUTO_INCREMENT));
  return StringToBoolean(is_read_only);
}

arrow::Result<bool> ColumnMetadata::GetIsSearchable() {
  std::string is_case_sensitive;
  ARROW_ASSIGN_OR_RAISE(is_case_sensitive, metadata_map_->Get(IS_AUTO_INCREMENT));
  return StringToBoolean(is_case_sensitive);
}

ColumnMetadata::ColumnMetadataBuilder ColumnMetadata::Builder() {
  const ColumnMetadataBuilder &builder = ColumnMetadataBuilder{};
  return builder;
}

std::shared_ptr<arrow::KeyValueMetadata> ColumnMetadata::GetMetadataMap() const {
  return metadata_map_;
}

ColumnMetadata::ColumnMetadataBuilder
&ColumnMetadata::ColumnMetadataBuilder::CatalogName(std::string &catalog_name) {
  metadata_map_->Append(ColumnMetadata::CATALOG_NAME, catalog_name);
  return *this;
}


ColumnMetadata::ColumnMetadataBuilder
&ColumnMetadata::ColumnMetadataBuilder::SchemaName(std::string &schema_name) {
  metadata_map_->Append(ColumnMetadata::SCHEMA_NAME, schema_name);
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder
&ColumnMetadata::ColumnMetadataBuilder::TableName(std::string &table_name) {
  metadata_map_->Append(ColumnMetadata::TABLE_NAME, table_name);
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder
&ColumnMetadata::ColumnMetadataBuilder::Precision(int32_t precision) {
  metadata_map_->Append(
    ColumnMetadata::PRECISION, std::to_string(precision));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder
&ColumnMetadata::ColumnMetadataBuilder::Scale(int32_t scale) {
  metadata_map_->Append(
    ColumnMetadata::SCALE, std::to_string(scale));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder &
ColumnMetadata::ColumnMetadataBuilder::IsAutoIncrement(bool is_auto_increment) {
  metadata_map_->Append(ColumnMetadata::IS_AUTO_INCREMENT,
                                            BooleanToString(is_auto_increment));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder &
ColumnMetadata::ColumnMetadataBuilder::IsCaseSensitive(bool is_case_sensitive) {
  metadata_map_->Append(ColumnMetadata::IS_CASE_SENSITIVE,
                                            BooleanToString(is_case_sensitive));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder
&ColumnMetadata::ColumnMetadataBuilder::IsReadOnly(bool is_read_only) {
  metadata_map_->Append(ColumnMetadata::IS_READ_ONLY,
                                            BooleanToString(is_read_only));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder
&ColumnMetadata::ColumnMetadataBuilder::IsSearchable(bool is_searchable) {
  metadata_map_->Append(ColumnMetadata::IS_SEARCHABLE,
                                            BooleanToString(is_searchable));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder::ColumnMetadataBuilder() : metadata_map_(
  std::make_shared<arrow::KeyValueMetadata>()) {
}

ColumnMetadata ColumnMetadata::ColumnMetadataBuilder::Build() const {
  return ColumnMetadata{metadata_map_};
}
}  // namespace sql
}  // namespace flight
}  // namespace arrow
