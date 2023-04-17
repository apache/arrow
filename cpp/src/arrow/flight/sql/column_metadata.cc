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

#include "arrow/util/string.h"

namespace arrow {

using internal::ToChars;

namespace flight {
namespace sql {
namespace {

/// \brief Constant variable used to convert boolean true value
///        to a string.
const char* BOOLEAN_TRUE_STR = "1";
/// \brief Constant variable used to convert boolean false value
///        to a string.
const char* BOOLEAN_FALSE_STR = "0";

std::string BooleanToString(bool boolean_value) {
  return boolean_value ? BOOLEAN_TRUE_STR : BOOLEAN_FALSE_STR;
}

bool StringToBoolean(const std::string& string_value) {
  return string_value == BOOLEAN_TRUE_STR;
}
}  // namespace

const char* ColumnMetadata::kCatalogName = "ARROW:FLIGHT:SQL:CATALOG_NAME";
const char* ColumnMetadata::kSchemaName = "ARROW:FLIGHT:SQL:SCHEMA_NAME";
const char* ColumnMetadata::kTableName = "ARROW:FLIGHT:SQL:TABLE_NAME";
const char* ColumnMetadata::kTypeName = "ARROW:FLIGHT:SQL:TYPE_NAME";
const char* ColumnMetadata::kPrecision = "ARROW:FLIGHT:SQL:PRECISION";
const char* ColumnMetadata::kScale = "ARROW:FLIGHT:SQL:SCALE";
const char* ColumnMetadata::kIsAutoIncrement = "ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT";
const char* ColumnMetadata::kIsCaseSensitive = "ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE";
const char* ColumnMetadata::kIsReadOnly = "ARROW:FLIGHT:SQL:IS_READ_ONLY";
const char* ColumnMetadata::kIsSearchable = "ARROW:FLIGHT:SQL:IS_SEARCHABLE";

ColumnMetadata::ColumnMetadata(
    std::shared_ptr<const arrow::KeyValueMetadata> metadata_map)
    : metadata_map_(std::move(metadata_map)) {}

arrow::Result<std::string> ColumnMetadata::GetCatalogName() const {
  return metadata_map_->Get(kCatalogName);
}

arrow::Result<std::string> ColumnMetadata::GetSchemaName() const {
  return metadata_map_->Get(kSchemaName);
}

arrow::Result<std::string> ColumnMetadata::GetTableName() const {
  return metadata_map_->Get(kTableName);
}

arrow::Result<std::string> ColumnMetadata::GetTypeName() const {
  return metadata_map_->Get(kTypeName);
}

arrow::Result<int32_t> ColumnMetadata::GetPrecision() const {
  std::string precision_string;
  ARROW_ASSIGN_OR_RAISE(precision_string, metadata_map_->Get(kPrecision));

  return std::stoi(precision_string);
}

arrow::Result<int32_t> ColumnMetadata::GetScale() const {
  std::string scale_string;
  ARROW_ASSIGN_OR_RAISE(scale_string, metadata_map_->Get(kScale));

  return std::stoi(scale_string);
}

arrow::Result<bool> ColumnMetadata::GetIsAutoIncrement() const {
  std::string auto_increment_string;
  ARROW_ASSIGN_OR_RAISE(auto_increment_string, metadata_map_->Get(kIsAutoIncrement));
  return StringToBoolean(auto_increment_string);
}

arrow::Result<bool> ColumnMetadata::GetIsCaseSensitive() const {
  std::string is_case_sensitive;
  ARROW_ASSIGN_OR_RAISE(is_case_sensitive, metadata_map_->Get(kIsAutoIncrement));
  return StringToBoolean(is_case_sensitive);
}

arrow::Result<bool> ColumnMetadata::GetIsReadOnly() const {
  std::string is_read_only;
  ARROW_ASSIGN_OR_RAISE(is_read_only, metadata_map_->Get(kIsAutoIncrement));
  return StringToBoolean(is_read_only);
}

arrow::Result<bool> ColumnMetadata::GetIsSearchable() const {
  std::string is_case_sensitive;
  ARROW_ASSIGN_OR_RAISE(is_case_sensitive, metadata_map_->Get(kIsAutoIncrement));
  return StringToBoolean(is_case_sensitive);
}

ColumnMetadata::ColumnMetadataBuilder ColumnMetadata::Builder() {
  return ColumnMetadataBuilder{};
}

const std::shared_ptr<const arrow::KeyValueMetadata>& ColumnMetadata::metadata_map()
    const {
  return metadata_map_;
}

ColumnMetadata::ColumnMetadataBuilder& ColumnMetadata::ColumnMetadataBuilder::CatalogName(
    const std::string& catalog_name) {
  metadata_map_->Append(ColumnMetadata::kCatalogName, catalog_name);
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder& ColumnMetadata::ColumnMetadataBuilder::SchemaName(
    const std::string& schema_name) {
  metadata_map_->Append(ColumnMetadata::kSchemaName, schema_name);
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder& ColumnMetadata::ColumnMetadataBuilder::TableName(
    const std::string& table_name) {
  metadata_map_->Append(ColumnMetadata::kTableName, table_name);
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder& ColumnMetadata::ColumnMetadataBuilder::TypeName(
    const std::string& type_name) {
  metadata_map_->Append(ColumnMetadata::kTypeName, type_name);
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder& ColumnMetadata::ColumnMetadataBuilder::Precision(
    int32_t precision) {
  metadata_map_->Append(ColumnMetadata::kPrecision, ToChars(precision));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder& ColumnMetadata::ColumnMetadataBuilder::Scale(
    int32_t scale) {
  metadata_map_->Append(ColumnMetadata::kScale, ToChars(scale));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder&
ColumnMetadata::ColumnMetadataBuilder::IsAutoIncrement(bool is_auto_increment) {
  metadata_map_->Append(ColumnMetadata::kIsAutoIncrement,
                        BooleanToString(is_auto_increment));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder&
ColumnMetadata::ColumnMetadataBuilder::IsCaseSensitive(bool is_case_sensitive) {
  metadata_map_->Append(ColumnMetadata::kIsCaseSensitive,
                        BooleanToString(is_case_sensitive));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder& ColumnMetadata::ColumnMetadataBuilder::IsReadOnly(
    bool is_read_only) {
  metadata_map_->Append(ColumnMetadata::kIsReadOnly, BooleanToString(is_read_only));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder&
ColumnMetadata::ColumnMetadataBuilder::IsSearchable(bool is_searchable) {
  metadata_map_->Append(ColumnMetadata::kIsSearchable, BooleanToString(is_searchable));
  return *this;
}

ColumnMetadata::ColumnMetadataBuilder::ColumnMetadataBuilder()
    : metadata_map_(std::make_shared<arrow::KeyValueMetadata>()) {}

ColumnMetadata ColumnMetadata::ColumnMetadataBuilder::Build() const {
  return ColumnMetadata{metadata_map_};
}
}  // namespace sql
}  // namespace flight
}  // namespace arrow
