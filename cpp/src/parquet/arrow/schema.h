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

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "parquet/platform.h"
#include "parquet/schema.h"

namespace arrow {

class Field;
class Schema;
class Status;

}  // namespace arrow

namespace parquet {

class ArrowReaderProperties;
class ArrowWriterProperties;
class WriterProperties;

namespace arrow {

/// \defgroup arrow-to-parquet-schema-conversion Functions to convert an Arrow
/// schema into a Parquet schema.
///
/// @{

PARQUET_EXPORT
::arrow::Status FieldToNode(const std::shared_ptr<::arrow::Field>& field,
                            const WriterProperties& properties,
                            const ArrowWriterProperties& arrow_properties,
                            schema::NodePtr* out);

PARQUET_EXPORT
::arrow::Status ToParquetSchema(const ::arrow::Schema* arrow_schema,
                                const WriterProperties& properties,
                                const ArrowWriterProperties& arrow_properties,
                                std::shared_ptr<SchemaDescriptor>* out);

PARQUET_EXPORT
::arrow::Status ToParquetSchema(const ::arrow::Schema* arrow_schema,
                                const WriterProperties& properties,
                                std::shared_ptr<SchemaDescriptor>* out);

/// @}

/// \defgroup parquet-to-arrow-schema-conversion Functions to convert a Parquet
/// schema into an Arrow schema.
///
/// @{

PARQUET_EXPORT
::arrow::Status FromParquetSchema(
    const SchemaDescriptor* parquet_schema, const ArrowReaderProperties& properties,
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& key_value_metadata,
    std::shared_ptr<::arrow::Schema>* out);

PARQUET_EXPORT
::arrow::Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
                                  const ArrowReaderProperties& properties,
                                  std::shared_ptr<::arrow::Schema>* out);

PARQUET_EXPORT
::arrow::Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
                                  std::shared_ptr<::arrow::Schema>* out);

/// @}

/// \brief Bridge between an arrow::Field and parquet column indices.
struct PARQUET_EXPORT SchemaField {
  std::shared_ptr<::arrow::Field> field;
  std::vector<SchemaField> children;

  // Only set for leaf nodes
  int column_index = -1;

  int16_t max_definition_level;
  int16_t max_repetition_level;

  bool is_leaf() const { return column_index != -1; }
};

/// \brief Bridge between a parquet Schema and an arrow Schema.
///
/// Expose parquet columns as a tree structure. Useful traverse and link
/// between arrow's Schema and parquet's Schema.
struct PARQUET_EXPORT SchemaManifest {
  static ::arrow::Status Make(
      const SchemaDescriptor* schema,
      const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata,
      const ArrowReaderProperties& properties, SchemaManifest* manifest);

  const SchemaDescriptor* descr;
  std::shared_ptr<::arrow::Schema> origin_schema;
  std::shared_ptr<const ::arrow::KeyValueMetadata> schema_metadata;
  std::vector<SchemaField> schema_fields;

  std::unordered_map<int, const SchemaField*> column_index_to_field;
  std::unordered_map<const SchemaField*, const SchemaField*> child_to_parent;

  ::arrow::Status GetColumnField(int column_index, const SchemaField** out) const {
    auto it = column_index_to_field.find(column_index);
    if (it == column_index_to_field.end()) {
      return ::arrow::Status::KeyError("Column index ", column_index,
                                       " not found in schema manifest, may be malformed");
    }
    *out = it->second;
    return ::arrow::Status::OK();
  }

  const SchemaField* GetParent(const SchemaField* field) const {
    // Returns nullptr also if not found
    auto it = child_to_parent.find(field);
    if (it == child_to_parent.end()) {
      return NULLPTR;
    }
    return it->second;
  }

  bool GetFieldIndices(const std::vector<int>& column_indices, std::vector<int>* out) {
    // Coalesce a list of schema field indices which are the roots of the
    // columns referred to by a list of column indices
    const schema::GroupNode* group = descr->group_node();
    std::unordered_set<int> already_added;
    out->clear();
    for (auto& column_idx : column_indices) {
      auto field_node = descr->GetColumnRoot(column_idx);
      auto field_idx = group->FieldIndex(*field_node);
      if (field_idx < 0) {
        return false;
      }
      auto insertion = already_added.insert(field_idx);
      if (insertion.second) {
        out->push_back(field_idx);
      }
    }
    return true;
  }
};

}  // namespace arrow
}  // namespace parquet
