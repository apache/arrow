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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/compute/exec/key_encode.h"  // for KeyColumnMetadata
#include "arrow/type.h"                     // for DataType, FieldRef, Field and Schema
#include "arrow/util/mutex.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

struct SchemaProjectionMap {
  static constexpr int kMissingField = -1;
  int num_cols;
  const int* source_to_base;
  const int* base_to_target;
  inline int get(int i) const {
    ARROW_DCHECK(i >= 0 && i < num_cols);
    ARROW_DCHECK(source_to_base[i] != kMissingField);
    return base_to_target[source_to_base[i]];
  }
};

/// Helper class for managing different projections of the same row schema.
/// Used to efficiently map any field in one projection to a corresponding field in
/// another projection.
/// Materialized mappings are generated lazily at the time of the first access.
/// Thread-safe apart from initialization.
template <typename ProjectionIdEnum>
class SchemaProjectionMaps {
 public:
  static constexpr int kMissingField = -1;

  Status Init(ProjectionIdEnum full_schema_handle, const Schema& schema,
              const std::vector<ProjectionIdEnum>& projection_handles,
              const std::vector<const std::vector<FieldRef>*>& projections) {
    ARROW_DCHECK(projection_handles.size() == projections.size());
    RegisterSchema(full_schema_handle, schema);
    for (size_t i = 0; i < projections.size(); ++i) {
      ARROW_RETURN_NOT_OK(
          RegisterProjectedSchema(projection_handles[i], *(projections[i]), schema));
    }
    RegisterEnd();
    return Status::OK();
  }

  int num_cols(ProjectionIdEnum schema_handle) const {
    int id = schema_id(schema_handle);
    return static_cast<int>(schemas_[id].second.size());
  }

  const KeyEncoder::KeyColumnMetadata& column_metadata(ProjectionIdEnum schema_handle,
                                                       int field_id) const {
    return field(schema_handle, field_id).column_metadata;
  }

  const std::string& field_name(ProjectionIdEnum schema_handle, int field_id) const {
    return field(schema_handle, field_id).field_name;
  }

  const std::shared_ptr<DataType>& data_type(ProjectionIdEnum schema_handle,
                                             int field_id) const {
    return field(schema_handle, field_id).data_type;
  }

  SchemaProjectionMap map(ProjectionIdEnum from, ProjectionIdEnum to) {
    int id_from = schema_id(from);
    int id_to = schema_id(to);
    SchemaProjectionMap result;
    result.num_cols = num_cols(from);
    result.source_to_base = mappings_[id_from].data();
    result.base_to_target = inverse_mappings_[id_to].data();
    return result;
  }

 protected:
  struct FieldInfo {
    int field_path;
    std::string field_name;
    std::shared_ptr<DataType> data_type;
    KeyEncoder::KeyColumnMetadata column_metadata;
  };

  void RegisterSchema(ProjectionIdEnum handle, const Schema& schema) {
    std::vector<FieldInfo> out_fields;
    const FieldVector& in_fields = schema.fields();
    out_fields.resize(in_fields.size());
    for (size_t i = 0; i < in_fields.size(); ++i) {
      const std::string& name = in_fields[i]->name();
      const std::shared_ptr<DataType>& type = in_fields[i]->type();
      out_fields[i].field_path = static_cast<int>(i);
      out_fields[i].field_name = name;
      out_fields[i].data_type = type;
      out_fields[i].column_metadata = ColumnMetadataFromDataType(type);
    }
    schemas_.push_back(std::make_pair(handle, out_fields));
  }

  Status RegisterProjectedSchema(ProjectionIdEnum handle,
                                 const std::vector<FieldRef>& selected_fields,
                                 const Schema& full_schema) {
    std::vector<FieldInfo> out_fields;
    const FieldVector& in_fields = full_schema.fields();
    out_fields.resize(selected_fields.size());
    for (size_t i = 0; i < selected_fields.size(); ++i) {
      // All fields must be found in schema without ambiguity
      ARROW_ASSIGN_OR_RAISE(auto match, selected_fields[i].FindOne(full_schema));
      const std::string& name = in_fields[match[0]]->name();
      const std::shared_ptr<DataType>& type = in_fields[match[0]]->type();
      out_fields[i].field_path = match[0];
      out_fields[i].field_name = name;
      out_fields[i].data_type = type;
      out_fields[i].column_metadata = ColumnMetadataFromDataType(type);
    }
    schemas_.push_back(std::make_pair(handle, out_fields));
    return Status::OK();
  }

  void RegisterEnd() {
    size_t size = schemas_.size();
    mappings_.resize(size);
    inverse_mappings_.resize(size);
    int id_base = 0;
    for (size_t i = 0; i < size; ++i) {
      GenerateMapForProjection(static_cast<int>(i), id_base);
    }
  }

  KeyEncoder::KeyColumnMetadata ColumnMetadataFromDataType(
      const std::shared_ptr<DataType>& type) {
    if (type->id() == Type::DICTIONARY) {
      auto bit_width = checked_cast<const FixedWidthType&>(*type).bit_width();
      ARROW_DCHECK(bit_width % 8 == 0);
      return KeyEncoder::KeyColumnMetadata(true, bit_width / 8);
    } else if (type->id() == Type::BOOL) {
      return KeyEncoder::KeyColumnMetadata(true, 0);
    } else if (is_fixed_width(type->id())) {
      return KeyEncoder::KeyColumnMetadata(
          true, checked_cast<const FixedWidthType&>(*type).bit_width() / 8);
    } else if (is_binary_like(type->id())) {
      return KeyEncoder::KeyColumnMetadata(false, sizeof(uint32_t));
    } else {
      ARROW_DCHECK(false);
      return KeyEncoder::KeyColumnMetadata(true, 0);
    }
  }

  int schema_id(ProjectionIdEnum schema_handle) const {
    for (size_t i = 0; i < schemas_.size(); ++i) {
      if (schemas_[i].first == schema_handle) {
        return static_cast<int>(i);
      }
    }
    // We should never get here
    ARROW_DCHECK(false);
    return -1;
  }

  const FieldInfo& field(ProjectionIdEnum schema_handle, int field_id) const {
    int id = schema_id(schema_handle);
    const std::vector<FieldInfo>& field_infos = schemas_[id].second;
    return field_infos[field_id];
  }

  void GenerateMapForProjection(int id_proj, int id_base) {
    int num_cols_proj = static_cast<int>(schemas_[id_proj].second.size());
    int num_cols_base = static_cast<int>(schemas_[id_base].second.size());

    std::vector<int>& mapping = mappings_[id_proj];
    std::vector<int>& inverse_mapping = inverse_mappings_[id_proj];
    mapping.resize(num_cols_proj);
    inverse_mapping.resize(num_cols_base);

    if (id_proj == id_base) {
      for (int i = 0; i < num_cols_base; ++i) {
        mapping[i] = inverse_mapping[i] = i;
      }
    } else {
      const std::vector<FieldInfo>& fields_proj = schemas_[id_proj].second;
      const std::vector<FieldInfo>& fields_base = schemas_[id_base].second;
      for (int i = 0; i < num_cols_base; ++i) {
        inverse_mapping[i] = SchemaProjectionMap::kMissingField;
      }
      for (int i = 0; i < num_cols_proj; ++i) {
        int field_id = SchemaProjectionMap::kMissingField;
        for (int j = 0; j < num_cols_base; ++j) {
          if (fields_proj[i].field_path == fields_base[j].field_path) {
            field_id = j;
            // If there are multiple matches for the same input field,
            // it will be mapped to the first match.
            break;
          }
        }
        ARROW_DCHECK(field_id != SchemaProjectionMap::kMissingField);
        mapping[i] = field_id;
        inverse_mapping[field_id] = i;
      }
    }
  }

  // vector used as a mapping from ProjectionIdEnum to fields
  std::vector<std::pair<ProjectionIdEnum, std::vector<FieldInfo>>> schemas_;
  std::vector<std::vector<int>> mappings_;
  std::vector<std::vector<int>> inverse_mappings_;
};

}  // namespace compute
}  // namespace arrow
