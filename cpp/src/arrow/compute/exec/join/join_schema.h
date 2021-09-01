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
#include <mutex>
#include <vector>

#include "arrow/compute/exec/key_encode.h"

/*
  This file implements helper classes for simple mapping of corresponding columns from one
  type of storage to another.
  For instance it is used to lookup a column id in input batch
  on either side of the join for a given key column, or map a join output column to a
  corresponding column stored in a hash table.
*/

namespace arrow {
namespace compute {

// Identifiers of all different row schemas that appear during processing of a join.
//
enum class JoinSchemaHandle {
  FIRST_INPUT,
  SECOND_INPUT,
  FIRST_KEY,
  FIRST_PAYLOAD,
  SECOND_KEY,
  SECOND_PAYLOAD,
  OUTPUT
};

struct JoinField {
  std::string field_namespace;
  std::string field_name;
  std::shared_ptr<DataType> full_data_type;
  KeyEncoder::KeyColumnMetadata data_type;
};

using schema_type = std::vector<JoinField>;

/* This is a helper class that makes it simple to map between input column id from
 * arbitrary input source to output column id for arbitrary output destination.
 * It is also thread-safe.
 * Materialized mappings are created lazily during the first access request.
 */
template <typename SchemaHandleType>
class ColumnMapper {
 public:
  // This call is not thread-safe. Registering needs to be executed on a single thread
  // before thread-safe read-only queries can be run.
  //
  void RegisterHandle(SchemaHandleType schema_handle,
                      std::shared_ptr<schema_type> schema) {
    schemas_.push_back(std::make_pair(schema_handle, schema));
  }

  void RegisterEnd() {
    size_t size = schemas_.size();
    mapping_pointers_.resize(size * size);
    mapping_buffers_.resize(size * size);
  }

  int num_cols(SchemaHandleType schema_handle) const {
    int id = schema_id(schema_handle);
    return static_cast<int>(schemas_[id].second->size());
  }

  const JoinField* field(SchemaHandleType schema_handle, int field_id) const {
    int id = schema_id(schema_handle);
    const schema_type* schema = schemas_[id].second.get();
    return &((*schema)[field_id]);
  }

  KeyEncoder::KeyColumnMetadata data_type(SchemaHandleType schema_handle,
                                          int field_id) const {
    return field(schema_handle, field_id)->data_type;
  }

  const int* map(SchemaHandleType from, SchemaHandleType to) {
    int id_from = schema_id(from);
    int id_to = schema_id(to);
    int num_schemas = static_cast<int>(schemas_.size());
    int pos = id_from * num_schemas + id_to;
    const int* ptr = mapping_pointers_[pos];
    if (!ptr) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!ptr) {
        int num_cols_from = static_cast<int>(schemas_[id_from].second->size());
        int num_cols_to = static_cast<int>(schemas_[id_to].second->size());
        mapping_buffers_[pos].resize(num_cols_from);
        const std::vector<JoinField>& fields_from = *(schemas_[id_from].second.get());
        const std::vector<JoinField>& fields_to = *(schemas_[id_to].second.get());
        for (int i = 0; i < num_cols_from; ++i) {
          int field_id = kMissingField;
          for (int j = 0; j < num_cols_to; ++j) {
            if (fields_from[i].field_namespace.compare(fields_to[j].field_namespace) ==
                    0 &&
                fields_from[i].field_name.compare(fields_to[j].field_name) == 0) {
              DCHECK(fields_from[i].data_type.is_fixed_length ==
                         fields_to[j].data_type.is_fixed_length &&
                     fields_from[i].data_type.fixed_length ==
                         fields_to[j].data_type.fixed_length);
              field_id = j;
              break;
            }
          }
          mapping_buffers_[pos][i] = field_id;
        }
        mapping_pointers_[pos] = mapping_buffers_[pos].data();
      }
      ptr = mapping_pointers_[pos];
    }
    return ptr;
  }

  static constexpr int kMissingField = -1;

 private:
  int schema_id(SchemaHandleType schema_handle) const {
    for (size_t i = 0; i < schemas_.size(); ++i) {
      if (schemas_[i].first == schema_handle) {
        return static_cast<int>(i);
      }
    }
    // We should never get here
    DCHECK(false);
    return -1;
  }

  std::vector<int*> mapping_pointers_;
  std::vector<std::vector<int>> mapping_buffers_;
  std::vector<std::pair<SchemaHandleType, std::shared_ptr<schema_type>>> schemas_;
  std::mutex mutex_;
};

using JoinColumnMapper = ColumnMapper<JoinSchemaHandle>;

}  // namespace compute
}  // namespace arrow