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

#ifndef PARQUET_SCHEMA_UTIL_H
#define PARQUET_SCHEMA_UTIL_H

#include <string>
#include <unordered_set>
#include <vector>

#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/types.h"

using parquet::LogicalType;
using parquet::ParquetException;
using parquet::SchemaDescriptor;
using parquet::schema::GroupNode;
using parquet::schema::Node;
using parquet::schema::NodePtr;

inline bool str_endswith_tuple(const std::string& str) {
  if (str.size() >= 6) {
    return str.substr(str.size() - 6, 6) == "_tuple";
  }
  return false;
}

// Special case mentioned in the format spec:
//   If the name is array or ends in _tuple, this should be a list of struct
//   even for single child elements.
inline bool HasStructListName(const GroupNode& node) {
  return (node.name() == "array" || str_endswith_tuple(node.name()));
}

// TODO(itaiin): This aux. function is to be deleted once repeated structs are supported
inline bool IsSimpleStruct(const Node* node) {
  if (!node->is_group()) return false;
  if (node->is_repeated()) return false;
  if (node->logical_type() == LogicalType::LIST) return false;
  // Special case mentioned in the format spec:
  //   If the name is array or ends in _tuple, this should be a list of struct
  //   even for single child elements.
  auto group = static_cast<const GroupNode*>(node);
  if (group->field_count() == 1 && HasStructListName(*group)) return false;

  return true;
}

// Coalesce a list of schema fields indices which are the roots of the
// columns referred by a list of column indices
inline bool ColumnIndicesToFieldIndices(const SchemaDescriptor& descr,
                                        const std::vector<int>& column_indices,
                                        std::vector<int>* out) {
  const GroupNode* group = descr.group_node();
  std::unordered_set<int> already_added;
  out->clear();
  for (auto& column_idx : column_indices) {
    auto field_node = descr.GetColumnRoot(column_idx);
    auto field_idx = group->FieldIndex(field_node->name());
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

#endif  // PARQUET_SCHEMA_UTIL_H
