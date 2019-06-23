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

// This module contains the logical parquet-cpp types (independent of Thrift
// structures), schema nodes, and related type tools

#ifndef PARQUET_SCHEMA_INTERNAL_H
#define PARQUET_SCHEMA_INTERNAL_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace parquet {

namespace format {
class SchemaElement;
}

namespace schema {

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
  if (node->converted_type() == ConvertedType::LIST) return false;
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

// ----------------------------------------------------------------------
// Conversion from Parquet Thrift metadata

std::shared_ptr<SchemaDescriptor> FromParquet(
    const std::vector<format::SchemaElement>& schema);

class FlatSchemaConverter {
 public:
  FlatSchemaConverter(const format::SchemaElement* elements, int length)
      : elements_(elements), length_(length), pos_(0), current_id_(0) {}

  std::unique_ptr<Node> Convert();

 private:
  const format::SchemaElement* elements_;
  int length_;
  int pos_;
  int current_id_;

  int next_id() { return current_id_++; }

  const format::SchemaElement& Next();

  std::unique_ptr<Node> NextNode();
};

// ----------------------------------------------------------------------
// Conversion to Parquet Thrift metadata

void ToParquet(const GroupNode* schema, std::vector<format::SchemaElement>* out);

// Converts nested parquet schema back to a flat vector of Thrift structs
class SchemaFlattener {
 public:
  SchemaFlattener(const GroupNode* schema, std::vector<format::SchemaElement>* out);

  void Flatten();

 private:
  const GroupNode* root_;
  std::vector<format::SchemaElement>* elements_;
};

}  // namespace schema
}  // namespace parquet

#endif  // PARQUET_SCHEMA_INTERNAL_H
