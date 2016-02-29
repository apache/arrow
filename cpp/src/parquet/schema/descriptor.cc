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

#include "parquet/schema/descriptor.h"

#include "parquet/exception.h"

namespace parquet_cpp {

using schema::NodePtr;
using schema::PrimitiveNode;
using schema::GroupNode;

void SchemaDescriptor::Init(std::unique_ptr<schema::Node> schema) {
  Init(NodePtr(schema.release()));
}

void SchemaDescriptor::Init(const NodePtr& schema) {
  schema_ = schema;

  if (!schema_->is_group()) {
    throw ParquetException("Must initialize with a schema group");
  }

  group_ = static_cast<const GroupNode*>(schema_.get());
  leaves_.clear();

  for (size_t i = 0; i < group_->field_count(); ++i) {
    BuildTree(group_->field(i), 0, 0);
  }
}

void SchemaDescriptor::BuildTree(const NodePtr& node, int16_t max_def_level,
    int16_t max_rep_level) {
  if (node->is_optional()) {
    ++max_def_level;
  } else if (node->is_repeated()) {
    // Repeated fields add a definition level. This is used to distinguish
    // between an empty list and a list with an item in it.
    ++max_rep_level;
    ++max_def_level;
  }

  // Now, walk the schema and create a ColumnDescriptor for each leaf node
  if (node->is_group()) {
    const GroupNode* group = static_cast<const GroupNode*>(node.get());
    for (size_t i = 0; i < group->field_count(); ++i) {
      BuildTree(group->field(i), max_def_level, max_rep_level);
    }
  } else {
    // Primitive node, append to leaves
    leaves_.push_back(ColumnDescriptor(node, max_def_level, max_rep_level, this));
  }
}

ColumnDescriptor::ColumnDescriptor(const schema::NodePtr& node,
    int16_t max_definition_level, int16_t max_repetition_level,
    const SchemaDescriptor* schema_descr) :
      node_(node),
      max_definition_level_(max_definition_level),
      max_repetition_level_(max_repetition_level),
      schema_descr_(schema_descr) {
  if (!node_->is_primitive()) {
    throw ParquetException("Must be a primitive type");
  }
  primitive_node_ = static_cast<const PrimitiveNode*>(node_.get());
}

const ColumnDescriptor* SchemaDescriptor::Column(size_t i) const {
  return &leaves_[i];
}

int ColumnDescriptor::type_scale() const {
  return primitive_node_->decimal_metadata().scale;
}

int ColumnDescriptor::type_precision() const {
  return primitive_node_->decimal_metadata().precision;
}

int ColumnDescriptor::type_length() const {
  return primitive_node_->type_length();
}

} // namespace parquet_cpp
