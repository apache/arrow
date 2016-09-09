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
#include "parquet/util/logging.h"

namespace parquet {

using schema::ColumnPath;
using schema::Node;
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

  group_node_ = static_cast<const GroupNode*>(schema_.get());
  leaves_.clear();

  for (int i = 0; i < group_node_->field_count(); ++i) {
    BuildTree(group_node_->field(i), 0, 0, group_node_->field(i));
  }
}

void SchemaDescriptor::BuildTree(const NodePtr& node, int16_t max_def_level,
    int16_t max_rep_level, const NodePtr& base) {
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
    for (int i = 0; i < group->field_count(); ++i) {
      BuildTree(group->field(i), max_def_level, max_rep_level, base);
    }
  } else {
    // Primitive node, append to leaves
    leaves_.push_back(ColumnDescriptor(node, max_def_level, max_rep_level, this));
    leaf_to_base_.emplace(leaves_.size() - 1, base);
  }
}

ColumnDescriptor::ColumnDescriptor(const schema::NodePtr& node,
    int16_t max_definition_level, int16_t max_repetition_level,
    const SchemaDescriptor* schema_descr)
    : node_(node),
      max_definition_level_(max_definition_level),
      max_repetition_level_(max_repetition_level),
      schema_descr_(schema_descr) {
  if (!node_->is_primitive()) { throw ParquetException("Must be a primitive type"); }
  primitive_node_ = static_cast<const PrimitiveNode*>(node_.get());
}

const ColumnDescriptor* SchemaDescriptor::Column(int i) const {
  DCHECK(i >= 0 && i < static_cast<int>(leaves_.size()));
  return &leaves_[i];
}

const schema::NodePtr& SchemaDescriptor::GetColumnRoot(int i) const {
  DCHECK(i >= 0 && i < static_cast<int>(leaves_.size()));
  return leaf_to_base_.find(i)->second;
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

const std::shared_ptr<ColumnPath> ColumnDescriptor::path() const {
  // Build the path in reverse order as we traverse the nodes to the top
  std::vector<std::string> rpath_;
  const Node* node = primitive_node_;
  // The schema node is not part of the ColumnPath
  while (node->parent()) {
    rpath_.push_back(node->name());
    node = node->parent();
  }

  // Build ColumnPath in correct order
  std::vector<std::string> path_(rpath_.crbegin(), rpath_.crend());
  return std::make_shared<ColumnPath>(std::move(path_));
}

}  // namespace parquet
