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

#include "parquet/schema/types.h"

#include <algorithm>
#include <memory>

#include "parquet/exception.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/thrift/util.h"

namespace parquet {

namespace schema {

// ----------------------------------------------------------------------
// ColumnPath

std::shared_ptr<ColumnPath> ColumnPath::FromDotString(const std::string& dotstring) {
  std::stringstream ss(dotstring);
  std::string item;
  std::vector<std::string> path;
  while (std::getline(ss, item, '.')) {
    path.push_back(item);
  }
  return std::shared_ptr<ColumnPath>(new ColumnPath(std::move(path)));
}

std::shared_ptr<ColumnPath> ColumnPath::extend(const std::string& node_name) const {
  std::vector<std::string> path;
  path.reserve(path_.size() + 1);
  path.resize(path_.size() + 1);
  std::copy(path_.cbegin(), path_.cend(), path.begin());
  path[path_.size()] = node_name;

  return std::shared_ptr<ColumnPath>(new ColumnPath(std::move(path)));
}

std::string ColumnPath::ToDotString() const {
  std::stringstream ss;
  for (auto it = path_.cbegin(); it != path_.cend(); ++it) {
    if (it != path_.cbegin()) { ss << "."; }
    ss << *it;
  }
  return ss.str();
}

const std::vector<std::string>& ColumnPath::ToDotVector() const {
  return path_;
}

// ----------------------------------------------------------------------
// Base node

bool Node::EqualsInternal(const Node* other) const {
  return type_ == other->type_ && name_ == other->name_ &&
         repetition_ == other->repetition_ && logical_type_ == other->logical_type_;
}

void Node::SetParent(const Node* parent) {
  parent_ = parent;
}

// ----------------------------------------------------------------------
// Primitive node

PrimitiveNode::PrimitiveNode(const std::string& name, Repetition::type repetition,
    Type::type type, LogicalType::type logical_type, int length, int precision, int scale,
    int id)
    : Node(Node::PRIMITIVE, name, repetition, logical_type, id),
      physical_type_(type),
      type_length_(length) {
  std::stringstream ss;
  decimal_metadata_.isset = false;
  // Check if the physical and logical types match
  // Mapping referred from Apache parquet-mr as on 2016-02-22
  switch (logical_type) {
    case LogicalType::NONE:
      // Logical type not set
      // Clients should be able to read these values
      decimal_metadata_.isset = true;
      decimal_metadata_.precision = precision;
      decimal_metadata_.scale = scale;
      break;
    case LogicalType::UTF8:
    case LogicalType::JSON:
    case LogicalType::BSON:
      if (type != Type::BYTE_ARRAY) {
        ss << logical_type_to_string(logical_type);
        ss << " can only annotate BYTE_ARRAY fields";
        throw ParquetException(ss.str());
      }
      break;
    case LogicalType::DECIMAL:
      if ((type != Type::INT32) && (type != Type::INT64) && (type != Type::BYTE_ARRAY) &&
          (type != Type::FIXED_LEN_BYTE_ARRAY)) {
        ss << "DECIMAL can only annotate INT32, INT64, BYTE_ARRAY, and FIXED";
        throw ParquetException(ss.str());
      }
      if (precision <= 0) {
        ss << "Invalid DECIMAL precision: " << precision;
        throw ParquetException(ss.str());
      }
      if (scale < 0) {
        ss << "Invalid DECIMAL scale: " << scale;
        throw ParquetException(ss.str());
      }
      if (scale > precision) {
        ss << "Invalid DECIMAL scale " << scale;
        ss << " cannot be greater than precision " << precision;
        throw ParquetException(ss.str());
      }
      decimal_metadata_.isset = true;
      decimal_metadata_.precision = precision;
      decimal_metadata_.scale = scale;
      break;
    case LogicalType::DATE:
    case LogicalType::TIME_MILLIS:
    case LogicalType::UINT_8:
    case LogicalType::UINT_16:
    case LogicalType::UINT_32:
    case LogicalType::INT_8:
    case LogicalType::INT_16:
    case LogicalType::INT_32:
      if (type != Type::INT32) {
        ss << logical_type_to_string(logical_type);
        ss << " can only annotate INT32";
        throw ParquetException(ss.str());
      }
      break;
    case LogicalType::TIMESTAMP_MILLIS:
    case LogicalType::UINT_64:
    case LogicalType::INT_64:
      if (type != Type::INT64) {
        ss << logical_type_to_string(logical_type);
        ss << " can only annotate INT64";
        throw ParquetException(ss.str());
      }
      break;
    case LogicalType::INTERVAL:
      if ((type != Type::FIXED_LEN_BYTE_ARRAY) || (length != 12)) {
        ss << "INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)";
        throw ParquetException(ss.str());
      }
      break;
    case LogicalType::ENUM:
      if (type != Type::BYTE_ARRAY) {
        ss << "ENUM can only annotate BYTE_ARRAY fields";
        throw ParquetException(ss.str());
      }
      break;
    default:
      ss << logical_type_to_string(logical_type);
      ss << " can not be applied to a primitive type";
      throw ParquetException(ss.str());
  }
  if (type == Type::FIXED_LEN_BYTE_ARRAY) {
    if (length <= 0) {
      ss << "Invalid FIXED_LEN_BYTE_ARRAY length: " << length;
      throw ParquetException(ss.str());
    }
    type_length_ = length;
  }
}

bool PrimitiveNode::EqualsInternal(const PrimitiveNode* other) const {
  bool is_equal = true;
  if ((physical_type_ != other->physical_type_) ||
      (logical_type_ != other->logical_type_)) {
    return false;
  }
  if (logical_type_ == LogicalType::DECIMAL) {
    is_equal &= (decimal_metadata_.precision == other->decimal_metadata_.precision) &&
                (decimal_metadata_.scale == other->decimal_metadata_.scale);
  }
  if (physical_type_ == Type::FIXED_LEN_BYTE_ARRAY) {
    is_equal &= (type_length_ == other->type_length_);
  }
  return is_equal;
}

bool PrimitiveNode::Equals(const Node* other) const {
  if (!Node::EqualsInternal(other)) { return false; }
  return EqualsInternal(static_cast<const PrimitiveNode*>(other));
}

void PrimitiveNode::Visit(Node::Visitor* visitor) {
  visitor->Visit(this);
}

void PrimitiveNode::VisitConst(Node::ConstVisitor* visitor) const {
  visitor->Visit(this);
}

// ----------------------------------------------------------------------
// Group node

bool GroupNode::EqualsInternal(const GroupNode* other) const {
  if (this == other) { return true; }
  if (this->field_count() != other->field_count()) { return false; }
  for (int i = 0; i < this->field_count(); ++i) {
    if (!this->field(i)->Equals(other->field(i).get())) { return false; }
  }
  return true;
}

bool GroupNode::Equals(const Node* other) const {
  if (!Node::EqualsInternal(other)) { return false; }
  return EqualsInternal(static_cast<const GroupNode*>(other));
}

void GroupNode::Visit(Node::Visitor* visitor) {
  visitor->Visit(this);
}

void GroupNode::VisitConst(Node::ConstVisitor* visitor) const {
  visitor->Visit(this);
}

// ----------------------------------------------------------------------
// Node construction from Parquet metadata

struct NodeParams {
  explicit NodeParams(const std::string& name) : name(name) {}

  const std::string& name;
  Repetition::type repetition;
  LogicalType::type logical_type;
};

static inline NodeParams GetNodeParams(const format::SchemaElement* element) {
  NodeParams params(element->name);

  params.repetition = FromThrift(element->repetition_type);
  if (element->__isset.converted_type) {
    params.logical_type = FromThrift(element->converted_type);
  } else {
    params.logical_type = LogicalType::NONE;
  }
  return params;
}

std::unique_ptr<Node> GroupNode::FromParquet(
    const void* opaque_element, int node_id, const NodeVector& fields) {
  const format::SchemaElement* element =
      static_cast<const format::SchemaElement*>(opaque_element);
  NodeParams params = GetNodeParams(element);
  return std::unique_ptr<Node>(new GroupNode(
      params.name, params.repetition, fields, params.logical_type, node_id));
}

std::unique_ptr<Node> PrimitiveNode::FromParquet(
    const void* opaque_element, int node_id) {
  const format::SchemaElement* element =
      static_cast<const format::SchemaElement*>(opaque_element);
  NodeParams params = GetNodeParams(element);

  std::unique_ptr<PrimitiveNode> result =
      std::unique_ptr<PrimitiveNode>(new PrimitiveNode(params.name, params.repetition,
          FromThrift(element->type), params.logical_type, element->type_length,
          element->precision, element->scale, node_id));

  // Return as unique_ptr to the base type
  return std::unique_ptr<Node>(result.release());
}

void GroupNode::ToParquet(void* opaque_element) const {
  format::SchemaElement* element = static_cast<format::SchemaElement*>(opaque_element);
  element->__set_name(name_);
  element->__set_num_children(field_count());
  element->__set_repetition_type(ToThrift(repetition_));
  if (logical_type_ != LogicalType::NONE) {
    element->__set_converted_type(ToThrift(logical_type_));
  }
  // FIXME: SchemaFlattener does this for us: element->__set_field_id(id_);
}

void PrimitiveNode::ToParquet(void* opaque_element) const {
  format::SchemaElement* element = static_cast<format::SchemaElement*>(opaque_element);

  element->__set_name(name_);
  element->__set_num_children(0);
  element->__set_repetition_type(ToThrift(repetition_));
  if (logical_type_ != LogicalType::NONE) {
    element->__set_converted_type(ToThrift(logical_type_));
  }
  element->__set_type(ToThrift(physical_type_));
  // FIXME: SchemaFlattener does this for us: element->__set_field_id(id_);
  element->__set_type_length(type_length_);
  if (decimal_metadata_.isset) {
    element->__set_precision(decimal_metadata_.precision);
    element->__set_scale(decimal_metadata_.scale);
  }
}

}  // namespace schema

}  // namespace parquet
