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

#include "parquet/schema.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "arrow/util/logging.h"
#include "parquet/exception.h"
#include "parquet/schema_internal.h"
#include "parquet/thrift_internal.h"

using parquet::format::SchemaElement;

namespace parquet {

namespace schema {

namespace {

void ThrowInvalidLogicalType(const LogicalType& logical_type) {
  std::stringstream ss;
  ss << "Invalid logical type: " << logical_type.ToString();
  throw ParquetException(ss.str());
}

}  // namespace

// ----------------------------------------------------------------------
// ColumnPath

std::shared_ptr<ColumnPath> ColumnPath::FromDotString(const std::string& dotstring) {
  std::stringstream ss(dotstring);
  std::string item;
  std::vector<std::string> path;
  while (std::getline(ss, item, '.')) {
    path.push_back(item);
  }
  return std::make_shared<ColumnPath>(std::move(path));
}

std::shared_ptr<ColumnPath> ColumnPath::FromNode(const Node& node) {
  // Build the path in reverse order as we traverse the nodes to the top
  std::vector<std::string> rpath_;
  const Node* cursor = &node;
  // The schema node is not part of the ColumnPath
  while (cursor->parent()) {
    rpath_.push_back(cursor->name());
    cursor = cursor->parent();
  }

  // Build ColumnPath in correct order
  std::vector<std::string> path(rpath_.crbegin(), rpath_.crend());
  return std::make_shared<ColumnPath>(std::move(path));
}

std::shared_ptr<ColumnPath> ColumnPath::extend(const std::string& node_name) const {
  std::vector<std::string> path;
  path.reserve(path_.size() + 1);
  path.resize(path_.size() + 1);
  std::copy(path_.cbegin(), path_.cend(), path.begin());
  path[path_.size()] = node_name;

  return std::make_shared<ColumnPath>(std::move(path));
}

std::string ColumnPath::ToDotString() const {
  std::stringstream ss;
  for (auto it = path_.cbegin(); it != path_.cend(); ++it) {
    if (it != path_.cbegin()) {
      ss << ".";
    }
    ss << *it;
  }
  return ss.str();
}

const std::vector<std::string>& ColumnPath::ToDotVector() const { return path_; }

// ----------------------------------------------------------------------
// Base node

const std::shared_ptr<ColumnPath> Node::path() const {
  // TODO(itaiin): Cache the result, or more precisely, cache ->ToDotString()
  //    since it is being used to access the leaf nodes
  return ColumnPath::FromNode(*this);
}

bool Node::EqualsInternal(const Node* other) const {
  return type_ == other->type_ && name_ == other->name_ &&
         repetition_ == other->repetition_ && converted_type_ == other->converted_type_ &&
         field_id_ == other->field_id() &&
         logical_type_->Equals(*(other->logical_type()));
}

void Node::SetParent(const Node* parent) { parent_ = parent; }

// ----------------------------------------------------------------------
// Primitive node

PrimitiveNode::PrimitiveNode(const std::string& name, Repetition::type repetition,
                             Type::type type, ConvertedType::type converted_type,
                             int length, int precision, int scale, int id)
    : Node(Node::PRIMITIVE, name, repetition, converted_type, id),
      physical_type_(type),
      type_length_(length) {
  std::stringstream ss;

  // PARQUET-842: In an earlier revision, decimal_metadata_.isset was being
  // set to true, but Impala will raise an incompatible metadata in such cases
  memset(&decimal_metadata_, 0, sizeof(decimal_metadata_));

  // Check if the physical and logical types match
  // Mapping referred from Apache parquet-mr as on 2016-02-22
  switch (converted_type) {
    case ConvertedType::NONE:
      // Logical type not set
      break;
    case ConvertedType::UTF8:
    case ConvertedType::JSON:
    case ConvertedType::BSON:
      if (type != Type::BYTE_ARRAY) {
        ss << ConvertedTypeToString(converted_type);
        ss << " can only annotate BYTE_ARRAY fields";
        throw ParquetException(ss.str());
      }
      break;
    case ConvertedType::DECIMAL:
      if ((type != Type::INT32) && (type != Type::INT64) && (type != Type::BYTE_ARRAY) &&
          (type != Type::FIXED_LEN_BYTE_ARRAY)) {
        ss << "DECIMAL can only annotate INT32, INT64, BYTE_ARRAY, and FIXED";
        throw ParquetException(ss.str());
      }
      if (precision <= 0) {
        ss << "Invalid DECIMAL precision: " << precision
           << ". Precision must be a number between 1 and 38 inclusive";
        throw ParquetException(ss.str());
      }
      if (scale < 0) {
        ss << "Invalid DECIMAL scale: " << scale
           << ". Scale must be a number between 0 and precision inclusive";
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
    case ConvertedType::DATE:
    case ConvertedType::TIME_MILLIS:
    case ConvertedType::UINT_8:
    case ConvertedType::UINT_16:
    case ConvertedType::UINT_32:
    case ConvertedType::INT_8:
    case ConvertedType::INT_16:
    case ConvertedType::INT_32:
      if (type != Type::INT32) {
        ss << ConvertedTypeToString(converted_type);
        ss << " can only annotate INT32";
        throw ParquetException(ss.str());
      }
      break;
    case ConvertedType::TIME_MICROS:
    case ConvertedType::TIMESTAMP_MILLIS:
    case ConvertedType::TIMESTAMP_MICROS:
    case ConvertedType::UINT_64:
    case ConvertedType::INT_64:
      if (type != Type::INT64) {
        ss << ConvertedTypeToString(converted_type);
        ss << " can only annotate INT64";
        throw ParquetException(ss.str());
      }
      break;
    case ConvertedType::INTERVAL:
      if ((type != Type::FIXED_LEN_BYTE_ARRAY) || (length != 12)) {
        ss << "INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)";
        throw ParquetException(ss.str());
      }
      break;
    case ConvertedType::ENUM:
      if (type != Type::BYTE_ARRAY) {
        ss << "ENUM can only annotate BYTE_ARRAY fields";
        throw ParquetException(ss.str());
      }
      break;
    case ConvertedType::NA:
      // NA can annotate any type
      break;
    default:
      ss << ConvertedTypeToString(converted_type);
      ss << " cannot be applied to a primitive type";
      throw ParquetException(ss.str());
  }
  // For forward compatibility, create an equivalent logical type
  logical_type_ = LogicalType::FromConvertedType(converted_type_, decimal_metadata_);
  if (!(logical_type_ && !logical_type_->is_nested() &&
        logical_type_->is_compatible(converted_type_, decimal_metadata_))) {
    ThrowInvalidLogicalType(*logical_type_);
  }

  if (type == Type::FIXED_LEN_BYTE_ARRAY) {
    if (length <= 0) {
      ss << "Invalid FIXED_LEN_BYTE_ARRAY length: " << length;
      throw ParquetException(ss.str());
    }
    type_length_ = length;
  }
}

PrimitiveNode::PrimitiveNode(const std::string& name, Repetition::type repetition,
                             std::shared_ptr<const LogicalType> logical_type,
                             Type::type physical_type, int physical_length, int id)
    : Node(Node::PRIMITIVE, name, repetition, std::move(logical_type), id),
      physical_type_(physical_type),
      type_length_(physical_length) {
  std::stringstream error;
  if (logical_type_) {
    // Check for logical type <=> node type consistency
    if (!logical_type_->is_nested()) {
      // Check for logical type <=> physical type consistency
      if (logical_type_->is_applicable(physical_type, physical_length)) {
        // For backward compatibility, assign equivalent legacy
        // converted type (if possible)
        converted_type_ = logical_type_->ToConvertedType(&decimal_metadata_);
      } else {
        error << logical_type_->ToString();
        error << " can not be applied to primitive type ";
        error << TypeToString(physical_type);
        throw ParquetException(error.str());
      }
    } else {
      error << "Nested logical type ";
      error << logical_type_->ToString();
      error << " can not be applied to non-group node";
      throw ParquetException(error.str());
    }
  } else {
    logical_type_ = NoLogicalType::Make();
    converted_type_ = logical_type_->ToConvertedType(&decimal_metadata_);
  }
  if (!(logical_type_ && !logical_type_->is_nested() &&
        logical_type_->is_compatible(converted_type_, decimal_metadata_))) {
    ThrowInvalidLogicalType(*logical_type_);
  }

  if (physical_type == Type::FIXED_LEN_BYTE_ARRAY) {
    if (physical_length <= 0) {
      error << "Invalid FIXED_LEN_BYTE_ARRAY length: " << physical_length;
      throw ParquetException(error.str());
    }
  }
}

bool PrimitiveNode::EqualsInternal(const PrimitiveNode* other) const {
  bool is_equal = true;
  if (physical_type_ != other->physical_type_) {
    return false;
  }
  if (converted_type_ == ConvertedType::DECIMAL) {
    is_equal &= (decimal_metadata_.precision == other->decimal_metadata_.precision) &&
                (decimal_metadata_.scale == other->decimal_metadata_.scale);
  }
  if (physical_type_ == Type::FIXED_LEN_BYTE_ARRAY) {
    is_equal &= (type_length_ == other->type_length_);
  }
  return is_equal;
}

bool PrimitiveNode::Equals(const Node* other) const {
  if (!Node::EqualsInternal(other)) {
    return false;
  }
  return EqualsInternal(static_cast<const PrimitiveNode*>(other));
}

void PrimitiveNode::Visit(Node::Visitor* visitor) { visitor->Visit(this); }

void PrimitiveNode::VisitConst(Node::ConstVisitor* visitor) const {
  visitor->Visit(this);
}

// ----------------------------------------------------------------------
// Group node

GroupNode::GroupNode(const std::string& name, Repetition::type repetition,
                     const NodeVector& fields, ConvertedType::type converted_type, int id)
    : Node(Node::GROUP, name, repetition, converted_type, id), fields_(fields) {
  // For forward compatibility, create an equivalent logical type
  logical_type_ = LogicalType::FromConvertedType(converted_type_);
  if (!(logical_type_ && (logical_type_->is_nested() || logical_type_->is_none()) &&
        logical_type_->is_compatible(converted_type_))) {
    ThrowInvalidLogicalType(*logical_type_);
  }

  field_name_to_idx_.clear();
  auto field_idx = 0;
  for (NodePtr& field : fields_) {
    field->SetParent(this);
    field_name_to_idx_.emplace(field->name(), field_idx++);
  }
}

GroupNode::GroupNode(const std::string& name, Repetition::type repetition,
                     const NodeVector& fields,
                     std::shared_ptr<const LogicalType> logical_type, int id)
    : Node(Node::GROUP, name, repetition, std::move(logical_type), id), fields_(fields) {
  if (logical_type_) {
    // Check for logical type <=> node type consistency
    if (logical_type_->is_nested()) {
      // For backward compatibility, assign equivalent legacy converted type (if possible)
      converted_type_ = logical_type_->ToConvertedType(nullptr);
    } else {
      std::stringstream error;
      error << "Logical type ";
      error << logical_type_->ToString();
      error << " can not be applied to group node";
      throw ParquetException(error.str());
    }
  } else {
    logical_type_ = NoLogicalType::Make();
    converted_type_ = logical_type_->ToConvertedType(nullptr);
  }
  if (!(logical_type_ && (logical_type_->is_nested() || logical_type_->is_none()) &&
        logical_type_->is_compatible(converted_type_))) {
    ThrowInvalidLogicalType(*logical_type_);
  }

  field_name_to_idx_.clear();
  auto field_idx = 0;
  for (NodePtr& field : fields_) {
    field->SetParent(this);
    field_name_to_idx_.emplace(field->name(), field_idx++);
  }
}

bool GroupNode::EqualsInternal(const GroupNode* other) const {
  if (this == other) {
    return true;
  }
  if (this->field_count() != other->field_count()) {
    return false;
  }
  for (int i = 0; i < this->field_count(); ++i) {
    if (!this->field(i)->Equals(other->field(i).get())) {
      return false;
    }
  }
  return true;
}

bool GroupNode::Equals(const Node* other) const {
  if (!Node::EqualsInternal(other)) {
    return false;
  }
  return EqualsInternal(static_cast<const GroupNode*>(other));
}

int GroupNode::FieldIndex(const std::string& name) const {
  auto search = field_name_to_idx_.find(name);
  if (search == field_name_to_idx_.end()) {
    // Not found
    return -1;
  }
  return search->second;
}

int GroupNode::FieldIndex(const Node& node) const {
  auto search = field_name_to_idx_.equal_range(node.name());
  for (auto it = search.first; it != search.second; ++it) {
    const int idx = it->second;
    if (&node == field(idx).get()) {
      return idx;
    }
  }
  return -1;
}

void GroupNode::Visit(Node::Visitor* visitor) { visitor->Visit(this); }

void GroupNode::VisitConst(Node::ConstVisitor* visitor) const { visitor->Visit(this); }

// ----------------------------------------------------------------------
// Node construction from Parquet metadata

std::unique_ptr<Node> GroupNode::FromParquet(const void* opaque_element,
                                             NodeVector fields) {
  const format::SchemaElement* element =
      static_cast<const format::SchemaElement*>(opaque_element);

  int field_id = -1;
  if (element->__isset.field_id) {
    field_id = element->field_id;
  }

  std::unique_ptr<GroupNode> group_node;
  if (element->__isset.logicalType) {
    // updated writer with logical type present
    group_node = std::unique_ptr<GroupNode>(
        new GroupNode(element->name, LoadEnumSafe(&element->repetition_type), fields,
                      LogicalType::FromThrift(element->logicalType), field_id));
  } else {
    group_node = std::unique_ptr<GroupNode>(new GroupNode(
        element->name, LoadEnumSafe(&element->repetition_type), fields,
        (element->__isset.converted_type ? LoadEnumSafe(&element->converted_type)
                                         : ConvertedType::NONE),
        field_id));
  }

  return std::unique_ptr<Node>(group_node.release());
}

std::unique_ptr<Node> PrimitiveNode::FromParquet(const void* opaque_element) {
  const format::SchemaElement* element =
      static_cast<const format::SchemaElement*>(opaque_element);

  int field_id = -1;
  if (element->__isset.field_id) {
    field_id = element->field_id;
  }

  std::unique_ptr<PrimitiveNode> primitive_node;
  if (element->__isset.logicalType) {
    // updated writer with logical type present
    primitive_node = std::unique_ptr<PrimitiveNode>(
        new PrimitiveNode(element->name, LoadEnumSafe(&element->repetition_type),
                          LogicalType::FromThrift(element->logicalType),
                          LoadEnumSafe(&element->type), element->type_length, field_id));
  } else if (element->__isset.converted_type) {
    // legacy writer with converted type present
    primitive_node = std::unique_ptr<PrimitiveNode>(new PrimitiveNode(
        element->name, LoadEnumSafe(&element->repetition_type),
        LoadEnumSafe(&element->type), LoadEnumSafe(&element->converted_type),
        element->type_length, element->precision, element->scale, field_id));
  } else {
    // logical type not present
    primitive_node = std::unique_ptr<PrimitiveNode>(new PrimitiveNode(
        element->name, LoadEnumSafe(&element->repetition_type), NoLogicalType::Make(),
        LoadEnumSafe(&element->type), element->type_length, field_id));
  }

  // Return as unique_ptr to the base type
  return std::unique_ptr<Node>(primitive_node.release());
}

bool GroupNode::HasRepeatedFields() const {
  for (int i = 0; i < this->field_count(); ++i) {
    auto field = this->field(i);
    if (field->repetition() == Repetition::REPEATED) {
      return true;
    }
    if (field->is_group()) {
      const auto& group = static_cast<const GroupNode&>(*field);
      return group.HasRepeatedFields();
    }
  }
  return false;
}

void GroupNode::ToParquet(void* opaque_element) const {
  format::SchemaElement* element = static_cast<format::SchemaElement*>(opaque_element);
  element->__set_name(name_);
  element->__set_num_children(field_count());
  element->__set_repetition_type(ToThrift(repetition_));
  if (converted_type_ != ConvertedType::NONE) {
    element->__set_converted_type(ToThrift(converted_type_));
  }
  if (field_id_ >= 0) {
    element->__set_field_id(field_id_);
  }
  if (logical_type_ && logical_type_->is_serialized()) {
    element->__set_logicalType(logical_type_->ToThrift());
  }
  return;
}

void PrimitiveNode::ToParquet(void* opaque_element) const {
  format::SchemaElement* element = static_cast<format::SchemaElement*>(opaque_element);
  element->__set_name(name_);
  element->__set_repetition_type(ToThrift(repetition_));
  if (converted_type_ != ConvertedType::NONE) {
    if (converted_type_ != ConvertedType::NA) {
      element->__set_converted_type(ToThrift(converted_type_));
    } else {
      // ConvertedType::NA is an unreleased, obsolete synonym for LogicalType::Null.
      // Never emit it (see PARQUET-1990 for discussion).
      if (!logical_type_ || !logical_type_->is_null()) {
        throw ParquetException(
            "ConvertedType::NA is obsolete, please use LogicalType::Null instead");
      }
    }
  }
  if (field_id_ >= 0) {
    element->__set_field_id(field_id_);
  }
  if (logical_type_ && logical_type_->is_serialized() &&
      // TODO(tpboudreau): remove the following conjunct to enable serialization
      // of IntervalTypes after parquet.thrift recognizes them
      !logical_type_->is_interval()) {
    element->__set_logicalType(logical_type_->ToThrift());
  }
  element->__set_type(ToThrift(physical_type_));
  if (physical_type_ == Type::FIXED_LEN_BYTE_ARRAY) {
    element->__set_type_length(type_length_);
  }
  if (decimal_metadata_.isset) {
    element->__set_precision(decimal_metadata_.precision);
    element->__set_scale(decimal_metadata_.scale);
  }
  return;
}

// ----------------------------------------------------------------------
// Schema converters

std::unique_ptr<Node> Unflatten(const format::SchemaElement* elements, int length) {
  if (elements[0].num_children == 0) {
    if (length == 1) {
      // Degenerate case of Parquet file with no columns
      return GroupNode::FromParquet(elements, {});
    } else {
      throw ParquetException(
          "Parquet schema had multiple nodes but root had no children");
    }
  }

  // We don't check that the root node is repeated since this is not
  // consistently set by implementations

  int pos = 0;

  std::function<std::unique_ptr<Node>()> NextNode = [&]() {
    if (pos == length) {
      throw ParquetException("Malformed schema: not enough elements");
    }
    const SchemaElement& element = elements[pos++];
    const void* opaque_element = static_cast<const void*>(&element);

    if (element.num_children == 0 && element.__isset.type) {
      // Leaf (primitive) node: always has a type
      return PrimitiveNode::FromParquet(opaque_element);
    } else {
      // Group node (may have 0 children, but cannot have a type)
      NodeVector fields;
      for (int i = 0; i < element.num_children; ++i) {
        std::unique_ptr<Node> field = NextNode();
        fields.push_back(NodePtr(field.release()));
      }
      return GroupNode::FromParquet(opaque_element, std::move(fields));
    }
  };
  return NextNode();
}

std::shared_ptr<SchemaDescriptor> FromParquet(const std::vector<SchemaElement>& schema) {
  if (schema.empty()) {
    throw ParquetException("Empty file schema (no root)");
  }
  std::unique_ptr<Node> root = Unflatten(&schema[0], static_cast<int>(schema.size()));
  std::shared_ptr<SchemaDescriptor> descr = std::make_shared<SchemaDescriptor>();
  descr->Init(std::shared_ptr<GroupNode>(static_cast<GroupNode*>(root.release())));
  return descr;
}

class SchemaVisitor : public Node::ConstVisitor {
 public:
  explicit SchemaVisitor(std::vector<format::SchemaElement>* elements)
      : elements_(elements) {}

  void Visit(const Node* node) override {
    format::SchemaElement element;
    node->ToParquet(&element);
    elements_->push_back(element);

    if (node->is_group()) {
      const GroupNode* group_node = static_cast<const GroupNode*>(node);
      for (int i = 0; i < group_node->field_count(); ++i) {
        group_node->field(i)->VisitConst(this);
      }
    }
  }

 private:
  std::vector<format::SchemaElement>* elements_;
};

void ToParquet(const GroupNode* schema, std::vector<format::SchemaElement>* out) {
  SchemaVisitor visitor(out);
  schema->VisitConst(&visitor);
}

// ----------------------------------------------------------------------
// Schema printing

static void PrintRepLevel(Repetition::type repetition, std::ostream& stream) {
  switch (repetition) {
    case Repetition::REQUIRED:
      stream << "required";
      break;
    case Repetition::OPTIONAL:
      stream << "optional";
      break;
    case Repetition::REPEATED:
      stream << "repeated";
      break;
    default:
      break;
  }
}

static void PrintType(const PrimitiveNode* node, std::ostream& stream) {
  switch (node->physical_type()) {
    case Type::BOOLEAN:
      stream << "boolean";
      break;
    case Type::INT32:
      stream << "int32";
      break;
    case Type::INT64:
      stream << "int64";
      break;
    case Type::INT96:
      stream << "int96";
      break;
    case Type::FLOAT:
      stream << "float";
      break;
    case Type::DOUBLE:
      stream << "double";
      break;
    case Type::BYTE_ARRAY:
      stream << "binary";
      break;
    case Type::FIXED_LEN_BYTE_ARRAY:
      stream << "fixed_len_byte_array(" << node->type_length() << ")";
      break;
    default:
      break;
  }
}

static void PrintConvertedType(const PrimitiveNode* node, std::ostream& stream) {
  auto lt = node->converted_type();
  auto la = node->logical_type();
  if (la && la->is_valid() && !la->is_none()) {
    stream << " (" << la->ToString() << ")";
  } else if (lt == ConvertedType::DECIMAL) {
    stream << " (" << ConvertedTypeToString(lt) << "("
           << node->decimal_metadata().precision << "," << node->decimal_metadata().scale
           << "))";
  } else if (lt != ConvertedType::NONE) {
    stream << " (" << ConvertedTypeToString(lt) << ")";
  }
}

struct SchemaPrinter : public Node::ConstVisitor {
  explicit SchemaPrinter(std::ostream& stream, int indent_width)
      : stream_(stream), indent_(0), indent_width_(2) {}

  void Indent() {
    if (indent_ > 0) {
      std::string spaces(indent_, ' ');
      stream_ << spaces;
    }
  }

  void Visit(const Node* node) {
    Indent();
    if (node->is_group()) {
      Visit(static_cast<const GroupNode*>(node));
    } else {
      // Primitive
      Visit(static_cast<const PrimitiveNode*>(node));
    }
  }

  void Visit(const PrimitiveNode* node) {
    PrintRepLevel(node->repetition(), stream_);
    stream_ << " ";
    PrintType(node, stream_);
    stream_ << " field_id=" << node->field_id() << " " << node->name();
    PrintConvertedType(node, stream_);
    stream_ << ";" << std::endl;
  }

  void Visit(const GroupNode* node) {
    PrintRepLevel(node->repetition(), stream_);
    stream_ << " group "
            << "field_id=" << node->field_id() << " " << node->name();
    auto lt = node->converted_type();
    auto la = node->logical_type();
    if (la && la->is_valid() && !la->is_none()) {
      stream_ << " (" << la->ToString() << ")";
    } else if (lt != ConvertedType::NONE) {
      stream_ << " (" << ConvertedTypeToString(lt) << ")";
    }
    stream_ << " {" << std::endl;

    indent_ += indent_width_;
    for (int i = 0; i < node->field_count(); ++i) {
      node->field(i)->VisitConst(this);
    }
    indent_ -= indent_width_;
    Indent();
    stream_ << "}" << std::endl;
  }

  std::ostream& stream_;
  int indent_;
  int indent_width_;
};

void PrintSchema(const Node* schema, std::ostream& stream, int indent_width) {
  SchemaPrinter printer(stream, indent_width);
  printer.Visit(schema);
}

}  // namespace schema

using schema::ColumnPath;
using schema::GroupNode;
using schema::Node;
using schema::NodePtr;
using schema::PrimitiveNode;

void SchemaDescriptor::Init(std::unique_ptr<schema::Node> schema) {
  Init(NodePtr(schema.release()));
}

class SchemaUpdater : public Node::Visitor {
 public:
  explicit SchemaUpdater(const std::vector<ColumnOrder>& column_orders)
      : column_orders_(column_orders), leaf_count_(0) {}

  void Visit(Node* node) override {
    if (node->is_group()) {
      GroupNode* group_node = static_cast<GroupNode*>(node);
      for (int i = 0; i < group_node->field_count(); ++i) {
        group_node->field(i)->Visit(this);
      }
    } else {  // leaf node
      PrimitiveNode* leaf_node = static_cast<PrimitiveNode*>(node);
      leaf_node->SetColumnOrder(column_orders_[leaf_count_++]);
    }
  }

 private:
  const std::vector<ColumnOrder>& column_orders_;
  int leaf_count_;
};

void SchemaDescriptor::updateColumnOrders(const std::vector<ColumnOrder>& column_orders) {
  if (static_cast<int>(column_orders.size()) != num_columns()) {
    throw ParquetException("Malformed schema: not enough ColumnOrder values");
  }
  SchemaUpdater visitor(column_orders);
  const_cast<GroupNode*>(group_node_)->Visit(&visitor);
}

void SchemaDescriptor::Init(NodePtr schema) {
  schema_ = std::move(schema);

  if (!schema_->is_group()) {
    throw ParquetException("Must initialize with a schema group");
  }

  group_node_ = static_cast<const GroupNode*>(schema_.get());
  leaves_.clear();

  for (int i = 0; i < group_node_->field_count(); ++i) {
    BuildTree(group_node_->field(i), 0, 0, group_node_->field(i));
  }
}

bool SchemaDescriptor::Equals(const SchemaDescriptor& other) const {
  if (this->num_columns() != other.num_columns()) {
    return false;
  }

  for (int i = 0; i < this->num_columns(); ++i) {
    if (!this->Column(i)->Equals(*other.Column(i))) {
      return false;
    }
  }

  return true;
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
    node_to_leaf_index_[static_cast<const PrimitiveNode*>(node.get())] =
        static_cast<int>(leaves_.size());

    // Primitive node, append to leaves
    leaves_.push_back(ColumnDescriptor(node, max_def_level, max_rep_level, this));
    leaf_to_base_.emplace(static_cast<int>(leaves_.size()) - 1, base);
    leaf_to_idx_.emplace(node->path()->ToDotString(),
                         static_cast<int>(leaves_.size()) - 1);
  }
}

int SchemaDescriptor::GetColumnIndex(const PrimitiveNode& node) const {
  auto it = node_to_leaf_index_.find(&node);
  if (it == node_to_leaf_index_.end()) {
    return -1;
  }
  return it->second;
}

ColumnDescriptor::ColumnDescriptor(schema::NodePtr node, int16_t max_definition_level,
                                   int16_t max_repetition_level,
                                   const SchemaDescriptor* schema_descr)
    : node_(std::move(node)),
      max_definition_level_(max_definition_level),
      max_repetition_level_(max_repetition_level) {
  if (!node_->is_primitive()) {
    throw ParquetException("Must be a primitive type");
  }
  primitive_node_ = static_cast<const PrimitiveNode*>(node_.get());
}

bool ColumnDescriptor::Equals(const ColumnDescriptor& other) const {
  return primitive_node_->Equals(other.primitive_node_) &&
         max_repetition_level() == other.max_repetition_level() &&
         max_definition_level() == other.max_definition_level();
}

const ColumnDescriptor* SchemaDescriptor::Column(int i) const {
  DCHECK(i >= 0 && i < static_cast<int>(leaves_.size()));
  return &leaves_[i];
}

int SchemaDescriptor::ColumnIndex(const std::string& node_path) const {
  auto search = leaf_to_idx_.find(node_path);
  if (search == leaf_to_idx_.end()) {
    // Not found
    return -1;
  }
  return search->second;
}

int SchemaDescriptor::ColumnIndex(const Node& node) const {
  auto search = leaf_to_idx_.equal_range(node.path()->ToDotString());
  for (auto it = search.first; it != search.second; ++it) {
    const int idx = it->second;
    if (&node == Column(idx)->schema_node().get()) {
      return idx;
    }
  }
  return -1;
}

const schema::Node* SchemaDescriptor::GetColumnRoot(int i) const {
  DCHECK(i >= 0 && i < static_cast<int>(leaves_.size()));
  return leaf_to_base_.find(i)->second.get();
}

bool SchemaDescriptor::HasRepeatedFields() const {
  return group_node_->HasRepeatedFields();
}

std::string SchemaDescriptor::ToString() const {
  std::ostringstream ss;
  PrintSchema(schema_.get(), ss);
  return ss.str();
}

std::string ColumnDescriptor::ToString() const {
  std::ostringstream ss;
  ss << "column descriptor = {" << std::endl
     << "  name: " << name() << "," << std::endl
     << "  path: " << path()->ToDotString() << "," << std::endl
     << "  physical_type: " << TypeToString(physical_type()) << "," << std::endl
     << "  converted_type: " << ConvertedTypeToString(converted_type()) << ","
     << std::endl
     << "  logical_type: " << logical_type()->ToString() << "," << std::endl
     << "  max_definition_level: " << max_definition_level() << "," << std::endl
     << "  max_repetition_level: " << max_repetition_level() << "," << std::endl;

  if (physical_type() == ::parquet::Type::FIXED_LEN_BYTE_ARRAY) {
    ss << "  length: " << type_length() << "," << std::endl;
  }

  if (converted_type() == parquet::ConvertedType::DECIMAL) {
    ss << "  precision: " << type_precision() << "," << std::endl
       << "  scale: " << type_scale() << "," << std::endl;
  }

  ss << "}";
  return ss.str();
}

int ColumnDescriptor::type_scale() const {
  return primitive_node_->decimal_metadata().scale;
}

int ColumnDescriptor::type_precision() const {
  return primitive_node_->decimal_metadata().precision;
}

int ColumnDescriptor::type_length() const { return primitive_node_->type_length(); }

const std::shared_ptr<ColumnPath> ColumnDescriptor::path() const {
  return primitive_node_->path();
}

}  // namespace parquet
