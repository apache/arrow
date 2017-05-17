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

#include "parquet/arrow/schema.h"

#include <string>
#include <unordered_set>
#include <vector>

#include "parquet/api/schema.h"

#include "arrow/api.h"

using arrow::Field;
using arrow::Status;
using arrow::TypePtr;

using ArrowType = arrow::Type;

using parquet::Repetition;
using parquet::schema::Node;
using parquet::schema::NodePtr;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

using ParquetType = parquet::Type;
using parquet::LogicalType;

namespace parquet {

namespace arrow {

const auto TIMESTAMP_MS = ::arrow::timestamp(::arrow::TimeUnit::MILLI);
const auto TIMESTAMP_US = ::arrow::timestamp(::arrow::TimeUnit::MICRO);
const auto TIMESTAMP_NS = ::arrow::timestamp(::arrow::TimeUnit::NANO);

TypePtr MakeDecimalType(const PrimitiveNode* node) {
  int precision = node->decimal_metadata().precision;
  int scale = node->decimal_metadata().scale;
  return std::make_shared<::arrow::DecimalType>(precision, scale);
}

static Status FromByteArray(const PrimitiveNode* node, TypePtr* out) {
  switch (node->logical_type()) {
    case LogicalType::UTF8:
      *out = ::arrow::utf8();
      break;
    case LogicalType::DECIMAL:
      *out = MakeDecimalType(node);
      break;
    default:
      // BINARY
      *out = ::arrow::binary();
      break;
  }
  return Status::OK();
}

static Status FromFLBA(const PrimitiveNode* node, TypePtr* out) {
  switch (node->logical_type()) {
    case LogicalType::NONE:
      *out = ::arrow::fixed_size_binary(node->type_length());
      break;
    case LogicalType::DECIMAL:
      *out = MakeDecimalType(node);
      break;
    default:
      std::stringstream ss;
      ss << "Unhandled logical type " << LogicalTypeToString(node->logical_type())
         << " for fixed-length binary array";
      return Status::NotImplemented(ss.str());
      break;
  }

  return Status::OK();
}

static Status FromInt32(const PrimitiveNode* node, TypePtr* out) {
  switch (node->logical_type()) {
    case LogicalType::NONE:
      *out = ::arrow::int32();
      break;
    case LogicalType::UINT_8:
      *out = ::arrow::uint8();
      break;
    case LogicalType::INT_8:
      *out = ::arrow::int8();
      break;
    case LogicalType::UINT_16:
      *out = ::arrow::uint16();
      break;
    case LogicalType::INT_16:
      *out = ::arrow::int16();
      break;
    case LogicalType::INT_32:
      *out = ::arrow::int32();
      break;
    case LogicalType::UINT_32:
      *out = ::arrow::uint32();
      break;
    case LogicalType::DATE:
      *out = ::arrow::date32();
      break;
    case LogicalType::TIME_MILLIS:
      *out = ::arrow::time32(::arrow::TimeUnit::MILLI);
      break;
    case LogicalType::DECIMAL:
      *out = MakeDecimalType(node);
      break;
    default:
      std::stringstream ss;
      ss << "Unhandled logical type " << LogicalTypeToString(node->logical_type())
         << " for INT32";
      return Status::NotImplemented(ss.str());
      break;
  }
  return Status::OK();
}

static Status FromInt64(const PrimitiveNode* node, TypePtr* out) {
  switch (node->logical_type()) {
    case LogicalType::NONE:
      *out = ::arrow::int64();
      break;
    case LogicalType::INT_64:
      *out = ::arrow::int64();
      break;
    case LogicalType::UINT_64:
      *out = ::arrow::uint64();
      break;
    case LogicalType::DECIMAL:
      *out = MakeDecimalType(node);
      break;
    case LogicalType::TIMESTAMP_MILLIS:
      *out = TIMESTAMP_MS;
      break;
    case LogicalType::TIMESTAMP_MICROS:
      *out = TIMESTAMP_US;
      break;
    case LogicalType::TIME_MICROS:
      *out = ::arrow::time64(::arrow::TimeUnit::MICRO);
      break;
    default:
      std::stringstream ss;
      ss << "Unhandled logical type " << LogicalTypeToString(node->logical_type())
         << " for INT64";
      return Status::NotImplemented(ss.str());
      break;
  }
  return Status::OK();
}

Status FromPrimitive(const PrimitiveNode* primitive, TypePtr* out) {
  switch (primitive->physical_type()) {
    case ParquetType::BOOLEAN:
      *out = ::arrow::boolean();
      break;
    case ParquetType::INT32:
      RETURN_NOT_OK(FromInt32(primitive, out));
      break;
    case ParquetType::INT64:
      RETURN_NOT_OK(FromInt64(primitive, out));
      break;
    case ParquetType::INT96:
      *out = TIMESTAMP_NS;
      break;
    case ParquetType::FLOAT:
      *out = ::arrow::float32();
      break;
    case ParquetType::DOUBLE:
      *out = ::arrow::float64();
      break;
    case ParquetType::BYTE_ARRAY:
      RETURN_NOT_OK(FromByteArray(primitive, out));
      break;
    case ParquetType::FIXED_LEN_BYTE_ARRAY:
      RETURN_NOT_OK(FromFLBA(primitive, out));
      break;
  }
  return Status::OK();
}

// Forward declaration
Status NodeToFieldInternal(const NodePtr& node,
    const std::unordered_set<NodePtr>* included_leaf_nodes, std::shared_ptr<Field>* out);

/*
 * Auxilary function to test if a parquet schema node is a leaf node
 * that should be included in a resulting arrow schema
 */
inline bool IsIncludedLeaf(
    const NodePtr& node, const std::unordered_set<NodePtr>* included_leaf_nodes) {
  if (included_leaf_nodes == nullptr) { return true; }
  auto search = included_leaf_nodes->find(node);
  return (search != included_leaf_nodes->end());
}

Status StructFromGroup(const GroupNode* group,
    const std::unordered_set<NodePtr>* included_leaf_nodes, TypePtr* out) {
  std::vector<std::shared_ptr<Field>> fields;
  std::shared_ptr<Field> field;

  *out = nullptr;

  for (int i = 0; i < group->field_count(); i++) {
    RETURN_NOT_OK(NodeToFieldInternal(group->field(i), included_leaf_nodes, &field));
    if (field != nullptr) { fields.push_back(field); }
  }
  if (fields.size() > 0) { *out = std::make_shared<::arrow::StructType>(fields); }
  return Status::OK();
}

bool str_endswith_tuple(const std::string& str) {
  if (str.size() >= 6) { return str.substr(str.size() - 6, 6) == "_tuple"; }
  return false;
}

Status NodeToList(const GroupNode* group,
    const std::unordered_set<NodePtr>* included_leaf_nodes, TypePtr* out) {
  *out = nullptr;
  if (group->field_count() == 1) {
    // This attempts to resolve the preferred 3-level list encoding.
    NodePtr list_node = group->field(0);
    if (list_node->is_group() && list_node->is_repeated()) {
      const GroupNode* list_group = static_cast<const GroupNode*>(list_node.get());
      // Special case mentioned in the format spec:
      //   If the name is array or ends in _tuple, this should be a list of struct
      //   even for single child elements.
      if (list_group->field_count() == 1 && list_node->name() != "array" &&
          !str_endswith_tuple(list_node->name())) {
        // List of primitive type
        std::shared_ptr<Field> item_field;
        RETURN_NOT_OK(
            NodeToFieldInternal(list_group->field(0), included_leaf_nodes, &item_field));

        if (item_field != nullptr) { *out = ::arrow::list(item_field); }
      } else {
        // List of struct
        std::shared_ptr<::arrow::DataType> inner_type;
        RETURN_NOT_OK(StructFromGroup(list_group, included_leaf_nodes, &inner_type));
        if (inner_type != nullptr) {
          auto item_field = std::make_shared<Field>(list_node->name(), inner_type, false);
          *out = ::arrow::list(item_field);
        }
      }
    } else if (list_node->is_repeated()) {
      // repeated primitive node
      std::shared_ptr<::arrow::DataType> inner_type;
      if (IsIncludedLeaf(static_cast<NodePtr>(list_node), included_leaf_nodes)) {
        const PrimitiveNode* primitive =
            static_cast<const PrimitiveNode*>(list_node.get());
        RETURN_NOT_OK(FromPrimitive(primitive, &inner_type));
        auto item_field = std::make_shared<Field>(list_node->name(), inner_type, false);
        *out = ::arrow::list(item_field);
      }
    } else {
      return Status::NotImplemented(
          "Non-repeated groups in a LIST-annotated group are not supported.");
    }
  } else {
    return Status::NotImplemented(
        "Only LIST-annotated groups with a single child can be handled.");
  }
  return Status::OK();
}

Status NodeToField(const NodePtr& node, std::shared_ptr<Field>* out) {
  return NodeToFieldInternal(node, nullptr, out);
}

Status NodeToFieldInternal(const NodePtr& node,
    const std::unordered_set<NodePtr>* included_leaf_nodes, std::shared_ptr<Field>* out) {
  std::shared_ptr<::arrow::DataType> type = nullptr;
  bool nullable = !node->is_required();

  *out = nullptr;

  if (node->is_repeated()) {
    // 1-level LIST encoding fields are required
    std::shared_ptr<::arrow::DataType> inner_type;
    if (node->is_group()) {
      const GroupNode* group = static_cast<const GroupNode*>(node.get());
      RETURN_NOT_OK(StructFromGroup(group, included_leaf_nodes, &inner_type));
    } else if (IsIncludedLeaf(static_cast<NodePtr>(node), included_leaf_nodes)) {
      const PrimitiveNode* primitive = static_cast<const PrimitiveNode*>(node.get());
      RETURN_NOT_OK(FromPrimitive(primitive, &inner_type));
    }
    if (inner_type != nullptr) {
      auto item_field = std::make_shared<Field>(node->name(), inner_type, false);
      type = ::arrow::list(item_field);
      nullable = false;
    }
  } else if (node->is_group()) {
    const GroupNode* group = static_cast<const GroupNode*>(node.get());
    if (node->logical_type() == LogicalType::LIST) {
      RETURN_NOT_OK(NodeToList(group, included_leaf_nodes, &type));
    } else {
      RETURN_NOT_OK(StructFromGroup(group, included_leaf_nodes, &type));
    }
  } else {
    // Primitive (leaf) node
    if (IsIncludedLeaf(static_cast<NodePtr>(node), included_leaf_nodes)) {
      const PrimitiveNode* primitive = static_cast<const PrimitiveNode*>(node.get());
      RETURN_NOT_OK(FromPrimitive(primitive, &type));
    }
  }
  if (type != nullptr) { *out = std::make_shared<Field>(node->name(), type, nullable); }
  return Status::OK();
}

Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata,
    std::shared_ptr<::arrow::Schema>* out) {
  const GroupNode* schema_node = parquet_schema->group_node();

  int num_fields = static_cast<int>(schema_node->field_count());
  std::vector<std::shared_ptr<Field>> fields(num_fields);
  for (int i = 0; i < num_fields; i++) {
    RETURN_NOT_OK(NodeToField(schema_node->field(i), &fields[i]));
  }

  *out = std::make_shared<::arrow::Schema>(fields, key_value_metadata);
  return Status::OK();
}

Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
    const std::vector<int>& column_indices,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata,
    std::shared_ptr<::arrow::Schema>* out) {
  // TODO(wesm): Consider adding an arrow::Schema name attribute, which comes
  // from the root Parquet node

  // Put the right leaf nodes in an unordered set
  // Index in column_indices should be unique, duplicate indices are merged into one and
  // ordering by its first appearing.
  int num_columns = static_cast<int>(column_indices.size());
  std::unordered_set<NodePtr> top_nodes;  // to deduplicate the top nodes
  std::vector<NodePtr> base_nodes;        // to keep the ordering
  std::unordered_set<NodePtr> included_leaf_nodes(num_columns);
  for (int i = 0; i < num_columns; i++) {
    auto column_desc = parquet_schema->Column(column_indices[i]);
    included_leaf_nodes.insert(column_desc->schema_node());
    auto column_root = parquet_schema->GetColumnRoot(column_indices[i]);
    auto insertion = top_nodes.insert(column_root);
    if (insertion.second) { base_nodes.push_back(column_root); }
  }

  std::vector<std::shared_ptr<Field>> fields;
  std::shared_ptr<Field> field;
  for (auto node : base_nodes) {
    RETURN_NOT_OK(NodeToFieldInternal(node, &included_leaf_nodes, &field));
    if (field != nullptr) { fields.push_back(field); }
  }

  *out = std::make_shared<::arrow::Schema>(fields, key_value_metadata);
  return Status::OK();
}

Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
    const std::vector<int>& column_indices, std::shared_ptr<::arrow::Schema>* out) {
  return FromParquetSchema(parquet_schema, column_indices, nullptr, out);
}

Status FromParquetSchema(
    const SchemaDescriptor* parquet_schema, std::shared_ptr<::arrow::Schema>* out) {
  return FromParquetSchema(parquet_schema, nullptr, out);
}

Status ListToNode(const std::shared_ptr<::arrow::ListType>& type, const std::string& name,
    bool nullable, const WriterProperties& properties, NodePtr* out) {
  Repetition::type repetition = nullable ? Repetition::OPTIONAL : Repetition::REQUIRED;

  NodePtr element;
  RETURN_NOT_OK(FieldToNode(type->value_field(), properties, &element));

  NodePtr list = GroupNode::Make("list", Repetition::REPEATED, {element});
  *out = GroupNode::Make(name, repetition, {list}, LogicalType::LIST);
  return Status::OK();
}

Status StructToNode(const std::shared_ptr<::arrow::StructType>& type,
    const std::string& name, bool nullable, const WriterProperties& properties,
    NodePtr* out) {
  Repetition::type repetition = nullable ? Repetition::OPTIONAL : Repetition::REQUIRED;

  std::vector<NodePtr> children(type->num_children());
  for (int i = 0; i < type->num_children(); i++) {
    RETURN_NOT_OK(FieldToNode(type->child(i), properties, &children[i]));
  }

  *out = GroupNode::Make(name, repetition, children);
  return Status::OK();
}

Status FieldToNode(const std::shared_ptr<Field>& field,
    const WriterProperties& properties, NodePtr* out) {
  LogicalType::type logical_type = LogicalType::NONE;
  ParquetType::type type;
  Repetition::type repetition =
      field->nullable() ? Repetition::OPTIONAL : Repetition::REQUIRED;
  int length = -1;

  switch (field->type()->id()) {
    // TODO:
    // case ArrowType::NA:
    // break;
    case ArrowType::BOOL:
      type = ParquetType::BOOLEAN;
      break;
    case ArrowType::UINT8:
      type = ParquetType::INT32;
      logical_type = LogicalType::UINT_8;
      break;
    case ArrowType::INT8:
      type = ParquetType::INT32;
      logical_type = LogicalType::INT_8;
      break;
    case ArrowType::UINT16:
      type = ParquetType::INT32;
      logical_type = LogicalType::UINT_16;
      break;
    case ArrowType::INT16:
      type = ParquetType::INT32;
      logical_type = LogicalType::INT_16;
      break;
    case ArrowType::UINT32:
      if (properties.version() == ::parquet::ParquetVersion::PARQUET_1_0) {
        type = ParquetType::INT64;
      } else {
        type = ParquetType::INT32;
        logical_type = LogicalType::UINT_32;
      }
      break;
    case ArrowType::INT32:
      type = ParquetType::INT32;
      break;
    case ArrowType::UINT64:
      type = ParquetType::INT64;
      logical_type = LogicalType::UINT_64;
      break;
    case ArrowType::INT64:
      type = ParquetType::INT64;
      break;
    case ArrowType::FLOAT:
      type = ParquetType::FLOAT;
      break;
    case ArrowType::DOUBLE:
      type = ParquetType::DOUBLE;
      break;
    case ArrowType::STRING:
      type = ParquetType::BYTE_ARRAY;
      logical_type = LogicalType::UTF8;
      break;
    case ArrowType::BINARY:
      type = ParquetType::BYTE_ARRAY;
      break;
    case ArrowType::FIXED_SIZE_BINARY: {
      type = ParquetType::FIXED_LEN_BYTE_ARRAY;
      auto fixed_size_binary_type =
          static_cast<::arrow::FixedSizeBinaryType*>(field->type().get());
      length = fixed_size_binary_type->byte_width();
    } break;
    case ArrowType::DATE32:
      type = ParquetType::INT32;
      logical_type = LogicalType::DATE;
      break;
    case ArrowType::DATE64:
      type = ParquetType::INT32;
      logical_type = LogicalType::DATE;
      break;
    case ArrowType::TIMESTAMP: {
      auto timestamp_type = static_cast<::arrow::TimestampType*>(field->type().get());
      auto unit = timestamp_type->unit();
      type = ParquetType::INT64;
      if (unit == ::arrow::TimeUnit::MILLI) {
        logical_type = LogicalType::TIMESTAMP_MILLIS;
      } else if (unit == ::arrow::TimeUnit::MICRO) {
        logical_type = LogicalType::TIMESTAMP_MICROS;
      } else {
        return Status::NotImplemented(
            "Only MILLI and MICRO units supported for Arrow timestamps with Parquet.");
      }
    } break;
    case ArrowType::TIME32:
      type = ParquetType::INT32;
      logical_type = LogicalType::TIME_MILLIS;
      break;
    case ArrowType::TIME64: {
      auto time_type = static_cast<::arrow::Time64Type*>(field->type().get());
      if (time_type->unit() == ::arrow::TimeUnit::NANO) {
        return Status::NotImplemented("Nanosecond time not supported in Parquet.");
      }
      type = ParquetType::INT64;
      logical_type = LogicalType::TIME_MICROS;
    } break;
    case ArrowType::STRUCT: {
      auto struct_type = std::static_pointer_cast<::arrow::StructType>(field->type());
      return StructToNode(struct_type, field->name(), field->nullable(), properties, out);
    } break;
    case ArrowType::LIST: {
      auto list_type = std::static_pointer_cast<::arrow::ListType>(field->type());
      return ListToNode(list_type, field->name(), field->nullable(), properties, out);
    } break;
    default:
      // TODO: LIST, DENSE_UNION, SPARE_UNION, JSON_SCALAR, DECIMAL, DECIMAL_TEXT, VARCHAR
      return Status::NotImplemented("unhandled type");
  }
  *out = PrimitiveNode::Make(field->name(), repetition, type, logical_type, length);
  return Status::OK();
}

Status ToParquetSchema(const ::arrow::Schema* arrow_schema,
    const WriterProperties& properties, std::shared_ptr<SchemaDescriptor>* out) {
  std::vector<NodePtr> nodes(arrow_schema->num_fields());
  for (int i = 0; i < arrow_schema->num_fields(); i++) {
    RETURN_NOT_OK(FieldToNode(arrow_schema->field(i), properties, &nodes[i]));
  }

  NodePtr schema = GroupNode::Make("schema", Repetition::REQUIRED, nodes);
  *out = std::make_shared<::parquet::SchemaDescriptor>();
  PARQUET_CATCH_NOT_OK((*out)->Init(schema));

  return Status::OK();
}

}  // namespace arrow
}  // namespace parquet
