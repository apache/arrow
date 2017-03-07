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
      *out = ::arrow::binary();
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
    case LogicalType::UINT_32:
      *out = ::arrow::uint32();
      break;
    case LogicalType::DATE:
      *out = ::arrow::date();
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
    case LogicalType::UINT_64:
      *out = ::arrow::uint64();
      break;
    case LogicalType::DECIMAL:
      *out = MakeDecimalType(node);
      break;
    case LogicalType::TIMESTAMP_MILLIS:
      *out = TIMESTAMP_MS;
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

Status StructFromGroup(const GroupNode* group, TypePtr* out) {
  std::vector<std::shared_ptr<Field>> fields(group->field_count());
  for (int i = 0; i < group->field_count(); i++) {
    RETURN_NOT_OK(NodeToField(group->field(i), &fields[i]));
  }
  *out = std::make_shared<::arrow::StructType>(fields);
  return Status::OK();
}

bool str_endswith_tuple(const std::string& str) {
  if (str.size() >= 6) { return str.substr(str.size() - 6, 6) == "_tuple"; }
  return false;
}

Status NodeToList(const GroupNode* group, TypePtr* out) {
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
        RETURN_NOT_OK(NodeToField(list_group->field(0), &item_field));
        *out = ::arrow::list(item_field);
      } else {
        // List of struct
        std::shared_ptr<::arrow::DataType> inner_type;
        RETURN_NOT_OK(StructFromGroup(list_group, &inner_type));
        auto item_field = std::make_shared<Field>(list_node->name(), inner_type, false);
        *out = ::arrow::list(item_field);
      }
    } else if (list_node->is_repeated()) {
      // repeated primitive node
      std::shared_ptr<::arrow::DataType> inner_type;
      const PrimitiveNode* primitive = static_cast<const PrimitiveNode*>(list_node.get());
      RETURN_NOT_OK(FromPrimitive(primitive, &inner_type));
      auto item_field = std::make_shared<Field>(list_node->name(), inner_type, false);
      *out = ::arrow::list(item_field);
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
  std::shared_ptr<::arrow::DataType> type;
  bool nullable = !node->is_required();

  if (node->is_repeated()) {
    // 1-level LIST encoding fields are required
    std::shared_ptr<::arrow::DataType> inner_type;
    const PrimitiveNode* primitive = static_cast<const PrimitiveNode*>(node.get());
    RETURN_NOT_OK(FromPrimitive(primitive, &inner_type));
    auto item_field = std::make_shared<Field>(node->name(), inner_type, false);
    type = ::arrow::list(item_field);
    nullable = false;
  } else if (node->is_group()) {
    const GroupNode* group = static_cast<const GroupNode*>(node.get());
    if (node->logical_type() == LogicalType::LIST) {
      RETURN_NOT_OK(NodeToList(group, &type));
    } else {
      RETURN_NOT_OK(StructFromGroup(group, &type));
    }
  } else {
    // Primitive (leaf) node
    const PrimitiveNode* primitive = static_cast<const PrimitiveNode*>(node.get());
    RETURN_NOT_OK(FromPrimitive(primitive, &type));
  }

  *out = std::make_shared<Field>(node->name(), type, nullable);
  return Status::OK();
}

Status FromParquetSchema(
    const SchemaDescriptor* parquet_schema, std::shared_ptr<::arrow::Schema>* out) {
  const GroupNode* schema_node = parquet_schema->group_node();

  std::vector<std::shared_ptr<Field>> fields(schema_node->field_count());
  for (int i = 0; i < schema_node->field_count(); i++) {
    RETURN_NOT_OK(NodeToField(schema_node->field(i), &fields[i]));
  }

  *out = std::make_shared<::arrow::Schema>(fields);
  return Status::OK();
}

Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
    const std::vector<int>& column_indices, std::shared_ptr<::arrow::Schema>* out) {
  // TODO(wesm): Consider adding an arrow::Schema name attribute, which comes
  // from the root Parquet node
  const GroupNode* schema_node = parquet_schema->group_node();

  int num_fields = static_cast<int>(column_indices.size());

  std::vector<std::shared_ptr<Field>> fields(num_fields);
  for (int i = 0; i < num_fields; i++) {
    RETURN_NOT_OK(NodeToField(schema_node->field(column_indices[i]), &fields[i]));
  }

  *out = std::make_shared<::arrow::Schema>(fields);
  return Status::OK();
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
      field->nullable ? Repetition::OPTIONAL : Repetition::REQUIRED;
  int length = -1;

  switch (field->type->type) {
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
    case ArrowType::DATE:
      type = ParquetType::INT32;
      logical_type = LogicalType::DATE;
      break;
    case ArrowType::TIMESTAMP: {
      auto timestamp_type = static_cast<::arrow::TimestampType*>(field->type.get());
      if (timestamp_type->unit != ::arrow::TimestampType::Unit::MILLI) {
        return Status::NotImplemented(
            "Other timestamp units than millisecond are not yet support with parquet.");
      }
      type = ParquetType::INT64;
      logical_type = LogicalType::TIMESTAMP_MILLIS;
    } break;
    case ArrowType::TIME:
      type = ParquetType::INT64;
      logical_type = LogicalType::TIME_MILLIS;
      break;
    case ArrowType::STRUCT: {
      auto struct_type = std::static_pointer_cast<::arrow::StructType>(field->type);
      return StructToNode(struct_type, field->name, field->nullable, properties, out);
    } break;
    case ArrowType::LIST: {
      auto list_type = std::static_pointer_cast<::arrow::ListType>(field->type);
      return ListToNode(list_type, field->name, field->nullable, properties, out);
    } break;
    default:
      // TODO: LIST, DENSE_UNION, SPARE_UNION, JSON_SCALAR, DECIMAL, DECIMAL_TEXT, VARCHAR
      return Status::NotImplemented("unhandled type");
  }
  *out = PrimitiveNode::Make(field->name, repetition, type, logical_type, length);
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
