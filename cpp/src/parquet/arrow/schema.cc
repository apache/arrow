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
#include "parquet/arrow/utils.h"

#include "arrow/types/decimal.h"
#include "arrow/types/string.h"
#include "arrow/util/status.h"

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

const auto BOOL = std::make_shared<::arrow::BooleanType>();
const auto UINT8 = std::make_shared<::arrow::UInt8Type>();
const auto INT8 = std::make_shared<::arrow::Int8Type>();
const auto UINT16 = std::make_shared<::arrow::UInt16Type>();
const auto INT16 = std::make_shared<::arrow::Int16Type>();
const auto UINT32 = std::make_shared<::arrow::UInt32Type>();
const auto INT32 = std::make_shared<::arrow::Int32Type>();
const auto UINT64 = std::make_shared<::arrow::UInt64Type>();
const auto INT64 = std::make_shared<::arrow::Int64Type>();
const auto FLOAT = std::make_shared<::arrow::FloatType>();
const auto DOUBLE = std::make_shared<::arrow::DoubleType>();
const auto UTF8 = std::make_shared<::arrow::StringType>();
const auto TIMESTAMP_MS =
    std::make_shared<::arrow::TimestampType>(::arrow::TimestampType::Unit::MILLI);
const auto BINARY =
    std::make_shared<::arrow::ListType>(std::make_shared<::arrow::Field>("", UINT8));

TypePtr MakeDecimalType(const PrimitiveNode* node) {
  int precision = node->decimal_metadata().precision;
  int scale = node->decimal_metadata().scale;
  return std::make_shared<::arrow::DecimalType>(precision, scale);
}

static Status FromByteArray(const PrimitiveNode* node, TypePtr* out) {
  switch (node->logical_type()) {
    case LogicalType::UTF8:
      *out = UTF8;
      break;
    case LogicalType::DECIMAL:
      *out = MakeDecimalType(node);
      break;
    default:
      // BINARY
      *out = BINARY;
      break;
  }
  return Status::OK();
}

static Status FromFLBA(const PrimitiveNode* node, TypePtr* out) {
  switch (node->logical_type()) {
    case LogicalType::NONE:
      *out = BINARY;
      break;
    case LogicalType::DECIMAL:
      *out = MakeDecimalType(node);
      break;
    default:
      return Status::NotImplemented("unhandled type");
      break;
  }

  return Status::OK();
}

static Status FromInt32(const PrimitiveNode* node, TypePtr* out) {
  switch (node->logical_type()) {
    case LogicalType::NONE:
      *out = INT32;
      break;
    case LogicalType::UINT_8:
      *out = UINT8;
      break;
    case LogicalType::INT_8:
      *out = INT8;
      break;
    case LogicalType::UINT_16:
      *out = UINT16;
      break;
    case LogicalType::INT_16:
      *out = INT16;
      break;
    case LogicalType::UINT_32:
      *out = UINT32;
      break;
    case LogicalType::DECIMAL:
      *out = MakeDecimalType(node);
      break;
    default:
      return Status::NotImplemented("Unhandled logical type for int32");
      break;
  }
  return Status::OK();
}

static Status FromInt64(const PrimitiveNode* node, TypePtr* out) {
  switch (node->logical_type()) {
    case LogicalType::NONE:
      *out = INT64;
      break;
    case LogicalType::UINT_64:
      *out = UINT64;
      break;
    case LogicalType::DECIMAL:
      *out = MakeDecimalType(node);
      break;
    case LogicalType::TIMESTAMP_MILLIS:
      *out = TIMESTAMP_MS;
      break;
    default:
      return Status::NotImplemented("Unhandled logical type for int64");
      break;
  }
  return Status::OK();
}

// TODO: Logical Type Handling
Status NodeToField(const NodePtr& node, std::shared_ptr<Field>* out) {
  std::shared_ptr<::arrow::DataType> type;

  if (node->is_repeated()) {
    return Status::NotImplemented("No support yet for repeated node types");
  }

  if (node->is_group()) {
    const GroupNode* group = static_cast<const GroupNode*>(node.get());
    std::vector<std::shared_ptr<Field>> fields(group->field_count());
    for (int i = 0; i < group->field_count(); i++) {
      RETURN_NOT_OK(NodeToField(group->field(i), &fields[i]));
    }
    type = std::make_shared<::arrow::StructType>(fields);
  } else {
    // Primitive (leaf) node
    const PrimitiveNode* primitive = static_cast<const PrimitiveNode*>(node.get());

    switch (primitive->physical_type()) {
      case ParquetType::BOOLEAN:
        type = BOOL;
        break;
      case ParquetType::INT32:
        RETURN_NOT_OK(FromInt32(primitive, &type));
        break;
      case ParquetType::INT64:
        RETURN_NOT_OK(FromInt64(primitive, &type));
        break;
      case ParquetType::INT96:
        // TODO: Do we have that type in Arrow?
        // type = TypePtr(new Int96Type());
        return Status::NotImplemented("int96");
      case ParquetType::FLOAT:
        type = FLOAT;
        break;
      case ParquetType::DOUBLE:
        type = DOUBLE;
        break;
      case ParquetType::BYTE_ARRAY:
        // TODO: Do we have that type in Arrow?
        RETURN_NOT_OK(FromByteArray(primitive, &type));
        break;
      case ParquetType::FIXED_LEN_BYTE_ARRAY:
        RETURN_NOT_OK(FromFLBA(primitive, &type));
        break;
    }
  }

  *out = std::make_shared<Field>(node->name(), type, !node->is_required());
  return Status::OK();
}

Status FromParquetSchema(
    const SchemaDescriptor* parquet_schema, std::shared_ptr<::arrow::Schema>* out) {
  // TODO(wesm): Consider adding an arrow::Schema name attribute, which comes
  // from the root Parquet node
  const GroupNode* schema_node = parquet_schema->group_node();

  std::vector<std::shared_ptr<Field>> fields(schema_node->field_count());
  for (int i = 0; i < schema_node->field_count(); i++) {
    RETURN_NOT_OK(NodeToField(schema_node->field(i), &fields[i]));
  }

  *out = std::make_shared<::arrow::Schema>(fields);
  return Status::OK();
}

Status StructToNode(const std::shared_ptr<::arrow::StructType>& type,
    const std::string& name, bool nullable, const WriterProperties& properties,
    NodePtr* out) {
  Repetition::type repetition = Repetition::REQUIRED;
  if (nullable) { repetition = Repetition::OPTIONAL; }

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
  Repetition::type repetition = Repetition::REQUIRED;
  if (field->nullable) { repetition = Repetition::OPTIONAL; }
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
    case ArrowType::TIMESTAMP_DOUBLE:
      type = ParquetType::INT64;
      // This is specified as seconds since the UNIX epoch
      // TODO: Converted type in Parquet?
      // logical_type = LogicalType::TIMESTAMP_MILLIS;
      break;
    case ArrowType::TIME:
      type = ParquetType::INT64;
      logical_type = LogicalType::TIME_MILLIS;
      break;
    case ArrowType::STRUCT: {
      auto struct_type = std::static_pointer_cast<::arrow::StructType>(field->type);
      return StructToNode(struct_type, field->name, field->nullable, properties, out);
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

  NodePtr schema = GroupNode::Make("schema", Repetition::REPEATED, nodes);
  *out = std::make_shared<::parquet::SchemaDescriptor>();
  PARQUET_CATCH_NOT_OK((*out)->Init(schema));

  return Status::OK();
}

}  // namespace arrow

}  // namespace parquet
