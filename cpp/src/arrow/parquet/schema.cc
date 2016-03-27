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

#include "arrow/parquet/schema.h"

#include <vector>

#include "parquet/api/schema.h"

#include "arrow/util/status.h"
#include "arrow/types/decimal.h"

using parquet_cpp::schema::Node;
using parquet_cpp::schema::NodePtr;
using parquet_cpp::schema::GroupNode;
using parquet_cpp::schema::PrimitiveNode;

using parquet_cpp::LogicalType;

namespace arrow {

namespace parquet {

const auto BOOL = std::make_shared<BooleanType>();
const auto UINT8 = std::make_shared<UInt8Type>();
const auto INT32 = std::make_shared<Int32Type>();
const auto INT64 = std::make_shared<Int64Type>();
const auto FLOAT = std::make_shared<FloatType>();
const auto DOUBLE = std::make_shared<DoubleType>();
const auto UTF8 = std::make_shared<StringType>();
const auto BINARY = std::make_shared<ListType>(
    std::make_shared<Field>("", UINT8));

TypePtr MakeDecimalType(const PrimitiveNode* node) {
  int precision = node->decimal_metadata().precision;
  int scale = node->decimal_metadata().scale;
  return std::make_shared<DecimalType>(precision, scale);
}

static Status FromByteArray(const PrimitiveNode* node, TypePtr* out) {
  switch (node->logical_type()) {
    case LogicalType::UTF8:
      *out = UTF8;
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
    default:
      return Status::NotImplemented("Unhandled logical type for int64");
      break;
  }
  return Status::OK();
}

// TODO: Logical Type Handling
Status NodeToField(const NodePtr& node, std::shared_ptr<Field>* out) {
  std::shared_ptr<DataType> type;

  if (node->is_repeated()) {
    return Status::NotImplemented("No support yet for repeated node types");
  }

  if (node->is_group()) {
    const GroupNode* group = static_cast<const GroupNode*>(node.get());
    std::vector<std::shared_ptr<Field>> fields(group->field_count());
    for (int i = 0; i < group->field_count(); i++) {
      RETURN_NOT_OK(NodeToField(group->field(i), &fields[i]));
    }
    type = std::make_shared<StructType>(fields);
  } else {
    // Primitive (leaf) node
    const PrimitiveNode* primitive = static_cast<const PrimitiveNode*>(node.get());

    switch (primitive->physical_type()) {
      case parquet_cpp::Type::BOOLEAN:
        type = BOOL;
        break;
      case parquet_cpp::Type::INT32:
        RETURN_NOT_OK(FromInt32(primitive, &type));
        break;
      case parquet_cpp::Type::INT64:
        RETURN_NOT_OK(FromInt64(primitive, &type));
        break;
      case parquet_cpp::Type::INT96:
        // TODO: Do we have that type in Arrow?
        // type = TypePtr(new Int96Type());
        return Status::NotImplemented("int96");
      case parquet_cpp::Type::FLOAT:
        type = FLOAT;
        break;
      case parquet_cpp::Type::DOUBLE:
        type = DOUBLE;
        break;
      case parquet_cpp::Type::BYTE_ARRAY:
        // TODO: Do we have that type in Arrow?
        RETURN_NOT_OK(FromByteArray(primitive, &type));
        break;
      case parquet_cpp::Type::FIXED_LEN_BYTE_ARRAY:
        RETURN_NOT_OK(FromFLBA(primitive, &type));
        break;
    }
  }

  *out = std::make_shared<Field>(node->name(), type, !node->is_required());
  return Status::OK();
}

Status FromParquetSchema(const parquet_cpp::SchemaDescriptor* parquet_schema,
    std::shared_ptr<Schema>* out) {
  // TODO(wesm): Consider adding an arrow::Schema name attribute, which comes
  // from the root Parquet node
  const GroupNode* schema_node = static_cast<const GroupNode*>(
      parquet_schema->schema().get());

  std::vector<std::shared_ptr<Field>> fields(schema_node->field_count());
  for (int i = 0; i < schema_node->field_count(); i++) {
    RETURN_NOT_OK(NodeToField(schema_node->field(i), &fields[i]));
  }

  *out = std::make_shared<Schema>(fields);
  return Status::OK();
}

} // namespace parquet

} // namespace arrow
