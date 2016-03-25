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

#include <vector>

#include "arrow/parquet/schema.h"
#include "arrow/types/decimal.h"

using parquet_cpp::schema::Node;
using parquet_cpp::schema::NodePtr;
using parquet_cpp::schema::GroupNode;
using parquet_cpp::schema::PrimitiveNode;

namespace arrow {

namespace parquet {


TypePtr MakeDecimalType(const PrimitiveNode* node) {
  int precision = node->decimal_metadata().precision;
  int scale = node->decimal_metadata().scale;
  return TypePtr(new DecimalType(precision, scale));
}

// TODO: Logical Type Handling
std::shared_ptr<Field> NodeToField(const NodePtr& node) {
  TypePtr type;

  if (node->is_group()) {
    const GroupNode* group = static_cast<const GroupNode*>(node.get());
    std::vector<std::shared_ptr<Field>> fields;
    for (int i = 0; i < group->field_count(); i++) {
      fields.push_back(NodeToField(group->field(i)));
    }
    type = TypePtr(new StructType(fields));
  } else {
    // Primitive (leaf) node
    const PrimitiveNode* primitive = static_cast<const PrimitiveNode*>(node.get());

    switch (primitive->physical_type()) {
      case parquet_cpp::Type::BOOLEAN:
        type = TypePtr(new BooleanType());
        break;
      case parquet_cpp::Type::INT32:
        type = TypePtr(new Int32Type());
        break;
      case parquet_cpp::Type::INT64:
        type = TypePtr(new Int64Type());
        break;
      case parquet_cpp::Type::INT96:
        // TODO: Do we have that type in Arrow?
        // type = TypePtr(new Int96Type());
        break;
      case parquet_cpp::Type::FLOAT:
        type = TypePtr(new FloatType());
        break;
      case parquet_cpp::Type::DOUBLE:
        type = TypePtr(new DoubleType());
        break;
      case parquet_cpp::Type::BYTE_ARRAY:
        // TODO: Do we have that type in Arrow?
        // type = TypePtr(new Int96Type());
        break;
      case parquet_cpp::Type::FIXED_LEN_BYTE_ARRAY:
        switch (primitive->logical_type()) {
          case parquet_cpp::LogicalType::DECIMAL:
            type = MakeDecimalType(primitive);
            break;
          default:
              // TODO: Do we have that type in Arrow?
            break;
        }
        break;
    }
  }

  if (node->is_repeated()) {
    type = TypePtr(new ListType(type));
  }

  return std::shared_ptr<Field>(new Field(node->name(), type, !node->is_required()));
}

std::shared_ptr<Schema> FromParquetSchema(
    const parquet_cpp::SchemaDescriptor* parquet_schema) {
  std::vector<std::shared_ptr<Field>> fields;
  const GroupNode* schema_node = static_cast<const GroupNode*>(
      parquet_schema->schema().get());

  // TODO: What to with the head node?
  for (int i = 0; i < schema_node->field_count(); i++) {
    fields.push_back(NodeToField(schema_node->field(i)));
  }

  return std::shared_ptr<Schema>(new Schema(fields));
}

} // namespace parquet

} // namespace arrow
