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

#include <algorithm>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

#include "parquet/arrow/writer.h"
#include "parquet/exception.h"
#include "parquet/properties.h"
#include "parquet/schema-internal.h"
#include "parquet/types.h"

using arrow::Field;
using arrow::Status;
using arrow::internal::checked_cast;

using ArrowType = arrow::DataType;
using ArrowTypeId = arrow::Type;

using parquet::Repetition;
using parquet::schema::GroupNode;
using parquet::schema::Node;
using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;

using ParquetType = parquet::Type;
using parquet::ConvertedType;
using parquet::LogicalType;

namespace parquet {

namespace arrow {

const auto TIMESTAMP_MS = ::arrow::timestamp(::arrow::TimeUnit::MILLI);
const auto TIMESTAMP_US = ::arrow::timestamp(::arrow::TimeUnit::MICRO);
const auto TIMESTAMP_NS = ::arrow::timestamp(::arrow::TimeUnit::NANO);

static Status MakeArrowDecimal(const LogicalType& logical_type,
                               std::shared_ptr<ArrowType>* out) {
  const auto& decimal = checked_cast<const DecimalLogicalType&>(logical_type);
  *out = ::arrow::decimal(decimal.precision(), decimal.scale());
  return Status::OK();
}

static Status MakeArrowInt(const LogicalType& logical_type,
                           std::shared_ptr<ArrowType>* out) {
  const auto& integer = checked_cast<const IntLogicalType&>(logical_type);
  switch (integer.bit_width()) {
    case 8:
      *out = integer.is_signed() ? ::arrow::int8() : ::arrow::uint8();
      break;
    case 16:
      *out = integer.is_signed() ? ::arrow::int16() : ::arrow::uint16();
      break;
    case 32:
      *out = integer.is_signed() ? ::arrow::int32() : ::arrow::uint32();
      break;
    default:
      return Status::TypeError(logical_type.ToString(),
                               " can not annotate physical type Int32");
  }
  return Status::OK();
}

static Status MakeArrowInt64(const LogicalType& logical_type,
                             std::shared_ptr<ArrowType>* out) {
  const auto& integer = checked_cast<const IntLogicalType&>(logical_type);
  switch (integer.bit_width()) {
    case 64:
      *out = integer.is_signed() ? ::arrow::int64() : ::arrow::uint64();
      break;
    default:
      return Status::TypeError(logical_type.ToString(),
                               " can not annotate physical type Int64");
  }
  return Status::OK();
}

static Status MakeArrowTime32(const LogicalType& logical_type,
                              std::shared_ptr<ArrowType>* out) {
  const auto& time = checked_cast<const TimeLogicalType&>(logical_type);
  switch (time.time_unit()) {
    case LogicalType::TimeUnit::MILLIS:
      *out = ::arrow::time32(::arrow::TimeUnit::MILLI);
      break;
    default:
      return Status::TypeError(logical_type.ToString(),
                               " can not annotate physical type Time32");
  }
  return Status::OK();
}

static Status MakeArrowTime64(const LogicalType& logical_type,
                              std::shared_ptr<ArrowType>* out) {
  const auto& time = checked_cast<const TimeLogicalType&>(logical_type);
  switch (time.time_unit()) {
    case LogicalType::TimeUnit::MICROS:
      *out = ::arrow::time64(::arrow::TimeUnit::MICRO);
      break;
    case LogicalType::TimeUnit::NANOS:
      *out = ::arrow::time64(::arrow::TimeUnit::NANO);
      break;
    default:
      return Status::TypeError(logical_type.ToString(),
                               " can not annotate physical type Time64");
  }
  return Status::OK();
}

static Status MakeArrowTimestamp(const LogicalType& logical_type,
                                 std::shared_ptr<ArrowType>* out) {
  static const char* utc = "UTC";
  const auto& timestamp = checked_cast<const TimestampLogicalType&>(logical_type);
  switch (timestamp.time_unit()) {
    case LogicalType::TimeUnit::MILLIS:
      *out = (timestamp.is_adjusted_to_utc()
                  ? ::arrow::timestamp(::arrow::TimeUnit::MILLI, utc)
                  : ::arrow::timestamp(::arrow::TimeUnit::MILLI));
      break;
    case LogicalType::TimeUnit::MICROS:
      *out = (timestamp.is_adjusted_to_utc()
                  ? ::arrow::timestamp(::arrow::TimeUnit::MICRO, utc)
                  : ::arrow::timestamp(::arrow::TimeUnit::MICRO));
      break;
    case LogicalType::TimeUnit::NANOS:
      *out = (timestamp.is_adjusted_to_utc()
                  ? ::arrow::timestamp(::arrow::TimeUnit::NANO, utc)
                  : ::arrow::timestamp(::arrow::TimeUnit::NANO));
      break;
    default:
      return Status::TypeError("Unrecognized time unit in timestamp logical_type: ",
                               logical_type.ToString());
  }
  return Status::OK();
}

static Status FromByteArray(const LogicalType& logical_type,
                            std::shared_ptr<ArrowType>* out) {
  switch (logical_type.type()) {
    case LogicalType::Type::STRING:
      *out = ::arrow::utf8();
      break;
    case LogicalType::Type::DECIMAL:
      RETURN_NOT_OK(MakeArrowDecimal(logical_type, out));
      break;
    case LogicalType::Type::NONE:
    case LogicalType::Type::ENUM:
    case LogicalType::Type::JSON:
    case LogicalType::Type::BSON:
      *out = ::arrow::binary();
      break;
    default:
      return Status::NotImplemented("Unhandled logical logical_type ",
                                    logical_type.ToString(), " for binary array");
  }
  return Status::OK();
}

static Status FromFLBA(const LogicalType& logical_type, int32_t physical_length,
                       std::shared_ptr<ArrowType>* out) {
  switch (logical_type.type()) {
    case LogicalType::Type::DECIMAL:
      RETURN_NOT_OK(MakeArrowDecimal(logical_type, out));
      break;
    case LogicalType::Type::NONE:
    case LogicalType::Type::INTERVAL:
    case LogicalType::Type::UUID:
      *out = ::arrow::fixed_size_binary(physical_length);
      break;
    default:
      return Status::NotImplemented("Unhandled logical logical_type ",
                                    logical_type.ToString(),
                                    " for fixed-length binary array");
  }

  return Status::OK();
}

static Status FromInt32(const LogicalType& logical_type,
                        std::shared_ptr<ArrowType>* out) {
  switch (logical_type.type()) {
    case LogicalType::Type::INT:
      RETURN_NOT_OK(MakeArrowInt(logical_type, out));
      break;
    case LogicalType::Type::DATE:
      *out = ::arrow::date32();
      break;
    case LogicalType::Type::TIME:
      RETURN_NOT_OK(MakeArrowTime32(logical_type, out));
      break;
    case LogicalType::Type::DECIMAL:
      RETURN_NOT_OK(MakeArrowDecimal(logical_type, out));
      break;
    case LogicalType::Type::NONE:
      *out = ::arrow::int32();
      break;
    default:
      return Status::NotImplemented("Unhandled logical type ", logical_type.ToString(),
                                    " for INT32");
  }
  return Status::OK();
}

static Status FromInt64(const LogicalType& logical_type,
                        std::shared_ptr<ArrowType>* out) {
  switch (logical_type.type()) {
    case LogicalType::Type::INT:
      RETURN_NOT_OK(MakeArrowInt64(logical_type, out));
      break;
    case LogicalType::Type::DECIMAL:
      RETURN_NOT_OK(MakeArrowDecimal(logical_type, out));
      break;
    case LogicalType::Type::TIMESTAMP:
      RETURN_NOT_OK(MakeArrowTimestamp(logical_type, out));
      break;
    case LogicalType::Type::TIME:
      RETURN_NOT_OK(MakeArrowTime64(logical_type, out));
      break;
    case LogicalType::Type::NONE:
      *out = ::arrow::int64();
      break;
    default:
      return Status::NotImplemented("Unhandled logical type ", logical_type.ToString(),
                                    " for INT64");
  }
  return Status::OK();
}

Status FromPrimitive(const PrimitiveNode& primitive, std::shared_ptr<ArrowType>* out) {
  const std::shared_ptr<const LogicalType>& logical_type = primitive.logical_type();
  if (logical_type->is_invalid() || logical_type->is_null()) {
    *out = ::arrow::null();
    return Status::OK();
  }

  switch (primitive.physical_type()) {
    case ParquetType::BOOLEAN:
      *out = ::arrow::boolean();
      break;
    case ParquetType::INT32:
      RETURN_NOT_OK(FromInt32(*logical_type, out));
      break;
    case ParquetType::INT64:
      RETURN_NOT_OK(FromInt64(*logical_type, out));
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
      RETURN_NOT_OK(FromByteArray(*logical_type, out));
      break;
    case ParquetType::FIXED_LEN_BYTE_ARRAY:
      RETURN_NOT_OK(FromFLBA(*logical_type, primitive.type_length(), out));
      break;
    default: {
      // PARQUET-1565: This can occur if the file is corrupt
      return Status::IOError("Invalid physical column type: ",
                             TypeToString(primitive.physical_type()));
    }
  }
  return Status::OK();
}

// Forward declaration
Status NodeToFieldInternal(const Node& node,
                           const std::unordered_set<const Node*>* included_leaf_nodes,
                           std::shared_ptr<Field>* out);

/*
 * Auxilary function to test if a parquet schema node is a leaf node
 * that should be included in a resulting arrow schema
 */
inline bool IsIncludedLeaf(const Node& node,
                           const std::unordered_set<const Node*>* included_leaf_nodes) {
  if (included_leaf_nodes == nullptr) {
    return true;
  }
  auto search = included_leaf_nodes->find(&node);
  return (search != included_leaf_nodes->end());
}

Status StructFromGroup(const GroupNode& group,
                       const std::unordered_set<const Node*>* included_leaf_nodes,
                       std::shared_ptr<ArrowType>* out) {
  std::vector<std::shared_ptr<Field>> fields;
  std::shared_ptr<Field> field;

  *out = nullptr;

  for (int i = 0; i < group.field_count(); i++) {
    RETURN_NOT_OK(NodeToFieldInternal(*group.field(i), included_leaf_nodes, &field));
    if (field != nullptr) {
      fields.push_back(field);
    }
  }
  if (fields.size() > 0) {
    *out = std::make_shared<::arrow::StructType>(fields);
  }
  return Status::OK();
}

Status NodeToList(const GroupNode& group,
                  const std::unordered_set<const Node*>* included_leaf_nodes,
                  std::shared_ptr<ArrowType>* out) {
  *out = nullptr;
  if (group.field_count() == 1) {
    // This attempts to resolve the preferred 3-level list encoding.
    const Node& list_node = *group.field(0);
    if (list_node.is_group() && list_node.is_repeated()) {
      const auto& list_group = static_cast<const GroupNode&>(list_node);
      // Special case mentioned in the format spec:
      //   If the name is array or ends in _tuple, this should be a list of struct
      //   even for single child elements.
      if (list_group.field_count() == 1 && !schema::HasStructListName(list_group)) {
        // List of primitive type
        std::shared_ptr<Field> item_field;
        RETURN_NOT_OK(
            NodeToFieldInternal(*list_group.field(0), included_leaf_nodes, &item_field));

        if (item_field != nullptr) {
          *out = ::arrow::list(item_field);
        }
      } else {
        // List of struct
        std::shared_ptr<ArrowType> inner_type;
        RETURN_NOT_OK(StructFromGroup(list_group, included_leaf_nodes, &inner_type));
        if (inner_type != nullptr) {
          auto item_field = std::make_shared<Field>(list_node.name(), inner_type, false);
          *out = ::arrow::list(item_field);
        }
      }
    } else if (list_node.is_repeated()) {
      // repeated primitive node
      std::shared_ptr<ArrowType> inner_type;
      if (IsIncludedLeaf(static_cast<const Node&>(list_node), included_leaf_nodes)) {
        RETURN_NOT_OK(
            FromPrimitive(static_cast<const PrimitiveNode&>(list_node), &inner_type));
        auto item_field = std::make_shared<Field>(list_node.name(), inner_type, false);
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

Status NodeToField(const Node& node, std::shared_ptr<Field>* out) {
  return NodeToFieldInternal(node, nullptr, out);
}

Status NodeToFieldInternal(const Node& node,
                           const std::unordered_set<const Node*>* included_leaf_nodes,
                           std::shared_ptr<Field>* out) {
  std::shared_ptr<ArrowType> type = nullptr;
  bool nullable = !node.is_required();

  *out = nullptr;

  if (node.is_repeated()) {
    // 1-level LIST encoding fields are required
    std::shared_ptr<ArrowType> inner_type;
    if (node.is_group()) {
      RETURN_NOT_OK(StructFromGroup(static_cast<const GroupNode&>(node),
                                    included_leaf_nodes, &inner_type));
    } else if (IsIncludedLeaf(node, included_leaf_nodes)) {
      RETURN_NOT_OK(FromPrimitive(static_cast<const PrimitiveNode&>(node), &inner_type));
    }
    if (inner_type != nullptr) {
      auto item_field = std::make_shared<Field>(node.name(), inner_type, false);
      type = ::arrow::list(item_field);
      nullable = false;
    }
  } else if (node.is_group()) {
    const auto& group = static_cast<const GroupNode&>(node);
    if (node.logical_type()->is_list()) {
      RETURN_NOT_OK(NodeToList(group, included_leaf_nodes, &type));
    } else {
      RETURN_NOT_OK(StructFromGroup(group, included_leaf_nodes, &type));
    }
  } else {
    // Primitive (leaf) node
    if (IsIncludedLeaf(node, included_leaf_nodes)) {
      RETURN_NOT_OK(FromPrimitive(static_cast<const PrimitiveNode&>(node), &type));
    }
  }
  if (type != nullptr) {
    *out = std::make_shared<Field>(node.name(), type, nullable);
  }
  return Status::OK();
}

Status FromParquetSchema(
    const SchemaDescriptor* parquet_schema,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata,
    std::shared_ptr<::arrow::Schema>* out) {
  const GroupNode& schema_node = *parquet_schema->group_node();

  int num_fields = static_cast<int>(schema_node.field_count());
  std::vector<std::shared_ptr<Field>> fields(num_fields);
  for (int i = 0; i < num_fields; i++) {
    RETURN_NOT_OK(NodeToField(*schema_node.field(i), &fields[i]));
  }

  *out = std::make_shared<::arrow::Schema>(fields, key_value_metadata);
  return Status::OK();
}

Status FromParquetSchema(
    const SchemaDescriptor* parquet_schema, const std::vector<int>& column_indices,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata,
    std::shared_ptr<::arrow::Schema>* out) {
  // TODO(wesm): Consider adding an arrow::Schema name attribute, which comes
  // from the root Parquet node

  // Put the right leaf nodes in an unordered set
  // Index in column_indices should be unique, duplicate indices are merged into one and
  // ordering by its first appearing.
  int num_columns = static_cast<int>(column_indices.size());
  std::unordered_set<const Node*> top_nodes;  // to deduplicate the top nodes
  std::vector<const Node*> base_nodes;        // to keep the ordering
  std::unordered_set<const Node*> included_leaf_nodes(num_columns);
  for (int i = 0; i < num_columns; i++) {
    const ColumnDescriptor* column_desc = parquet_schema->Column(column_indices[i]);
    included_leaf_nodes.insert(column_desc->schema_node().get());
    const Node* column_root = parquet_schema->GetColumnRoot(column_indices[i]);
    auto insertion = top_nodes.insert(column_root);
    if (insertion.second) {
      base_nodes.push_back(column_root);
    }
  }

  std::vector<std::shared_ptr<Field>> fields;
  std::shared_ptr<Field> field;
  for (auto node : base_nodes) {
    RETURN_NOT_OK(NodeToFieldInternal(*node, &included_leaf_nodes, &field));
    if (field != nullptr) {
      fields.push_back(field);
    }
  }

  *out = std::make_shared<::arrow::Schema>(fields, key_value_metadata);
  return Status::OK();
}

Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
                         const std::vector<int>& column_indices,
                         std::shared_ptr<::arrow::Schema>* out) {
  return FromParquetSchema(parquet_schema, column_indices, nullptr, out);
}

Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
                         std::shared_ptr<::arrow::Schema>* out) {
  return FromParquetSchema(parquet_schema, nullptr, out);
}

Status ListToNode(const std::shared_ptr<::arrow::ListType>& type, const std::string& name,
                  bool nullable, const WriterProperties& properties,
                  const ArrowWriterProperties& arrow_properties, NodePtr* out) {
  Repetition::type repetition = nullable ? Repetition::OPTIONAL : Repetition::REQUIRED;

  NodePtr element;
  RETURN_NOT_OK(FieldToNode(type->value_field(), properties, arrow_properties, &element));

  NodePtr list = GroupNode::Make("list", Repetition::REPEATED, {element});
  *out = GroupNode::Make(name, repetition, {list}, LogicalType::List());
  return Status::OK();
}

Status StructToNode(const std::shared_ptr<::arrow::StructType>& type,
                    const std::string& name, bool nullable,
                    const WriterProperties& properties,
                    const ArrowWriterProperties& arrow_properties, NodePtr* out) {
  Repetition::type repetition = nullable ? Repetition::OPTIONAL : Repetition::REQUIRED;

  std::vector<NodePtr> children(type->num_children());
  for (int i = 0; i < type->num_children(); i++) {
    RETURN_NOT_OK(
        FieldToNode(type->child(i), properties, arrow_properties, &children[i]));
  }

  *out = GroupNode::Make(name, repetition, children);
  return Status::OK();
}

static std::shared_ptr<const LogicalType> TimestampLogicalTypeFromArrowTimestamp(
    const ::arrow::TimestampType& timestamp_type, ::arrow::TimeUnit::type time_unit) {
  const bool utc = !(timestamp_type.timezone().empty());
  switch (time_unit) {
    case ::arrow::TimeUnit::MILLI:
      return LogicalType::Timestamp(utc, LogicalType::TimeUnit::MILLIS);
    case ::arrow::TimeUnit::MICRO:
      return LogicalType::Timestamp(utc, LogicalType::TimeUnit::MICROS);
    case ::arrow::TimeUnit::NANO:
      return LogicalType::Timestamp(utc, LogicalType::TimeUnit::NANOS);
    case ::arrow::TimeUnit::SECOND:
      // No equivalent parquet logical type.
      break;
  }
  return LogicalType::None();
}

static Status GetTimestampMetadata(const ::arrow::TimestampType& type,
                                   const WriterProperties& properties,
                                   const ArrowWriterProperties& arrow_properties,
                                   ParquetType::type* physical_type,
                                   std::shared_ptr<const LogicalType>* logical_type) {
  const bool coerce = arrow_properties.coerce_timestamps_enabled();
  const auto target_unit =
      coerce ? arrow_properties.coerce_timestamps_unit() : type.unit();

  // The user is explicitly asking for Impala int96 encoding, there is no
  // logical type.
  if (arrow_properties.support_deprecated_int96_timestamps()) {
    *physical_type = ParquetType::INT96;
    return Status::OK();
  }

  *physical_type = ParquetType::INT64;
  *logical_type = TimestampLogicalTypeFromArrowTimestamp(type, target_unit);

  // The user is explicitly asking for timestamp data to be converted to the
  // specified units (target_unit).
  if (coerce) {
    if (properties.version() == ::parquet::ParquetVersion::PARQUET_1_0) {
      switch (target_unit) {
        case ::arrow::TimeUnit::MILLI:
        case ::arrow::TimeUnit::MICRO:
          break;
        case ::arrow::TimeUnit::NANO:
        case ::arrow::TimeUnit::SECOND:
          return Status::NotImplemented(
              "For Parquet version 1.0 files, can only coerce Arrow timestamps to "
              "milliseconds or microseconds");
      }
    } else {
      switch (target_unit) {
        case ::arrow::TimeUnit::MILLI:
        case ::arrow::TimeUnit::MICRO:
        case ::arrow::TimeUnit::NANO:
          break;
        case ::arrow::TimeUnit::SECOND:
          return Status::NotImplemented(
              "For Parquet files, can only coerce Arrow timestamps to milliseconds, "
              "microseconds, or nanoseconds");
      }
    }
    return Status::OK();
  }

  // The user implicitly wants timestamp data to retain its original time units,
  // however the ConvertedType field used to indicate logical types for Parquet
  // version 1.0 fields does not allow for nanosecond time units and so nanoseconds
  // must be coerced to microseconds.
  if (properties.version() == ::parquet::ParquetVersion::PARQUET_1_0 &&
      type.unit() == ::arrow::TimeUnit::NANO) {
    *logical_type =
        TimestampLogicalTypeFromArrowTimestamp(type, ::arrow::TimeUnit::MICRO);
    return Status::OK();
  }

  // The user implicitly wants timestamp data to retain its original time units,
  // however the Arrow seconds time unit can not be represented (annotated) in
  // any version of Parquet and so must be coerced to milliseconds.
  if (type.unit() == ::arrow::TimeUnit::SECOND) {
    *logical_type =
        TimestampLogicalTypeFromArrowTimestamp(type, ::arrow::TimeUnit::MILLI);
    return Status::OK();
  }

  return Status::OK();
}

Status FieldToNode(const std::shared_ptr<Field>& field,
                   const WriterProperties& properties,
                   const ArrowWriterProperties& arrow_properties, NodePtr* out) {
  std::shared_ptr<const LogicalType> logical_type = LogicalType::None();
  ParquetType::type type;
  Repetition::type repetition =
      field->nullable() ? Repetition::OPTIONAL : Repetition::REQUIRED;

  int length = -1;
  int precision = -1;
  int scale = -1;

  switch (field->type()->id()) {
    case ArrowTypeId::NA:
      type = ParquetType::INT32;
      logical_type = LogicalType::Null();
      break;
    case ArrowTypeId::BOOL:
      type = ParquetType::BOOLEAN;
      break;
    case ArrowTypeId::UINT8:
      type = ParquetType::INT32;
      logical_type = LogicalType::Int(8, false);
      break;
    case ArrowTypeId::INT8:
      type = ParquetType::INT32;
      logical_type = LogicalType::Int(8, true);
      break;
    case ArrowTypeId::UINT16:
      type = ParquetType::INT32;
      logical_type = LogicalType::Int(16, false);
      break;
    case ArrowTypeId::INT16:
      type = ParquetType::INT32;
      logical_type = LogicalType::Int(16, true);
      break;
    case ArrowTypeId::UINT32:
      if (properties.version() == ::parquet::ParquetVersion::PARQUET_1_0) {
        type = ParquetType::INT64;
      } else {
        type = ParquetType::INT32;
        logical_type = LogicalType::Int(32, false);
      }
      break;
    case ArrowTypeId::INT32:
      type = ParquetType::INT32;
      break;
    case ArrowTypeId::UINT64:
      type = ParquetType::INT64;
      logical_type = LogicalType::Int(64, false);
      break;
    case ArrowTypeId::INT64:
      type = ParquetType::INT64;
      break;
    case ArrowTypeId::FLOAT:
      type = ParquetType::FLOAT;
      break;
    case ArrowTypeId::DOUBLE:
      type = ParquetType::DOUBLE;
      break;
    case ArrowTypeId::STRING:
      type = ParquetType::BYTE_ARRAY;
      logical_type = LogicalType::String();
      break;
    case ArrowTypeId::BINARY:
      type = ParquetType::BYTE_ARRAY;
      break;
    case ArrowTypeId::FIXED_SIZE_BINARY: {
      type = ParquetType::FIXED_LEN_BYTE_ARRAY;
      const auto& fixed_size_binary_type =
          static_cast<const ::arrow::FixedSizeBinaryType&>(*field->type());
      length = fixed_size_binary_type.byte_width();
    } break;
    case ArrowTypeId::DECIMAL: {
      type = ParquetType::FIXED_LEN_BYTE_ARRAY;
      const auto& decimal_type =
          static_cast<const ::arrow::Decimal128Type&>(*field->type());
      precision = decimal_type.precision();
      scale = decimal_type.scale();
      length = DecimalSize(precision);
      PARQUET_CATCH_NOT_OK(logical_type = LogicalType::Decimal(precision, scale));
    } break;
    case ArrowTypeId::DATE32:
      type = ParquetType::INT32;
      logical_type = LogicalType::Date();
      break;
    case ArrowTypeId::DATE64:
      type = ParquetType::INT32;
      logical_type = LogicalType::Date();
      break;
    case ArrowTypeId::TIMESTAMP:
      RETURN_NOT_OK(
          GetTimestampMetadata(static_cast<::arrow::TimestampType&>(*field->type()),
                               properties, arrow_properties, &type, &logical_type));
      break;
    case ArrowTypeId::TIME32:
      type = ParquetType::INT32;
      logical_type =
          LogicalType::Time(/*is_adjusted_to_utc=*/true, LogicalType::TimeUnit::MILLIS);
      break;
    case ArrowTypeId::TIME64: {
      type = ParquetType::INT64;
      auto time_type = static_cast<::arrow::Time64Type*>(field->type().get());
      if (time_type->unit() == ::arrow::TimeUnit::NANO) {
        logical_type =
            LogicalType::Time(/*is_adjusted_to_utc=*/true, LogicalType::TimeUnit::NANOS);
      } else {
        logical_type =
            LogicalType::Time(/*is_adjusted_to_utc=*/true, LogicalType::TimeUnit::MICROS);
      }
    } break;
    case ArrowTypeId::STRUCT: {
      auto struct_type = std::static_pointer_cast<::arrow::StructType>(field->type());
      return StructToNode(struct_type, field->name(), field->nullable(), properties,
                          arrow_properties, out);
    }
    case ArrowTypeId::LIST: {
      auto list_type = std::static_pointer_cast<::arrow::ListType>(field->type());
      return ListToNode(list_type, field->name(), field->nullable(), properties,
                        arrow_properties, out);
    }
    case ArrowTypeId::DICTIONARY: {
      // Parquet has no Dictionary type, dictionary-encoded is handled on
      // the encoding, not the schema level.
      const ::arrow::DictionaryType& dict_type =
          static_cast<const ::arrow::DictionaryType&>(*field->type());
      std::shared_ptr<::arrow::Field> unpacked_field = ::arrow::field(
          field->name(), dict_type.value_type(), field->nullable(), field->metadata());
      return FieldToNode(unpacked_field, properties, arrow_properties, out);
    }
    default: {
      // TODO: DENSE_UNION, SPARE_UNION, JSON_SCALAR, DECIMAL_TEXT, VARCHAR
      return Status::NotImplemented(
          "Unhandled type for Arrow to Parquet schema conversion: ",
          field->type()->ToString());
    }
  }

  PARQUET_CATCH_NOT_OK(*out = PrimitiveNode::Make(field->name(), repetition, logical_type,
                                                  type, length));

  return Status::OK();
}

Status ToParquetSchema(const ::arrow::Schema* arrow_schema,
                       const WriterProperties& properties,
                       const ArrowWriterProperties& arrow_properties,
                       std::shared_ptr<SchemaDescriptor>* out) {
  std::vector<NodePtr> nodes(arrow_schema->num_fields());
  for (int i = 0; i < arrow_schema->num_fields(); i++) {
    RETURN_NOT_OK(
        FieldToNode(arrow_schema->field(i), properties, arrow_properties, &nodes[i]));
  }

  NodePtr schema = GroupNode::Make("schema", Repetition::REQUIRED, nodes);
  *out = std::make_shared<::parquet::SchemaDescriptor>();
  PARQUET_CATCH_NOT_OK((*out)->Init(schema));

  return Status::OK();
}

Status ToParquetSchema(const ::arrow::Schema* arrow_schema,
                       const WriterProperties& properties,
                       std::shared_ptr<SchemaDescriptor>* out) {
  return ToParquetSchema(arrow_schema, properties, *default_arrow_writer_properties(),
                         out);
}

/// \brief Compute the number of bytes required to represent a decimal of a
/// given precision. Taken from the Apache Impala codebase. The comments next
/// to the return values are the maximum value that can be represented in 2's
/// complement with the returned number of bytes.
int32_t DecimalSize(int32_t precision) {
  DCHECK_GE(precision, 1) << "decimal precision must be greater than or equal to 1, got "
                          << precision;
  DCHECK_LE(precision, 38) << "decimal precision must be less than or equal to 38, got "
                           << precision;

  switch (precision) {
    case 1:
    case 2:
      return 1;  // 127
    case 3:
    case 4:
      return 2;  // 32,767
    case 5:
    case 6:
      return 3;  // 8,388,607
    case 7:
    case 8:
    case 9:
      return 4;  // 2,147,483,427
    case 10:
    case 11:
      return 5;  // 549,755,813,887
    case 12:
    case 13:
    case 14:
      return 6;  // 140,737,488,355,327
    case 15:
    case 16:
      return 7;  // 36,028,797,018,963,967
    case 17:
    case 18:
      return 8;  // 9,223,372,036,854,775,807
    case 19:
    case 20:
    case 21:
      return 9;  // 2,361,183,241,434,822,606,847
    case 22:
    case 23:
      return 10;  // 604,462,909,807,314,587,353,087
    case 24:
    case 25:
    case 26:
      return 11;  // 154,742,504,910,672,534,362,390,527
    case 27:
    case 28:
      return 12;  // 39,614,081,257,132,168,796,771,975,167
    case 29:
    case 30:
    case 31:
      return 13;  // 10,141,204,801,825,835,211,973,625,643,007
    case 32:
    case 33:
      return 14;  // 2,596,148,429,267,413,814,265,248,164,610,047
    case 34:
    case 35:
      return 15;  // 664,613,997,892,457,936,451,903,530,140,172,287
    case 36:
    case 37:
    case 38:
      return 16;  // 170,141,183,460,469,231,731,687,303,715,884,105,727
    default:
      break;
  }
  DCHECK(false);
  return -1;
}

}  // namespace arrow
}  // namespace parquet
