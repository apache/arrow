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
#include <deque>
#include <sstream>
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
#include "parquet/schema.h"
#include "parquet/types.h"

using arrow::Field;
using arrow::Status;
using arrow::internal::checked_cast;

using ArrowType = arrow::DataType;
using ArrowTypeId = arrow::Type;

using parquet::Repetition;
using parquet::schema::ColumnPath;
using parquet::schema::GroupNode;
using parquet::schema::Node;
using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;

using ParquetType = parquet::Type;
using parquet::LogicalAnnotation;
using parquet::LogicalType;

namespace parquet {

namespace arrow {

static const char* key_prefix = "org.apache.arrow.field[";
static const char* key_suffix = "].timestamp.timezone";

const auto TIMESTAMP_MS = ::arrow::timestamp(::arrow::TimeUnit::MILLI);
const auto TIMESTAMP_US = ::arrow::timestamp(::arrow::TimeUnit::MICRO);
const auto TIMESTAMP_NS = ::arrow::timestamp(::arrow::TimeUnit::NANO);

static Status MakeArrowDecimal(const std::shared_ptr<const LogicalAnnotation>& annotation,
                               std::shared_ptr<ArrowType>* out) {
  const auto& decimal = checked_cast<const DecimalAnnotation&>(*annotation);
  *out = ::arrow::decimal(decimal.precision(), decimal.scale());
  return Status::OK();
}

static Status MakeArrowInt(const std::shared_ptr<const LogicalAnnotation>& annotation,
                           std::shared_ptr<ArrowType>* out) {
  const auto& integer = checked_cast<const IntAnnotation&>(*annotation);
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
      return Status::TypeError(annotation->ToString(),
                               " can not annotate physical type Int32");
  }
  return Status::OK();
}

static Status MakeArrowInt64(const std::shared_ptr<const LogicalAnnotation>& annotation,
                             std::shared_ptr<ArrowType>* out) {
  const auto& integer = checked_cast<const IntAnnotation&>(*annotation);
  switch (integer.bit_width()) {
    case 64:
      *out = integer.is_signed() ? ::arrow::int64() : ::arrow::uint64();
      break;
    default:
      return Status::TypeError(annotation->ToString(),
                               " can not annotate physical type Int64");
  }
  return Status::OK();
}

static Status MakeArrowTime32(const std::shared_ptr<const LogicalAnnotation>& annotation,
                              std::shared_ptr<ArrowType>* out) {
  const auto& time = checked_cast<const TimeAnnotation&>(*annotation);
  switch (time.time_unit()) {
    case LogicalAnnotation::TimeUnit::MILLIS:
      *out = ::arrow::time32(::arrow::TimeUnit::MILLI);
      break;
    default:
      return Status::TypeError(annotation->ToString(),
                               " can not annotate physical type Time32");
  }
  return Status::OK();
}

static Status MakeArrowTime64(const std::shared_ptr<const LogicalAnnotation>& annotation,
                              std::shared_ptr<ArrowType>* out) {
  const auto& time = checked_cast<const TimeAnnotation&>(*annotation);
  switch (time.time_unit()) {
    case LogicalAnnotation::TimeUnit::MICROS:
      *out = ::arrow::time64(::arrow::TimeUnit::MICRO);
      break;
    case LogicalAnnotation::TimeUnit::NANOS:
      *out = ::arrow::time64(::arrow::TimeUnit::NANO);
      break;
    default:
      return Status::TypeError(annotation->ToString(),
                               " can not annotate physical type Time64");
  }
  return Status::OK();
}

static int FetchMetadataTimezoneIndex(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  int index = -1;
  if (metadata) {
    const std::string key(key_prefix + path + key_suffix);
    index = metadata->FindKey(key);
  }
  return index;
}

static Status MakeArrowTimestamp(
    const std::shared_ptr<const LogicalAnnotation>& annotation, const std::string& path,
    const std::shared_ptr<const KeyValueMetadata>& metadata,
    std::shared_ptr<ArrowType>* out) {
  const auto& timestamp = checked_cast<const TimestampAnnotation&>(*annotation);
  ::arrow::TimeUnit::type time_unit;

  switch (timestamp.time_unit()) {
    case LogicalAnnotation::TimeUnit::MILLIS:
      time_unit = ::arrow::TimeUnit::MILLI;
      break;
    case LogicalAnnotation::TimeUnit::MICROS:
      time_unit = ::arrow::TimeUnit::MICRO;
      break;
    case LogicalAnnotation::TimeUnit::NANOS:
      time_unit = ::arrow::TimeUnit::NANO;
      break;
    default:
      return Status::TypeError("Unrecognized time unit in timestamp annotation: ",
                               annotation->ToString());
  }

  // Attempt to recover optional timezone for this field from file metadata
  int index = FetchMetadataTimezoneIndex(path, metadata);

  *out = (timestamp.is_adjusted_to_utc()
              ? ::arrow::timestamp(time_unit, "UTC")
              : (index == -1 ? ::arrow::timestamp(time_unit)
                             : ::arrow::timestamp(time_unit, metadata->value(index))));

  return Status::OK();
}

static Status FromByteArray(const std::shared_ptr<const LogicalAnnotation>& annotation,
                            std::shared_ptr<ArrowType>* out) {
  switch (annotation->type()) {
    case LogicalAnnotation::Type::STRING:
      *out = ::arrow::utf8();
      break;
    case LogicalAnnotation::Type::DECIMAL:
      RETURN_NOT_OK(MakeArrowDecimal(annotation, out));
      break;
    case LogicalAnnotation::Type::NONE:
    case LogicalAnnotation::Type::ENUM:
    case LogicalAnnotation::Type::JSON:
    case LogicalAnnotation::Type::BSON:
      *out = ::arrow::binary();
      break;
    default:
      return Status::NotImplemented("Unhandled logical annotation ",
                                    annotation->ToString(), " for binary array");
  }
  return Status::OK();
}

static Status FromFLBA(const std::shared_ptr<const LogicalAnnotation>& annotation,
                       int32_t physical_length, std::shared_ptr<ArrowType>* out) {
  switch (annotation->type()) {
    case LogicalAnnotation::Type::DECIMAL:
      RETURN_NOT_OK(MakeArrowDecimal(annotation, out));
      break;
    case LogicalAnnotation::Type::NONE:
    case LogicalAnnotation::Type::INTERVAL:
    case LogicalAnnotation::Type::UUID:
      *out = ::arrow::fixed_size_binary(physical_length);
      break;
    default:
      return Status::NotImplemented("Unhandled logical annotation ",
                                    annotation->ToString(),
                                    " for fixed-length binary array");
  }

  return Status::OK();
}

static Status FromInt32(const std::shared_ptr<const LogicalAnnotation>& annotation,
                        std::shared_ptr<ArrowType>* out) {
  switch (annotation->type()) {
    case LogicalAnnotation::Type::INT:
      RETURN_NOT_OK(MakeArrowInt(annotation, out));
      break;
    case LogicalAnnotation::Type::DATE:
      *out = ::arrow::date32();
      break;
    case LogicalAnnotation::Type::TIME:
      RETURN_NOT_OK(MakeArrowTime32(annotation, out));
      break;
    case LogicalAnnotation::Type::DECIMAL:
      RETURN_NOT_OK(MakeArrowDecimal(annotation, out));
      break;
    case LogicalAnnotation::Type::NONE:
      *out = ::arrow::int32();
      break;
    default:
      return Status::NotImplemented("Unhandled logical type ", annotation->ToString(),
                                    " for INT32");
  }
  return Status::OK();
}

static Status FromInt64(const std::shared_ptr<const LogicalAnnotation>& annotation,
                        const std::string& path,
                        const std::shared_ptr<const KeyValueMetadata>& metadata,
                        std::shared_ptr<ArrowType>* out) {
  switch (annotation->type()) {
    case LogicalAnnotation::Type::INT:
      RETURN_NOT_OK(MakeArrowInt64(annotation, out));
      break;
    case LogicalAnnotation::Type::DECIMAL:
      RETURN_NOT_OK(MakeArrowDecimal(annotation, out));
      break;
    case LogicalAnnotation::Type::TIMESTAMP:
      RETURN_NOT_OK(MakeArrowTimestamp(annotation, path, metadata, out));
      break;
    case LogicalAnnotation::Type::TIME:
      RETURN_NOT_OK(MakeArrowTime64(annotation, out));
      break;
    case LogicalAnnotation::Type::NONE:
      *out = ::arrow::int64();
      break;
    default:
      return Status::NotImplemented("Unhandled logical type ", annotation->ToString(),
                                    " for INT64");
  }
  return Status::OK();
}

Status FromPrimitive(const PrimitiveNode& primitive,
                     const std::shared_ptr<const KeyValueMetadata>& metadata,
                     std::shared_ptr<ArrowType>* out) {
  const std::shared_ptr<const LogicalAnnotation>& annotation =
      primitive.logical_annotation();
  if (annotation->is_invalid() || annotation->is_null()) {
    *out = ::arrow::null();
    return Status::OK();
  }

  switch (primitive.physical_type()) {
    case ParquetType::BOOLEAN:
      *out = ::arrow::boolean();
      break;
    case ParquetType::INT32:
      RETURN_NOT_OK(FromInt32(annotation, out));
      break;
    case ParquetType::INT64:
      RETURN_NOT_OK(FromInt64(annotation, ColumnPath::FromNode(primitive)->ToDotString(),
                              metadata, out));
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
      RETURN_NOT_OK(FromByteArray(annotation, out));
      break;
    case ParquetType::FIXED_LEN_BYTE_ARRAY:
      RETURN_NOT_OK(FromFLBA(annotation, primitive.type_length(), out));
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
                           const std::shared_ptr<const KeyValueMetadata>& metadata,
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
                       const std::shared_ptr<const KeyValueMetadata>& metadata,
                       std::shared_ptr<ArrowType>* out) {
  std::vector<std::shared_ptr<Field>> fields;
  std::shared_ptr<Field> field;

  *out = nullptr;

  for (int i = 0; i < group.field_count(); i++) {
    RETURN_NOT_OK(
        NodeToFieldInternal(*group.field(i), included_leaf_nodes, metadata, &field));
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
                  const std::shared_ptr<const KeyValueMetadata>& metadata,
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
        RETURN_NOT_OK(NodeToFieldInternal(*list_group.field(0), included_leaf_nodes,
                                          metadata, &item_field));

        if (item_field != nullptr) {
          *out = ::arrow::list(item_field);
        }
      } else {
        // List of struct
        std::shared_ptr<ArrowType> inner_type;
        RETURN_NOT_OK(
            StructFromGroup(list_group, included_leaf_nodes, metadata, &inner_type));
        if (inner_type != nullptr) {
          auto item_field = std::make_shared<Field>(list_node.name(), inner_type, false);
          *out = ::arrow::list(item_field);
        }
      }
    } else if (list_node.is_repeated()) {
      // repeated primitive node
      std::shared_ptr<ArrowType> inner_type;
      if (IsIncludedLeaf(static_cast<const Node&>(list_node), included_leaf_nodes)) {
        RETURN_NOT_OK(FromPrimitive(static_cast<const PrimitiveNode&>(list_node),
                                    metadata, &inner_type));
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
  return NodeToFieldInternal(node, nullptr, nullptr, out);
}

Status NodeToFieldInternal(const Node& node,
                           const std::unordered_set<const Node*>* included_leaf_nodes,
                           const std::shared_ptr<const KeyValueMetadata>& metadata,
                           std::shared_ptr<Field>* out) {
  std::shared_ptr<ArrowType> type = nullptr;
  bool nullable = !node.is_required();

  *out = nullptr;

  if (node.is_repeated()) {
    // 1-level LIST encoding fields are required
    std::shared_ptr<ArrowType> inner_type;
    if (node.is_group()) {
      RETURN_NOT_OK(StructFromGroup(static_cast<const GroupNode&>(node),
                                    included_leaf_nodes, metadata, &inner_type));
    } else if (IsIncludedLeaf(node, included_leaf_nodes)) {
      RETURN_NOT_OK(
          FromPrimitive(static_cast<const PrimitiveNode&>(node), metadata, &inner_type));
    }
    if (inner_type != nullptr) {
      auto item_field = std::make_shared<Field>(node.name(), inner_type, false);
      type = ::arrow::list(item_field);
      nullable = false;
    }
  } else if (node.is_group()) {
    const auto& group = static_cast<const GroupNode&>(node);
    if (node.logical_annotation()->is_list()) {
      RETURN_NOT_OK(NodeToList(group, included_leaf_nodes, metadata, &type));
    } else {
      RETURN_NOT_OK(StructFromGroup(group, included_leaf_nodes, metadata, &type));
    }
  } else {
    // Primitive (leaf) node
    if (IsIncludedLeaf(node, included_leaf_nodes)) {
      RETURN_NOT_OK(
          FromPrimitive(static_cast<const PrimitiveNode&>(node), metadata, &type));
    }
  }
  if (type != nullptr) {
    *out = std::make_shared<Field>(node.name(), type, nullable);
  }
  return Status::OK();
}

static std::shared_ptr<const KeyValueMetadata> FilterParquetMetadata(
    const std::shared_ptr<const KeyValueMetadata>& metadata) {
  // Return a copy of the input metadata stripped of any key-values pairs
  // that held Arrow/Parquet storage specific information
  std::shared_ptr<KeyValueMetadata> filtered_metadata =
      std::make_shared<KeyValueMetadata>();
  if (metadata) {
    for (int i = 0; i < metadata->size(); ++i) {
      const std::string& key = metadata->key(i);
      const std::string& value = metadata->value(i);
      // Discard keys like "org.apache.arrow.field[*].timestamp.timezone"
      size_t key_size = key.size();
      size_t key_prefix_size = strlen(key_prefix);
      size_t key_suffix_size = strlen(key_suffix);
      bool timezone_storage_key =
          (key_size >= (key_prefix_size + key_suffix_size)) &&
          (key.compare(0, key_prefix_size, key_prefix) == 0) &&
          (key.compare(key_size - key_suffix_size, key_suffix_size, key_suffix) == 0);
      if (!timezone_storage_key) {
        filtered_metadata->Append(key, value);
      }
    }
  }
  if (filtered_metadata->size() == 0) {
    filtered_metadata.reset();
  }
  return filtered_metadata;
}

Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
                         const std::shared_ptr<const KeyValueMetadata>& parquet_metadata,
                         std::shared_ptr<::arrow::Schema>* out) {
  const GroupNode& schema_node = *parquet_schema->group_node();

  int num_fields = static_cast<int>(schema_node.field_count());
  std::vector<std::shared_ptr<Field>> fields(num_fields);
  for (int i = 0; i < num_fields; i++) {
    RETURN_NOT_OK(NodeToFieldInternal(*schema_node.field(i), nullptr, parquet_metadata,
                                      &fields[i]));
  }

  *out =
      std::make_shared<::arrow::Schema>(fields, FilterParquetMetadata(parquet_metadata));
  return Status::OK();
}

Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
                         const std::vector<int>& column_indices,
                         const std::shared_ptr<const KeyValueMetadata>& parquet_metadata,
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
    RETURN_NOT_OK(
        NodeToFieldInternal(*node, &included_leaf_nodes, parquet_metadata, &field));
    if (field != nullptr) {
      fields.push_back(field);
    }
  }

  *out =
      std::make_shared<::arrow::Schema>(fields, FilterParquetMetadata(parquet_metadata));
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

class PresumedColumnPath {
  // Pre-constructs the eventual column path dot string for a Parquet node
  // before it is fully constructed from Arrow fields
 public:
  PresumedColumnPath() = default;

  void Extend(const std::shared_ptr<Field>& field) {
    switch (field->type()->id()) {
      case ArrowTypeId::DICTIONARY:
        path_.push_back({false, ""});
        break;
      case ArrowTypeId::LIST:
        path_.push_back({true, field->name() + ".list"});
        break;
      default:
        path_.push_back({true, field->name()});
        break;
    }
    return;
  }

  void Retract() {
    DCHECK(!path_.empty());
    path_.pop_back();
    return;
  }

  std::string ToDotString() const {
    std::stringstream ss;
    int i = 0;
    for (auto c = path_.cbegin(); c != path_.cend(); ++c) {
      if (c->include) {
        if (i > 0) {
          ss << ".";
        }
        ss << c->component;
        ++i;
      }
    }
    return ss.str();
  }

 private:
  struct Component {
    bool include;
    std::string component;
  };
  std::deque<Component> path_;
};

Status FieldToNodeInternal(const std::shared_ptr<Field>& field,
                           const WriterProperties& properties,
                           const ArrowWriterProperties& arrow_properties,
                           PresumedColumnPath& path,
                           std::unordered_map<std::string, std::string>& metadata,
                           NodePtr* out);

Status ListToNode(const std::shared_ptr<::arrow::ListType>& type, const std::string& name,
                  bool nullable, const WriterProperties& properties,
                  const ArrowWriterProperties& arrow_properties, PresumedColumnPath& path,
                  std::unordered_map<std::string, std::string>& metadata, NodePtr* out) {
  const Repetition::type repetition =
      nullable ? Repetition::OPTIONAL : Repetition::REQUIRED;

  NodePtr element;
  RETURN_NOT_OK(FieldToNodeInternal(type->value_field(), properties, arrow_properties,
                                    path, metadata, &element));

  NodePtr list = GroupNode::Make("list", Repetition::REPEATED, {element});
  *out = GroupNode::Make(name, repetition, {list}, LogicalAnnotation::List());
  return Status::OK();
}

Status StructToNode(const std::shared_ptr<::arrow::StructType>& type,
                    const std::string& name, bool nullable,
                    const WriterProperties& properties,
                    const ArrowWriterProperties& arrow_properties,
                    PresumedColumnPath& path,
                    std::unordered_map<std::string, std::string>& metadata,
                    NodePtr* out) {
  const Repetition::type repetition =
      nullable ? Repetition::OPTIONAL : Repetition::REQUIRED;

  std::vector<NodePtr> children(type->num_children());
  for (int i = 0; i < type->num_children(); i++) {
    RETURN_NOT_OK(FieldToNodeInternal(type->child(i), properties, arrow_properties, path,
                                      metadata, &children[i]));
  }

  *out = GroupNode::Make(name, repetition, children);
  return Status::OK();
}

static bool HasUTCTimezone(const std::string& timezone) {
  static const std::vector<std::string> utczones{"UTC", "utc"};
  return std::any_of(utczones.begin(), utczones.end(),
                     [timezone](const std::string& utc) { return timezone == utc; });
}

static void StoreMetadataTimezone(const std::string& path,
                                  std::unordered_map<std::string, std::string>& metadata,
                                  const std::string& timezone) {
  const std::string key(key_prefix + path + key_suffix);
  if (metadata.find(key) != metadata.end()) {
    std::stringstream ms;
    ms << "Duplicate field name '" << path
       << "' for timestamp with timezone field in Arrow schema.";
    throw ParquetException(ms.str());
  }
  metadata.insert(std::make_pair(key, timezone));
  return;
}

static std::shared_ptr<const LogicalAnnotation> TimestampAnnotationFromArrowTimestamp(
    const std::string& path, const ::arrow::TimestampType& timestamp_type,
    ::arrow::TimeUnit::type time_unit,
    std::unordered_map<std::string, std::string>& metadata) {
  const std::string& timezone = timestamp_type.timezone();
  const bool utc = HasUTCTimezone(timezone);

  if (!utc && !timezone.empty()) {
    // Attempt to preserve timezone for this field as file metadata
    StoreMetadataTimezone(path, metadata, timezone);
  }

  switch (time_unit) {
    case ::arrow::TimeUnit::MILLI:
      return LogicalAnnotation::Timestamp(utc, LogicalAnnotation::TimeUnit::MILLIS);
    case ::arrow::TimeUnit::MICRO:
      return LogicalAnnotation::Timestamp(utc, LogicalAnnotation::TimeUnit::MICROS);
    case ::arrow::TimeUnit::NANO:
      return LogicalAnnotation::Timestamp(utc, LogicalAnnotation::TimeUnit::NANOS);
    case ::arrow::TimeUnit::SECOND:
      // No equivalent parquet logical type.
      break;
  }
  return LogicalAnnotation::None();
}

static Status GetTimestampMetadata(const ::arrow::TimestampType& type,
                                   const std::string& name,
                                   const ArrowWriterProperties& properties,
                                   const std::string& path,
                                   std::unordered_map<std::string, std::string>& metadata,
                                   ParquetType::type* physical_type,
                                   std::shared_ptr<const LogicalAnnotation>* annotation) {
  const bool coerce = properties.coerce_timestamps_enabled();
  const auto target_unit = coerce ? properties.coerce_timestamps_unit() : type.unit();

  // The user is explicitly asking for Impala int96 encoding, there is no
  // logical type.
  if (properties.support_deprecated_int96_timestamps()) {
    *physical_type = ParquetType::INT96;
    return Status::OK();
  }

  *physical_type = ParquetType::INT64;
  PARQUET_CATCH_NOT_OK(*annotation = TimestampAnnotationFromArrowTimestamp(
                           path, type, target_unit, metadata));

  // The user is explicitly asking for timestamp data to be converted to the
  // specified units (target_unit).
  if (coerce) {
    switch (target_unit) {
      case ::arrow::TimeUnit::MILLI:
      case ::arrow::TimeUnit::MICRO:
      case ::arrow::TimeUnit::NANO:
        break;
      case ::arrow::TimeUnit::SECOND:
        return Status::NotImplemented(
            "Can only coerce Arrow timestamps to milliseconds, microseconds, or "
            "nanoseconds");
    }
    return Status::OK();
  }

  // The user implicitly wants timestamp data to retain its original time units,
  // however the Arrow seconds time unit can not be represented (annotated) in Parquet.
  if (type.unit() == ::arrow::TimeUnit::SECOND) {
    return Status::NotImplemented(
        "Only MILLI, MICRO, and NANO units supported for Arrow timestamps with Parquet.");
  }

  return Status::OK();
}

Status FieldToNodeInternal(const std::shared_ptr<Field>& field,
                           const WriterProperties& properties,
                           const ArrowWriterProperties& arrow_properties,
                           PresumedColumnPath& path,
                           std::unordered_map<std::string, std::string>& metadata,
                           NodePtr* out) {
  const Repetition::type repetition =
      field->nullable() ? Repetition::OPTIONAL : Repetition::REQUIRED;
  std::shared_ptr<const LogicalAnnotation> annotation = LogicalAnnotation::None();
  ParquetType::type type;

  int length = -1;
  int precision = -1;
  int scale = -1;

  path.Extend(field);

  switch (field->type()->id()) {
    case ArrowTypeId::NA:
      type = ParquetType::INT32;
      annotation = LogicalAnnotation::Null();
      break;
    case ArrowTypeId::BOOL:
      type = ParquetType::BOOLEAN;
      break;
    case ArrowTypeId::UINT8:
      type = ParquetType::INT32;
      PARQUET_CATCH_NOT_OK(annotation = LogicalAnnotation::Int(8, false));
      break;
    case ArrowTypeId::INT8:
      type = ParquetType::INT32;
      PARQUET_CATCH_NOT_OK(annotation = LogicalAnnotation::Int(8, true));
      break;
    case ArrowTypeId::UINT16:
      type = ParquetType::INT32;
      PARQUET_CATCH_NOT_OK(annotation = LogicalAnnotation::Int(16, false));
      break;
    case ArrowTypeId::INT16:
      type = ParquetType::INT32;
      PARQUET_CATCH_NOT_OK(annotation = LogicalAnnotation::Int(16, true));
      break;
    case ArrowTypeId::UINT32:
      if (properties.version() == ::parquet::ParquetVersion::PARQUET_1_0) {
        type = ParquetType::INT64;
      } else {
        type = ParquetType::INT32;
        PARQUET_CATCH_NOT_OK(annotation = LogicalAnnotation::Int(32, false));
      }
      break;
    case ArrowTypeId::INT32:
      type = ParquetType::INT32;
      break;
    case ArrowTypeId::UINT64:
      type = ParquetType::INT64;
      PARQUET_CATCH_NOT_OK(annotation = LogicalAnnotation::Int(64, false));
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
      annotation = LogicalAnnotation::String();
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
      PARQUET_CATCH_NOT_OK(annotation = LogicalAnnotation::Decimal(precision, scale));
    } break;
    case ArrowTypeId::DATE32:
      type = ParquetType::INT32;
      annotation = LogicalAnnotation::Date();
      break;
    case ArrowTypeId::DATE64:
      type = ParquetType::INT32;
      annotation = LogicalAnnotation::Date();
      break;
    case ArrowTypeId::TIMESTAMP:
      RETURN_NOT_OK(GetTimestampMetadata(
          static_cast<::arrow::TimestampType&>(*field->type()), field->name(),
          arrow_properties, path.ToDotString(), metadata, &type, &annotation));
      break;
    case ArrowTypeId::TIME32:
      type = ParquetType::INT32;
      PARQUET_CATCH_NOT_OK(annotation = LogicalAnnotation::Time(
                               false, LogicalAnnotation::TimeUnit::MILLIS));
      break;
    case ArrowTypeId::TIME64: {
      type = ParquetType::INT64;
      auto time_type = static_cast<::arrow::Time64Type*>(field->type().get());
      if (time_type->unit() == ::arrow::TimeUnit::NANO) {
        PARQUET_CATCH_NOT_OK(annotation = LogicalAnnotation::Time(
                                 false, LogicalAnnotation::TimeUnit::NANOS));
      } else {
        PARQUET_CATCH_NOT_OK(annotation = LogicalAnnotation::Time(
                                 false, LogicalAnnotation::TimeUnit::MICROS));
      }
    } break;
    case ArrowTypeId::STRUCT: {
      auto struct_type = std::static_pointer_cast<::arrow::StructType>(field->type());
      return StructToNode(struct_type, field->name(), field->nullable(), properties,
                          arrow_properties, path, metadata, out);
    }
    case ArrowTypeId::LIST: {
      auto list_type = std::static_pointer_cast<::arrow::ListType>(field->type());
      return ListToNode(list_type, field->name(), field->nullable(), properties,
                        arrow_properties, path, metadata, out);
    }
    case ArrowTypeId::DICTIONARY: {
      // Parquet has no Dictionary type, dictionary-encoded is handled on
      // the encoding, not the schema level.
      const ::arrow::DictionaryType& dict_type =
          static_cast<const ::arrow::DictionaryType&>(*field->type());
      std::shared_ptr<::arrow::Field> unpacked_field = ::arrow::field(
          field->name(), dict_type.value_type(), field->nullable(), field->metadata());
      return FieldToNodeInternal(unpacked_field, properties, arrow_properties, path,
                                 metadata, out);
    }
    default: {
      // TODO: DENSE_UNION, SPARE_UNION, JSON_SCALAR, DECIMAL_TEXT, VARCHAR
      return Status::NotImplemented(
          "Unhandled type for Arrow to Parquet schema conversion: ",
          field->type()->ToString());
    }
  }

  path.Retract();

  PARQUET_CATCH_NOT_OK(*out = PrimitiveNode::Make(field->name(), repetition, annotation,
                                                  type, length));

  return Status::OK();
}

Status FieldToNode(const std::shared_ptr<Field>& field,
                   const WriterProperties& properties,
                   const ArrowWriterProperties& arrow_properties,
                   std::unordered_map<std::string, std::string>& parquet_metadata_map,
                   NodePtr* out) {
  PresumedColumnPath parquet_column_path;
  return FieldToNodeInternal(field, properties, arrow_properties, parquet_column_path,
                             parquet_metadata_map, out);
}

Status FieldToNode(const std::shared_ptr<Field>& field,
                   const WriterProperties& properties,
                   const ArrowWriterProperties& arrow_properties, NodePtr* out) {
  PresumedColumnPath parquet_column_path;
  std::unordered_map<std::string, std::string> dummy_metadata_map;
  return FieldToNodeInternal(field, properties, arrow_properties, parquet_column_path,
                             dummy_metadata_map, out);
}

Status ToParquetSchema(const ::arrow::Schema* arrow_schema,
                       const WriterProperties& properties,
                       const ArrowWriterProperties& arrow_properties,
                       std::shared_ptr<const KeyValueMetadata>* parquet_metadata_out,
                       std::shared_ptr<SchemaDescriptor>* parquet_schema_out) {
  std::unordered_map<std::string, std::string> parquet_metadata_map;
  std::vector<NodePtr> parquet_nodes(arrow_schema->num_fields());

  // TODO(tpboudreau): consider making construction of metadata map in
  // FieldToNodeInternal() conditional on whether caller asked for metadata
  // (parquet_metadata_out != NULL); metadata construction (even if
  // unnecessary) can cause FieldToNodeInternal() to fail
  for (int i = 0; i < arrow_schema->num_fields(); i++) {
    PresumedColumnPath parquet_column_path;
    RETURN_NOT_OK(FieldToNodeInternal(arrow_schema->field(i), properties,
                                      arrow_properties, parquet_column_path,
                                      parquet_metadata_map, &parquet_nodes[i]));
  }

  if (parquet_metadata_out) {
    if (arrow_schema->HasMetadata()) {
      // merge Arrow application metadata with Arrow/Parquet storage specific metadata
      auto arrow_metadata = arrow_schema->metadata();
      std::unordered_map<std::string, std::string> arrow_metadata_map;
      arrow_metadata->ToUnorderedMap(&arrow_metadata_map);
      parquet_metadata_map.insert(arrow_metadata_map.cbegin(), arrow_metadata_map.cend());
    }
    *parquet_metadata_out =
        std::make_shared<const KeyValueMetadata>(parquet_metadata_map);
  }

  NodePtr parquet_schema = GroupNode::Make("schema", Repetition::REQUIRED, parquet_nodes);
  *parquet_schema_out = std::make_shared<::parquet::SchemaDescriptor>();
  PARQUET_CATCH_NOT_OK((*parquet_schema_out)->Init(parquet_schema));

  return Status::OK();
}

Status ToParquetSchema(const ::arrow::Schema* arrow_schema,
                       const WriterProperties& properties,
                       std::shared_ptr<const KeyValueMetadata>* parquet_metadata_out,
                       std::shared_ptr<SchemaDescriptor>* parquet_schema_out) {
  return ToParquetSchema(arrow_schema, properties, *default_arrow_writer_properties(),
                         parquet_metadata_out, parquet_schema_out);
}

Status ToParquetSchema(const ::arrow::Schema* arrow_schema,
                       const WriterProperties& properties,
                       const ArrowWriterProperties& arrow_properties,
                       std::shared_ptr<SchemaDescriptor>* parquet_schema_out) {
  return ToParquetSchema(arrow_schema, properties, arrow_properties, nullptr,
                         parquet_schema_out);
}

Status ToParquetSchema(const ::arrow::Schema* arrow_schema,
                       const WriterProperties& properties,
                       std::shared_ptr<SchemaDescriptor>* parquet_schema_out) {
  return ToParquetSchema(arrow_schema, properties, *default_arrow_writer_properties(),
                         nullptr, parquet_schema_out);
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
