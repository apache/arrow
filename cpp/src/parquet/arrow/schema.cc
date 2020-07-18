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

#include "arrow/extension_type.h"
#include "arrow/ipc/api.h"
#include "arrow/result_internal.h"
#include "arrow/type.h"
#include "arrow/util/base64.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "parquet/arrow/schema_internal.h"
#include "parquet/exception.h"
#include "parquet/properties.h"
#include "parquet/types.h"

using arrow::Field;
using arrow::KeyValueMetadata;
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

using parquet::internal::DecimalSize;

namespace parquet {

namespace arrow {

// ----------------------------------------------------------------------
// Parquet to Arrow schema conversion

namespace {
Repetition::type RepitionFromNullable(bool is_nullable) {
  return is_nullable ? Repetition::OPTIONAL : Repetition::REQUIRED;
}

Status FieldToNode(const std::string& name, const std::shared_ptr<Field>& field,
                   const WriterProperties& properties,
                   const ArrowWriterProperties& arrow_properties, NodePtr* out);

Status ListToNode(const std::shared_ptr<::arrow::BaseListType>& type,
                  const std::string& name, bool nullable,
                  const WriterProperties& properties,
                  const ArrowWriterProperties& arrow_properties, NodePtr* out) {
  NodePtr element;
  std::string value_name =
      arrow_properties.compliant_nested_types() ? "element" : type->value_field()->name();
  RETURN_NOT_OK(FieldToNode(value_name, type->value_field(), properties, arrow_properties,
                            &element));

  NodePtr list = GroupNode::Make("list", Repetition::REPEATED, {element});
  *out =
      GroupNode::Make(name, RepitionFromNullable(nullable), {list}, LogicalType::List());
  return Status::OK();
}

Status MapToNode(const std::shared_ptr<::arrow::MapType>& type, const std::string& name,
                 bool nullable, const WriterProperties& properties,
                 const ArrowWriterProperties& arrow_properties, NodePtr* out) {
  // TODO: Should we offer a non-compliant mode that forwards the type names?
  NodePtr key_node;
  RETURN_NOT_OK(
      FieldToNode("key", type->key_field(), properties, arrow_properties, &key_node));

  NodePtr value_node;
  RETURN_NOT_OK(FieldToNode("value", type->item_field(), properties, arrow_properties,
                            &value_node));

  NodePtr key_value =
      GroupNode::Make("key_value", Repetition::REPEATED, {key_node, value_node});
  *out = GroupNode::Make(name, RepitionFromNullable(nullable), {key_value},
                         LogicalType::Map());
  return Status::OK();
}

Status StructToNode(const std::shared_ptr<::arrow::StructType>& type,
                    const std::string& name, bool nullable,
                    const WriterProperties& properties,
                    const ArrowWriterProperties& arrow_properties, NodePtr* out) {
  std::vector<NodePtr> children(type->num_fields());
  for (int i = 0; i < type->num_fields(); i++) {
    RETURN_NOT_OK(FieldToNode(type->field(i)->name(), type->field(i), properties,
                              arrow_properties, &children[i]));
  }

  *out = GroupNode::Make(name, RepitionFromNullable(nullable), children);
  return Status::OK();
}

static std::shared_ptr<const LogicalType> TimestampLogicalTypeFromArrowTimestamp(
    const ::arrow::TimestampType& timestamp_type, ::arrow::TimeUnit::type time_unit) {
  const bool utc = !(timestamp_type.timezone().empty());
  // ARROW-5878(wesm): for forward compatibility reasons, and because
  // there's no other way to signal to old readers that values are
  // timestamps, we force the ConvertedType field to be set to the
  // corresponding TIMESTAMP_* value. This does cause some ambiguity
  // as Parquet readers have not been consistent about the
  // interpretation of TIMESTAMP_* values as being UTC-normalized.
  switch (time_unit) {
    case ::arrow::TimeUnit::MILLI:
      return LogicalType::Timestamp(utc, LogicalType::TimeUnit::MILLIS,
                                    /*is_from_converted_type=*/false,
                                    /*force_set_converted_type=*/true);
    case ::arrow::TimeUnit::MICRO:
      return LogicalType::Timestamp(utc, LogicalType::TimeUnit::MICROS,
                                    /*is_from_converted_type=*/false,
                                    /*force_set_converted_type=*/true);
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

Status FieldToNode(const std::string& name, const std::shared_ptr<Field>& field,
                   const WriterProperties& properties,
                   const ArrowWriterProperties& arrow_properties, NodePtr* out) {
  std::shared_ptr<const LogicalType> logical_type = LogicalType::None();
  ParquetType::type type;
  Repetition::type repetition = RepitionFromNullable(field->nullable());

  int length = -1;
  int precision = -1;
  int scale = -1;

  switch (field->type()->id()) {
    case ArrowTypeId::NA: {
      type = ParquetType::INT32;
      logical_type = LogicalType::Null();
      if (repetition != Repetition::OPTIONAL) {
        return Status::Invalid("NullType Arrow field must be nullable");
      }
    } break;
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
    case ArrowTypeId::LARGE_STRING:
    case ArrowTypeId::STRING:
      type = ParquetType::BYTE_ARRAY;
      logical_type = LogicalType::String();
      break;
    case ArrowTypeId::LARGE_BINARY:
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
      return StructToNode(struct_type, name, field->nullable(), properties,
                          arrow_properties, out);
    }
    case ArrowTypeId::FIXED_SIZE_LIST:
    case ArrowTypeId::LARGE_LIST:
    case ArrowTypeId::LIST: {
      auto list_type = std::static_pointer_cast<::arrow::BaseListType>(field->type());
      return ListToNode(list_type, name, field->nullable(), properties, arrow_properties,
                        out);
    }
    case ArrowTypeId::DICTIONARY: {
      // Parquet has no Dictionary type, dictionary-encoded is handled on
      // the encoding, not the schema level.
      const ::arrow::DictionaryType& dict_type =
          static_cast<const ::arrow::DictionaryType&>(*field->type());
      std::shared_ptr<::arrow::Field> unpacked_field = ::arrow::field(
          name, dict_type.value_type(), field->nullable(), field->metadata());
      return FieldToNode(name, unpacked_field, properties, arrow_properties, out);
    }
    case ArrowTypeId::EXTENSION: {
      auto ext_type = std::static_pointer_cast<::arrow::ExtensionType>(field->type());
      std::shared_ptr<::arrow::Field> storage_field = ::arrow::field(
          name, ext_type->storage_type(), field->nullable(), field->metadata());
      return FieldToNode(name, storage_field, properties, arrow_properties, out);
    }
    case ArrowTypeId::MAP: {
      auto map_type = std::static_pointer_cast<::arrow::MapType>(field->type());
      return MapToNode(map_type, name, field->nullable(), properties, arrow_properties,
                       out);
    }

    default: {
      // TODO: DENSE_UNION, SPARE_UNION, JSON_SCALAR, DECIMAL_TEXT, VARCHAR
      return Status::NotImplemented(
          "Unhandled type for Arrow to Parquet schema conversion: ",
          field->type()->ToString());
    }
  }

  PARQUET_CATCH_NOT_OK(*out = PrimitiveNode::Make(name, repetition, logical_type, type,
                                                  length));

  return Status::OK();
}

struct SchemaTreeContext {
  SchemaManifest* manifest;
  ArrowReaderProperties properties;
  const SchemaDescriptor* schema;

  void LinkParent(const SchemaField* child, const SchemaField* parent) {
    manifest->child_to_parent[child] = parent;
  }

  void RecordLeaf(const SchemaField* leaf) {
    manifest->column_index_to_field[leaf->column_index] = leaf;
  }
};

bool IsDictionaryReadSupported(const ArrowType& type) {
  // Only supported currently for BYTE_ARRAY types
  return type.id() == ::arrow::Type::BINARY || type.id() == ::arrow::Type::STRING;
}

// ----------------------------------------------------------------------
// Schema logic

::arrow::Result<std::shared_ptr<ArrowType>> GetTypeForNode(
    int column_index, const schema::PrimitiveNode& primitive_node,
    SchemaTreeContext* ctx) {
  ASSIGN_OR_RAISE(std::shared_ptr<ArrowType> storage_type, GetArrowType(primitive_node));
  if (ctx->properties.read_dictionary(column_index) &&
      IsDictionaryReadSupported(*storage_type)) {
    return ::arrow::dictionary(::arrow::int32(), storage_type);
  }
  return storage_type;
}

Status NodeToSchemaField(const Node& node, int16_t max_def_level, int16_t max_rep_level,
                         SchemaTreeContext* ctx, const SchemaField* parent,
                         SchemaField* out);

Status GroupToSchemaField(const GroupNode& node, int16_t max_def_level,
                          int16_t max_rep_level, SchemaTreeContext* ctx,
                          const SchemaField* parent, SchemaField* out);

Status PopulateLeaf(int column_index, const std::shared_ptr<Field>& field,
                    int16_t max_def_level, int16_t max_rep_level, SchemaTreeContext* ctx,
                    const SchemaField* parent, SchemaField* out) {
  out->field = field;
  out->column_index = column_index;
  out->definition_level = max_def_level;
  out->repetition_level = max_rep_level;
  ctx->RecordLeaf(out);
  ctx->LinkParent(out, parent);
  return Status::OK();
}

// Special case mentioned in the format spec:
//   If the name is array or ends in _tuple, this should be a list of struct
//   even for single child elements.
bool HasStructListName(const GroupNode& node) {
  ::arrow::util::string_view name{node.name()};
  return name == "array" || name.ends_with("_tuple");
}

std::shared_ptr<::arrow::KeyValueMetadata> FieldIdMetadata(int field_id) {
  return ::arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(field_id)});
}

Status GroupToStruct(const GroupNode& node, int16_t current_def_level,
                     int16_t current_rep_level, SchemaTreeContext* ctx,
                     const SchemaField* parent, SchemaField* out) {
  std::vector<std::shared_ptr<Field>> arrow_fields;
  out->children.resize(node.field_count());
  for (int i = 0; i < node.field_count(); i++) {
    RETURN_NOT_OK(NodeToSchemaField(*node.field(i), current_def_level, current_rep_level,
                                    ctx, out, &out->children[i]));
    arrow_fields.push_back(out->children[i].field);
  }
  auto struct_type = ::arrow::struct_(arrow_fields);
  out->field = ::arrow::field(node.name(), struct_type, node.is_optional(),
                              FieldIdMetadata(node.field_id()));
  out->definition_level = current_def_level;
  out->repetition_level = current_rep_level;
  return Status::OK();
}

Status ListToSchemaField(const GroupNode& group, int16_t current_def_level,
                         int16_t current_rep_level, SchemaTreeContext* ctx,
                         const SchemaField* parent, SchemaField* out) {
  if (group.field_count() != 1) {
    return Status::NotImplemented(
        "Only LIST-annotated groups with a single child can be handled.");
  }

  out->children.resize(group.field_count());
  SchemaField* child_field = &out->children[0];

  ctx->LinkParent(out, parent);
  ctx->LinkParent(child_field, out);

  const Node& list_node = *group.field(0);

  if (!list_node.is_repeated()) {
    return Status::NotImplemented(
        "Non-repeated nodes in a LIST-annotated group are not supported.");
  }

  ++current_def_level;
  ++current_rep_level;
  if (list_node.is_group()) {
    // Resolve 3-level encoding
    //
    // required/optional group name=whatever {
    //   repeated group name=list {
    //     required/optional TYPE item;
    //   }
    // }
    //
    // yields list<item: TYPE ?nullable> ?nullable
    //
    // We distinguish the special base that we have
    //
    // required/optional group name=whatever {
    //   repeated group name=array or $SOMETHING_tuple {
    //     required/optional TYPE item;
    //   }
    // }
    //
    // In this latter case, the inner type of the list should be a struct
    // rather than a primitive value
    //
    // yields list<item: struct<item: TYPE ?nullable> not null> ?nullable
    const auto& list_group = static_cast<const GroupNode&>(list_node);
    // Special case mentioned in the format spec:
    //   If the name is array or ends in _tuple, this should be a list of struct
    //   even for single child elements.
    if (list_group.field_count() == 1 && !HasStructListName(list_group)) {
      // List of primitive type
      RETURN_NOT_OK(NodeToSchemaField(*list_group.field(0), current_def_level,
                                      current_rep_level, ctx, out, child_field));
    } else {
      RETURN_NOT_OK(GroupToStruct(list_group, current_def_level, current_rep_level, ctx,
                                  out, child_field));
    }
  } else {
    // Two-level list encoding
    //
    // required/optional group LIST {
    //   repeated TYPE;
    // }
    const auto& primitive_node = static_cast<const PrimitiveNode&>(list_node);
    int column_index = ctx->schema->GetColumnIndex(primitive_node);
    ASSIGN_OR_RAISE(std::shared_ptr<ArrowType> type,
                    GetTypeForNode(column_index, primitive_node, ctx));
    auto item_field = ::arrow::field(list_node.name(), type, /*nullable=*/false,
                                     FieldIdMetadata(list_node.field_id()));
    RETURN_NOT_OK(PopulateLeaf(column_index, item_field, current_def_level,
                               current_rep_level, ctx, out, child_field));
  }
  out->field = ::arrow::field(group.name(), ::arrow::list(child_field->field),
                              group.is_optional(), FieldIdMetadata(group.field_id()));
  out->definition_level = current_def_level;
  out->repetition_level = current_rep_level;
  return Status::OK();
}

Status GroupToSchemaField(const GroupNode& node, int16_t current_def_level,
                          int16_t current_rep_level, SchemaTreeContext* ctx,
                          const SchemaField* parent, SchemaField* out) {
  if (node.logical_type()->is_list()) {
    return ListToSchemaField(node, current_def_level, current_rep_level, ctx, parent,
                             out);
  }
  std::shared_ptr<ArrowType> type;
  if (node.is_repeated()) {
    // Simple repeated struct
    //
    // repeated group $NAME {
    //   r/o TYPE[0] f0
    //   r/o TYPE[1] f1
    // }
    out->children.resize(1);
    RETURN_NOT_OK(GroupToStruct(node, current_def_level, current_rep_level, ctx, out,
                                &out->children[0]));
    out->field = ::arrow::field(node.name(), ::arrow::list(out->children[0].field),
                                node.is_optional(), FieldIdMetadata(node.field_id()));
    out->definition_level = current_def_level;
    out->repetition_level = current_rep_level;
    return Status::OK();
  } else {
    return GroupToStruct(node, current_def_level, current_rep_level, ctx, parent, out);
  }
}

Status NodeToSchemaField(const Node& node, int16_t current_def_level,
                         int16_t current_rep_level, SchemaTreeContext* ctx,
                         const SchemaField* parent, SchemaField* out) {
  /// Workhorse function for converting a Parquet schema node to an Arrow
  /// type. Handles different conventions for nested data
  if (node.is_optional()) {
    ++current_def_level;
  } else if (node.is_repeated()) {
    // Repeated fields add both a repetition and definition level. This is used
    // to distinguish between an empty list and a list with an item in it.
    ++current_rep_level;
    ++current_def_level;
  }

  ctx->LinkParent(out, parent);

  // Now, walk the schema and create a ColumnDescriptor for each leaf node
  if (node.is_group()) {
    // A nested field, but we don't know what kind yet
    return GroupToSchemaField(static_cast<const GroupNode&>(node), current_def_level,
                              current_rep_level, ctx, parent, out);
  } else {
    // Either a normal flat primitive type, or a list type encoded with 1-level
    // list encoding. Note that the 3-level encoding is the form recommended by
    // the parquet specification, but technically we can have either
    //
    // required/optional $TYPE $FIELD_NAME
    //
    // or
    //
    // repeated $TYPE $FIELD_NAME
    const auto& primitive_node = static_cast<const PrimitiveNode&>(node);
    int column_index = ctx->schema->GetColumnIndex(primitive_node);
    ASSIGN_OR_RAISE(std::shared_ptr<ArrowType> type,
                    GetTypeForNode(column_index, primitive_node, ctx));
    if (node.is_repeated()) {
      // One-level list encoding, e.g.
      // a: repeated int32;
      out->children.resize(1);
      auto child_field = ::arrow::field(node.name(), type, /*nullable=*/false);
      RETURN_NOT_OK(PopulateLeaf(column_index, child_field, current_def_level,
                                 current_rep_level, ctx, out, &out->children[0]));

      out->field = ::arrow::field(node.name(), ::arrow::list(child_field),
                                  /*nullable=*/false, FieldIdMetadata(node.field_id()));
      // Is this right?
      out->definition_level = current_def_level;
      out->repetition_level = current_rep_level;
      return Status::OK();
    } else {
      // A normal (required/optional) primitive node
      return PopulateLeaf(column_index,
                          ::arrow::field(node.name(), type, node.is_optional(),
                                         FieldIdMetadata(node.field_id())),
                          current_def_level, current_rep_level, ctx, parent, out);
    }
  }
}

// Get the original Arrow schema, as serialized in the Parquet metadata
Status GetOriginSchema(const std::shared_ptr<const KeyValueMetadata>& metadata,
                       std::shared_ptr<const KeyValueMetadata>* clean_metadata,
                       std::shared_ptr<::arrow::Schema>* out) {
  if (metadata == nullptr) {
    *out = nullptr;
    *clean_metadata = nullptr;
    return Status::OK();
  }

  static const std::string kArrowSchemaKey = "ARROW:schema";
  int schema_index = metadata->FindKey(kArrowSchemaKey);
  if (schema_index == -1) {
    *out = nullptr;
    *clean_metadata = metadata;
    return Status::OK();
  }

  // The original Arrow schema was serialized using the store_schema option.
  // We deserialize it here and use it to inform read options such as
  // dictionary-encoded fields.
  auto decoded = ::arrow::util::base64_decode(metadata->value(schema_index));
  auto schema_buf = std::make_shared<Buffer>(decoded);

  ::arrow::ipc::DictionaryMemo dict_memo;
  ::arrow::io::BufferReader input(schema_buf);

  ARROW_ASSIGN_OR_RAISE(*out, ::arrow::ipc::ReadSchema(&input, &dict_memo));

  if (metadata->size() > 1) {
    // Copy the metadata without the schema key
    auto new_metadata = ::arrow::key_value_metadata({}, {});
    new_metadata->reserve(metadata->size() - 1);
    for (int64_t i = 0; i < metadata->size(); ++i) {
      if (i == schema_index) continue;
      new_metadata->Append(metadata->key(i), metadata->value(i));
    }
    *clean_metadata = new_metadata;
  } else {
    // No other keys, let metadata be null
    *clean_metadata = nullptr;
  }
  return Status::OK();
}

// Restore original Arrow field information that was serialized as Parquet metadata
// but that is not necessarily present in the field reconstitued from Parquet data
// (for example, Parquet timestamp types doesn't carry timezone information).
Status ApplyOriginalMetadata(std::shared_ptr<Field> field, const Field& origin_field,
                             std::shared_ptr<Field>* out) {
  auto origin_type = origin_field.type();
  if (field->type()->id() == ::arrow::Type::TIMESTAMP) {
    // Restore time zone, if any
    const auto& ts_type = static_cast<const ::arrow::TimestampType&>(*field->type());
    const auto& ts_origin_type = static_cast<const ::arrow::TimestampType&>(*origin_type);

    // If the unit is the same and the data is tz-aware, then set the original
    // time zone, since Parquet has no native storage for timezones
    if (ts_type.unit() == ts_origin_type.unit() && ts_type.timezone() == "UTC" &&
        ts_origin_type.timezone() != "") {
      field = field->WithType(origin_type);
    }
  }
  if (origin_type->id() == ::arrow::Type::DICTIONARY &&
      field->type()->id() != ::arrow::Type::DICTIONARY &&
      IsDictionaryReadSupported(*field->type())) {
    const auto& dict_origin_type =
        static_cast<const ::arrow::DictionaryType&>(*origin_type);
    field = field->WithType(
        ::arrow::dictionary(::arrow::int32(), field->type(), dict_origin_type.ordered()));
  }

  if (origin_type->id() == ::arrow::Type::EXTENSION) {
    // Restore extension type, if the storage type is as read from Parquet
    const auto& ex_type = checked_cast<const ::arrow::ExtensionType&>(*origin_type);
    if (ex_type.storage_type()->Equals(*field->type())) {
      field = field->WithType(origin_type);
    }
  }

  // Restore field metadata
  std::shared_ptr<const KeyValueMetadata> field_metadata = origin_field.metadata();
  if (field_metadata != nullptr) {
    if (field->metadata()) {
      // Prefer the metadata keys (like field_id) from the current metadata
      field_metadata = field_metadata->Merge(*field->metadata());
    }
    field = field->WithMetadata(field_metadata);
  }
  *out = field;
  return Status::OK();
}

}  // namespace

Status FieldToNode(const std::shared_ptr<Field>& field,
                   const WriterProperties& properties,
                   const ArrowWriterProperties& arrow_properties, NodePtr* out) {
  return FieldToNode(field->name(), field, properties, arrow_properties, out);
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

Status FromParquetSchema(
    const SchemaDescriptor* schema, const ArrowReaderProperties& properties,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata,
    std::shared_ptr<::arrow::Schema>* out) {
  SchemaManifest manifest;
  RETURN_NOT_OK(SchemaManifest::Make(schema, key_value_metadata, properties, &manifest));
  std::vector<std::shared_ptr<Field>> fields(manifest.schema_fields.size());

  for (int i = 0; i < static_cast<int>(fields.size()); i++) {
    const auto& schema_field = manifest.schema_fields[i];
    fields[i] = schema_field.field;
  }
  if (manifest.origin_schema) {
    // ARROW-8980: If the ARROW:schema was in the input metadata, then
    // manifest.origin_schema will have it scrubbed out
    *out = ::arrow::schema(fields, manifest.origin_schema->metadata());
  } else {
    *out = ::arrow::schema(fields, key_value_metadata);
  }
  return Status::OK();
}

Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
                         const ArrowReaderProperties& properties,
                         std::shared_ptr<::arrow::Schema>* out) {
  return FromParquetSchema(parquet_schema, properties, nullptr, out);
}

Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
                         std::shared_ptr<::arrow::Schema>* out) {
  ArrowReaderProperties properties;
  return FromParquetSchema(parquet_schema, properties, nullptr, out);
}

Status SchemaManifest::Make(const SchemaDescriptor* schema,
                            const std::shared_ptr<const KeyValueMetadata>& metadata,
                            const ArrowReaderProperties& properties,
                            SchemaManifest* manifest) {
  RETURN_NOT_OK(
      GetOriginSchema(metadata, &manifest->schema_metadata, &manifest->origin_schema));

  SchemaTreeContext ctx;
  ctx.manifest = manifest;
  ctx.properties = properties;
  ctx.schema = schema;
  const GroupNode& schema_node = *schema->group_node();
  manifest->descr = schema;
  manifest->schema_fields.resize(schema_node.field_count());
  for (int i = 0; i < static_cast<int>(schema_node.field_count()); ++i) {
    SchemaField* out_field = &manifest->schema_fields[i];
    RETURN_NOT_OK(NodeToSchemaField(*schema_node.field(i), 0, 0, &ctx,
                                    /*parent=*/nullptr, out_field));

    // TODO(wesm): as follow up to ARROW-3246, we should really pass the origin
    // schema (if any) through all functions in the schema reconstruction, but
    // I'm being lazy and just setting dictionary fields at the top level for
    // now
    if (manifest->origin_schema == nullptr) {
      continue;
    }
    auto origin_field = manifest->origin_schema->field(i);
    RETURN_NOT_OK(
        ApplyOriginalMetadata(out_field->field, *origin_field, &out_field->field));
  }
  return Status::OK();
}

}  // namespace arrow
}  // namespace parquet
