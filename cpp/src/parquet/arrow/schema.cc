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

#include <functional>
#include <string>
#include <vector>

#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/api.h"
#include "arrow/result_internal.h"
#include "arrow/type.h"
#include "arrow/util/base64.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"

#include "parquet/arrow/schema_internal.h"
#include "parquet/exception.h"
#include "parquet/properties.h"
#include "parquet/types.h"

using arrow::DecimalType;
using arrow::Field;
using arrow::FieldVector;
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

using parquet::internal::LevelInfo;

namespace parquet {

namespace arrow {

// ----------------------------------------------------------------------
// Parquet to Arrow schema conversion

namespace {

Repetition::type RepetitionFromNullable(bool is_nullable) {
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
  *out = GroupNode::Make(name, RepetitionFromNullable(nullable), {list},
                         LogicalType::List());
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
  *out = GroupNode::Make(name, RepetitionFromNullable(nullable), {key_value},
                         LogicalType::Map());
  return Status::OK();
}

Status StructToNode(const std::shared_ptr<::arrow::StructType>& type,
                    const std::string& name, bool nullable,
                    const WriterProperties& properties,
                    const ArrowWriterProperties& arrow_properties, NodePtr* out) {
  std::vector<NodePtr> children(type->num_fields());
  if (type->num_fields() != 0) {
    for (int i = 0; i < type->num_fields(); i++) {
      RETURN_NOT_OK(FieldToNode(type->field(i)->name(), type->field(i), properties,
                                arrow_properties, &children[i]));
    }
  } else {
    // XXX (ARROW-10928) We could add a dummy primitive node but that would
    // require special handling when writing and reading, to avoid column index
    // mismatches.
    return Status::NotImplemented("Cannot write struct type '", name,
                                  "' with no child field to Parquet. "
                                  "Consider adding a dummy child field.");
  }

  *out = GroupNode::Make(name, RepetitionFromNullable(nullable), std::move(children));
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

static constexpr char FIELD_ID_KEY[] = "PARQUET:field_id";

std::shared_ptr<::arrow::KeyValueMetadata> FieldIdMetadata(int field_id) {
  if (field_id >= 0) {
    return ::arrow::key_value_metadata({FIELD_ID_KEY}, {std::to_string(field_id)});
  } else {
    return nullptr;
  }
}

int FieldIdFromMetadata(
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata) {
  if (!metadata) {
    return -1;
  }
  int key = metadata->FindKey(FIELD_ID_KEY);
  if (key < 0) {
    return -1;
  }
  std::string field_id_str = metadata->value(key);
  int field_id;
  if (::arrow::internal::ParseValue<::arrow::Int32Type>(
          field_id_str.c_str(), field_id_str.length(), &field_id)) {
    if (field_id < 0) {
      // Thrift should convert any negative value to null but normalize to -1 here in case
      // we later check this in logic.
      return -1;
    }
    return field_id;
  } else {
    return -1;
  }
}

Status FieldToNode(const std::string& name, const std::shared_ptr<Field>& field,
                   const WriterProperties& properties,
                   const ArrowWriterProperties& arrow_properties, NodePtr* out) {
  std::shared_ptr<const LogicalType> logical_type = LogicalType::None();
  ParquetType::type type;
  Repetition::type repetition = RepetitionFromNullable(field->nullable());

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
    case ArrowTypeId::DECIMAL128:
    case ArrowTypeId::DECIMAL256: {
      type = ParquetType::FIXED_LEN_BYTE_ARRAY;
      const auto& decimal_type = static_cast<const ::arrow::DecimalType&>(*field->type());
      precision = decimal_type.precision();
      scale = decimal_type.scale();
      length = DecimalType::DecimalSize(precision);
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

  int field_id = FieldIdFromMetadata(field->metadata());
  PARQUET_CATCH_NOT_OK(*out = PrimitiveNode::Make(name, repetition, logical_type, type,
                                                  length, field_id));

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
  ASSIGN_OR_RAISE(
      std::shared_ptr<ArrowType> storage_type,
      GetArrowType(primitive_node, ctx->properties.coerce_int96_timestamp_unit()));
  if (ctx->properties.read_dictionary(column_index) &&
      IsDictionaryReadSupported(*storage_type)) {
    return ::arrow::dictionary(::arrow::int32(), storage_type);
  }
  return storage_type;
}

Status NodeToSchemaField(const Node& node, LevelInfo current_levels,
                         SchemaTreeContext* ctx, const SchemaField* parent,
                         SchemaField* out);

Status GroupToSchemaField(const GroupNode& node, LevelInfo current_levels,
                          SchemaTreeContext* ctx, const SchemaField* parent,
                          SchemaField* out);

Status PopulateLeaf(int column_index, const std::shared_ptr<Field>& field,
                    LevelInfo current_levels, SchemaTreeContext* ctx,
                    const SchemaField* parent, SchemaField* out) {
  out->field = field;
  out->column_index = column_index;
  out->level_info = current_levels;
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

Status GroupToStruct(const GroupNode& node, LevelInfo current_levels,
                     SchemaTreeContext* ctx, const SchemaField* parent,
                     SchemaField* out) {
  std::vector<std::shared_ptr<Field>> arrow_fields;
  out->children.resize(node.field_count());
  // All level increments for the node are expected to happen by callers.
  // This is required because repeated elements need to have there own
  // SchemaField.

  for (int i = 0; i < node.field_count(); i++) {
    RETURN_NOT_OK(
        NodeToSchemaField(*node.field(i), current_levels, ctx, out, &out->children[i]));
    arrow_fields.push_back(out->children[i].field);
  }
  auto struct_type = ::arrow::struct_(arrow_fields);
  out->field = ::arrow::field(node.name(), struct_type, node.is_optional(),
                              FieldIdMetadata(node.field_id()));
  out->level_info = current_levels;
  return Status::OK();
}

Status ListToSchemaField(const GroupNode& group, LevelInfo current_levels,
                         SchemaTreeContext* ctx, const SchemaField* parent,
                         SchemaField* out);

Status MapToSchemaField(const GroupNode& group, LevelInfo current_levels,
                        SchemaTreeContext* ctx, const SchemaField* parent,
                        SchemaField* out) {
  if (group.field_count() != 1) {
    return Status::Invalid("MAP-annotated groups must have a single child.");
  }
  if (group.is_repeated()) {
    return Status::Invalid("MAP-annotated groups must not be repeated.");
  }

  const Node& key_value_node = *group.field(0);

  if (!key_value_node.is_repeated()) {
    return Status::Invalid(
        "Non-repeated key value in a MAP-annotated group are not supported.");
  }

  if (!key_value_node.is_group()) {
    return Status::Invalid("Key-value node must be a group.");
  }

  const GroupNode& key_value = checked_cast<const GroupNode&>(key_value_node);
  if (key_value.field_count() != 1 && key_value.field_count() != 2) {
    return Status::Invalid("Key-value map node must have 1 or 2 child elements. Found: ",
                           key_value.field_count());
  }
  const Node& key_node = *key_value.field(0);
  if (!key_node.is_required()) {
    return Status::Invalid("Map keys must be annotated as required.");
  }
  // Arrow doesn't support 1 column maps (i.e. Sets).  The options are to either
  // make the values column nullable, or process the map as a list.  We choose the latter
  // as it is simpler.
  if (key_value.field_count() == 1) {
    return ListToSchemaField(group, current_levels, ctx, parent, out);
  }

  current_levels.Increment(group);
  int16_t repeated_ancestor_def_level = current_levels.IncrementRepeated();

  out->children.resize(1);
  SchemaField* key_value_field = &out->children[0];

  key_value_field->children.resize(2);
  SchemaField* key_field = &key_value_field->children[0];
  SchemaField* value_field = &key_value_field->children[1];

  ctx->LinkParent(out, parent);
  ctx->LinkParent(key_value_field, out);
  ctx->LinkParent(key_field, key_value_field);
  ctx->LinkParent(value_field, key_value_field);

  // required/optional group name=whatever {
  //   repeated group name=key_values{
  //     required TYPE key;
  // required/optional TYPE value;
  //   }
  // }
  //

  RETURN_NOT_OK(NodeToSchemaField(*key_value.field(0), current_levels, ctx,
                                  key_value_field, key_field));
  RETURN_NOT_OK(NodeToSchemaField(*key_value.field(1), current_levels, ctx,
                                  key_value_field, value_field));

  key_value_field->field = ::arrow::field(
      group.name(), ::arrow::struct_({key_field->field, value_field->field}),
      /*nullable=*/false, FieldIdMetadata(key_value.field_id()));
  key_value_field->level_info = current_levels;

  out->field = ::arrow::field(group.name(),
                              ::arrow::map(key_field->field->type(), value_field->field),
                              group.is_optional(), FieldIdMetadata(group.field_id()));
  out->level_info = current_levels;
  // At this point current levels contains the def level for this list,
  // we need to reset to the prior parent.
  out->level_info.repeated_ancestor_def_level = repeated_ancestor_def_level;
  return Status::OK();
}

Status ListToSchemaField(const GroupNode& group, LevelInfo current_levels,
                         SchemaTreeContext* ctx, const SchemaField* parent,
                         SchemaField* out) {
  if (group.field_count() != 1) {
    return Status::Invalid("LIST-annotated groups must have a single child.");
  }
  if (group.is_repeated()) {
    return Status::Invalid("LIST-annotated groups must not be repeated.");
  }
  current_levels.Increment(group);

  out->children.resize(group.field_count());
  SchemaField* child_field = &out->children[0];

  ctx->LinkParent(out, parent);
  ctx->LinkParent(child_field, out);

  const Node& list_node = *group.field(0);

  if (!list_node.is_repeated()) {
    return Status::Invalid(
        "Non-repeated nodes in a LIST-annotated group are not supported.");
  }

  int16_t repeated_ancestor_def_level = current_levels.IncrementRepeated();
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
    // We distinguish the special case that we have
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
      RETURN_NOT_OK(
          NodeToSchemaField(*list_group.field(0), current_levels, ctx, out, child_field));
    } else {
      RETURN_NOT_OK(GroupToStruct(list_group, current_levels, ctx, out, child_field));
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
    RETURN_NOT_OK(
        PopulateLeaf(column_index, item_field, current_levels, ctx, out, child_field));
  }
  out->field = ::arrow::field(group.name(), ::arrow::list(child_field->field),
                              group.is_optional(), FieldIdMetadata(group.field_id()));
  out->level_info = current_levels;
  // At this point current levels contains the def level for this list,
  // we need to reset to the prior parent.
  out->level_info.repeated_ancestor_def_level = repeated_ancestor_def_level;
  return Status::OK();
}

Status GroupToSchemaField(const GroupNode& node, LevelInfo current_levels,
                          SchemaTreeContext* ctx, const SchemaField* parent,
                          SchemaField* out) {
  if (node.logical_type()->is_list()) {
    return ListToSchemaField(node, current_levels, ctx, parent, out);
  } else if (node.logical_type()->is_map()) {
    return MapToSchemaField(node, current_levels, ctx, parent, out);
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

    int16_t repeated_ancestor_def_level = current_levels.IncrementRepeated();
    RETURN_NOT_OK(GroupToStruct(node, current_levels, ctx, out, &out->children[0]));
    out->field = ::arrow::field(node.name(), ::arrow::list(out->children[0].field),
                                /*nullable=*/false, FieldIdMetadata(node.field_id()));

    ctx->LinkParent(&out->children[0], out);
    out->level_info = current_levels;
    // At this point current_levels contains this list as the def level, we need to
    // use the previous ancenstor of thi slist.
    out->level_info.repeated_ancestor_def_level = repeated_ancestor_def_level;
    return Status::OK();
  } else {
    current_levels.Increment(node);
    return GroupToStruct(node, current_levels, ctx, parent, out);
  }
}

Status NodeToSchemaField(const Node& node, LevelInfo current_levels,
                         SchemaTreeContext* ctx, const SchemaField* parent,
                         SchemaField* out) {
  // Workhorse function for converting a Parquet schema node to an Arrow
  // type. Handles different conventions for nested data.

  ctx->LinkParent(out, parent);

  // Now, walk the schema and create a ColumnDescriptor for each leaf node
  if (node.is_group()) {
    // A nested field, but we don't know what kind yet
    return GroupToSchemaField(static_cast<const GroupNode&>(node), current_levels, ctx,
                              parent, out);
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
      int16_t repeated_ancestor_def_level = current_levels.IncrementRepeated();
      out->children.resize(1);
      auto child_field = ::arrow::field(node.name(), type, /*nullable=*/false);
      RETURN_NOT_OK(PopulateLeaf(column_index, child_field, current_levels, ctx, out,
                                 &out->children[0]));

      out->field = ::arrow::field(node.name(), ::arrow::list(child_field),
                                  /*nullable=*/false, FieldIdMetadata(node.field_id()));
      out->level_info = current_levels;
      // At this point current_levels has consider this list the ancestor so restore
      // the actual ancenstor.
      out->level_info.repeated_ancestor_def_level = repeated_ancestor_def_level;
      return Status::OK();
    } else {
      current_levels.Increment(node);
      // A normal (required/optional) primitive node
      return PopulateLeaf(column_index,
                          ::arrow::field(node.name(), type, node.is_optional(),
                                         FieldIdMetadata(node.field_id())),
                          current_levels, ctx, parent, out);
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

Result<bool> ApplyOriginalMetadata(const Field& origin_field, SchemaField* inferred);

std::function<std::shared_ptr<::arrow::DataType>(FieldVector)> GetNestedFactory(
    const ArrowType& origin_type, const ArrowType& inferred_type) {
  switch (inferred_type.id()) {
    case ::arrow::Type::STRUCT:
      if (origin_type.id() == ::arrow::Type::STRUCT) {
        return ::arrow::struct_;
      }
      break;
    case ::arrow::Type::LIST:
      if (origin_type.id() == ::arrow::Type::LIST) {
        return [](FieldVector fields) {
          DCHECK_EQ(fields.size(), 1);
          return ::arrow::list(std::move(fields[0]));
        };
      }
      if (origin_type.id() == ::arrow::Type::LARGE_LIST) {
        return [](FieldVector fields) {
          DCHECK_EQ(fields.size(), 1);
          return ::arrow::large_list(std::move(fields[0]));
        };
      }
      if (origin_type.id() == ::arrow::Type::FIXED_SIZE_LIST) {
        const auto list_size =
            checked_cast<const ::arrow::FixedSizeListType&>(origin_type).list_size();
        return [list_size](FieldVector fields) {
          DCHECK_EQ(fields.size(), 1);
          return ::arrow::fixed_size_list(std::move(fields[0]), list_size);
        };
      }
      break;
    default:
      break;
  }
  return {};
}

Result<bool> ApplyOriginalStorageMetadata(const Field& origin_field,
                                          SchemaField* inferred) {
  bool modified = false;

  auto origin_type = origin_field.type();
  auto inferred_type = inferred->field->type();

  const int num_children = inferred_type->num_fields();

  if (num_children > 0 && origin_type->num_fields() == num_children) {
    DCHECK_EQ(static_cast<int>(inferred->children.size()), num_children);
    const auto factory = GetNestedFactory(*origin_type, *inferred_type);
    if (factory) {
      // The type may be modified (e.g. LargeList) while the children stay the same
      modified |= origin_type->id() != inferred_type->id();

      // Apply original metadata recursively to children
      for (int i = 0; i < inferred_type->num_fields(); ++i) {
        ARROW_ASSIGN_OR_RAISE(
            const bool child_modified,
            ApplyOriginalMetadata(*origin_type->field(i), &inferred->children[i]));
        modified |= child_modified;
      }
      if (modified) {
        // Recreate this field using the modified child fields
        ::arrow::FieldVector modified_children(inferred_type->num_fields());
        for (int i = 0; i < inferred_type->num_fields(); ++i) {
          modified_children[i] = inferred->children[i].field;
        }
        inferred->field =
            inferred->field->WithType(factory(std::move(modified_children)));
      }
    }
  }

  if (origin_type->id() == ::arrow::Type::TIMESTAMP &&
      inferred_type->id() == ::arrow::Type::TIMESTAMP) {
    // Restore time zone, if any
    const auto& ts_type = checked_cast<const ::arrow::TimestampType&>(*inferred_type);
    const auto& ts_origin_type =
        checked_cast<const ::arrow::TimestampType&>(*origin_type);

    // If the data is tz-aware, then set the original time zone, since Parquet
    // has no native storage for timezones
    if (ts_type.timezone() == "UTC" && ts_origin_type.timezone() != "") {
      if (ts_type.unit() == ts_origin_type.unit()) {
        inferred->field = inferred->field->WithType(origin_type);
      } else {
        auto ts_type_new = ::arrow::timestamp(ts_type.unit(), ts_origin_type.timezone());
        inferred->field = inferred->field->WithType(ts_type_new);
      }
    }
    modified = true;
  }

  if (origin_type->id() == ::arrow::Type::DICTIONARY &&
      inferred_type->id() != ::arrow::Type::DICTIONARY &&
      IsDictionaryReadSupported(*inferred_type)) {
    // Direct dictionary reads are only suppored for a couple primitive types,
    // so no need to recurse on value types.
    const auto& dict_origin_type =
        checked_cast<const ::arrow::DictionaryType&>(*origin_type);
    inferred->field = inferred->field->WithType(
        ::arrow::dictionary(::arrow::int32(), inferred_type, dict_origin_type.ordered()));
    modified = true;
  }

  if ((origin_type->id() == ::arrow::Type::LARGE_BINARY &&
       inferred_type->id() == ::arrow::Type::BINARY) ||
      (origin_type->id() == ::arrow::Type::LARGE_STRING &&
       inferred_type->id() == ::arrow::Type::STRING)) {
    // Read back binary-like arrays with the intended offset width.
    inferred->field = inferred->field->WithType(origin_type);
    modified = true;
  }

  if (origin_type->id() == ::arrow::Type::DECIMAL256 &&
      inferred_type->id() == ::arrow::Type::DECIMAL128) {
    inferred->field = inferred->field->WithType(origin_type);
    modified = true;
  }

  // Restore field metadata
  std::shared_ptr<const KeyValueMetadata> field_metadata = origin_field.metadata();
  if (field_metadata != nullptr) {
    if (inferred->field->metadata()) {
      // Prefer the metadata keys (like field_id) from the current metadata
      field_metadata = field_metadata->Merge(*inferred->field->metadata());
    }
    inferred->field = inferred->field->WithMetadata(field_metadata);
    modified = true;
  }

  return modified;
}

Result<bool> ApplyOriginalMetadata(const Field& origin_field, SchemaField* inferred) {
  bool modified = false;

  auto origin_type = origin_field.type();
  auto inferred_type = inferred->field->type();

  if (origin_type->id() == ::arrow::Type::EXTENSION) {
    const auto& ex_type = checked_cast<const ::arrow::ExtensionType&>(*origin_type);
    auto origin_storage_field = origin_field.WithType(ex_type.storage_type());

    // Apply metadata recursively to storage type
    RETURN_NOT_OK(ApplyOriginalStorageMetadata(*origin_storage_field, inferred));

    // Restore extension type, if the storage type is the same as inferred
    // from the Parquet type
    if (ex_type.storage_type()->Equals(*inferred->field->type())) {
      inferred->field = inferred->field->WithType(origin_type);
    }
    modified = true;
  } else {
    ARROW_ASSIGN_OR_RAISE(modified, ApplyOriginalStorageMetadata(origin_field, inferred));
  }

  return modified;
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
  SchemaTreeContext ctx;
  ctx.manifest = manifest;
  ctx.properties = properties;
  ctx.schema = schema;
  const GroupNode& schema_node = *schema->group_node();
  manifest->descr = schema;
  manifest->schema_fields.resize(schema_node.field_count());

  // Try to deserialize original Arrow schema
  RETURN_NOT_OK(
      GetOriginSchema(metadata, &manifest->schema_metadata, &manifest->origin_schema));
  // Ignore original schema if it's not compatible with the Parquet schema
  if (manifest->origin_schema != nullptr &&
      manifest->origin_schema->num_fields() != schema_node.field_count()) {
    manifest->origin_schema = nullptr;
  }

  for (int i = 0; i < static_cast<int>(schema_node.field_count()); ++i) {
    SchemaField* out_field = &manifest->schema_fields[i];
    RETURN_NOT_OK(NodeToSchemaField(*schema_node.field(i), LevelInfo(), &ctx,
                                    /*parent=*/nullptr, out_field));

    // TODO(wesm): as follow up to ARROW-3246, we should really pass the origin
    // schema (if any) through all functions in the schema reconstruction, but
    // I'm being lazy and just setting dictionary fields at the top level for
    // now
    if (manifest->origin_schema == nullptr) {
      continue;
    }

    auto origin_field = manifest->origin_schema->field(i);
    RETURN_NOT_OK(ApplyOriginalMetadata(*origin_field, out_field));
  }
  return Status::OK();
}

}  // namespace arrow
}  // namespace parquet
