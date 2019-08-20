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

#include "parquet/arrow/reader_internal.h"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/compute/kernel.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

#include "parquet/arrow/reader.h"
#include "parquet/column_reader.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

using arrow::Array;
using arrow::BooleanArray;
using arrow::ChunkedArray;
using arrow::DataType;
using arrow::Field;
using arrow::Int32Array;
using arrow::ListArray;
using arrow::MemoryPool;
using arrow::ResizableBuffer;
using arrow::Status;
using arrow::StructArray;
using arrow::Table;
using arrow::TimestampArray;
using arrow::compute::Datum;

using ::arrow::BitUtil::FromBigEndian;
using ::arrow::internal::checked_cast;
using ::arrow::internal::SafeLeftShift;
using ::arrow::util::SafeLoadAs;

using parquet::internal::RecordReader;
using parquet::schema::GroupNode;
using parquet::schema::Node;
using parquet::schema::PrimitiveNode;
using ParquetType = parquet::Type;

namespace parquet {
namespace arrow {

template <typename ArrowType>
using ArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

// ----------------------------------------------------------------------
// Schema logic

static Status MakeArrowDecimal(const LogicalType& logical_type,
                               std::shared_ptr<DataType>* out) {
  const auto& decimal = checked_cast<const DecimalLogicalType&>(logical_type);
  *out = ::arrow::decimal(decimal.precision(), decimal.scale());
  return Status::OK();
}

static Status MakeArrowInt(const LogicalType& logical_type,
                           std::shared_ptr<DataType>* out) {
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
                             std::shared_ptr<DataType>* out) {
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
                              std::shared_ptr<DataType>* out) {
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
                              std::shared_ptr<DataType>* out) {
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
                                 std::shared_ptr<DataType>* out) {
  const auto& timestamp = checked_cast<const TimestampLogicalType&>(logical_type);
  const bool utc_normalized =
      timestamp.is_from_converted_type() ? false : timestamp.is_adjusted_to_utc();
  static const char* utc_timezone = "UTC";
  switch (timestamp.time_unit()) {
    case LogicalType::TimeUnit::MILLIS:
      *out = (utc_normalized ? ::arrow::timestamp(::arrow::TimeUnit::MILLI, utc_timezone)
                             : ::arrow::timestamp(::arrow::TimeUnit::MILLI));
      break;
    case LogicalType::TimeUnit::MICROS:
      *out = (utc_normalized ? ::arrow::timestamp(::arrow::TimeUnit::MICRO, utc_timezone)
                             : ::arrow::timestamp(::arrow::TimeUnit::MICRO));
      break;
    case LogicalType::TimeUnit::NANOS:
      *out = (utc_normalized ? ::arrow::timestamp(::arrow::TimeUnit::NANO, utc_timezone)
                             : ::arrow::timestamp(::arrow::TimeUnit::NANO));
      break;
    default:
      return Status::TypeError("Unrecognized time unit in timestamp logical_type: ",
                               logical_type.ToString());
  }
  return Status::OK();
}

static Status FromByteArray(const LogicalType& logical_type,
                            std::shared_ptr<DataType>* out) {
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
                       std::shared_ptr<DataType>* out) {
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

static Status FromInt32(const LogicalType& logical_type, std::shared_ptr<DataType>* out) {
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

static Status FromInt64(const LogicalType& logical_type, std::shared_ptr<DataType>* out) {
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

Status GetPrimitiveType(const schema::PrimitiveNode& primitive,
                        std::shared_ptr<DataType>* out) {
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
      *out = ::arrow::timestamp(::arrow::TimeUnit::NANO);
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

bool IsDictionaryReadSupported(const DataType& type) {
  // Only supported currently for BYTE_ARRAY types
  return type.id() == ::arrow::Type::BINARY || type.id() == ::arrow::Type::STRING;
}

Status GetTypeForNode(int column_index, const schema::PrimitiveNode& primitive_node,
                      SchemaTreeContext* ctx, std::shared_ptr<DataType>* out) {
  std::shared_ptr<DataType> storage_type;
  RETURN_NOT_OK(GetPrimitiveType(primitive_node, &storage_type));
  if (ctx->properties.read_dictionary(column_index) &&
      IsDictionaryReadSupported(*storage_type)) {
    *out = ::arrow::dictionary(::arrow::int32(), storage_type);
  } else {
    *out = storage_type;
  }
  return Status::OK();
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
  out->max_definition_level = max_def_level;
  out->max_repetition_level = max_rep_level;
  ctx->RecordLeaf(out);
  ctx->LinkParent(out, parent);
  return Status::OK();
}

// Special case mentioned in the format spec:
//   If the name is array or ends in _tuple, this should be a list of struct
//   even for single child elements.
bool HasStructListName(const GroupNode& node) {
  return node.name() == "array" || boost::algorithm::ends_with(node.name(), "_tuple");
}

Status GroupToStruct(const GroupNode& node, int16_t max_def_level, int16_t max_rep_level,
                     SchemaTreeContext* ctx, const SchemaField* parent,
                     SchemaField* out) {
  std::vector<std::shared_ptr<Field>> arrow_fields;
  out->children.resize(node.field_count());
  for (int i = 0; i < node.field_count(); i++) {
    RETURN_NOT_OK(NodeToSchemaField(*node.field(i), max_def_level, max_rep_level, ctx,
                                    out, &out->children[i]));
    arrow_fields.push_back(out->children[i].field);
  }
  auto struct_type = ::arrow::struct_(arrow_fields);
  out->field = ::arrow::field(node.name(), struct_type, node.is_optional());
  out->max_definition_level = max_def_level;
  out->max_repetition_level = max_rep_level;
  return Status::OK();
}

Status ListToSchemaField(const GroupNode& group, int16_t max_def_level,
                         int16_t max_rep_level, SchemaTreeContext* ctx,
                         const SchemaField* parent, SchemaField* out) {
  if (group.field_count() != 1) {
    return Status::NotImplemented(
        "Only LIST-annotated groups with a single child can be handled.");
  }

  out->children.resize(1);
  SchemaField* child_field = &out->children[0];

  ctx->LinkParent(out, parent);
  ctx->LinkParent(child_field, out);

  const Node& list_node = *group.field(0);

  if (!list_node.is_repeated()) {
    return Status::NotImplemented(
        "Non-repeated nodes in a LIST-annotated group are not supported.");
  }

  ++max_def_level;
  ++max_rep_level;
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
      RETURN_NOT_OK(NodeToSchemaField(*list_group.field(0), max_def_level, max_rep_level,
                                      ctx, out, child_field));
    } else {
      RETURN_NOT_OK(
          GroupToStruct(list_group, max_def_level, max_rep_level, ctx, out, child_field));
    }
  } else {
    // Two-level list encoding
    //
    // required/optional group LIST {
    //   repeated TYPE;
    // }
    const auto& primitive_node = static_cast<const PrimitiveNode&>(list_node);
    int column_index = ctx->schema->GetColumnIndex(primitive_node);
    std::shared_ptr<DataType> type;
    RETURN_NOT_OK(GetTypeForNode(column_index, primitive_node, ctx, &type));
    auto item_field = ::arrow::field(list_node.name(), type, /*nullable=*/false);
    RETURN_NOT_OK(PopulateLeaf(column_index, item_field, max_def_level, max_rep_level,
                               ctx, out, child_field));
  }
  out->field = ::arrow::field(group.name(), ::arrow::list(child_field->field),
                              group.is_optional());
  out->max_definition_level = max_def_level;
  out->max_repetition_level = max_rep_level;
  return Status::OK();
}

Status GroupToSchemaField(const GroupNode& node, int16_t max_def_level,
                          int16_t max_rep_level, SchemaTreeContext* ctx,
                          const SchemaField* parent, SchemaField* out) {
  if (node.logical_type()->is_list()) {
    return ListToSchemaField(node, max_def_level, max_rep_level, ctx, parent, out);
  }
  std::shared_ptr<DataType> type;
  if (node.is_repeated()) {
    // Simple repeated struct
    //
    // repeated group $NAME {
    //   r/o TYPE[0] f0
    //   r/o TYPE[1] f1
    // }
    out->children.resize(1);
    RETURN_NOT_OK(
        GroupToStruct(node, max_def_level, max_rep_level, ctx, out, &out->children[0]));
    out->field = ::arrow::field(node.name(), ::arrow::list(out->children[0].field),
                                node.is_optional());
    out->max_definition_level = max_def_level;
    out->max_repetition_level = max_rep_level;
    return Status::OK();
  } else {
    return GroupToStruct(node, max_def_level, max_rep_level, ctx, parent, out);
  }
}

Status NodeToSchemaField(const Node& node, int16_t max_def_level, int16_t max_rep_level,
                         SchemaTreeContext* ctx, const SchemaField* parent,
                         SchemaField* out) {
  /// Workhorse function for converting a Parquet schema node to an Arrow
  /// type. Handles different conventions for nested data
  if (node.is_optional()) {
    ++max_def_level;
  } else if (node.is_repeated()) {
    // Repeated fields add both a repetition and definition level. This is used
    // to distinguish between an empty list and a list with an item in it.
    ++max_rep_level;
    ++max_def_level;
  }

  ctx->LinkParent(out, parent);

  // Now, walk the schema and create a ColumnDescriptor for each leaf node
  if (node.is_group()) {
    // A nested field, but we don't know what kind yet
    return GroupToSchemaField(static_cast<const GroupNode&>(node), max_def_level,
                              max_rep_level, ctx, parent, out);
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
    std::shared_ptr<DataType> type;
    RETURN_NOT_OK(GetTypeForNode(column_index, primitive_node, ctx, &type));
    if (node.is_repeated()) {
      // One-level list encoding, e.g.
      // a: repeated int32;
      out->children.resize(1);
      auto child_field = ::arrow::field(node.name(), type, /*nullable=*/false);
      RETURN_NOT_OK(PopulateLeaf(column_index, child_field, max_def_level, max_rep_level,
                                 ctx, out, &out->children[0]));

      out->field = ::arrow::field(node.name(), ::arrow::list(child_field),
                                  /*nullable=*/false);
      // Is this right?
      out->max_definition_level = max_def_level;
      out->max_repetition_level = max_rep_level;
      return Status::OK();
    } else {
      // A normal (required/optional) primitive node
      return PopulateLeaf(column_index,
                          ::arrow::field(node.name(), type, node.is_optional()),
                          max_def_level, max_rep_level, ctx, parent, out);
    }
  }
}

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

  // The original Arrow schema was serialized using the store_schema option. We
  // deserialize it here and use it to inform read options such as
  // dictionary-encoded fields
  auto schema_buf = std::make_shared<Buffer>(metadata->value(schema_index));

  ::arrow::ipc::DictionaryMemo dict_memo;
  ::arrow::io::BufferReader input(schema_buf);
  RETURN_NOT_OK(::arrow::ipc::ReadSchema(&input, &dict_memo, out));

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
    field = field->WithType(::arrow::dictionary(::arrow::int32(), field->type()));
  }
  *out = field;
  return Status::OK();
}

Status BuildSchemaManifest(const SchemaDescriptor* schema,
                           const std::shared_ptr<const KeyValueMetadata>& metadata,
                           const ArrowReaderProperties& properties,
                           SchemaManifest* manifest) {
  std::shared_ptr<::arrow::Schema> origin_schema;
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

Status FromParquetSchema(
    const SchemaDescriptor* schema, const ArrowReaderProperties& properties,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata,
    std::shared_ptr<::arrow::Schema>* out) {
  SchemaManifest manifest;
  RETURN_NOT_OK(BuildSchemaManifest(schema, key_value_metadata, properties, &manifest));
  std::vector<std::shared_ptr<Field>> fields(manifest.schema_fields.size());
  for (int i = 0; i < static_cast<int>(fields.size()); i++) {
    fields[i] = manifest.schema_fields[i].field;
  }
  *out = ::arrow::schema(fields, key_value_metadata);
  return Status::OK();
}

Status FromParquetSchema(const SchemaDescriptor* parquet_schema,
                         const ArrowReaderProperties& properties,
                         std::shared_ptr<::arrow::Schema>* out) {
  return FromParquetSchema(parquet_schema, properties, nullptr, out);
}

// ----------------------------------------------------------------------
// Primitive types

template <typename ArrowType, typename ParquetType>
Status TransferInt(RecordReader* reader, MemoryPool* pool,
                   const std::shared_ptr<DataType>& type, Datum* out) {
  using ArrowCType = typename ArrowType::c_type;
  using ParquetCType = typename ParquetType::c_type;
  int64_t length = reader->values_written();
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * sizeof(ArrowCType), &data));

  auto values = reinterpret_cast<const ParquetCType*>(reader->values());
  auto out_ptr = reinterpret_cast<ArrowCType*>(data->mutable_data());
  std::copy(values, values + length, out_ptr);
  *out = std::make_shared<ArrayType<ArrowType>>(
      type, length, data, reader->ReleaseIsValid(), reader->null_count());
  return Status::OK();
}

std::shared_ptr<Array> TransferZeroCopy(RecordReader* reader,
                                        const std::shared_ptr<DataType>& type) {
  std::vector<std::shared_ptr<Buffer>> buffers = {reader->ReleaseIsValid(),
                                                  reader->ReleaseValues()};
  auto data = std::make_shared<::arrow::ArrayData>(type, reader->values_written(),
                                                   buffers, reader->null_count());
  return ::arrow::MakeArray(data);
}

Status TransferBool(RecordReader* reader, MemoryPool* pool, Datum* out) {
  int64_t length = reader->values_written();
  std::shared_ptr<Buffer> data;

  const int64_t buffer_size = BitUtil::BytesForBits(length);
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, buffer_size, &data));

  // Transfer boolean values to packed bitmap
  auto values = reinterpret_cast<const bool*>(reader->values());
  uint8_t* data_ptr = data->mutable_data();
  memset(data_ptr, 0, buffer_size);

  for (int64_t i = 0; i < length; i++) {
    if (values[i]) {
      ::arrow::BitUtil::SetBit(data_ptr, i);
    }
  }

  *out = std::make_shared<BooleanArray>(length, data, reader->ReleaseIsValid(),
                                        reader->null_count());
  return Status::OK();
}

Status TransferInt96(RecordReader* reader, MemoryPool* pool,
                     const std::shared_ptr<DataType>& type, Datum* out) {
  int64_t length = reader->values_written();
  auto values = reinterpret_cast<const Int96*>(reader->values());
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * sizeof(int64_t), &data));
  auto data_ptr = reinterpret_cast<int64_t*>(data->mutable_data());
  for (int64_t i = 0; i < length; i++) {
    *data_ptr++ = Int96GetNanoSeconds(values[i]);
  }
  *out = std::make_shared<TimestampArray>(type, length, data, reader->ReleaseIsValid(),
                                          reader->null_count());
  return Status::OK();
}

Status TransferDate64(RecordReader* reader, MemoryPool* pool,
                      const std::shared_ptr<DataType>& type, Datum* out) {
  int64_t length = reader->values_written();
  auto values = reinterpret_cast<const int32_t*>(reader->values());

  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * sizeof(int64_t), &data));
  auto out_ptr = reinterpret_cast<int64_t*>(data->mutable_data());

  for (int64_t i = 0; i < length; i++) {
    *out_ptr++ = static_cast<int64_t>(values[i]) * kMillisecondsPerDay;
  }

  *out = std::make_shared<::arrow::Date64Array>(
      type, length, data, reader->ReleaseIsValid(), reader->null_count());
  return Status::OK();
}

// ----------------------------------------------------------------------
// Binary, direct to dictionary-encoded

// Some ugly hacks here for now to handle binary dictionaries casted to their
// logical type
std::shared_ptr<Array> ShallowCast(const Array& arr,
                                   const std::shared_ptr<DataType>& new_type) {
  std::shared_ptr<::arrow::ArrayData> new_data = arr.data()->Copy();
  new_data->type = new_type;
  if (new_type->id() == ::arrow::Type::DICTIONARY) {
    // Cast dictionary, too
    const auto& dict_type = static_cast<const ::arrow::DictionaryType&>(*new_type);
    new_data->dictionary = ShallowCast(*new_data->dictionary, dict_type.value_type());
  }
  return MakeArray(new_data);
}

std::shared_ptr<ChunkedArray> CastChunksTo(
    const ChunkedArray& data, const std::shared_ptr<DataType>& logical_value_type) {
  std::vector<std::shared_ptr<Array>> string_chunks;
  for (int i = 0; i < data.num_chunks(); ++i) {
    string_chunks.push_back(ShallowCast(*data.chunk(i), logical_value_type));
  }
  return std::make_shared<ChunkedArray>(string_chunks);
}

Status TransferDictionary(RecordReader* reader,
                          const std::shared_ptr<DataType>& logical_value_type,
                          std::shared_ptr<ChunkedArray>* out) {
  auto dict_reader = dynamic_cast<internal::DictionaryRecordReader*>(reader);
  DCHECK(dict_reader);
  *out = dict_reader->GetResult();
  if (!logical_value_type->Equals(*(*out)->type())) {
    *out = CastChunksTo(**out, logical_value_type);
  }
  return Status::OK();
}

Status TransferBinary(RecordReader* reader,
                      const std::shared_ptr<DataType>& logical_value_type,
                      std::shared_ptr<ChunkedArray>* out) {
  if (reader->read_dictionary()) {
    return TransferDictionary(
        reader, ::arrow::dictionary(::arrow::int32(), logical_value_type), out);
  }
  auto binary_reader = dynamic_cast<internal::BinaryRecordReader*>(reader);
  DCHECK(binary_reader);
  *out = std::make_shared<ChunkedArray>(binary_reader->GetBuilderChunks());
  if (!logical_value_type->Equals(*(*out)->type())) {
    *out = CastChunksTo(**out, logical_value_type);
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// INT32 / INT64 / BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY -> Decimal128

static uint64_t BytesToInteger(const uint8_t* bytes, int32_t start, int32_t stop) {
  const int32_t length = stop - start;

  DCHECK_GE(length, 0);
  DCHECK_LE(length, 8);

  switch (length) {
    case 0:
      return 0;
    case 1:
      return bytes[start];
    case 2:
      return FromBigEndian(SafeLoadAs<uint16_t>(bytes + start));
    case 3: {
      const uint64_t first_two_bytes = FromBigEndian(SafeLoadAs<uint16_t>(bytes + start));
      const uint64_t last_byte = bytes[stop - 1];
      return first_two_bytes << 8 | last_byte;
    }
    case 4:
      return FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
    case 5: {
      const uint64_t first_four_bytes =
          FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
      const uint64_t last_byte = bytes[stop - 1];
      return first_four_bytes << 8 | last_byte;
    }
    case 6: {
      const uint64_t first_four_bytes =
          FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
      const uint64_t last_two_bytes =
          FromBigEndian(SafeLoadAs<uint16_t>(bytes + start + 4));
      return first_four_bytes << 16 | last_two_bytes;
    }
    case 7: {
      const uint64_t first_four_bytes =
          FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
      const uint64_t second_two_bytes =
          FromBigEndian(SafeLoadAs<uint16_t>(bytes + start + 4));
      const uint64_t last_byte = bytes[stop - 1];
      return first_four_bytes << 24 | second_two_bytes << 8 | last_byte;
    }
    case 8:
      return FromBigEndian(SafeLoadAs<uint64_t>(bytes + start));
    default: {
      DCHECK(false);
      return UINT64_MAX;
    }
  }
}

static constexpr int32_t kMinDecimalBytes = 1;
static constexpr int32_t kMaxDecimalBytes = 16;

/// \brief Convert a sequence of big-endian bytes to one int64_t (high bits) and one
/// uint64_t (low bits).
static void BytesToIntegerPair(const uint8_t* bytes, const int32_t length,
                               int64_t* out_high, uint64_t* out_low) {
  DCHECK_GE(length, kMinDecimalBytes);
  DCHECK_LE(length, kMaxDecimalBytes);

  // XXX This code is copied from Decimal::FromBigEndian

  int64_t high, low;

  // Bytes are coming in big-endian, so the first byte is the MSB and therefore holds the
  // sign bit.
  const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;

  // 1. Extract the high bytes
  // Stop byte of the high bytes
  const int32_t high_bits_offset = std::max(0, length - 8);
  const auto high_bits = BytesToInteger(bytes, 0, high_bits_offset);

  if (high_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    high = high_bits;
  } else {
    high = -1 * (is_negative && length < kMaxDecimalBytes);
    // Shift left enough bits to make room for the incoming int64_t
    high = SafeLeftShift(high, high_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    high |= high_bits;
  }

  // 2. Extract the low bytes
  // Stop byte of the low bytes
  const int32_t low_bits_offset = std::min(length, 8);
  const auto low_bits = BytesToInteger(bytes, high_bits_offset, length);

  if (low_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    low = low_bits;
  } else {
    // Sign extend the low bits if necessary
    low = -1 * (is_negative && length < 8);
    // Shift left enough bits to make room for the incoming int64_t
    low = SafeLeftShift(low, low_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    low |= low_bits;
  }

  *out_high = high;
  *out_low = static_cast<uint64_t>(low);
}

static inline void RawBytesToDecimalBytes(const uint8_t* value, int32_t byte_width,
                                          uint8_t* out_buf) {
  // view the first 8 bytes as an unsigned 64-bit integer
  auto low = reinterpret_cast<uint64_t*>(out_buf);

  // view the second 8 bytes as a signed 64-bit integer
  auto high = reinterpret_cast<int64_t*>(out_buf + sizeof(uint64_t));

  // Convert the fixed size binary array bytes into a Decimal128 compatible layout
  BytesToIntegerPair(value, byte_width, high, low);
}

template <typename T>
Status ConvertToDecimal128(const Array& array, const std::shared_ptr<DataType>&,
                           MemoryPool* pool, std::shared_ptr<Array>*) {
  return Status::NotImplemented("not implemented");
}

template <>
Status ConvertToDecimal128<FLBAType>(const Array& array,
                                     const std::shared_ptr<DataType>& type,
                                     MemoryPool* pool, std::shared_ptr<Array>* out) {
  const auto& fixed_size_binary_array =
      static_cast<const ::arrow::FixedSizeBinaryArray&>(array);

  // The byte width of each decimal value
  const int32_t type_length =
      static_cast<const ::arrow::Decimal128Type&>(*type).byte_width();

  // number of elements in the entire array
  const int64_t length = fixed_size_binary_array.length();

  // Get the byte width of the values in the FixedSizeBinaryArray. Most of the time
  // this will be different from the decimal array width because we write the minimum
  // number of bytes necessary to represent a given precision
  const int32_t byte_width =
      static_cast<const ::arrow::FixedSizeBinaryType&>(*fixed_size_binary_array.type())
          .byte_width();

  // allocate memory for the decimal array
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * type_length, &data));

  // raw bytes that we can write to
  uint8_t* out_ptr = data->mutable_data();

  // convert each FixedSizeBinary value to valid decimal bytes
  const int64_t null_count = fixed_size_binary_array.null_count();
  if (null_count > 0) {
    for (int64_t i = 0; i < length; ++i, out_ptr += type_length) {
      if (!fixed_size_binary_array.IsNull(i)) {
        RawBytesToDecimalBytes(fixed_size_binary_array.GetValue(i), byte_width, out_ptr);
      }
    }
  } else {
    for (int64_t i = 0; i < length; ++i, out_ptr += type_length) {
      RawBytesToDecimalBytes(fixed_size_binary_array.GetValue(i), byte_width, out_ptr);
    }
  }

  *out = std::make_shared<::arrow::Decimal128Array>(
      type, length, data, fixed_size_binary_array.null_bitmap(), null_count);

  return Status::OK();
}

template <>
Status ConvertToDecimal128<ByteArrayType>(const Array& array,
                                          const std::shared_ptr<DataType>& type,
                                          MemoryPool* pool, std::shared_ptr<Array>* out) {
  const auto& binary_array = static_cast<const ::arrow::BinaryArray&>(array);
  const int64_t length = binary_array.length();

  const auto& decimal_type = static_cast<const ::arrow::Decimal128Type&>(*type);
  const int64_t type_length = decimal_type.byte_width();

  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * type_length, &data));

  // raw bytes that we can write to
  uint8_t* out_ptr = data->mutable_data();

  const int64_t null_count = binary_array.null_count();

  // convert each BinaryArray value to valid decimal bytes
  for (int64_t i = 0; i < length; i++, out_ptr += type_length) {
    int32_t record_len = 0;
    const uint8_t* record_loc = binary_array.GetValue(i, &record_len);

    if ((record_len < 0) || (record_len > type_length)) {
      return Status::Invalid("Invalid BYTE_ARRAY size");
    }

    auto out_ptr_view = reinterpret_cast<uint64_t*>(out_ptr);
    out_ptr_view[0] = 0;
    out_ptr_view[1] = 0;

    // only convert rows that are not null if there are nulls, or
    // all rows, if there are not
    if (((null_count > 0) && !binary_array.IsNull(i)) || (null_count <= 0)) {
      RawBytesToDecimalBytes(record_loc, record_len, out_ptr);
    }
  }

  *out = std::make_shared<::arrow::Decimal128Array>(
      type, length, data, binary_array.null_bitmap(), null_count);
  return Status::OK();
}

/// \brief Convert an Int32 or Int64 array into a Decimal128Array
/// The parquet spec allows systems to write decimals in int32, int64 if the values are
/// small enough to fit in less 4 bytes or less than 8 bytes, respectively.
/// This function implements the conversion from int32 and int64 arrays to decimal arrays.
template <typename ParquetIntegerType,
          typename = typename std::enable_if<
              std::is_same<ParquetIntegerType, Int32Type>::value ||
              std::is_same<ParquetIntegerType, Int64Type>::value>::type>
static Status DecimalIntegerTransfer(RecordReader* reader, MemoryPool* pool,
                                     const std::shared_ptr<DataType>& type, Datum* out) {
  DCHECK_EQ(type->id(), ::arrow::Type::DECIMAL);

  const int64_t length = reader->values_written();

  using ElementType = typename ParquetIntegerType::c_type;
  static_assert(std::is_same<ElementType, int32_t>::value ||
                    std::is_same<ElementType, int64_t>::value,
                "ElementType must be int32_t or int64_t");

  const auto values = reinterpret_cast<const ElementType*>(reader->values());

  const auto& decimal_type = static_cast<const ::arrow::Decimal128Type&>(*type);
  const int64_t type_length = decimal_type.byte_width();

  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * type_length, &data));
  uint8_t* out_ptr = data->mutable_data();

  using ::arrow::BitUtil::FromLittleEndian;

  for (int64_t i = 0; i < length; ++i, out_ptr += type_length) {
    // sign/zero extend int32_t values, otherwise a no-op
    const auto value = static_cast<int64_t>(values[i]);

    auto out_ptr_view = reinterpret_cast<uint64_t*>(out_ptr);

    // No-op on little endian machines, byteswap on big endian
    out_ptr_view[0] = FromLittleEndian(static_cast<uint64_t>(value));

    // no need to byteswap here because we're sign/zero extending exactly 8 bytes
    out_ptr_view[1] = static_cast<uint64_t>(value < 0 ? -1 : 0);
  }

  if (reader->nullable_values()) {
    std::shared_ptr<ResizableBuffer> is_valid = reader->ReleaseIsValid();
    *out = std::make_shared<::arrow::Decimal128Array>(type, length, data, is_valid,
                                                      reader->null_count());
  } else {
    *out = std::make_shared<::arrow::Decimal128Array>(type, length, data);
  }
  return Status::OK();
}

/// \brief Convert an arrow::BinaryArray to an arrow::Decimal128Array
/// We do this by:
/// 1. Creating an arrow::BinaryArray from the RecordReader's builder
/// 2. Allocating a buffer for the arrow::Decimal128Array
/// 3. Converting the big-endian bytes in each BinaryArray entry to two integers
///    representing the high and low bits of each decimal value.
template <typename ParquetType>
Status TransferDecimal(RecordReader* reader, MemoryPool* pool,
                       const std::shared_ptr<DataType>& type, Datum* out) {
  DCHECK_EQ(type->id(), ::arrow::Type::DECIMAL);

  auto binary_reader = dynamic_cast<internal::BinaryRecordReader*>(reader);
  DCHECK(binary_reader);
  ::arrow::ArrayVector chunks = binary_reader->GetBuilderChunks();
  for (size_t i = 0; i < chunks.size(); ++i) {
    std::shared_ptr<Array> chunk_as_decimal;
    RETURN_NOT_OK(
        ConvertToDecimal128<ParquetType>(*chunks[i], type, pool, &chunk_as_decimal));
    // Replace the chunk, which will hopefully also free memory as we go
    chunks[i] = chunk_as_decimal;
  }
  *out = std::make_shared<ChunkedArray>(chunks);
  return Status::OK();
}

#define TRANSFER_INT32(ENUM, ArrowType)                                              \
  case ::arrow::Type::ENUM: {                                                        \
    Status s = TransferInt<ArrowType, Int32Type>(reader, pool, value_type, &result); \
    RETURN_NOT_OK(s);                                                                \
  } break;

#define TRANSFER_INT64(ENUM, ArrowType)                                              \
  case ::arrow::Type::ENUM: {                                                        \
    Status s = TransferInt<ArrowType, Int64Type>(reader, pool, value_type, &result); \
    RETURN_NOT_OK(s);                                                                \
  } break;

Status TransferColumnData(internal::RecordReader* reader,
                          std::shared_ptr<DataType> value_type,
                          const ColumnDescriptor* descr, MemoryPool* pool,
                          std::shared_ptr<ChunkedArray>* out) {
  Datum result;
  std::shared_ptr<ChunkedArray> chunked_result;
  switch (value_type->id()) {
    case ::arrow::Type::DICTIONARY: {
      RETURN_NOT_OK(TransferDictionary(reader, value_type, &chunked_result));
      result = chunked_result;
    } break;
    case ::arrow::Type::NA: {
      result = std::make_shared<::arrow::NullArray>(reader->values_written());
      break;
    }
    case ::arrow::Type::INT32:
    case ::arrow::Type::INT64:
    case ::arrow::Type::FLOAT:
    case ::arrow::Type::DOUBLE:
      result = TransferZeroCopy(reader, value_type);
      break;
    case ::arrow::Type::BOOL:
      RETURN_NOT_OK(TransferBool(reader, pool, &result));
      break;
      TRANSFER_INT32(UINT8, ::arrow::UInt8Type);
      TRANSFER_INT32(INT8, ::arrow::Int8Type);
      TRANSFER_INT32(UINT16, ::arrow::UInt16Type);
      TRANSFER_INT32(INT16, ::arrow::Int16Type);
      TRANSFER_INT32(UINT32, ::arrow::UInt32Type);
      TRANSFER_INT64(UINT64, ::arrow::UInt64Type);
      TRANSFER_INT32(DATE32, ::arrow::Date32Type);
      TRANSFER_INT32(TIME32, ::arrow::Time32Type);
      TRANSFER_INT64(TIME64, ::arrow::Time64Type);
    case ::arrow::Type::DATE64:
      RETURN_NOT_OK(TransferDate64(reader, pool, value_type, &result));
      break;
    case ::arrow::Type::FIXED_SIZE_BINARY:
    case ::arrow::Type::BINARY:
    case ::arrow::Type::STRING: {
      RETURN_NOT_OK(TransferBinary(reader, value_type, &chunked_result));
      result = chunked_result;
    } break;
    case ::arrow::Type::DECIMAL: {
      switch (descr->physical_type()) {
        case ::parquet::Type::INT32: {
          RETURN_NOT_OK(
              DecimalIntegerTransfer<Int32Type>(reader, pool, value_type, &result));
        } break;
        case ::parquet::Type::INT64: {
          RETURN_NOT_OK(
              DecimalIntegerTransfer<Int64Type>(reader, pool, value_type, &result));
        } break;
        case ::parquet::Type::BYTE_ARRAY: {
          RETURN_NOT_OK(
              TransferDecimal<ByteArrayType>(reader, pool, value_type, &result));
        } break;
        case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
          RETURN_NOT_OK(TransferDecimal<FLBAType>(reader, pool, value_type, &result));
        } break;
        default:
          return Status::Invalid(
              "Physical type for decimal must be int32, int64, byte array, or fixed "
              "length binary");
      }
    } break;
    case ::arrow::Type::TIMESTAMP: {
      const ::arrow::TimestampType& timestamp_type =
          static_cast<::arrow::TimestampType&>(*value_type);
      switch (timestamp_type.unit()) {
        case ::arrow::TimeUnit::MILLI:
        case ::arrow::TimeUnit::MICRO: {
          result = TransferZeroCopy(reader, value_type);
        } break;
        case ::arrow::TimeUnit::NANO: {
          if (descr->physical_type() == ::parquet::Type::INT96) {
            RETURN_NOT_OK(TransferInt96(reader, pool, value_type, &result));
          } else {
            result = TransferZeroCopy(reader, value_type);
          }
        } break;
        default:
          return Status::NotImplemented("TimeUnit not supported");
      }
    } break;
    default:
      return Status::NotImplemented("No support for reading columns of type ",
                                    value_type->ToString());
  }

  DCHECK_NE(result.kind(), Datum::NONE);

  if (result.kind() == Datum::ARRAY) {
    *out = std::make_shared<ChunkedArray>(result.make_array());
  } else if (result.kind() == Datum::CHUNKED_ARRAY) {
    *out = result.chunked_array();
  } else {
    DCHECK(false) << "Should be impossible";
  }

  return Status::OK();
}

Status ReconstructNestedList(const std::shared_ptr<Array>& arr,
                             std::shared_ptr<Field> field, int16_t max_def_level,
                             int16_t max_rep_level, const int16_t* def_levels,
                             const int16_t* rep_levels, int64_t total_levels,
                             ::arrow::MemoryPool* pool, std::shared_ptr<Array>* out) {
  // Walk downwards to extract nullability
  std::vector<bool> nullable;
  std::vector<std::shared_ptr<::arrow::Int32Builder>> offset_builders;
  std::vector<std::shared_ptr<::arrow::BooleanBuilder>> valid_bits_builders;
  nullable.push_back(field->nullable());
  while (field->type()->num_children() > 0) {
    if (field->type()->num_children() > 1) {
      return Status::NotImplemented("Fields with more than one child are not supported.");
    } else {
      if (field->type()->id() != ::arrow::Type::LIST) {
        return Status::NotImplemented("Currently only nesting with Lists is supported.");
      }
      field = field->type()->child(0);
    }
    offset_builders.emplace_back(
        std::make_shared<::arrow::Int32Builder>(::arrow::int32(), pool));
    valid_bits_builders.emplace_back(
        std::make_shared<::arrow::BooleanBuilder>(::arrow::boolean(), pool));
    nullable.push_back(field->nullable());
  }

  int64_t list_depth = offset_builders.size();
  // This describes the minimal definition that describes a level that
  // reflects a value in the primitive values array.
  int16_t values_def_level = max_def_level;
  if (nullable[nullable.size() - 1]) {
    values_def_level--;
  }

  // The definition levels that are needed so that a list is declared
  // as empty and not null.
  std::vector<int16_t> empty_def_level(list_depth);
  int def_level = 0;
  for (int i = 0; i < list_depth; i++) {
    if (nullable[i]) {
      def_level++;
    }
    empty_def_level[i] = static_cast<int16_t>(def_level);
    def_level++;
  }

  int32_t values_offset = 0;
  std::vector<int64_t> null_counts(list_depth, 0);
  for (int64_t i = 0; i < total_levels; i++) {
    int16_t rep_level = rep_levels[i];
    if (rep_level < max_rep_level) {
      for (int64_t j = rep_level; j < list_depth; j++) {
        if (j == (list_depth - 1)) {
          RETURN_NOT_OK(offset_builders[j]->Append(values_offset));
        } else {
          RETURN_NOT_OK(offset_builders[j]->Append(
              static_cast<int32_t>(offset_builders[j + 1]->length())));
        }

        if (((empty_def_level[j] - 1) == def_levels[i]) && (nullable[j])) {
          RETURN_NOT_OK(valid_bits_builders[j]->Append(false));
          null_counts[j]++;
          break;
        } else {
          RETURN_NOT_OK(valid_bits_builders[j]->Append(true));
          if (empty_def_level[j] == def_levels[i]) {
            break;
          }
        }
      }
    }
    if (def_levels[i] >= values_def_level) {
      values_offset++;
    }
  }
  // Add the final offset to all lists
  for (int64_t j = 0; j < list_depth; j++) {
    if (j == (list_depth - 1)) {
      RETURN_NOT_OK(offset_builders[j]->Append(values_offset));
    } else {
      RETURN_NOT_OK(offset_builders[j]->Append(
          static_cast<int32_t>(offset_builders[j + 1]->length())));
    }
  }

  std::vector<std::shared_ptr<Buffer>> offsets;
  std::vector<std::shared_ptr<Buffer>> valid_bits;
  std::vector<int64_t> list_lengths;
  for (int64_t j = 0; j < list_depth; j++) {
    list_lengths.push_back(offset_builders[j]->length() - 1);
    std::shared_ptr<Array> array;
    RETURN_NOT_OK(offset_builders[j]->Finish(&array));
    offsets.emplace_back(std::static_pointer_cast<Int32Array>(array)->values());
    RETURN_NOT_OK(valid_bits_builders[j]->Finish(&array));
    valid_bits.emplace_back(std::static_pointer_cast<BooleanArray>(array)->values());
  }

  *out = arr;

  // TODO(wesm): Use passed-in field
  for (int64_t j = list_depth - 1; j >= 0; j--) {
    auto list_type =
        ::arrow::list(::arrow::field("item", (*out)->type(), nullable[j + 1]));
    *out = std::make_shared<::arrow::ListArray>(list_type, list_lengths[j], offsets[j],
                                                *out, valid_bits[j], null_counts[j]);
  }
  return Status::OK();
}

}  // namespace arrow
}  // namespace parquet
