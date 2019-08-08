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

#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

#include "parquet/exception.h"
#include "parquet/properties.h"
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

// ----------------------------------------------------------------------
// Parquet to Arrow schema conversion

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
      length = internal::DecimalSize(precision);
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

}  // namespace arrow
}  // namespace parquet
