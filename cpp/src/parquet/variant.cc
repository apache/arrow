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

#include "parquet/variant.h"

#include <cstdint>
#include <iostream>
#include <string_view>

#include <arrow/util/endian.h>
#include <arrow/util/logging.h>

#include "parquet/exception.h"

namespace parquet::variant {

std::string VariantBasicTypeToString(VariantBasicType type) {
  switch (type) {
    case VariantBasicType::Primitive:
      return "Primitive";
    case VariantBasicType::ShortString:
      return "ShortString";
    case VariantBasicType::Object:
      return "Object";
    case VariantBasicType::Array:
      return "Array";
    default:
      return "Unknown";
  }
}

std::string VariantPrimitiveTypeToString(VariantPrimitiveType type) {
  switch (type) {
    case VariantPrimitiveType::NullType:
      return "NullType";
    case VariantPrimitiveType::BooleanTrue:
      return "BooleanTrue";
    case VariantPrimitiveType::BooleanFalse:
      return "BooleanFalse";
    case VariantPrimitiveType::Int8:
      return "Int8";
    case VariantPrimitiveType::Int16:
      return "Int16";
    case VariantPrimitiveType::Int32:
      return "Int32";
    case VariantPrimitiveType::Int64:
      return "Int64";
    case VariantPrimitiveType::Double:
      return "Double";
    case VariantPrimitiveType::Decimal4:
      return "Decimal4";
    case VariantPrimitiveType::Decimal8:
      return "Decimal8";
    case VariantPrimitiveType::Decimal16:
      return "Decimal16";
    case VariantPrimitiveType::Date:
      return "Date";
    case VariantPrimitiveType::Timestamp:
      return "Timestamp";
    case VariantPrimitiveType::TimestampNtz:
      return "TimestampNtz";
    case VariantPrimitiveType::Float:
      return "Float";
    case VariantPrimitiveType::Binary:
      return "Binary";
    case VariantPrimitiveType::String:
      return "String";
    case VariantPrimitiveType::TimeNtz:
      return "TimeNtz";
    case VariantPrimitiveType::TimestampTz:
      return "TimestampTZ";
    case VariantPrimitiveType::TimestampNtzNanos:
      return "TimestampNtzNanos";
    case VariantPrimitiveType::Uuid:
      return "Uuid";
    default:
      return "Unknown";
  }
}

std::string VariantTypeToString(VariantType type) {
  switch (type) {
    case VariantType::Object:
      return "OBJECT";
    case VariantType::Array:
      return "ARRAY";
    case VariantType::Null:
      return "NULL";
    case VariantType::Boolean:
      return "BOOLEAN";
    case VariantType::Int8:
      return "INT8";
    case VariantType::Int16:
      return "INT16";
    case VariantType::Int32:
      return "INT32";
    case VariantType::Int64:
      return "INT64";
    case VariantType::String:
      return "STRING";
    case VariantType::Double:
      return "DOUBLE";
    case VariantType::Decimal4:
      return "DECIMAL4";
    case VariantType::Decimal8:
      return "DECIMAL8";
    case VariantType::Decimal16:
      return "DECIMAL16";
    case VariantType::Date:
      return "DATE";
    case VariantType::TimestampTz:
      return "TIMESTAMP_TZ";
    case VariantType::TimestampNtz:
      return "TIMESTAMP_Ntz";
    case VariantType::Float:
      return "FLOAT";
    case VariantType::Binary:
      return "BINARY";
    case VariantType::Time:
      return "TIME";
    case VariantType::TimestampNanosTz:
      return "TIMESTAMP_NANOS_TZ";
    case VariantType::TimestampNanosNtz:
      return "TIMESTAMP_NANOS_Ntz";
    case VariantType::Uuid:
      return "UUID";
    default:
      return "UNKNOWN";
  }
}

inline uint32_t readLittleEndianU32(const void* from, uint8_t size) {
  ARROW_DCHECK_LE(size, 4);
  ARROW_DCHECK_GE(size, 1);

  uint32_t result = 0;
  memcpy(&result, from, size);
  return ::arrow::bit_util::FromLittleEndian(result);
}

VariantMetadata::VariantMetadata(std::string_view metadata) : metadata_(metadata) {
  if (metadata.size() < kHeaderSizeBytes + kMinimalOffsetSizeBytes * 2) {
    // Empty metadata is at least 3 bytes: version, dictionarySize and
    // at least one offset.
    throw ParquetException("Invalid Variant metadata: too short: size=" +
                           std::to_string(metadata.size()));
  }
  if (version() != kSupportedVersion) {
    // Currently we only supports version 1.
    throw ParquetException("Unsupported Variant metadata version: " +
                           std::to_string(version()));
  }
  uint8_t offset_sz = offset_size();
  if (offset_sz < kMinimalOffsetSizeBytes || offset_sz > kMaximumOffsetSizeBytes) {
    throw ParquetException("Invalid Variant metadata: invalid offset size: " +
                           std::to_string(offset_sz));
  }
  dictionary_size_ = loadDictionarySize(metadata, offset_sz);
  if (kHeaderSizeBytes + (dictionary_size_ + 1) * offset_sz > metadata_.size()) {
    throw ParquetException(
        "Invalid Variant metadata: offset out of range: " +
        std::to_string((dictionary_size_ + kHeaderSizeBytes) * offset_sz) + " > " +
        std::to_string(metadata_.size()));
  }
}

uint8_t VariantMetadata::version() const {
  return static_cast<uint8_t>(metadata_[0]) & kVersionMask;
}

bool VariantMetadata::sorted_and_unique() const {
  return (metadata_[0] & kSortedStringMask) != 0;
}

uint8_t VariantMetadata::offset_size() const {
  // Since it stores offsetSize - 1, we add 1 here.
  return ((metadata_[0] >> kOffsetSizeBitShift) & kOffsetSizeMask) + 1;
}

uint32_t VariantMetadata::loadDictionarySize(std::string_view metadata,
                                             uint8_t offset_size) {
  if (static_cast<size_t>(offset_size + kHeaderSizeBytes) > metadata.size()) {
    throw ParquetException("Invalid Variant metadata: too short for dictionary size");
  }
  return readLittleEndianU32(metadata.data() + kHeaderSizeBytes, offset_size);
}

uint32_t VariantMetadata::dictionary_size() const { return dictionary_size_; }

std::string_view VariantMetadata::GetMetadataKey(uint32_t variant_id) const {
  uint32_t offset_bytes = offset_size();
  uint32_t dictionary_bytes = dictionary_size();

  if (variant_id >= dictionary_bytes) {
    throw ParquetException("Invalid Variant metadata: variant_id out of range: " +
                           std::to_string(variant_id) +
                           " >= " + std::to_string(dictionary_bytes));
  }

  size_t offset_start_pos = kHeaderSizeBytes + offset_bytes + (variant_id * offset_bytes);

  // Index range of offsets are already checked in ctor, so no need to check again.
  uint32_t variant_offset =
      readLittleEndianU32(metadata_.data() + offset_start_pos, offset_bytes);
  uint32_t variant_next_offset = readLittleEndianU32(
      metadata_.data() + offset_start_pos + offset_bytes, offset_bytes);
  uint32_t key_size = variant_next_offset - variant_offset;

  size_t string_start =
      kHeaderSizeBytes + offset_bytes * (dictionary_bytes + 2) + variant_offset;
  if (string_start + key_size > metadata_.size()) {
    throw ParquetException("Invalid Variant metadata: string data out of range: " +
                           std::to_string(string_start) + " + " +
                           std::to_string(key_size) + " > " +
                           std::to_string(metadata_.size()));
  }
  return {metadata_.data() + string_start, key_size};
}

::arrow::internal::SmallVector<uint32_t, 1> VariantMetadata::GetMetadataId(
    std::string_view key) const {
  uint32_t offset_bytes = offset_size();
  uint32_t dictionary_bytes = dictionary_size();

  if ((dictionary_bytes + kHeaderSizeBytes) * offset_bytes > metadata_.size()) {
    throw ParquetException("Invalid Variant metadata: offset out of range");
  }
  const bool sort_and_unique = sorted_and_unique();
  // TODO(mwish): This can be optimized by using binary search if the metadata is sorted.
  ::arrow::internal::SmallVector<uint32_t, 1> vector;
  uint32_t variant_offset = 0;
  uint32_t variant_next_offset = 0;
  for (uint32_t i = 0; i < dictionary_bytes; ++i) {
    size_t offset_start_pos = 1 + offset_bytes + (i * offset_bytes);
    variant_offset = variant_next_offset;
    variant_next_offset = readLittleEndianU32(
        metadata_.data() + offset_start_pos + offset_bytes, offset_bytes);
    uint32_t key_size = variant_next_offset - variant_offset;

    size_t string_start = 1 + offset_bytes * (dictionary_bytes + 2) + variant_offset;
    if (string_start + key_size > metadata_.size()) {
      throw ParquetException("Invalid Variant metadata: string data out of range");
    }
    std::string_view current_key{metadata_.data() + string_start, key_size};
    if (current_key == key) {
      vector.push_back(i);
      if (sort_and_unique) {
        break;
      }
    }
  }
  return vector;
}

VariantBasicType VariantValue::getBasicType() const {
  if (value.empty()) {
    throw ParquetException("Empty variant value");
  }
  return static_cast<VariantBasicType>(value[0] & kBasicTypeMask);
}
VariantType VariantValue::getType() const {
  VariantBasicType basic_type = getBasicType();
  switch (basic_type) {
    case VariantBasicType::Primitive: {
      auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
      switch (primitive_type) {
        case VariantPrimitiveType::NullType:
          return VariantType::Null;
        case VariantPrimitiveType::BooleanTrue:
        case VariantPrimitiveType::BooleanFalse:
          return VariantType::Boolean;
        case VariantPrimitiveType::Int8:
          return VariantType::Int8;
        case VariantPrimitiveType::Int16:
          return VariantType::Int16;
        case VariantPrimitiveType::Int32:
          return VariantType::Int32;
        case VariantPrimitiveType::Int64:
          return VariantType::Int64;
        case VariantPrimitiveType::Double:
          return VariantType::Double;
        case VariantPrimitiveType::Decimal4:
          return VariantType::Decimal4;
        case VariantPrimitiveType::Decimal8:
          return VariantType::Decimal8;
        case VariantPrimitiveType::Decimal16:
          return VariantType::Decimal16;
        case VariantPrimitiveType::Date:
          return VariantType::Date;
        case VariantPrimitiveType::Timestamp:
          return VariantType::TimestampTz;
        case VariantPrimitiveType::TimestampNtz:
          return VariantType::TimestampNtz;
        case VariantPrimitiveType::Float:
          return VariantType::Float;
        case VariantPrimitiveType::Binary:
          return VariantType::Binary;
        case VariantPrimitiveType::String:
          return VariantType::String;
        case VariantPrimitiveType::TimeNtz:
          return VariantType::Time;
        case VariantPrimitiveType::TimestampTz:
          return VariantType::TimestampNanosTz;
        case VariantPrimitiveType::TimestampNtzNanos:
          return VariantType::TimestampNanosNtz;
        case VariantPrimitiveType::Uuid:
          return VariantType::Uuid;
        default:
          throw ParquetException("Unknown primitive type: " +
                                 std::to_string(static_cast<int>(primitive_type)));
      }
    }
    case VariantBasicType::ShortString:
      return VariantType::String;
    case VariantBasicType::Object:
      return VariantType::Object;
    case VariantBasicType::Array:
      return VariantType::Array;
    default:
      throw ParquetException("Unknown basic type: " +
                             std::to_string(static_cast<int>(basic_type)));
  }
}

std::string VariantValue::typeDebugString() const {
  VariantType type = getType();
  switch (type) {
    case VariantType::Object:
      return "Object";
    case VariantType::Array:
      return "Array";
    case VariantType::Null:
      return "Null";
    case VariantType::Boolean:
      return "Boolean";
    case VariantType::Int8:
      return "Int8";
    case VariantType::Int16:
      return "Int16";
    case VariantType::Int32:
      return "Int32";
    case VariantType::Int64:
      return "Int64";
    case VariantType::String:
      return "String";
    case VariantType::Double:
      return "Double";
    case VariantType::Decimal4:
      return "Decimal4";
    case VariantType::Decimal8:
      return "Decimal8";
    case VariantType::Decimal16:
      return "Decimal16";
    case VariantType::Date:
      return "Date";
    case VariantType::TimestampTz:
      return "TimestampTz";
    case VariantType::TimestampNtz:
      return "TimestampNtz";
    case VariantType::Float:
      return "Float";
    case VariantType::Binary:
      return "Binary";
    case VariantType::Time:
      return "Time";
    case VariantType::TimestampNanosTz:
      return "TimestampNanosTz";
    case VariantType::TimestampNanosNtz:
      return "TimestampNanosNtz";
    case VariantType::Uuid:
      return "Uuid";
    default:
      return "Unknown";
  }
}

bool VariantValue::getBool() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Expected primitive type, but got: " +
                           VariantBasicTypeToString(getBasicType()));
  }

  int8_t primitive_type = static_cast<int8_t>(value[0]) >> 2;
  if (primitive_type == static_cast<int8_t>(VariantPrimitiveType::BooleanTrue)) {
    return true;
  }
  if (primitive_type == static_cast<int8_t>(VariantPrimitiveType::BooleanFalse)) {
    return false;
  }

  throw ParquetException("Not a variant primitive boolean type with primitive type: " +
                         std::to_string(primitive_type));
}

void VariantValue::checkBasicType(VariantBasicType type) const {
  if (getBasicType() != type) {
    throw ParquetException("Expected basic type: " + VariantBasicTypeToString(type) +
                           ", but got: " + VariantBasicTypeToString(getBasicType()));
  }
}

void VariantValue::checkPrimitiveType(VariantPrimitiveType type,
                                      size_t size_required) const {
  checkBasicType(VariantBasicType::Primitive);

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != type) {
    throw ParquetException(
        "Expected primitive type: " + VariantPrimitiveTypeToString(type) +
        ", but got: " + VariantPrimitiveTypeToString(primitive_type));
  }

  if (value.size() < size_required) {
    throw ParquetException("Invalid value: too short, expected at least " +
                           std::to_string(size_required) + " bytes for type " +
                           VariantPrimitiveTypeToString(type) +
                           ", but got: " + std::to_string(value.size()) + " bytes");
  }
}

template <typename PrimitiveType>
PrimitiveType VariantValue::getPrimitiveType(VariantPrimitiveType type) const {
  checkPrimitiveType(type, sizeof(PrimitiveType) + 1);

  PrimitiveType primitive_value{};
  memcpy(&primitive_value, value.data() + 1, sizeof(PrimitiveType));
  // Here we should cast from Little endian.
  primitive_value = ::arrow::bit_util::FromLittleEndian(primitive_value);
  return primitive_value;
}

int8_t VariantValue::getInt8() const {
  return getPrimitiveType<int8_t>(VariantPrimitiveType::Int8);
}

int16_t VariantValue::getInt16() const {
  return getPrimitiveType<int16_t>(VariantPrimitiveType::Int16);
}

int32_t VariantValue::getInt32() const {
  return getPrimitiveType<int32_t>(VariantPrimitiveType::Int32);
}

int64_t VariantValue::getInt64() const {
  return getPrimitiveType<int64_t>(VariantPrimitiveType::Int64);
}

float VariantValue::getFloat() const {
  return getPrimitiveType<float>(VariantPrimitiveType::Float);
}

double VariantValue::getDouble() const {
  return getPrimitiveType<double>(VariantPrimitiveType::Double);
}

std::string_view VariantValue::getPrimitiveBinaryType(VariantPrimitiveType type) const {
  checkPrimitiveType(type, /*size_required=*/5);

  uint32_t length;
  memcpy(&length, value.data() + 1, sizeof(uint32_t));
  length = ::arrow::bit_util::FromLittleEndian(length);

  if (value.size() < length + 5) {
    throw ParquetException("Invalid string value: too short for specified length");
  }

  return {value.data() + 5, length};
}

std::string_view VariantValue::getString() const {
  VariantBasicType basic_type = getBasicType();

  if (basic_type == VariantBasicType::ShortString) {
    uint8_t length = (value[0] >> 2) & kMaxShortStrSizeMask;
    if (value.size() < static_cast<size_t>(length + 1)) {
      throw ParquetException(
          "Invalid short string: too short: " + std::to_string(value.size()) +
          " for at least " + std::to_string(length + 1));
    }
    return {value.data() + 1, length};
  }
  if (basic_type == VariantBasicType::Primitive) {
    // TODO(mwish): Should we validate utf8 here?
    return getPrimitiveBinaryType(VariantPrimitiveType::String);
  }

  throw ParquetException("Not a primitive or short string type calls getString");
}

std::string_view VariantValue::getBinary() const {
  return getPrimitiveBinaryType(VariantPrimitiveType::Binary);
}

template <typename DecimalType>
DecimalValue<DecimalType> VariantValue::getPrimitiveDecimalType(
    VariantPrimitiveType type) const {
  using DecimalValueType = typename DecimalType::ValueType;
  checkPrimitiveType(type, sizeof(DecimalValueType) + 2);

  uint8_t scale = value[1];
  DecimalValueType decimal_value;
  memcpy(&decimal_value, value.data() + 2, sizeof(DecimalValueType));
  decimal_value = ::arrow::bit_util::FromLittleEndian(decimal_value);

  return {scale, DecimalType(decimal_value)};
}

DecimalValue<::arrow::Decimal32> VariantValue::getDecimal4() const {
  return getPrimitiveDecimalType<::arrow::Decimal32>(VariantPrimitiveType::Decimal4);
}

DecimalValue<::arrow::Decimal64> VariantValue::getDecimal8() const {
  return getPrimitiveDecimalType<::arrow::Decimal64>(VariantPrimitiveType::Decimal8);
}

DecimalValue<::arrow::Decimal128> VariantValue::getDecimal16() const {
  checkPrimitiveType(VariantPrimitiveType::Decimal16,
                     /*size_required=*/sizeof(int64_t) * 2 + 2);

  uint8_t scale = value[1];

  // TODO(mwish): Do we have better way for this?
  std::array<int64_t, 2> low_high_bits;
  memcpy(&low_high_bits[0], value.data() + 2, sizeof(int64_t));
  memcpy(&low_high_bits[1], value.data() + 10, sizeof(int64_t));
  ::arrow::bit_util::little_endian::ToNative(low_high_bits);
  return {scale, ::arrow::Decimal128(low_high_bits[1], low_high_bits[0])};
}

int32_t VariantValue::getDate() const {
  return getPrimitiveType<int32_t>(VariantPrimitiveType::Date);
}

int64_t VariantValue::getTimeNtz() const {
  return getPrimitiveType<int64_t>(VariantPrimitiveType::TimeNtz);
}

int64_t VariantValue::getTimestamp() const {
  return getPrimitiveType<int64_t>(VariantPrimitiveType::Timestamp);
}

int64_t VariantValue::getTimestampNtz() const {
  return getPrimitiveType<int64_t>(VariantPrimitiveType::TimestampNtz);
}

std::array<uint8_t, 16> VariantValue::getUuid() const {
  checkPrimitiveType(VariantPrimitiveType::Uuid, /*size_required=*/17);
  std::array<uint8_t, 16> uuid_value;
  memcpy(uuid_value.data(), value.data() + 1, sizeof(uuid_value));
#if ARROW_LITTLE_ENDIAN
  std::array<uint8_t, 16> uuid_value_le;
  ::arrow::bit_util::ByteSwap(uuid_value_le.data(), uuid_value.data(), uuid_value.size());
  return uuid_value_le;
#else
  return uuid_value;
#endif
}

std::string VariantValue::ObjectInfo::toDebugString() const {
  std::stringstream ss;
  ss << "ObjectInfo{"
     << "num_elements=" << num_elements << ", id_size=" << static_cast<int>(id_size)
     << ", id_size=" << static_cast<int>(id_size)
     << ", offset_size=" << static_cast<int>(offset_size)
     << ", id_start_offset=" << id_start_offset
     << ", offset_start_offset=" << offset_start_offset
     << ", data_start_offset=" << data_start_offset << "}";
  return ss.str();
}

VariantValue::ObjectInfo VariantValue::getObjectInfo() const {
  checkBasicType(VariantBasicType::Object);
  uint8_t value_header = value[0] >> 2;
  uint8_t field_offset_size = (value_header & 0b11) + 1;
  uint8_t field_id_size = ((value_header >> 2) & 0b11) + 1;
  bool is_large = ((value_header >> 4) & 0b1);
  uint8_t num_elements_size = is_large ? 4 : 1;
  if (value.size() < static_cast<size_t>(1 + num_elements_size)) {
    throw ParquetException(
        "Invalid object value: too short: " + std::to_string(value.size()) +
        " for at least " + std::to_string(1 + num_elements_size));
  }
  // parse num_elements
  uint32_t num_elements = readLittleEndianU32(value.data() + 1, num_elements_size);
  ObjectInfo info{};
  info.num_elements = num_elements;
  info.id_size = field_id_size;
  info.offset_size = field_offset_size;
  info.id_start_offset = 1 + num_elements_size;
  info.offset_start_offset = info.id_start_offset + num_elements * field_id_size;
  info.data_start_offset =
      info.offset_start_offset + (num_elements + 1) * field_offset_size;
  // Check the boundary with the final offset
  if (info.data_start_offset > value.size()) {
    throw ParquetException("Invalid object value: data_start_offset=" +
                           std::to_string(info.data_start_offset) +
                           ", value_size=" + std::to_string(value.size()));
  }
  {
    uint32_t final_offset = readLittleEndianU32(
        value.data() + info.offset_start_offset + num_elements * field_offset_size,
        field_offset_size);
    // It could be less than value size since it could be a sub-object.
    if (final_offset + info.data_start_offset > value.size()) {
      throw ParquetException(
          "Invalid object value: final_offset=" + std::to_string(final_offset) +
          ", data_start_offset=" + std::to_string(info.data_start_offset) +
          ", value_size=" + std::to_string(value.size()));
    }
  }
  return info;
}

std::optional<VariantValue> VariantValue::getObjectValueByKey(
    std::string_view key) const {
  ObjectInfo info = getObjectInfo();

  return getObjectValueByKey(key, info);
}

std::optional<VariantValue> VariantValue::getObjectValueByKey(
    std::string_view key, const VariantValue::ObjectInfo& info) const {
  ARROW_DCHECK_EQ(getObjectInfo(), info);
  auto metadata_ids = metadata.GetMetadataId(key);
  if (metadata_ids.empty()) {
    return std::nullopt;
  }
  for (uint32_t variant_id : metadata_ids) {
    auto variant_value = getObjectFieldByFieldId(variant_id, info);
    if (variant_value.has_value()) {
      return variant_value;
    }
  }
  return std::nullopt;
}

std::optional<VariantValue> VariantValue::getObjectFieldByFieldId(
    uint32_t variant_id, const ObjectInfo& info) const {
  ARROW_DCHECK_EQ(getObjectInfo(), info);

  std::optional<uint32_t> field_offset_opt;
  // Get the field offset
  // TODO(mwish): Using binary search to optimize it.
  for (uint32_t i = 0; i < info.num_elements; ++i) {
    uint32_t variant_field_id = readLittleEndianU32(
        value.data() + info.id_start_offset + i * info.id_size, info.id_size);
    if (variant_field_id == variant_id) {
      field_offset_opt = i;
      break;
    }
  }
  if (!field_offset_opt.has_value()) {
    return std::nullopt;
  }
  uint32_t field_offset = field_offset_opt.value();
  // Read the offset and next offset
  uint32_t offset = readLittleEndianU32(
      value.data() + info.offset_start_offset + field_offset * info.offset_size,
      info.offset_size);

  if (info.data_start_offset + offset > value.size()) {
    throw ParquetException("Invalid object field offsets: data_start_offset=" +
                           std::to_string(info.data_start_offset) +
                           ", offset=" + std::to_string(offset) +
                           ", value_size=" + std::to_string(value.size()));
  }

  // Create a VariantValue for the field
  VariantValue field_value{metadata, value.substr(info.data_start_offset + offset)};

  return field_value;
}

std::optional<VariantValue> VariantValue::getObjectFieldByFieldId(
    uint32_t variant_id) const {
  ObjectInfo info = getObjectInfo();

  return getObjectFieldByFieldId(variant_id, info);
}

VariantValue::ArrayInfo VariantValue::getArrayInfo() const {
  checkBasicType(VariantBasicType::Array);
  uint8_t value_header = value[0] >> 2;
  uint8_t field_offset_size = (value_header & 0b11) + 1;
  bool is_large = ((value_header >> 2) & 0b1);

  // check the array header
  uint8_t num_elements_size = is_large ? 4 : 1;
  if (value.size() < static_cast<size_t>(1 + num_elements_size)) {
    throw ParquetException(
        "Invalid array value: too short: " + std::to_string(value.size()) +
        " for at least " + std::to_string(1 + num_elements_size));
  }

  uint32_t num_elements = readLittleEndianU32(value.data() + 1, num_elements_size);
  ArrayInfo info{};
  info.num_elements = num_elements;
  info.offset_size = field_offset_size;
  info.offset_start_offset = 1 + num_elements_size;
  info.data_start_offset =
      info.offset_start_offset + (num_elements + 1) * field_offset_size;

  // Boundary check
  if (info.data_start_offset > value.size()) {
    throw ParquetException("Invalid array value: data_start_offset=" +
                           std::to_string(info.data_start_offset) +
                           ", value_size=" + std::to_string(value.size()));
  }

  // Validate final offset is equal to the size of the value,
  // it would work since even empty array would have an offset of 0.
  {
    uint32_t final_offset = readLittleEndianU32(
        value.data() + info.offset_start_offset + num_elements * field_offset_size,
        field_offset_size);

    if (info.data_start_offset + final_offset > value.size()) {
      throw ParquetException(
          "Invalid array value: final_offset=" + std::to_string(final_offset) +
          ", data_start_offset=" + std::to_string(info.data_start_offset) +
          ", value_size=" + std::to_string(value.size()));
    }
  }

  // checking the element is incremental.
  // TODO(mwish): Remove this or encapsulate this range check to function
  for (uint32_t i = 0; i < num_elements; ++i) {
    uint32_t offset = readLittleEndianU32(
        value.data() + info.offset_start_offset + i * field_offset_size,
        field_offset_size);
    uint32_t next_offset = readLittleEndianU32(
        value.data() + info.offset_start_offset + (i + 1) * field_offset_size,
        field_offset_size);
    if (offset > next_offset) {
      throw ParquetException(
          "Invalid array value: offsets not monotonically increasing: " +
          std::to_string(offset) + " > " + std::to_string(next_offset));
    }
  }

  return info;
}

VariantValue VariantValue::getArrayValueByIndex(uint32_t index,
                                                const ArrayInfo& info) const {
  if (index >= info.num_elements) {
    throw ParquetException("Array index out of range: " + std::to_string(index) +
                           " >= " + std::to_string(info.num_elements));
  }

  // Read the offset and next offset
  uint32_t offset = readLittleEndianU32(
      value.data() + info.offset_start_offset + index * info.offset_size,
      info.offset_size);
  uint32_t next_offset = readLittleEndianU32(
      value.data() + info.offset_start_offset + (index + 1) * info.offset_size,
      info.offset_size);

  // Create a VariantValue for the element
  VariantValue element_value{
      metadata, std::string_view(value.data() + info.data_start_offset + offset,
                                 next_offset - offset)};

  return element_value;
}

VariantValue VariantValue::getArrayValueByIndex(uint32_t index) const {
  ArrayInfo info = getArrayInfo();
  return getArrayValueByIndex(index, info);
}

}  // namespace parquet::variant
