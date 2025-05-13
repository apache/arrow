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

#include "arrow/util/endian.h"
#include "parquet/exception.h"

namespace parquet::variant {

VariantMetadata::VariantMetadata(std::string_view metadata) : metadata_(metadata) {
  if (metadata.size() < 2) {
    throw ParquetException("Invalid Variant metadata: too short: " +
                           std::to_string(metadata.size()));
  }
}

int8_t VariantMetadata::version() const {
  return static_cast<int8_t>(metadata_[0]) & 0x0F;
}

bool VariantMetadata::sortedStrings() const { return (metadata_[0] & 0b10000) != 0; }

uint8_t VariantMetadata::offsetSize() const { return ((metadata_[0] >> 6) & 0x3) + 1; }

uint32_t VariantMetadata::dictionarySize() const {
  uint8_t length = offsetSize();
  if (length > 4) {
    throw ParquetException("Invalid offset size: " + std::to_string(length));
  }
  if (length + 1 > metadata_.size()) {
    throw ParquetException("Invalid Variant metadata: too short for dictionary size");
  }
  uint32_t dict_size = 0;
  memcpy(&dict_size, metadata_.data() + 1, length);
  dict_size = arrow::bit_util::FromLittleEndian(dict_size);
  return dict_size;
}

std::string_view VariantMetadata::getMetadataKey(int32_t variantId) const {
  uint32_t offset_size = offsetSize();
  uint32_t dict_size = dictionarySize();

  if (variantId < 0 || variantId >= static_cast<int32_t>(dict_size)) {
    throw ParquetException("Invalid Variant metadata: variantId out of range");
  }

  if ((dict_size + 1) * offset_size > metadata_.size()) {
    throw ParquetException("Invalid Variant metadata: offset out of range");
  }

  size_t offset_start_pos = 1 + offset_size + (variantId * offset_size);

  uint32_t variant_offset = 0;
  uint32_t variant_next_offset = 0;
  memcpy(&variant_offset, metadata_.data() + offset_start_pos, offset_size);
  variant_offset = arrow::bit_util::FromLittleEndian(variant_offset);
  memcpy(&variant_next_offset, metadata_.data() + offset_start_pos + offset_size,
         offset_size);
  variant_next_offset = arrow::bit_util::FromLittleEndian(variant_next_offset);

  uint32_t key_size = variant_next_offset - variant_offset;

  size_t string_start = 1 + offset_size * (dict_size + 2) + variant_offset;
  if (string_start + key_size > metadata_.size()) {
    throw ParquetException("Invalid Variant metadata: string data out of range");
  }
  return std::string_view(metadata_.data() + string_start, key_size);
}

VariantBasicType VariantValue::getBasicType() const {
  if (value.empty()) {
    throw ParquetException("Empty variant value");
  }
  return static_cast<VariantBasicType>(value[0] & BASIC_TYPE_MASK);
}

VariantType VariantValue::getType() const {
  VariantBasicType basic_type = getBasicType();
  std::cout << "Variant first byte:" << static_cast<int>(value[0] >> 2) << ", "
            << (value[0] && BASIC_TYPE_MASK) << '\n';
  switch (basic_type) {
    case VariantBasicType::Primitive: {
      auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
      switch (primitive_type) {
        case VariantPrimitiveType::NullType:
          return VariantType::VARIANT_NULL;
        case VariantPrimitiveType::BooleanTrue:
        case VariantPrimitiveType::BooleanFalse:
          return VariantType::BOOLEAN;
        case VariantPrimitiveType::Int8:
          return VariantType::BYTE;
        case VariantPrimitiveType::Int16:
          return VariantType::SHORT;
        case VariantPrimitiveType::Int32:
          return VariantType::INT;
        case VariantPrimitiveType::Int64:
          return VariantType::LONG;
        case VariantPrimitiveType::Double:
          return VariantType::DOUBLE;
        case VariantPrimitiveType::Decimal4:
          return VariantType::DECIMAL4;
        case VariantPrimitiveType::Decimal8:
          return VariantType::DECIMAL8;
        case VariantPrimitiveType::Decimal16:
          return VariantType::DECIMAL16;
        case VariantPrimitiveType::Date:
          return VariantType::DATE;
        case VariantPrimitiveType::Timestamp:
          return VariantType::TIMESTAMP_TZ;
        case VariantPrimitiveType::TimestampNTZ:
          return VariantType::TIMESTAMP_NTZ;
        case VariantPrimitiveType::Float:
          return VariantType::FLOAT;
        case VariantPrimitiveType::Binary:
          return VariantType::BINARY;
        case VariantPrimitiveType::String:
          return VariantType::STRING;
        case VariantPrimitiveType::TimeNTZ:
          return VariantType::TIME;
        case VariantPrimitiveType::TimestampTZ:
          return VariantType::TIMESTAMP_NANOS_TZ;
        case VariantPrimitiveType::TimestampNTZNanos:
          return VariantType::TIMESTAMP_NANOS_NTZ;
        case VariantPrimitiveType::Uuid:
          return VariantType::UUID;
        default:
          throw ParquetException("Unknown primitive type: " +
                                 std::to_string(static_cast<int>(primitive_type)));
      }
    }
    case VariantBasicType::ShortString:
      return VariantType::STRING;
    case VariantBasicType::Object:
      return VariantType::OBJECT;
    case VariantBasicType::Array:
      return VariantType::ARRAY;
    default:
      throw ParquetException("Unknown basic type: " +
                             std::to_string(static_cast<int>(basic_type)));
  }
}

std::string VariantValue::typeDebugString() const {
  VariantType type = getType();
  switch (type) {
    case VariantType::OBJECT:
      return "OBJECT";
    case VariantType::ARRAY:
      return "ARRAY";
    case VariantType::VARIANT_NULL:
      return "NULL";
    case VariantType::BOOLEAN:
      return "BOOLEAN";
    case VariantType::BYTE:
      return "BYTE";
    case VariantType::SHORT:
      return "SHORT";
    case VariantType::INT:
      return "INT";
    case VariantType::LONG:
      return "LONG";
    case VariantType::STRING:
      return "STRING";
    case VariantType::DOUBLE:
      return "DOUBLE";
    case VariantType::DECIMAL4:
      return "DECIMAL4";
    case VariantType::DECIMAL8:
      return "DECIMAL8";
    case VariantType::DECIMAL16:
      return "DECIMAL16";
    case VariantType::DATE:
      return "DATE";
    case VariantType::TIMESTAMP_TZ:
      return "TIMESTAMP_TZ";
    case VariantType::TIMESTAMP_NTZ:
      return "TIMESTAMP_NTZ";
    case VariantType::FLOAT:
      return "FLOAT";
    case VariantType::BINARY:
      return "BINARY";
    case VariantType::TIME:
      return "TIME";
    case VariantType::TIMESTAMP_NANOS_TZ:
      return "TIMESTAMP_NANOS_TZ";
    case VariantType::TIMESTAMP_NANOS_NTZ:
      return "TIMESTAMP_NANOS_NTZ";
    case VariantType::UUID:
      return "UUID";
    default:
      return "UNKNOWN";
  }
}

bool VariantValue::getBool() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type == VariantPrimitiveType::BooleanTrue) {
    return true;
  } else if (primitive_type == VariantPrimitiveType::BooleanFalse) {
    return false;
  }

  throw ParquetException("Not a boolean type");
}

int8_t VariantValue::getInt8() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Int8) {
    throw ParquetException("Not an Int8 type");
  }

  if (value.size() < 2) {
    throw ParquetException("Invalid Int8 value: too short");
  }

  return static_cast<int8_t>(value[1]);
}

int16_t VariantValue::getInt16() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Int16) {
    throw ParquetException("Not an Int16 type");
  }

  if (value.size() < 3) {
    throw ParquetException("Invalid Int16 value: too short");
  }

  int16_t result;
  memcpy(&result, value.data() + 1, sizeof(int16_t));
  return arrow::bit_util::FromLittleEndian(result);
}

int32_t VariantValue::getInt32() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Int32) {
    throw ParquetException("Not an Int32 type");
  }

  if (value.size() < 5) {
    throw ParquetException("Invalid Int32 value: too short");
  }

  int32_t result;
  memcpy(&result, value.data() + 1, sizeof(int32_t));
  return arrow::bit_util::FromLittleEndian(result);
}

int64_t VariantValue::getInt64() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Int64) {
    throw ParquetException("Not an Int64 type");
  }

  if (value.size() < 9) {
    throw ParquetException("Invalid Int64 value: too short");
  }

  int64_t result;
  memcpy(&result, value.data() + 1, sizeof(int64_t));
  return arrow::bit_util::FromLittleEndian(result);
}

std::string_view VariantValue::getString() const {
  VariantBasicType basic_type = getBasicType();

  if (basic_type == VariantBasicType::ShortString) {
    uint8_t length = (value[0] >> 2) & MAX_SHORT_STR_SIZE_MASK;
    if (value.size() < length + 1) {
      throw ParquetException("Invalid short string: too short");
    }
    return std::string_view(value.data() + 1, length);
  } else if (basic_type == VariantBasicType::Primitive) {
    auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
    if (primitive_type != VariantPrimitiveType::String) {
      throw ParquetException("Not a string type");
    }

    if (value.size() < 5) {
      throw ParquetException("Invalid string value: too short");
    }

    uint32_t length;
    memcpy(&length, value.data() + 1, sizeof(uint32_t));
    length = arrow::bit_util::FromLittleEndian(length);

    if (value.size() < length + 5) {
      throw ParquetException("Invalid string value: too short for specified length");
    }

    return std::string_view(value.data() + 5, length);
  }

  throw ParquetException("Not a string type");
}

std::string_view VariantValue::getBinary() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Binary) {
    throw ParquetException("Not a binary type");
  }

  if (value.size() < 5) {
    throw ParquetException("Invalid binary value: too short");
  }

  uint32_t length;
  memcpy(&length, value.data() + 1, sizeof(uint32_t));
  length = arrow::bit_util::FromLittleEndian(length);

  if (value.size() < length + 5) {
    throw ParquetException("Invalid binary value: too short for specified length");
  }

  return std::string_view(value.data() + 5, length);
}

float VariantValue::getFloat() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Float) {
    throw ParquetException("Not a float type");
  }

  if (value.size() < 5) {
    throw ParquetException("Invalid float value: too short");
  }

  float result;
  memcpy(&result, value.data() + 1, sizeof(float));
  return arrow::bit_util::FromLittleEndian(result);
}

double VariantValue::getDouble() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Double) {
    throw ParquetException("Not a double type");
  }

  if (value.size() < 9) {
    throw ParquetException("Invalid double value: too short");
  }

  double result;
  memcpy(&result, value.data() + 1, sizeof(double));
  return arrow::bit_util::FromLittleEndian(result);
}

DecimalValue<::arrow::Decimal32> VariantValue::getDecimal4() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Decimal4) {
    throw ParquetException("Not a decimal4 type");
  }

  if (value.size() < 6) {
    throw ParquetException("Invalid decimal4 value: too short");
  }

  uint8_t scale = value[1];
  int32_t decimal_value;
  memcpy(&decimal_value, value.data() + 2, sizeof(int32_t));
  decimal_value = arrow::bit_util::FromLittleEndian(decimal_value);

  return {scale, ::arrow::Decimal32(decimal_value)};
}

DecimalValue<::arrow::Decimal64> VariantValue::getDecimal8() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Decimal8) {
    throw ParquetException("Not a decimal8 type");
  }

  if (value.size() < 10) {
    throw ParquetException("Invalid decimal8 value: too short");
  }

  uint8_t scale = value[1];
  int64_t decimal_value;
  memcpy(&decimal_value, value.data() + 2, sizeof(int64_t));
  decimal_value = arrow::bit_util::FromLittleEndian(decimal_value);

  return {scale, ::arrow::Decimal64(decimal_value)};
}

DecimalValue<::arrow::Decimal128> VariantValue::getDecimal16() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Decimal16) {
    throw ParquetException("Not a decimal16 type");
  }

  if (value.size() < 18) {
    throw ParquetException("Invalid decimal16 value: too short");
  }

  uint8_t scale = value[1];

  // Decimal128 is stored as two int64_t values (low bits, high bits)
  int64_t low_bits, high_bits;
  memcpy(&low_bits, value.data() + 2, sizeof(int64_t));
  memcpy(&high_bits, value.data() + 10, sizeof(int64_t));
  low_bits = arrow::bit_util::FromLittleEndian(low_bits);
  high_bits = arrow::bit_util::FromLittleEndian(high_bits);

  return {scale, ::arrow::Decimal128(high_bits, low_bits)};
}

int64_t VariantValue::timeNTZ() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::TimeNTZ) {
    throw ParquetException("Not a timeNTZ type");
  }

  if (value.size() < 9) {
    throw ParquetException("Invalid timeNTZ value: too short");
  }

  int64_t result;
  memcpy(&result, value.data() + 1, sizeof(int64_t));
  return arrow::bit_util::FromLittleEndian(result);
}

int64_t VariantValue::getTimestamp() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Timestamp) {
    throw ParquetException("Not a timestamp type");
  }

  if (value.size() < 9) {
    throw ParquetException("Invalid timestamp value: too short");
  }

  int64_t result;
  memcpy(&result, value.data() + 1, sizeof(int64_t));
  return arrow::bit_util::FromLittleEndian(result);
}

int64_t VariantValue::getTimestampNTZ() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::TimestampNTZ) {
    throw ParquetException("Not a timestampNTZ type");
  }

  if (value.size() < 9) {
    throw ParquetException("Invalid timestampNTZ value: too short");
  }

  int64_t result;
  memcpy(&result, value.data() + 1, sizeof(int64_t));
  return arrow::bit_util::FromLittleEndian(result);
}

const uint8_t* VariantValue::getUuid() const {
  if (getBasicType() != VariantBasicType::Primitive) {
    throw ParquetException("Not a primitive type");
  }

  auto primitive_type = static_cast<VariantPrimitiveType>(value[0] >> 2);
  if (primitive_type != VariantPrimitiveType::Uuid) {
    throw ParquetException("Not a UUID type");
  }

  if (value.size() < 17) {
    throw ParquetException("Invalid UUID value: too short");
  }

  return reinterpret_cast<const uint8_t*>(value.data() + 1);
}

VariantValue::ObjectInfo VariantValue::getObjectInfo() const {
  if (getBasicType() != VariantBasicType::Object) {
    throw ParquetException("Not an object type");
  }

  if (value.size() < 5) {
    throw ParquetException("Invalid object value: too short");
  }

  uint32_t num_elements;
  memcpy(&num_elements, value.data() + 1, sizeof(uint32_t));
  num_elements = arrow::bit_util::FromLittleEndian(num_elements);

  if (value.size() < 6) {
    throw ParquetException("Invalid object value: too short for id_size");
  }

  uint8_t id_size = value[5];

  if (value.size() < 7) {
    throw ParquetException("Invalid object value: too short for offset_size");
  }

  uint8_t offset_size = value[6];

  if (offset_size < 1 || offset_size > 4 || id_size < 1 || id_size > 4) {
    throw ParquetException("Invalid object value: invalid id_size or offset_size");
  }

  uint32_t id_start_offset = 7;
  uint32_t offset_start_offset = id_start_offset + num_elements * id_size;
  uint32_t data_start_offset = offset_start_offset + (num_elements + 1) * offset_size;

  return {num_elements,        id_size,          offset_size, id_start_offset,
          offset_start_offset, data_start_offset};
}

std::optional<VariantValue> VariantValue::getObjectValueByKey(
    std::string_view key) const {
  if (getBasicType() != VariantBasicType::Object) {
    throw ParquetException("Not an object type");
  }

  ObjectInfo info = getObjectInfo();

  for (uint32_t i = 0; i < info.num_elements; ++i) {
    std::string_view field_key;
    std::optional<VariantValue> field_value = getObjectFieldByFieldId(i, &field_key);

    if (field_key == key) {
      return field_value;
    }
  }

  return std::nullopt;
}

std::optional<VariantValue> VariantValue::getObjectFieldByFieldId(
    uint32_t variantId, std::string_view* key) const {
  ObjectInfo info = getObjectInfo();

  if (variantId >= info.num_elements) {
    throw ParquetException("Field ID out of range");
  }

  // Read the field ID
  uint32_t field_id = 0;
  memcpy(&field_id, value.data() + info.id_start_offset + variantId * info.id_size,
         info.id_size);
  field_id = arrow::bit_util::FromLittleEndian(field_id);

  // Get the key from metadata
  if (key != nullptr) {
    *key = metadata.getMetadataKey(field_id);
  }

  // Read the offset and next offset
  uint32_t offset = 0, next_offset = 0;
  memcpy(&offset, value.data() + info.offset_start_offset + variantId * info.offset_size,
         info.offset_size);
  memcpy(&next_offset,
         value.data() + info.offset_start_offset + (variantId + 1) * info.offset_size,
         info.offset_size);
  offset = arrow::bit_util::FromLittleEndian(offset);
  next_offset = arrow::bit_util::FromLittleEndian(next_offset);

  if (offset == next_offset) {
    // Field is not present (null)
    return std::nullopt;
  }

  if (info.data_start_offset + offset >= value.size() ||
      info.data_start_offset + next_offset > value.size() || offset > next_offset) {
    throw ParquetException("Invalid object field offsets");
  }

  // Create a VariantValue for the field
  VariantValue field_value{
      .metadata = metadata,
      .value = std::string_view(value.data() + info.data_start_offset + offset,
                                next_offset - offset)};

  return field_value;
}

VariantValue::ArrayInfo VariantValue::getArrayInfo() const {
  if (getBasicType() != VariantBasicType::Array) {
    throw ParquetException("Not an array type");
  }

  if (value.size() < 6) {
    throw ParquetException("Invalid array value: too short");
  }

  uint32_t num_elements;
  memcpy(&num_elements, value.data() + 1, sizeof(uint32_t));
  num_elements = arrow::bit_util::FromLittleEndian(num_elements);

  if (value.size() < 6) {
    throw ParquetException("Invalid array value: too short for offset_size");
  }

  uint8_t offset_size = value[5];

  if (offset_size < 1 || offset_size > 4) {
    throw ParquetException("Invalid array value: invalid offset_size");
  }

  uint32_t offset_start_offset = 6;
  uint32_t data_start_offset = offset_start_offset + (num_elements + 1) * offset_size;

  return {num_elements, offset_size, offset_start_offset, data_start_offset};
}

VariantValue VariantValue::getArrayValueByIndex(uint32_t index) const {
  ArrayInfo info = getArrayInfo();

  if (index >= info.num_elements) {
    throw ParquetException("Array index out of range");
  }

  // Read the offset and next offset
  uint32_t offset = 0, next_offset = 0;
  memcpy(&offset, value.data() + info.offset_start_offset + index * info.offset_size,
         info.offset_size);
  memcpy(&next_offset,
         value.data() + info.offset_start_offset + (index + 1) * info.offset_size,
         info.offset_size);
  offset = arrow::bit_util::FromLittleEndian(offset);
  next_offset = arrow::bit_util::FromLittleEndian(next_offset);

  if (info.data_start_offset + offset >= value.size() ||
      info.data_start_offset + next_offset > value.size() || offset > next_offset) {
    throw ParquetException("Invalid array element offsets");
  }

  // Create a VariantValue for the element
  VariantValue element_value{
      .metadata = metadata,
      .value = std::string_view(value.data() + info.data_start_offset + offset,
                                next_offset - offset)};

  return element_value;
}

}  // namespace parquet::variant
