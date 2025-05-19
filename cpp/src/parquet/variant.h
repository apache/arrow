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

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include <arrow/util/decimal.h>
#include <arrow/util/small_vector.h>

#include "parquet/platform.h"

namespace parquet::variant {

// TODO(mwish): Should I use parquet::ByteArray rather than
//  std::string_view?

enum class VariantBasicType {
  /// One of the primitive types
  Primitive = 0,
  /// A string with a length less than 64 bytes
  ShortString = 1,
  /// A collection of (string-key, variant-value) pairs
  Object = 2,
  /// An ordered sequence of variant values
  Array = 3
};

PARQUET_EXPORT std::string VariantBasicTypeToString(VariantBasicType type);

enum class VariantPrimitiveType : int8_t {
  /// Equivalent Parquet Type: UNKNOWN
  NullType = 0,
  /// Equivalent Parquet Type: BOOLEAN
  BooleanTrue = 1,
  /// Equivalent Parquet Type: BOOLEAN
  BooleanFalse = 2,
  /// Equivalent Parquet Type: INT(8, signed)
  Int8 = 3,
  /// Equivalent Parquet Type: INT(16, signed)
  Int16 = 4,
  /// Equivalent Parquet Type: INT(32, signed)
  Int32 = 5,
  /// Equivalent Parquet Type: INT(64, signed)
  Int64 = 6,
  /// Equivalent Parquet Type: DOUBLE
  Double = 7,
  /// Equivalent Parquet Type: DECIMAL(precision, scale)
  Decimal4 = 8,
  /// Equivalent Parquet Type: DECIMAL(precision, scale)
  Decimal8 = 9,
  /// Equivalent Parquet Type: DECIMAL(precision, scale)
  Decimal16 = 10,
  /// Equivalent Parquet Type: DATE
  Date = 11,
  /// Equivalent Parquet Type: TIMESTAMP(isAdjustedToUTC=true, MICROS)
  TimestampMicros = 12,
  /// Equivalent Parquet Type: TIMESTAMP(isAdjustedToUTC=false, MICROS)
  TimestampMicrosNtz = 13,
  /// Equivalent Parquet Type: FLOAT
  Float = 14,
  /// Equivalent Parquet Type: BINARY
  Binary = 15,
  /// Equivalent Parquet Type: STRING
  String = 16,
  /// Equivalent Parquet Type: TIME(isAdjustedToUTC=false, MICROS)
  TimeMicrosNtz = 17,
  /// Equivalent Parquet Type: TIMESTAMP(isAdjustedToUTC=true, NANOS)
  TimestampNanosTz = 18,  // Assuming TZ stands for TimeZone, and follows the document's
                          // 'timestamp with time zone'
  /// Equivalent Parquet Type: TIMESTAMP(isAdjustedToUTC=false, NANOS)
  TimestampNanosNtz = 19,  // Differentiating from TimestampNtz (MICROS)
  /// Equivalent Parquet Type: UUID
  Uuid = 20
};

PARQUET_EXPORT std::string VariantPrimitiveTypeToString(VariantPrimitiveType type);

/// VariantType is from basic type and primitive type.
enum class VariantType {
  Object,
  Array,
  Null,
  Boolean,
  Int8,
  Int16,
  Int32,
  Int64,
  String,
  Double,
  Decimal4,
  Decimal8,
  Decimal16,
  Date,
  TimestampMicrosTz,
  TimestampMicrosNtz,
  Float,
  Binary,
  Time,
  TimestampNanosTz,
  TimestampNanosNtz,
  Uuid
};

PARQUET_EXPORT std::string VariantTypeToString(VariantType type);

class PARQUET_EXPORT VariantMetadata {
 public:
  explicit VariantMetadata(std::string_view metadata);
  /// \brief Get the variant metadata version. Currently, always 1.
  uint8_t version() const;
  /// \brief Get the metadata key for a given variant field id.
  /// \throw ParquetException if the variant_id is out of range(larger than
  ///        dictionary_size).
  std::string_view GetMetadataKey(uint32_t variant_id) const;
  /// \brief Get the metadata id for a given key.
  /// From the discussion in ML:
  /// https://lists.apache.org/thread/b68tjmrjmy64mbv9dknpmqs28vnzjj96 if
  /// !sorted_and_unique(), the metadata key is not guaranteed to be unique, so we use a
  /// vector to store all the metadata ids.
  ::arrow::internal::SmallVector<uint32_t, 1> GetMetadataId(std::string_view key) const;

  bool sorted_and_unique() const;
  uint8_t offset_size() const;
  uint32_t dictionary_size() const;

  /// Metadata for primitive types and any nested types
  /// without key dictionary.
  static constexpr char kEmptyMetadataChars[] = {0x1, 0x0, 0x0};

 private:
  static uint32_t loadDictionarySize(std::string_view metadata, uint8_t offset_size);

 private:
  static constexpr uint8_t kVersionMask = 0b1111;
  static constexpr uint8_t kSortedStringMask = 0b10000;
  static constexpr size_t kHeaderSizeBytes = 1;
  static constexpr size_t kMinimalOffsetSizeBytes = 1;
  static constexpr size_t kMaximumOffsetSizeBytes = 4;
  // mask is applied after shift, it's like 0b11000000 before shift.
  static constexpr uint8_t kOffsetSizeMask = 0b11;
  static constexpr uint8_t kOffsetSizeBitShift = 6;
  static constexpr uint8_t kSupportedVersion = 1;

 private:
  std::string_view metadata_;
  uint32_t dictionary_size_{0};
};

template <typename DecimalType>
struct PARQUET_EXPORT DecimalValue {
  uint8_t scale;
  DecimalType value;
};

class PARQUET_EXPORT VariantValue {
 public:
  VariantValue(std::string_view metadata, std::string_view value);

  VariantBasicType getBasicType() const;
  VariantType getType() const;
  std::string_view typeDebugString() const;
  const VariantMetadata& metadata() const;

  // Note: Null doesn't need visitor.

  /// \brief Get the primitive boolean value.
  /// \throw ParquetException if the type is not a boolean type.
  bool getBool() const;
  /// \brief Get the primitive int8 value.
  /// \throw ParquetException if the type is not an int8 type.
  int8_t getInt8() const;
  /// \brief Get the primitive int16 value.
  /// \throw ParquetException if the type is not an int16 type.
  int16_t getInt16() const;
  /// \brief Get the primitive int32 value.
  /// \throw ParquetException if the type is not an int32 type.
  int32_t getInt32() const;
  /// \brief Get the primitive int64 value.
  /// \throw ParquetException if the type is not an int64 type.
  int64_t getInt64() const;
  /// \brief Get the string value, including both short string optimization and primitive
  /// string type. \throw ParquetException if the type is not a string type.
  std::string_view getString() const;
  /// \brief Get the binary value.
  /// \throw ParquetException if the type is not a binary type.
  std::string_view getBinary() const;
  /// \brief Get the primitive float value.
  /// \throw ParquetException if the type is not a float type.
  float getFloat() const;
  /// \brief Get the primitive double value.
  /// \throw ParquetException if the type is not a double type.
  double getDouble() const;

  /// \brief Get the decimal value with 4 bytes precision.
  /// \throw ParquetException if the type is not a decimal4 type.
  DecimalValue<::arrow::Decimal32> getDecimal4() const;
  /// \brief Get the decimal value with 8 bytes precision.
  /// \throw ParquetException if the type is not a decimal8 type.
  DecimalValue<::arrow::Decimal64> getDecimal8() const;
  /// \brief Get the decimal value with 16 bytes precision.
  /// \throw ParquetException if the type is not a decimal16 type.
  DecimalValue<::arrow::Decimal128> getDecimal16() const;

  /// \brief Get the date value as days since Unix epoch.
  /// \throw ParquetException if the type is not a date type.
  int32_t getDate() const;
  /// \brief Get the time value without timezone as microseconds since midnight.
  /// \throw ParquetException if the type is not a time type.
  int64_t getTimeMicrosNtz() const;
  /// \brief Get the timestamp value with UTC timezone as microseconds since Unix epoch.
  /// \throw ParquetException if the type is not a timestamp type.
  int64_t getTimestampMicros() const;
  /// \brief Get the timestamp value without timezone as microseconds since Unix epoch.
  /// \throw ParquetException if the type is not a timestamp without timezone type.
  int64_t getTimestampMicrosNtz() const;
  /// \brief Get the timestamp value with UTC timezone as nanoseconds since Unix epoch.
  /// \throw ParquetException if the type is not a timestamp type.
  int64_t getTimestampNanosTz() const;
  /// \brief Get the timestamp value without timezone as nanoseconds since Unix epoch.
  /// \throw ParquetException if the type is not a timestamp without timezone type.
  int64_t getTimestampNanosNtz() const;
  /// \brief Get the UUID value as a 16-byte array.
  /// \throw ParquetException if the type is not a UUID type.
  std::array<uint8_t, 16> getUuid() const;

  /// \brief Get the num_elements of the array or object.
  ///        For array, it returns the number of elements in the array.
  ///        For object, it returns the number of fields in the object.
  /// \throw ParquetException if the type is not an array or object type.
  uint32_t num_elements() const;

  /// \brief Get the value of the object field by key.
  /// \return returns the value of the field with the given key, or empty if the key
  ///         doesn't exist.
  /// \throw ParquetException if the type is not an object type.
  std::optional<VariantValue> getObjectValueByKey(std::string_view key) const;
  /// \brief Get the value of the object field by field id.
  /// \return returns the value of the field with the given field id, or empty if the
  ///         field id doesn't exist.
  /// \throw ParquetException if the type is not an object type.
  std::optional<VariantValue> getObjectFieldByFieldId(uint32_t variant_id) const;

  // Would throw ParquetException if index is out of range.
  VariantValue getArrayValueByIndex(uint32_t index) const;

 private:
  static constexpr uint8_t kBasicTypeMask = 0b00000011;
  static constexpr uint8_t kPrimitiveTypeMask = 0b00111111;
  /// The inclusive maximum value of the type info value. It is the size limit of
  /// ShortString.
  static constexpr uint8_t kMaxShortStrSizeMask = 0b00111111;

  /// ComplexInfo is used to store the metadata of the array or object.
  /// For array, it doesn't have id_size and id_start_offset.
  struct ComplexInfo {
    uint32_t num_elements;
    uint32_t id_start_offset;
    uint32_t offset_start_offset;
    uint32_t data_start_offset;
    uint8_t id_size;
    uint8_t offset_size;
  };

 private:
  VariantValue(VariantMetadata metadata, std::string_view value);

  template <typename PrimitiveType>
  PrimitiveType getPrimitiveType(VariantPrimitiveType type) const;

  // An extra function because decimal uses 1 byte for scale.
  template <typename DecimalType>
  DecimalValue<DecimalType> getPrimitiveDecimalType(VariantPrimitiveType type) const;

  // An extra function because binary/string uses 4 bytes for length.
  std::string_view getPrimitiveBinaryType(VariantPrimitiveType type) const;
  void checkBasicType(VariantBasicType type) const;
  void checkPrimitiveType(VariantPrimitiveType type, size_t size_required) const;

  static ComplexInfo getArrayInfo(std::string_view value);
  static ComplexInfo getObjectInfo(std::string_view value);

 private:
  VariantMetadata metadata_;
  std::string_view value_;

  ComplexInfo complex_info_{};
};

}  // namespace parquet::variant
