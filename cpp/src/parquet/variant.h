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
  Timestamp = 12,
  /// Equivalent Parquet Type: TIMESTAMP(isAdjustedToUTC=false, MICROS)
  TimestampNtz = 13,
  /// Equivalent Parquet Type: FLOAT
  Float = 14,
  /// Equivalent Parquet Type: BINARY
  Binary = 15,
  /// Equivalent Parquet Type: STRING
  String = 16,
  /// Equivalent Parquet Type: TIME(isAdjustedToUTC=false, MICROS)
  TimeNtz = 17,
  /// Equivalent Parquet Type: TIMESTAMP(isAdjustedToUTC=true, NANOS)
  TimestampTz = 18,  // Assuming TZ stands for TimeZone, and follows the document's
                     // 'timestamp with time zone'
  /// Equivalent Parquet Type: TIMESTAMP(isAdjustedToUTC=false, NANOS)
  TimestampNtzNanos = 19,  // Differentiating from TimestampNtz (MICROS)
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
  TimestampTz,
  TimestampNtz,
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
  ///        dictionary size).
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

struct PARQUET_EXPORT VariantValue {
  VariantMetadata metadata;
  std::string_view value;

  VariantBasicType getBasicType() const;
  VariantType getType() const;
  std::string typeDebugString() const;

  /// \defgroup ValueAccessors
  /// @{

  // Note: Null doesn't need visitor.
  bool getBool() const;
  int8_t getInt8() const;
  int16_t getInt16() const;
  int32_t getInt32() const;
  int64_t getInt64() const;
  /// Include short_string optimization and primitive string type
  std::string_view getString() const;
  std::string_view getBinary() const;
  float getFloat() const;
  double getDouble() const;

  DecimalValue<::arrow::Decimal32> getDecimal4() const;
  DecimalValue<::arrow::Decimal64> getDecimal8() const;
  DecimalValue<::arrow::Decimal128> getDecimal16() const;

  int32_t getDate() const;
  int64_t getTimeNtz() const;
  // timestamp with adjusted to UTC
  int64_t getTimestamp() const;
  int64_t getTimestampNtz() const;
  // 16 bytes UUID
  std::array<uint8_t, 16> getUuid() const;

  /// }@

  struct ObjectInfo {
    uint32_t num_elements;
    uint32_t id_size;
    uint32_t offset_size;
    uint32_t id_start_offset;
    uint32_t offset_start_offset;
    uint32_t data_start_offset;

    bool operator==(const ObjectInfo& info) const {
      return num_elements == info.num_elements && id_size == info.id_size &&
             offset_size == info.offset_size && id_start_offset == info.id_start_offset &&
             offset_start_offset == info.offset_start_offset &&
             data_start_offset == info.data_start_offset;
    }

    std::string toDebugString() const;
  };
  ObjectInfo getObjectInfo() const;
  std::optional<VariantValue> getObjectValueByKey(std::string_view key) const;
  std::optional<VariantValue> getObjectValueByKey(std::string_view key,
                                                  const ObjectInfo& info) const;
  std::optional<VariantValue> getObjectFieldByFieldId(uint32_t variant_id) const;
  std::optional<VariantValue> getObjectFieldByFieldId(uint32_t variant_id,
                                                      const ObjectInfo& info) const;

  struct ArrayInfo {
    uint32_t num_elements;
    uint32_t offset_size;
    uint32_t offset_start_offset;
    uint32_t data_start_offset;
  };
  ArrayInfo getArrayInfo() const;
  // Would throw ParquetException if index is out of range.
  VariantValue getArrayValueByIndex(uint32_t index) const;
  VariantValue getArrayValueByIndex(uint32_t index, const ArrayInfo& info) const;

 private:
  static constexpr uint8_t kBasicTypeMask = 0b00000011;
  static constexpr uint8_t kPrimitiveTypeMask = 0b00111111;
  /** The inclusive maximum value of the type info value. It is the size limit of
   * `SHORT_STR`. */
  static constexpr uint8_t kMaxShortStrSizeMask = 0b00111111;

 private:
  template <typename PrimitiveType>
  PrimitiveType getPrimitiveType(VariantPrimitiveType type) const;

  // An extra function because decimal uses 1 byte for scale.
  template <typename DecimalType>
  DecimalValue<DecimalType> getPrimitiveDecimalType(VariantPrimitiveType type) const;

  // An extra function because binary/string uses 4 bytes for length.
  std::string_view getPrimitiveBinaryType(VariantPrimitiveType type) const;
  void checkBasicType(VariantBasicType type) const;
  void checkPrimitiveType(VariantPrimitiveType type, size_t size_required) const;
};

}  // namespace parquet::variant
