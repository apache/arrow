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
#include <string_view>

namespace parquet::variant {

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

enum class VariantPrimitiveType {
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
  TimestampNTZ = 13,
  /// Equivalent Parquet Type: FLOAT
  Float = 14,
  /// Equivalent Parquet Type: BINARY
  Binary = 15,
  /// Equivalent Parquet Type: STRING
  String = 16,
  /// Equivalent Parquet Type: TIME(isAdjustedToUTC=false, MICROS)
  TimeNTZ = 17,
  /// Equivalent Parquet Type: TIMESTAMP(isAdjustedToUTC=true, NANOS)
  TimestampTZ = 18,  // Assuming TZ stands for TimeZone, and follows the document's
                     // 'timestamp with time zone'
  /// Equivalent Parquet Type: TIMESTAMP(isAdjustedToUTC=false, NANOS)
  TimestampNTZNanos = 19,  // Differentiating from TimestampNTZ (MICROS)
  /// Equivalent Parquet Type: UUID
  Uuid = 20
};

// TODO(mwish): should I use ByteArray as interface here?
struct VariantMetadata {
  int8_t offset_size() const;
  bool sorted_strings() const;
  int8_t version(std::string_view metadata) const;
  int32_t dictionary_size(std::string_view metadata) const;
  int32_t offset(std::string_view metadata, int32_t offset_idx) const;
  std::string_view dictionary_bytes(std::string_view metadata) const;

  std::string_view metadata;
};

// TODO(mwish): Adding interface here.
struct VariantValue {
  std::string_view value;
};

}  // namespace parquet::variant