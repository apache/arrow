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

#ifndef PARQUET_TYPES_H
#define PARQUET_TYPES_H

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <sstream>
#include <string>

#include "arrow/util/macros.h"

#include "parquet/util/macros.h"
#include "parquet/util/visibility.h"

namespace parquet {

// ----------------------------------------------------------------------
// Metadata enums to match Thrift metadata
//
// The reason we maintain our own enums is to avoid transitive dependency on
// the compiled Thrift headers (and thus thrift/Thrift.h) for users of the
// public API. After building parquet-cpp, you should not need to include
// Thrift headers in your application. This means some boilerplate to convert
// between our types and Parquet's Thrift types.
//
// We can also add special values like NONE to distinguish between metadata
// values being set and not set. As an example consider ConvertedType and
// CompressionCodec

// Mirrors parquet::Type
struct Type {
  enum type {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7
  };
};

// Mirrors parquet::ConvertedType
struct LogicalType {
  enum type {
    NONE,
    UTF8,
    MAP,
    MAP_KEY_VALUE,
    LIST,
    ENUM,
    DECIMAL,
    DATE,
    TIME_MILLIS,
    TIME_MICROS,
    TIMESTAMP_MILLIS,
    TIMESTAMP_MICROS,
    UINT_8,
    UINT_16,
    UINT_32,
    UINT_64,
    INT_8,
    INT_16,
    INT_32,
    INT_64,
    JSON,
    BSON,
    INTERVAL,
    NA = 25
  };
};

// Mirrors parquet::FieldRepetitionType
struct Repetition {
  enum type { REQUIRED = 0, OPTIONAL = 1, REPEATED = 2 };
};

// Data encodings. Mirrors parquet::Encoding
struct Encoding {
  enum type {
    PLAIN = 0,
    PLAIN_DICTIONARY = 2,
    RLE = 3,
    BIT_PACKED = 4,
    DELTA_BINARY_PACKED = 5,
    DELTA_LENGTH_BYTE_ARRAY = 6,
    DELTA_BYTE_ARRAY = 7,
    RLE_DICTIONARY = 8
  };
};

// Compression, mirrors parquet::CompressionCodec
struct Compression {
  enum type { UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD };
};

// parquet::PageType
struct PageType {
  enum type { DATA_PAGE, INDEX_PAGE, DICTIONARY_PAGE, DATA_PAGE_V2 };
};

// Reference:
// parquet-mr/parquet-hadoop/src/main/java/org/apache/parquet/
//                            format/converter/ParquetMetadataConverter.java
// Sort order for page and column statistics. Types are associated with sort
// orders (e.g., UTF8 columns should use UNSIGNED) and column stats are
// aggregated using a sort order. As of parquet-format version 2.3.1, the
// order used to aggregate stats is always SIGNED and is not stored in the
// Parquet file. These stats are discarded for types that need unsigned.
// See PARQUET-686.
struct SortOrder {
  enum type { SIGNED, UNSIGNED, UNKNOWN };
};

class ColumnOrder {
 public:
  enum type { UNDEFINED, TYPE_DEFINED_ORDER };
  explicit ColumnOrder(ColumnOrder::type column_order) : column_order_(column_order) {}
  // Default to Type Defined Order
  ColumnOrder() : column_order_(type::TYPE_DEFINED_ORDER) {}
  ColumnOrder::type get_order() { return column_order_; }

  static ColumnOrder undefined_;
  static ColumnOrder type_defined_;

 private:
  ColumnOrder::type column_order_;
};

// ----------------------------------------------------------------------

struct ByteArray {
  ByteArray() : len(0), ptr(nullptr) {}
  ByteArray(uint32_t len, const uint8_t* ptr) : len(len), ptr(ptr) {}
  uint32_t len;
  const uint8_t* ptr;
};

inline bool operator==(const ByteArray& left, const ByteArray& right) {
  return left.len == right.len && 0 == std::memcmp(left.ptr, right.ptr, left.len);
}

inline bool operator!=(const ByteArray& left, const ByteArray& right) {
  return !(left == right);
}

struct FixedLenByteArray {
  FixedLenByteArray() : ptr(nullptr) {}
  explicit FixedLenByteArray(const uint8_t* ptr) : ptr(ptr) {}
  const uint8_t* ptr;
};

using FLBA = FixedLenByteArray;

MANUALLY_ALIGNED_STRUCT(1) Int96 { uint32_t value[3]; };
STRUCT_END(Int96, 12);

inline bool operator==(const Int96& left, const Int96& right) {
  return std::equal(left.value, left.value + 3, right.value);
}

inline bool operator!=(const Int96& left, const Int96& right) { return !(left == right); }

static inline std::string ByteArrayToString(const ByteArray& a) {
  return std::string(reinterpret_cast<const char*>(a.ptr), a.len);
}

static inline std::string Int96ToString(const Int96& a) {
  std::ostringstream result;
  std::copy(a.value, a.value + 3, std::ostream_iterator<uint32_t>(result, " "));
  return result.str();
}

static inline std::string FixedLenByteArrayToString(const FixedLenByteArray& a, int len) {
  std::ostringstream result;
  std::copy(a.ptr, a.ptr + len, std::ostream_iterator<uint32_t>(result, " "));
  return result.str();
}

template <Type::type TYPE>
struct type_traits {};

template <>
struct type_traits<Type::BOOLEAN> {
  using value_type = bool;

  static constexpr int value_byte_size = 1;
  static constexpr const char* printf_code = "d";
};

template <>
struct type_traits<Type::INT32> {
  using value_type = int32_t;

  static constexpr int value_byte_size = 4;
  static constexpr const char* printf_code = "d";
};

template <>
struct type_traits<Type::INT64> {
  using value_type = int64_t;

  static constexpr int value_byte_size = 8;
  static constexpr const char* printf_code = "ld";
};

template <>
struct type_traits<Type::INT96> {
  using value_type = Int96;

  static constexpr int value_byte_size = 12;
  static constexpr const char* printf_code = "s";
};

template <>
struct type_traits<Type::FLOAT> {
  using value_type = float;

  static constexpr int value_byte_size = 4;
  static constexpr const char* printf_code = "f";
};

template <>
struct type_traits<Type::DOUBLE> {
  using value_type = double;

  static constexpr int value_byte_size = 8;
  static constexpr const char* printf_code = "lf";
};

template <>
struct type_traits<Type::BYTE_ARRAY> {
  using value_type = ByteArray;

  static constexpr int value_byte_size = sizeof(ByteArray);
  static constexpr const char* printf_code = "s";
};

template <>
struct type_traits<Type::FIXED_LEN_BYTE_ARRAY> {
  using value_type = FixedLenByteArray;

  static constexpr int value_byte_size = sizeof(FixedLenByteArray);
  static constexpr const char* printf_code = "s";
};

template <Type::type TYPE>
struct DataType {
  using c_type = typename type_traits<TYPE>::value_type;
  static constexpr Type::type type_num = TYPE;
};

using BooleanType = DataType<Type::BOOLEAN>;
using Int32Type = DataType<Type::INT32>;
using Int64Type = DataType<Type::INT64>;
using Int96Type = DataType<Type::INT96>;
using FloatType = DataType<Type::FLOAT>;
using DoubleType = DataType<Type::DOUBLE>;
using ByteArrayType = DataType<Type::BYTE_ARRAY>;
using FLBAType = DataType<Type::FIXED_LEN_BYTE_ARRAY>;

template <typename Type>
inline std::string format_fwf(int width) {
  std::stringstream ss;
  ss << "%-" << width << type_traits<Type::type_num>::printf_code;
  return ss.str();
}

PARQUET_EXPORT std::string CompressionToString(Compression::type t);

PARQUET_EXPORT std::string EncodingToString(Encoding::type t);

PARQUET_EXPORT std::string LogicalTypeToString(LogicalType::type t);

PARQUET_EXPORT std::string TypeToString(Type::type t);

PARQUET_EXPORT std::string FormatStatValue(Type::type parquet_type,
                                           const std::string& val);

/// \deprecated Since 1.5.0
PARQUET_DEPRECATED("Use std::string instead of char* as input")
PARQUET_EXPORT std::string FormatStatValue(Type::type parquet_type, const char* val);

PARQUET_EXPORT int GetTypeByteSize(Type::type t);

PARQUET_EXPORT SortOrder::type DefaultSortOrder(Type::type primitive);

PARQUET_EXPORT SortOrder::type GetSortOrder(LogicalType::type converted,
                                            Type::type primitive);

}  // namespace parquet

#endif  // PARQUET_TYPES_H
