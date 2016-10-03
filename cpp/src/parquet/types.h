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
#include <sstream>
#include <string>

#include "parquet/util/compiler-util.h"
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
    INTERVAL
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
  enum type { UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI };
};

// parquet::PageType
struct PageType {
  enum type { DATA_PAGE, INDEX_PAGE, DICTIONARY_PAGE, DATA_PAGE_V2 };
};

// ----------------------------------------------------------------------

struct ByteArray {
  ByteArray() : len(0), ptr(nullptr) {}
  ByteArray(uint32_t len, const uint8_t* ptr) : len(len), ptr(ptr) {}
  uint32_t len;
  const uint8_t* ptr;

  bool operator==(const ByteArray& other) const {
    return this->len == other.len && 0 == memcmp(this->ptr, other.ptr, this->len);
  }

  bool operator!=(const ByteArray& other) const {
    return this->len != other.len || 0 != memcmp(this->ptr, other.ptr, this->len);
  }
};

struct FixedLenByteArray {
  FixedLenByteArray() : ptr(nullptr) {}
  explicit FixedLenByteArray(const uint8_t* ptr) : ptr(ptr) {}
  const uint8_t* ptr;
};

typedef FixedLenByteArray FLBA;

MANUALLY_ALIGNED_STRUCT(1) Int96 {
  uint32_t value[3];

  bool operator==(const Int96& other) const {
    return 0 == memcmp(this->value, other.value, 3 * sizeof(uint32_t));
  }

  bool operator!=(const Int96& other) const { return !(*this == other); }
};
STRUCT_END(Int96, 12);

static inline std::string ByteArrayToString(const ByteArray& a) {
  return std::string(reinterpret_cast<const char*>(a.ptr), a.len);
}

static inline std::string Int96ToString(const Int96& a) {
  std::stringstream result;
  for (int i = 0; i < 3; i++) {
    result << a.value[i] << " ";
  }
  return result.str();
}

static inline std::string FixedLenByteArrayToString(const FixedLenByteArray& a, int len) {
  const uint8_t* bytes = reinterpret_cast<const uint8_t*>(a.ptr);
  std::stringstream result;
  for (int i = 0; i < len; i++) {
    result << (uint32_t)bytes[i] << " ";
  }
  return result.str();
}

static inline int ByteCompare(const ByteArray& x1, const ByteArray& x2) {
  uint32_t len = std::min(x1.len, x2.len);
  int cmp = memcmp(x1.ptr, x2.ptr, len);
  if (cmp != 0) return cmp;
  if (len < x1.len) return 1;
  if (len < x2.len) return -1;
  return 0;
}

template <int TYPE>
struct type_traits {};

template <>
struct type_traits<Type::BOOLEAN> {
  typedef bool value_type;
  static constexpr int value_byte_size = 1;

  static constexpr const char* printf_code = "d";
};

template <>
struct type_traits<Type::INT32> {
  typedef int32_t value_type;

  static constexpr int value_byte_size = 4;
  static constexpr const char* printf_code = "d";
};

template <>
struct type_traits<Type::INT64> {
  typedef int64_t value_type;

  static constexpr int value_byte_size = 8;
  static constexpr const char* printf_code = "ld";
};

template <>
struct type_traits<Type::INT96> {
  typedef Int96 value_type;

  static constexpr int value_byte_size = 12;
  static constexpr const char* printf_code = "s";
};

template <>
struct type_traits<Type::FLOAT> {
  typedef float value_type;

  static constexpr int value_byte_size = 4;
  static constexpr const char* printf_code = "f";
};

template <>
struct type_traits<Type::DOUBLE> {
  typedef double value_type;

  static constexpr int value_byte_size = 8;
  static constexpr const char* printf_code = "lf";
};

template <>
struct type_traits<Type::BYTE_ARRAY> {
  typedef ByteArray value_type;

  static constexpr int value_byte_size = sizeof(ByteArray);
  static constexpr const char* printf_code = "s";
};

template <>
struct type_traits<Type::FIXED_LEN_BYTE_ARRAY> {
  typedef FixedLenByteArray value_type;

  static constexpr int value_byte_size = sizeof(FixedLenByteArray);
  static constexpr const char* printf_code = "s";
};

template <Type::type TYPE>
struct DataType {
  static constexpr Type::type type_num = TYPE;
  typedef typename type_traits<TYPE>::value_type c_type;
};

typedef DataType<Type::BOOLEAN> BooleanType;
typedef DataType<Type::INT32> Int32Type;
typedef DataType<Type::INT64> Int64Type;
typedef DataType<Type::INT96> Int96Type;
typedef DataType<Type::FLOAT> FloatType;
typedef DataType<Type::DOUBLE> DoubleType;
typedef DataType<Type::BYTE_ARRAY> ByteArrayType;
typedef DataType<Type::FIXED_LEN_BYTE_ARRAY> FLBAType;

template <typename Type>
inline std::string format_fwf(int width) {
  std::stringstream ss;
  ss << "%-" << width << type_traits<Type::type_num>::printf_code;
  return ss.str();
}

std::string CompressionToString(Compression::type t);

std::string EncodingToString(Encoding::type t);

std::string LogicalTypeToString(LogicalType::type t);

std::string TypeToString(Type::type t);

std::string FormatStatValue(Type::type parquet_type, const char* val);

int GetTypeByteSize(Type::type t);
}  // namespace parquet

#endif  // PARQUET_TYPES_H
