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
#include <string>

#include "parquet/thrift/parquet_types.h"

namespace parquet_cpp {

struct ByteArray {
  uint32_t len;
  const uint8_t* ptr;
};


static inline std::string ByteArrayToString(const ByteArray& a) {
  return std::string(reinterpret_cast<const char*>(a.ptr), a.len);
}

static inline int ByteCompare(const ByteArray& x1, const ByteArray& x2) {
  int len = std::min(x1.len, x2.len);
  int cmp = memcmp(x1.ptr, x2.ptr, len);
  if (cmp != 0) return cmp;
  if (len < x1.len) return 1;
  if (len < x2.len) return -1;
  return 0;
}

template <int TYPE>
struct type_traits {
};

template <>
struct type_traits<parquet::Type::BOOLEAN> {
  typedef bool value_type;
  static constexpr parquet::Type::type parquet_type = parquet::Type::BOOLEAN;

  static constexpr size_t value_byte_size = 1;
};

template <>
struct type_traits<parquet::Type::INT32> {
  typedef int32_t value_type;
  static constexpr parquet::Type::type parquet_type = parquet::Type::INT32;

  static constexpr size_t value_byte_size = 4;
};

template <>
struct type_traits<parquet::Type::INT64> {
  typedef int64_t value_type;
  static constexpr parquet::Type::type parquet_type = parquet::Type::INT64;

  static constexpr size_t value_byte_size = 8;
};

template <>
struct type_traits<parquet::Type::INT96> {
  // TODO
  typedef void* value_type;
  static constexpr parquet::Type::type parquet_type = parquet::Type::INT96;

  static constexpr size_t value_byte_size = 12;
};

template <>
struct type_traits<parquet::Type::FLOAT> {
  typedef float value_type;
  static constexpr parquet::Type::type parquet_type = parquet::Type::FLOAT;

  static constexpr size_t value_byte_size = 4;
};

template <>
struct type_traits<parquet::Type::DOUBLE> {
  typedef double value_type;
  static constexpr parquet::Type::type parquet_type = parquet::Type::DOUBLE;

  static constexpr size_t value_byte_size = 8;
};

template <>
struct type_traits<parquet::Type::BYTE_ARRAY> {
  typedef ByteArray value_type;
  static constexpr parquet::Type::type parquet_type = parquet::Type::BYTE_ARRAY;

  static constexpr size_t value_byte_size = sizeof(ByteArray);
};

} // namespace parquet_cpp

#endif // PARQUET_TYPES_H
