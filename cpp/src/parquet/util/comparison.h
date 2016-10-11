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

#ifndef PARQUET_UTIL_COMPARISON_H
#define PARQUET_UTIL_COMPARISON_H

#include <algorithm>

#include "parquet/schema/descriptor.h"
#include "parquet/types.h"

namespace parquet {

template <typename T>
struct Compare {
  explicit Compare(const ColumnDescriptor* descr) : type_length_(descr->type_length()) {}

  inline bool operator()(const T& a, const T& b) { return a < b; }

 private:
  int32_t type_length_;
};

template <>
inline bool Compare<Int96>::operator()(const Int96& a, const Int96& b) {
  return std::lexicographical_compare(a.value, a.value + 3, b.value, b.value + 3);
}

template <>
inline bool Compare<ByteArray>::operator()(const ByteArray& a, const ByteArray& b) {
  auto aptr = reinterpret_cast<const int8_t*>(a.ptr);
  auto bptr = reinterpret_cast<const int8_t*>(b.ptr);
  return std::lexicographical_compare(aptr, aptr + a.len, bptr, bptr + b.len);
}

template <>
inline bool Compare<FLBA>::operator()(const FLBA& a, const FLBA& b) {
  auto aptr = reinterpret_cast<const int8_t*>(a.ptr);
  auto bptr = reinterpret_cast<const int8_t*>(b.ptr);
  return std::lexicographical_compare(
      aptr, aptr + type_length_, bptr, bptr + type_length_);
}

}  // namespace parquet

#endif  // PARQUET_UTIL_COMPARISON_H
