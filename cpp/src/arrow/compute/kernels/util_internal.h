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
#include <type_traits>
#include <utility>

#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/math_constants.h"

namespace arrow {

using internal::CountAndSetBits;
using internal::CountSetBits;

namespace compute {

class ScalarFunction;

namespace internal {

template <typename T>
using maybe_make_unsigned =
    typename std::conditional<std::is_integral<T>::value && !std::is_same<T, bool>::value,
                              std::make_unsigned<T>, std::common_type<T>>::type;

template <typename T, typename Unsigned = typename maybe_make_unsigned<T>::type>
constexpr Unsigned to_unsigned(T signed_) {
  return static_cast<Unsigned>(signed_);
}

// Return (min, max) of a numerical array, ignore nulls.
// For empty array, return the maximal number limit as 'min', and minimal limit as 'max'.
template <typename T>
ARROW_NOINLINE std::pair<T, T> GetMinMax(const ArraySpan& data) {
  T min = std::numeric_limits<T>::max();
  T max = std::numeric_limits<T>::lowest();

  const T* values = data.GetValues<T>(1);
  arrow::internal::VisitSetBitRunsVoid(data.buffers[0].data, data.offset, data.length,
                                       [&](int64_t pos, int64_t len) {
                                         for (int64_t i = 0; i < len; ++i) {
                                           min = std::min(min, values[pos + i]);
                                           max = std::max(max, values[pos + i]);
                                         }
                                       });

  return std::make_pair(min, max);
}

template <typename T>
std::pair<T, T> GetMinMax(const ChunkedArray& arr) {
  T min = std::numeric_limits<T>::max();
  T max = std::numeric_limits<T>::lowest();

  for (const auto& chunk : arr.chunks()) {
    T local_min, local_max;
    std::tie(local_min, local_max) = GetMinMax<T>(*chunk->data());
    min = std::min(min, local_min);
    max = std::max(max, local_max);
  }

  return std::make_pair(min, max);
}

// Count value occurrences of an array, ignore nulls.
// 'counts' must be zeroed and with enough size.
template <typename T>
ARROW_NOINLINE int64_t CountValues(const ArraySpan& data, T min, uint64_t* counts) {
  const int64_t n = data.length - data.GetNullCount();
  if (n > 0) {
    const T* values = data.GetValues<T>(1);
    arrow::internal::VisitSetBitRunsVoid(data.buffers[0].data, data.offset, data.length,
                                         [&](int64_t pos, int64_t len) {
                                           for (int64_t i = 0; i < len; ++i) {
                                             ++counts[values[pos + i] - min];
                                           }
                                         });
  }
  return n;
}

template <typename T>
int64_t CountValues(const ChunkedArray& values, T min, uint64_t* counts) {
  int64_t n = 0;
  for (const auto& array : values.chunks()) {
    n += CountValues<T>(*array->data(), min, counts);
  }
  return n;
}

// Copy numerical array values to a buffer, ignore nulls.
template <typename T>
ARROW_NOINLINE int64_t CopyNonNullValues(const ArraySpan& data, T* out) {
  const int64_t n = data.length - data.GetNullCount();
  if (n > 0) {
    int64_t index = 0;
    const T* values = data.GetValues<T>(1);
    arrow::internal::VisitSetBitRunsVoid(
        data.buffers[0].data, data.offset, data.length, [&](int64_t pos, int64_t len) {
          memcpy(out + index, values + pos, len * sizeof(T));
          index += len;
        });
  }
  return n;
}

template <typename T>
int64_t CopyNonNullValues(const ChunkedArray& arr, T* out) {
  int64_t n = 0;
  for (const auto& chunk : arr.chunks()) {
    n += CopyNonNullValues(*chunk->data(), out + n);
  }
  return n;
}

ExecValue GetExecValue(const Datum& value);

int64_t GetTrueCount(const ArraySpan& mask);

template <template <typename... Args> class KernelGenerator, typename Op>
ArrayKernelExec GenerateArithmeticFloatingPoint(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::FLOAT:
      return KernelGenerator<FloatType, FloatType, Op>::Exec;
    case Type::DOUBLE:
      return KernelGenerator<DoubleType, DoubleType, Op>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}

// A scalar kernel that ignores (assumed all-null) inputs and returns null.
void AddNullExec(ScalarFunction* func);

}  // namespace internal
}  // namespace compute
}  // namespace arrow
