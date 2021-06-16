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
#include <utility>

#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/util/bit_run_reader.h"

namespace arrow {
namespace compute {
namespace internal {

// Used in some kernels and testing - not provided by default in MSVC
// and _USE_MATH_DEFINES is not reliable with unity builds
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif
#ifndef M_PI_2
#define M_PI_2 1.57079632679489661923
#endif
#ifndef M_PI_4
#define M_PI_4 0.785398163397448309616
#endif

// An internal data structure for unpacking a primitive argument to pass to a
// kernel implementation
struct PrimitiveArg {
  const uint8_t* is_valid;
  // If the bit_width is a multiple of 8 (i.e. not boolean), then "data" should
  // be shifted by offset * (bit_width / 8). For bit-packed data, the offset
  // must be used when indexing.
  const uint8_t* data;
  int bit_width;
  int64_t length;
  int64_t offset;
  // This may be kUnknownNullCount if the null_count has not yet been computed,
  // so use null_count != 0 to determine "may have nulls".
  int64_t null_count;
};

// Get validity bitmap data or return nullptr if there is no validity buffer
const uint8_t* GetValidityBitmap(const ArrayData& data);

int GetBitWidth(const DataType& type);

// Reduce code size by dealing with the unboxing of the kernel inputs once
// rather than duplicating compiled code to do all these in each kernel.
PrimitiveArg GetPrimitiveArg(const ArrayData& arr);

// Augment a unary ArrayKernelExec which supports only array-like inputs with support for
// scalar inputs. Scalars will be transformed to 1-long arrays with the scalar's value (or
// null if the scalar is null) as its only element. This 1-long array will be passed to
// the original exec, then the only element of the resulting array will be extracted as
// the output scalar. This could be far more efficient, but instead of optimizing this
// it'd be better to support scalar inputs "upstream" in original exec.
ArrayKernelExec TrivialScalarUnaryAsArraysExec(
    ArrayKernelExec exec, NullHandling::type null_handling = NullHandling::INTERSECTION);

// Return (min, max) of a numerical array, ignore nulls.
// For empty array, return the maximal number limit as 'min', and minimal limit as 'max'.
template <typename T>
ARROW_NOINLINE std::pair<T, T> GetMinMax(const ArrayData& data) {
  T min = std::numeric_limits<T>::max();
  T max = std::numeric_limits<T>::lowest();

  const T* values = data.GetValues<T>(1);
  arrow::internal::VisitSetBitRunsVoid(data.buffers[0], data.offset, data.length,
                                       [&](int64_t pos, int64_t len) {
                                         for (int64_t i = 0; i < len; ++i) {
                                           min = std::min(min, values[pos + i]);
                                           max = std::max(max, values[pos + i]);
                                         }
                                       });

  return std::make_pair(min, max);
}

template <typename T>
std::pair<T, T> GetMinMax(const Datum& datum) {
  T min = std::numeric_limits<T>::max();
  T max = std::numeric_limits<T>::lowest();

  for (const auto& array : datum.chunks()) {
    T local_min, local_max;
    std::tie(local_min, local_max) = GetMinMax<T>(*array->data());
    min = std::min(min, local_min);
    max = std::max(max, local_max);
  }

  return std::make_pair(min, max);
}

// Count value occurrences of an array, ignore nulls.
// 'counts' must be zeroed and with enough size.
template <typename T>
ARROW_NOINLINE int64_t CountValues(uint64_t* counts, const ArrayData& data, T min) {
  const int64_t n = data.length - data.GetNullCount();
  if (n > 0) {
    const T* values = data.GetValues<T>(1);
    arrow::internal::VisitSetBitRunsVoid(data.buffers[0], data.offset, data.length,
                                         [&](int64_t pos, int64_t len) {
                                           for (int64_t i = 0; i < len; ++i) {
                                             ++counts[values[pos + i] - min];
                                           }
                                         });
  }
  return n;
}

template <typename T>
int64_t CountValues(uint64_t* counts, const Datum& datum, T min) {
  int64_t n = 0;
  for (const auto& array : datum.chunks()) {
    n += CountValues<T>(counts, *array->data(), min);
  }
  return n;
}

// Copy numerical array values to a buffer, ignore nulls.
template <typename T>
ARROW_NOINLINE int64_t CopyNonNullValues(const ArrayData& data, T* out) {
  const int64_t n = data.length - data.GetNullCount();
  if (n > 0) {
    int64_t index = 0;
    const T* values = data.GetValues<T>(1);
    arrow::internal::VisitSetBitRunsVoid(
        data.buffers[0], data.offset, data.length, [&](int64_t pos, int64_t len) {
          memcpy(out + index, values + pos, len * sizeof(T));
          index += len;
        });
  }
  return n;
}

template <typename T>
int64_t CopyNonNullValues(const Datum& datum, T* out) {
  int64_t n = 0;
  for (const auto& array : datum.chunks()) {
    n += CopyNonNullValues(*array->data(), out + n);
  }
  return n;
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
