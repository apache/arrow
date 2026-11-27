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

#include "arrow/record_batch.h"
#include "arrow/type_traits.h"

namespace arrow::acero {

// Map an integral time value onto uint64_t, preserving order.
//
// For a signed T this flips the sign bit within T's own width, which is a
// strictly increasing bijection from T onto the unsigned type of the same
// width, and then zero-extends. Biasing after conversion to uint64_t would
// not work: a negative t is sign-extended first, so `t + (1 << (W - 1))`
// wraps back into [0, 2^(W-1)) and collides with the non-negative values.
//
// Order is preserved exactly, so differences within a type are exact too,
// which is what the as-of join tolerance comparisons rely on.
template <typename T, enable_if_t<std::is_integral<T>::value, bool> = true>
inline uint64_t NormalizeTime(T t) {
  using Unsigned = typename std::make_unsigned<T>::type;
  auto normalized = static_cast<Unsigned>(t);
  if (std::is_signed<T>::value) {
    normalized = static_cast<Unsigned>(normalized ^
                                       (static_cast<Unsigned>(1) << (8 * sizeof(T) - 1)));
  }
  return static_cast<uint64_t>(normalized);
}

uint64_t GetTime(const RecordBatch* batch, Type::type time_type, int col, uint64_t row);

}  // namespace arrow::acero
