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

#include "arrow/type_traits.h"

#include <type_traits>
#include <vector>

namespace arrow {
namespace internal {

template <typename T, typename R = T>
using enable_if_c_integer =
    ::arrow::enable_if_t<std::integral_constant<bool, std::is_integral<T>::value>::value,
                         R>;

template <typename T>
inline enable_if_c_integer<T> Bisect(const int64_t index, const std::vector<T>& offsets) {
  // Like std::upper_bound(), but hand-written as it can help the compiler.
  // Search [lo, lo + n)
  T lo = 0;
  T n = static_cast<T>(offsets.size());
  while (n > 1) {
    const T m = n >> 1;
    const T mid = lo + m;
    if (static_cast<T>(index) >= offsets[mid]) {
      lo = mid;
      n -= m;
    } else {
      n = m;
    }
  }
  return lo;
}

template <typename T>
inline enable_if_c_integer<T, std::vector<T>> ConvertLengthsToOffsets(
    std::vector<T> lengths) {
  // Offsets are stored in-place
  T offset = 0;
  for (auto& v : lengths) {
    const auto this_length = v;
    v = offset;
    offset += this_length;
  }
  lengths.push_back(offset);
  return lengths;
}

}  // namespace internal
}  // namespace arrow
