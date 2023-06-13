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

#include "arrow/array/data.h"
#include "arrow/result.h"

namespace arrow {
namespace list_util {

/// \brief Get the child array holding the values from a List or ListView array
inline const ArraySpan& ValuesArray(const ArraySpan& span) { return span.child_data[0]; }

namespace internal {

/// \brief Calculate the smallest continuous range of values used by the
/// var-length list-like input (list, map and list-view types).
///
/// \param input The input array such that is_var_length_list_like(input.type)
/// is true
/// \return A pair of (offset, length) describing the range
ARROW_EXPORT Result<std::pair<int64_t, int64_t>> RangeOfValuesUsed(
    const ArraySpan& input);

}  // namespace internal

}  // namespace list_util
}  // namespace arrow
