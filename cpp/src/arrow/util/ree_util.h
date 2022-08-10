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

#include <algorithm>
#include <cstdint>

#include "arrow/array/data.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace ree_util {

/// \brief Get the physical offset from a logical offset given run end values using binary
/// search. Returns num_run_ends if the physical offset is not within the first
/// num_run_ends elements.
template <typename RunEndsType>
int64_t FindPhysicalOffset(const RunEndsType* run_ends, int64_t num_run_ends,
                           int64_t logical_offset);

/// \brief Get the physical offset of an REE ArraySpan. Warning: calling this may result
/// in in an O(log(N)) binary search on the run ends buffer
int64_t GetPhysicalOffset(const ArraySpan& span);

/// \brief Get the physical length of an REE ArraySpan. Avoid calling this method in a
/// context where you can easily calculate the value yourself. Calling this can result in
/// an O(log(N)) binary search on the run ends buffer
int64_t GetPhysicalLength(const ArraySpan& span);

/// \brief Get the child array holding the data values from an REE array
static inline const ArraySpan& RunEndsArray(const ArraySpan& span) {
  return span.child_data[0];
}

/// \brief Get a pointer to run ends values of an REE array
template <typename RunEndsType>
static inline const RunEndsType* RunEnds(const ArraySpan& span) {
  return RunEndsArray(span).GetValues<RunEndsType>(1);
}

/// \brief Get the child array holding the data values from an REE array
static inline const ArraySpan& ValuesArray(const ArraySpan& span) {
  return span.child_data[1];
}

}  // namespace ree_util
}  // namespace arrow
