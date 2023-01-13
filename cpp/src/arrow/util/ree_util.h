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
#include <cassert>
#include <cstdint>

#include "arrow/array/data.h"
#include "arrow/type_traits.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace ree_util {

/// \brief Get the child array holding the run ends from an REE array
inline const ArraySpan& RunEndsArray(const ArraySpan& span) { return span.child_data[0]; }

/// \brief Get the child array holding the data values from an REE array
inline const ArraySpan& ValuesArray(const ArraySpan& span) { return span.child_data[1]; }

/// \brief Get a pointer to run ends values of an REE array
template <typename RunEndsType>
const RunEndsType* RunEnds(const ArraySpan& span) {
  assert(RunEndsArray(span).type->id() == CTypeTraits<RunEndsType>::ArrowType::type_id);
  return RunEndsArray(span).GetValues<RunEndsType>(1);
}

namespace internal {

/// \brief Uses binary-search to find the physical offset given a logical offset
/// and run-end values
///
/// \return the physical offset or run_ends_size if the physical offset is not
/// found in run_ends
template <typename RunEndsType>
int64_t FindPhysicalIndex(const RunEndsType* run_ends, int64_t run_ends_size, int64_t i,
                          int64_t absolute_offset) {
  auto it = std::upper_bound(run_ends, run_ends + run_ends_size, absolute_offset + i);
  int64_t result = std::distance(run_ends, it);
  assert(result <= run_ends_size);
  return result;
}

/// \brief Uses binary-search to calculate the number of physical values (and
/// run-ends) necessary to represent the logical range of values from
/// offset to length
template <typename RunEndsType>
int64_t FindPhysicalLength(int64_t length, int64_t offset, const RunEndsType* run_ends,
                           int64_t run_ends_size) {
  // The physical length is calculated by finding the offset of the last element
  // and adding 1 to it, so first we ensure there is at least one element.
  if (length == 0) {
    return 0;
  }
  const int64_t physical_offset =
      FindPhysicalIndex<RunEndsType>(run_ends, run_ends_size, 0, offset);
  const int64_t physical_index_of_last = FindPhysicalIndex<RunEndsType>(
      run_ends + physical_offset, run_ends_size - physical_offset, length - 1, offset);

  assert(physical_index_of_last < run_ends_size - physical_offset);
  return physical_index_of_last + 1;
}

/// \brief Find the physical index into the values array of the REE ArraySpan
///
/// This function uses binary-search, so it has a O(log N) cost.
template <typename RunEndsType>
int64_t FindPhysicalIndex(const ArraySpan& span, int64_t i, int64_t absolute_offset) {
  const int64_t run_ends_size = RunEndsArray(span).length;
  return FindPhysicalIndex(RunEnds<RunEndsType>(span), run_ends_size, i, absolute_offset);
}

/// \brief Find the physical length of an REE ArraySpan
///
/// The physical length of an REE is the number of physical values (and
/// run-ends) necessary to represent the logical range of values from
/// offset to length.
///
/// Avoid calling this function if the physical length can be estabilished in
/// some other way (e.g. when iterating over the runs sequentially until the
/// end). This function uses binary-search, so it has a O(log N) cost.
template <typename RunEndsType>
int64_t FindPhysicalLength(const ArraySpan& span) {
  return FindPhysicalLength(
      /*logical_length=*/span.length,
      /*logical_offset=*/span.offset,
      /*run_ends=*/RunEnds<RunEndsType>(span),
      /*run_ends_size=*/RunEndsArray(span).length);
}

}  // namespace internal

/// \brief Find the physical index into the values array of the REE ArraySpan
///
/// This function uses binary-search, so it has a O(log N) cost.
int64_t FindPhysicalIndex(const ArraySpan& span, int64_t i, int64_t absolute_offset);

/// \brief Find the physical length of an REE ArraySpan
///
/// The physical length of an REE is the number of physical values (and
/// run-ends) necessary to represent the logical range of values from
/// offset to length.
///
/// Avoid calling this function if the physical length can be estabilished in
/// some other way (e.g. when iterating over the runs sequentially until the
/// end). This function uses binary-search, so it has a O(log N) cost.
int64_t FindPhysicalLength(const ArraySpan& span);

}  // namespace ree_util
}  // namespace arrow
