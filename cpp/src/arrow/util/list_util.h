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
#include <memory>
#include <utility>

#include "arrow/array/data.h"
#include "arrow/result.h"

namespace arrow {
namespace list_util {
namespace internal {

/// \brief Calculate the smallest continuous range of values used by the
/// var-length list-like input (list, map and list-view types).
///
/// \param input The input array such that is_var_length_list_like(input.type)
/// is true
/// \return A pair of (offset, length) describing the range
ARROW_EXPORT Result<std::pair<int64_t, int64_t>> RangeOfValuesUsed(
    const ArraySpan& input);

/// \brief Calculate the sum of the sizes of all valid lists or list-views
///
/// This is usually the same as the length of the RangeOfValuesUsed() range, but
/// it can be:
/// - Smaller: when the child array contains many values that are not
/// referenced by the lists or list-views in the parent array
/// - Greater: when the list-views share child array ranges
///
/// \param input The input array such that is_var_length_list_like(input.type)
/// is true
/// \return The sum of all list or list-view sizes
ARROW_EXPORT Result<int64_t> SumOfLogicalListSizes(const ArraySpan& input);

/// \brief Materialize a (large)list array from a list-view array
///
/// The returned array data has `dest_type_id`'s layout: freshly computed list
/// offsets and a child array gathering the values referenced by every view, in
/// list order. The value type is the input's, so that callers may cast the
/// child afterwards.
///
/// \param input The input list-view array
/// \param dest_type_id Either `Type::LIST` or `Type::LARGE_LIST`
/// \param pool MemoryPool used for the allocations
ARROW_EXPORT Result<std::shared_ptr<ArrayData>> ListFromListView(const ArraySpan& input,
                                                                 Type::type dest_type_id,
                                                                 MemoryPool* pool);

}  // namespace internal

}  // namespace list_util
}  // namespace arrow
