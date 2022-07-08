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

#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

#include "arrow/array/data.h"

namespace arrow {
namespace rle_util {

int64_t FindPhysicalOffset(const int64_t* accumulated_run_lengths,
                           int64_t physical_length, int64_t logical_offset);

const int64_t* AccumulatedRunLengths(const ArraySpan& span) {
  return span.GetValues<const int64_t>(0);
}

const ArraySpan& RunsArray(const ArraySpan& span) {
  return span.child_data[0];
}

template <typename CallbackType>
void VisitMergedRuns(const ArraySpan &a, const ArraySpan &b, CallbackType callback) {
  int64_t a_physical_offset = rle_util::FindPhysicalOffset(AccumulatedRunLengths(a),
                                                           RunsArray(a).length, a.offset);
  int64_t b_physical_offset = rle_util::FindPhysicalOffset(AccumulatedRunLengths(b),
                                                           RunsArray(b).length, b.offset);

  int64_t logical_length = a.length;


  int64_t a_run_index = 0;
  int64_t b_run_index = 0;
  int64_t logical_position = 0;

  while (logical_position < logical_length) {
    const int64_t a_run_end = a[a_run_index] - logical_offset;
    const int64_t b_run_end = b[b_run_index] - logical_offset;

    ARROW_DCHECK_GT(a_run_end, logical_position);
    ARROW_DCHECK_GT(b_run_end, logical_position);

    const int64_t merged_run_end = std::min(a_run_end, b_run_end);
    const int64_t merged_run_length = merged_run_end - logical_position;
    callback(merged_run_length, a_run_index, b_run_index);

    logical_position = merged_run_end;
    if (logical_position == a_run_end) {
      a_run_index++;
    }
    if (logical_position == b_run_end) {
      b_run_index++;
    }
  }
}

}  // namespace rle_util
}  // namespace arrow
