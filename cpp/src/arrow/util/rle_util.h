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

static const int64_t* RunEnds(const ArraySpan& span) {
  return span.GetValues<const int64_t>(0, /*absolute_offset=*/0);
}

static const ArraySpan& DataArray(const ArraySpan& span) { return span.child_data[0]; }

template <typename CallbackType>
void VisitMergedRuns(const ArraySpan& a, const ArraySpan& b, CallbackType callback) {
  const int64_t a_physical_offset =
      rle_util::FindPhysicalOffset(RunEnds(a), DataArray(a).length, a.offset);
  const int64_t b_physical_offset =
      rle_util::FindPhysicalOffset(RunEnds(b), DataArray(b).length, b.offset);

  ARROW_DCHECK_EQ(a.length, b.length);
  const int64_t logical_length = a.length;

  // run indices from the start of the run ends buffers without offset
  int64_t a_run_index = a_physical_offset;
  int64_t b_run_index = b_physical_offset;
  int64_t logical_position = 0;

  while (logical_position < logical_length) {
    // logical indices of the end of the run we are currently in in each input
    const int64_t a_run_end = RunEnds(a)[a_run_index] - a.offset;
    const int64_t b_run_end = RunEnds(b)[b_run_index] - b.offset;

    ARROW_DCHECK_GT(a_run_end, logical_position);
    ARROW_DCHECK_GT(b_run_end, logical_position);

    const int64_t merged_run_end = std::min(a_run_end, b_run_end);
    const int64_t merged_run_length = merged_run_end - logical_position;

    // callback to code that wants to work on the data - we give it physical indices
    // including all offsets. This includes the additional offset the data array may have.
    callback(merged_run_length, a_run_index + DataArray(a).offset,
             b_run_index + DataArray(b).offset);

    logical_position = merged_run_end;
    if (logical_position == a_run_end) {
      a_run_index++;
    }
    if (logical_position == b_run_end) {
      b_run_index++;
    }
  }
}

// TODO: this may fit better into some testing header
void AddArtificialOffsetInChildArray(ArrayData* array, int64_t offset);

}  // namespace rle_util
}  // namespace arrow
