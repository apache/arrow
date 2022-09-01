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

/// \brief Get the physical offset from a logical offset given run end values using binary
/// search
int64_t FindPhysicalOffset(const int32_t* run_ends, int64_t num_run_ends,
                           int64_t logical_offset);

/// \brief Get the physical offset of an RLE ArraySpan. Warning: calling this may result
/// in in an O(log(N)) binary search on the run ends buffer
int64_t GetPhysicalOffset(const ArraySpan& span);

/// \brief Get the physical length of an RLE ArraySpan. Avoid calling this method in a
/// context where you can easily calculate the value yourself. Calling this can result in
/// an O(log(N)) binary search on the run ends buffer
int64_t GetPhysicalLength(const ArraySpan& span);

/// \brief Get the child array holding the data values from an RLE array
static inline const ArraySpan& RunEndsArray(const ArraySpan& span) {
  return span.child_data[0];
}

/// \brief Get a pointer to run ends values of an RLE array
static inline const int32_t* RunEnds(const ArraySpan& span) {
  return RunEndsArray(span).GetValues<int32_t>(1);
}

/// \brief Get the child array holding the data values from an RLE array
static inline const ArraySpan& ValuesArray(const ArraySpan& span) {
  return span.child_data[1];
}

/// \brief Iterate over two run-length encoded arrays in segments of runs that are inside
/// run boundaries in each input
template <size_t NUM_INPUTS>
class MergedRunsIterator {
 public:
  // end iterator
  MergedRunsIterator() {}

  // TODO: genereric constructor
  MergedRunsIterator(const ArraySpan& a) {
    static_assert(NUM_INPUTS == 1, "incorrect number of inputs");

    inputs[0] = &a;

    logical_length = a.length;

    for (size_t input_id = 0; input_id < NUM_INPUTS; input_id++) {
      const ArraySpan* input = inputs[input_id];
      run_index[input_id] = rle_util::FindPhysicalOffset(
          RunEnds(*input), RunEndsArray(*input).length, input->offset);
    }
    FindMergedRun();
  }

  MergedRunsIterator(const ArraySpan& a, const ArraySpan& b) {
    static_assert(NUM_INPUTS == 2, "incorrect number of inputs");

    inputs[0] = &a;
    inputs[1] = &b;

    ARROW_DCHECK_EQ(a.length, b.length);
    logical_length = a.length;

    for (size_t input_id = 0; input_id < NUM_INPUTS; input_id++) {
      const ArraySpan* input = inputs[input_id];
      run_index[input_id] = rle_util::FindPhysicalOffset(
          RunEnds(*input), RunEndsArray(*input).length, input->offset);
    }
    FindMergedRun();
  }

  MergedRunsIterator(const MergedRunsIterator& other) = default;

  MergedRunsIterator& operator++() {
    logical_position += merged_run_length;

    for (size_t input_id = 0; input_id < NUM_INPUTS; input_id++) {
      if (logical_position == run_end[input_id]) {
        run_index[input_id]++;
      }
    }
    if (!isEnd()) {
      FindMergedRun();
    }

    return *this;
  }

  MergedRunsIterator& operator++(int) {
    MergedRunsIterator& result = *this;
    ++(*this);
    return result;
  }

  bool operator==(const MergedRunsIterator& other) const {
    return (isEnd() && other.isEnd()) ||
           (!isEnd() && !other.isEnd() && logical_position == other.logical_position);
  }

  bool operator!=(const MergedRunsIterator& other) const { return !(*this == other); }

  int64_t physical_index(int64_t input_id) const {
    return run_index[input_id] + ValuesArray(*inputs[input_id]).offset;
  }
  int64_t run_length() const { return merged_run_length; }

 private:
  void FindMergedRun() {
    int64_t merged_run_end = std::numeric_limits<int64_t>::max();
    for (size_t input_id = 0; input_id < NUM_INPUTS; input_id++) {
      // logical indices of the end of the run we are currently in each input
      run_end[input_id] =
          RunEnds(*inputs[input_id])[run_index[input_id]] - inputs[input_id]->offset;
      // the logical length may end in the middle of a run, in case the array was sliced
      run_end[input_id] = std::min(run_end[input_id], logical_length);
      ARROW_DCHECK_GT(run_end[input_id], logical_position);

      merged_run_end = std::min(merged_run_end, run_end[input_id]);
    }
    merged_run_length = merged_run_end - logical_position;
  }

  bool isEnd() const { return logical_position == logical_length; }

  std::array<const ArraySpan*, NUM_INPUTS> inputs;
  std::array<int64_t, NUM_INPUTS> physical_offset;
  std::array<int64_t, NUM_INPUTS> run_index;
  // logical indices of the end of the run we are currently in each input
  std::array<int64_t, NUM_INPUTS> run_end;
  int64_t logical_position = 0;
  int64_t logical_length = 0;
  int64_t merged_run_length;
};

// TODO: this may fit better into some testing header
void AddArtificialOffsetInChildArray(ArrayData* array, int64_t offset);

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
