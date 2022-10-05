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
/// search. Returns num_run_ends if the physical offset is not within the first
/// num_run_ends elements.
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
    if (!isEnd()) {
      FindMergedRun();
    }
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
    if (!isEnd()) {
      FindMergedRun();
    }
  }

  MergedRunsIterator(const MergedRunsIterator& other) = default;

  MergedRunsIterator& operator++() {
    logical_position = merged_run_end;

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

  /// \brief returns a physical index into the values array buffers of a given input,
  /// pointing to the value of the current run. The index includes the array offset, so it
  /// can be used to access a buffer directly
  int64_t index_into_buffer(int64_t input_id) const {
    return run_index[input_id] + ValuesArray(*inputs[input_id]).offset;
  }
  /// \brief returns a physical index into the values array of a given input, pointing to
  /// the value of the current run
  int64_t index_into_array(int64_t input_id) const { return run_index[input_id]; }
  /// \brief returns the logical length of the current run
  int64_t run_length() const { return merged_run_end - logical_position; }
  /// \brief returns the accumulated length of all runs from the beginning of the array
  /// including the current one
  int64_t accumulated_run_length() const { return merged_run_end; }

 private:
  void FindMergedRun() {
    merged_run_end = std::numeric_limits<int64_t>::max();
    for (size_t input_id = 0; input_id < NUM_INPUTS; input_id++) {
      // logical indices of the end of the run we are currently in each input
      run_end[input_id] =
          RunEnds(*inputs[input_id])[run_index[input_id]] - inputs[input_id]->offset;
      // the logical length may end in the middle of a run, in case the array was sliced
      run_end[input_id] = std::min(run_end[input_id], logical_length);
      ARROW_DCHECK_GT(run_end[input_id], logical_position);

      merged_run_end = std::min(merged_run_end, run_end[input_id]);
    }
  }

  bool isEnd() const { return logical_position == logical_length; }

  std::array<const ArraySpan*, NUM_INPUTS> inputs;
  std::array<int64_t, NUM_INPUTS> run_index;
  // logical indices of the end of the run we are currently in each input
  std::array<int64_t, NUM_INPUTS> run_end;
  int64_t logical_position = 0;
  int64_t logical_length = 0;
  int64_t merged_run_end;
};

// TODO: this may fit better into some testing header
void AddArtificialOffsetInChildArray(ArrayData* array, int64_t offset);

}  // namespace rle_util
}  // namespace arrow
