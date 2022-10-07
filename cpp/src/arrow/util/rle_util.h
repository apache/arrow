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
template <typename RunEndsType>
int64_t FindPhysicalOffset(const RunEndsType* run_ends, int64_t num_run_ends,
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
template <typename RunEndsType>
static inline const RunEndsType* RunEnds(const ArraySpan& span) {
  return RunEndsArray(span).GetValues<RunEndsType>(1);
}

/// \brief Get the child array holding the data values from an RLE array
static inline const ArraySpan& ValuesArray(const ArraySpan& span) {
  return span.child_data[1];
}

/// \brief Iterate over two run-length encoded arrays in segments of runs that are inside
/// run boundaries in each input
template <typename... RunEndsTypes>
class MergedRunsIterator {
 public:
  static constexpr size_t NUM_INPUTS = sizeof...(RunEndsTypes);
  template <typename... InputTypes>
  explicit MergedRunsIterator(InputTypes&... array_spans) : inputs(array_spans...) {
    static_assert(sizeof...(InputTypes) == sizeof...(RunEndsTypes),
                  "number of run ends types and input ArraySpans must be the same");
    if constexpr (NUM_INPUTS == 0) {
      // end interator
      logical_length_ = 0;
    } else {
      logical_length_ = FindCommonLength();
      if (!isEnd()) {
        FindMergedRun();
      }
    }
  }

  /*explicit MergedRunsIterator(ArraySpan array_span) : inputs(Input<int32_t>(array_span))
  {
    //static_assert(sizeof...(InputTypes) == sizeof...(RunEndsTypes), "number of run ends
  types and input ArraySpans must be the same"); if constexpr (NUM_INPUTS == 0) {
      // end interator
      logical_length_ = 0;
    } else {
      logical_length_ = FindCommonLength();
      if (!isEnd()) {
        FindMergedRun();
      }
    }
  }
  explicit MergedRunsIterator(ArraySpan array_span_a, ArraySpan array_span_b) :
  inputs(Input<int32_t>(array_span_a), Input<int32_t>(array_span_b)) {
    //static_assert(sizeof...(InputTypes) == sizeof...(RunEndsTypes), "number of run ends
  types and input ArraySpans must be the same"); if constexpr (NUM_INPUTS == 0) {
      // end interator
      logical_length_ = 0;
    } else {
      logical_length_ = FindCommonLength();
      if (!isEnd()) {
        FindMergedRun();
      }
    }
  }*/

  MergedRunsIterator(const MergedRunsIterator& other) = default;

  MergedRunsIterator& operator++() {
    logical_position_ = merged_run_end_;
    IncrementInputs();
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

  template <typename... OthersInputs>
  bool operator==(const MergedRunsIterator<OthersInputs...>& other) const {
    return (isEnd() && other.isEnd()) ||
           (!isEnd() && !other.isEnd() && logical_position_ == other.logical_position());
  }

  template <typename... OthersInputs>
  bool operator!=(const MergedRunsIterator<OthersInputs...>& other) const {
    return !(*this == other);
  }

  /// \brief returns a physical index into the values array buffers of a given input,
  /// pointing to the value of the current run. The index includes the array offset, so it
  /// can be used to access a buffer directly
  template <size_t input_id>
  int64_t index_into_buffer() const {
    auto& input = std::get<input_id>(inputs);
    return input.run_index + ValuesArray(input.array_span).offset;
  }
  /// \brief returns a physical index into the values array of a given input, pointing to
  /// the value of the current run
  template <size_t input_id>
  int64_t index_into_array() const {
    return std::get<input_id>(inputs).run_index;
  }
  /// \brief returns the logical length of the current run
  int64_t run_length() const { return merged_run_end_ - logical_position_; }
  /// \brief returns the accumulated length of all runs from the beginning of the array
  /// including the current one
  int64_t accumulated_run_length() const { return merged_run_end_; }

  bool isEnd() const { return logical_position_ == logical_length_; }
  int64_t logical_position() const { return logical_position_; }

 private:
  template <typename RunEndsType>
  struct Input {
    Input(const ArraySpan& array_span) : array_span{array_span} {
      run_ends = RunEnds<RunEndsType>(array_span);
      run_index = rle_util::FindPhysicalOffset(run_ends, RunEndsArray(array_span).length,
                                               array_span.offset);
      // actual value found later by FindMergedRun:
      current_run_end = 0;
    }

    const ArraySpan& array_span;
    const RunEndsType* run_ends;
    int64_t run_index;
    int64_t current_run_end;
  };

  template <size_t input_id = 0>
  void FindMergedRun() {
    if constexpr (input_id == 0) {
      merged_run_end_ = std::numeric_limits<int64_t>::max();
    }
    auto& input = std::get<input_id>(inputs);
    // logical indices of the end of the run we are currently in each input
    input.current_run_end = input.run_ends[input.run_index] - input.array_span.offset;
    // the logical length may end in the middle of a run, in case the array was sliced
    input.current_run_end = std::min(input.current_run_end, logical_length_);
    ARROW_DCHECK_GT(input.current_run_end, logical_position_);
    merged_run_end_ = std::min(merged_run_end_, input.current_run_end);
    if constexpr (input_id < NUM_INPUTS - 1) {
      FindMergedRun<input_id + 1>();
    }
  }

  template <size_t input_id = 0>
  int64_t FindCommonLength() {
    int64_t our_length = std::get<input_id>(inputs).array_span.length;
    if constexpr (input_id < NUM_INPUTS - 1) {
      int64_t other_length = FindCommonLength<input_id + 1>();
      ARROW_CHECK_EQ(our_length, other_length)
          << "MergedRunsIteratror can only be used on arrays of the same length";
    }
    return our_length;
  }

  template <size_t input_id = 0>
  void IncrementInputs() {
    auto& input = std::get<input_id>(inputs);
    if (logical_position_ == input.current_run_end) {
      input.run_index++;
    }
    if constexpr (input_id < NUM_INPUTS - 1) {
      IncrementInputs<input_id + 1>();
    }
  }

  std::tuple<Input<RunEndsTypes>...> inputs;
  int64_t logical_position_ = 0;
  int64_t logical_length_ = 0;
  int64_t merged_run_end_;
};

// TODO: this may fit better into some testing header
void AddArtificialOffsetInChildArray(ArrayData* array, int64_t offset);

}  // namespace rle_util
}  // namespace arrow
