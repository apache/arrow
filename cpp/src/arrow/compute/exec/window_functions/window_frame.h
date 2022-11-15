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
#include <vector>
#include "arrow/compute/exec/util.h"

namespace arrow {
namespace compute {

// A collection of window frames for a sequence of rows in the window frame sort
// order.
//
struct WindowFrames {
  // Every frame is associated with a single row.
  //
  // This is the index of the first row (in the window frame sort order) for the
  // first frame.
  //
  int64_t first_row_index;

  // Number of frames in this collection
  //
  int64_t num_frames;

  // Maximum number of ranges that make up each single frame.
  //
  static constexpr int kMaxRangesInFrame = 3;

  // Number of ranges that make up each single frame.
  // Every frame will have exactly that many ranges, but any number of these
  // ranges can be empty.
  //
  int num_ranges_per_frame;

  // Range can be empty, in that case begin == end. Otherwise begin < end.
  //
  // Ranges in a single frame must be disjoint but beginning of next range can
  // be equal to the end of the previous one.
  //
  // Beginning of each next range must be greater or equal to the end of the
  // previous range.
  //
  const int64_t* begins[kMaxRangesInFrame];
  const int64_t* ends[kMaxRangesInFrame];

  // Check if a collection of frames represents sliding frames,
  // that is for every boundary (left and right) of every range, the values
  // across all frames are non-decreasing.
  //
  bool IsSliding() const {
    for (int64_t i = 1; i < num_frames; ++i) {
      if (!(begins[i] >= begins[i - 1] && ends[i] >= ends[i - 1])) {
        return false;
      }
    }
    return true;
  }

  // Check if a collection of frames represent cumulative frames,
  // that is for every range, two adjacent frames either share the same
  // beginning with end of the later one being no lesser than the end of the
  // previous one, or the later one begins at or after the end of the previous
  // one.
  //
  bool IsCummulative() const {
    for (int64_t i = 1; i < num_frames; ++i) {
      if (!((begins[i] >= ends[i - 1] || begins[i] == begins[i - 1]) &&
            (ends[i] >= ends[i - 1]))) {
        return false;
      }
    }
    return true;
  }

  // Check if the row for which the frame is defined is included in any of the
  // ranges defining that frame.
  //
  bool IsRowInsideItsFrame(int64_t frame_index) const {
    bool is_inside = false;
    int64_t row_index = first_row_index + frame_index;
    for (int64_t range_index = 0; range_index < num_ranges_per_frame; ++range_index) {
      int64_t range_begin = begins[range_index][frame_index];
      int64_t range_end = ends[range_index][frame_index];
      is_inside = is_inside || (row_index >= range_begin && row_index < range_end);
    }
    return is_inside;
  }
};

enum class WindowFrameSequenceType { CUMMULATIVE, SLIDING, GENERIC };

}  // namespace compute
}  // namespace arrow
