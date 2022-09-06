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
#include <set>
#include "arrow/compute/exec/util.h"

namespace arrow {
namespace compute {

struct WindowFrames {
  static constexpr int kMaxRangesInFrame = 3;

  int num_ranges_in_frame;
  int64_t num_frames;

  // Range can be empty, in that case begin == end. Otherwise begin < end.
  //
  // Ranges in a single frame must be disjoint but begin of next range can be
  // equal to the end of the previous one.
  //
  const int64_t* begins[kMaxRangesInFrame];
  const int64_t* ends[kMaxRangesInFrame];

  // Row filter has bits set to 0 for rows that should not be included in the
  // range.
  //
  // Null row filter means that all rows are qualified.
  //
  const uint8_t* row_filter;

  bool FramesProgressing() const {
    for (int64_t i = 1; i < num_frames; ++i) {
      if (!(begins[i] >= begins[i - 1] && ends[i] >= ends[i - 1])) {
        return false;
      }
    }
    return true;
  }

  bool FramesExpanding() const {
    for (int64_t i = 1; i < num_frames; ++i) {
      if (!((begins[i] >= ends[i - 1] || begins[i] == begins[i - 1]) &&
            (ends[i] >= ends[i - 1]))) {
        return false;
      }
    }
    return true;
  }
};

inline void GenerateTestFrames(Random64BitCopy& rand, int64_t num_rows,
                               std::vector<int64_t>& begins, std::vector<int64_t>& ends,
                               bool progressive, bool expansive) {
  begins.resize(num_rows);
  ends.resize(num_rows);

  if (!progressive && !expansive) {
    constexpr int64_t max_frame_length = 100;
    for (int64_t i = 0; i < num_rows; ++i) {
      int64_t length = rand.from_range(static_cast<int64_t>(0LL),
                                       std::min(num_rows, max_frame_length));
      int64_t begin = rand.from_range(static_cast<int64_t>(0LL), num_rows - length);
      begins[i] = begin;
      ends[i] = begin + length;
    }
  } else if (progressive && !expansive) {
    int64_t dist = rand.from_range(static_cast<int64_t>(1LL),
                                   std::max(static_cast<int64_t>(1LL), num_rows / 4));
    std::vector<int64_t> pos;
    for (int64_t i = 0; i < num_rows + dist; ++i) {
      pos.push_back(rand.from_range(static_cast<int64_t>(0LL), num_rows));
    }
    std::sort(pos.begin(), pos.end());
    for (int64_t i = 0; i < num_rows; ++i) {
      begins[i] = pos[i];
      ends[i] = pos[i + dist];
    }
  } else {
    int64_t num_partitions =
        rand.from_range(static_cast<int64_t>(1LL), bit_util::CeilDiv(num_rows, 128LL));
    std::set<int64_t> partition_ends_set;
    std::vector<int64_t> partition_ends;
    partition_ends_set.insert(num_rows);
    partition_ends.push_back(num_rows);
    for (int64_t i = 1; i < num_partitions; ++i) {
      int64_t partition_end;
      for (;;) {
        partition_end = rand.from_range(static_cast<int64_t>(1LL), num_rows - 1);
        if (partition_ends_set.find(partition_end) == partition_ends_set.end()) {
          break;
        }
      }
      partition_ends.push_back(partition_end);
      partition_ends_set.insert(partition_end);
    }
    std::sort(partition_ends.begin(), partition_ends.end());
    for (int64_t ipartition = 0; ipartition < num_partitions; ++ipartition) {
      int64_t partition_begin = ipartition == 0 ? 0LL : partition_ends[ipartition - 1];
      int64_t partition_end = partition_ends[ipartition];
      int64_t partition_length = partition_end - partition_begin;
      int64_t begin = rand.from_range(0LL, 2LL);

      if (begin >= partition_length) {
        begin = partition_length - 1;
      }
      int64_t end = begin + rand.from_range(0LL, 2LL);
      if (end > partition_length) {
        end = partition_length;
      }
      begins[partition_begin + 0] = partition_begin + begin;
      ends[partition_begin + 0] = partition_begin + end;
      for (int64_t i = 1; i < partition_length; ++i) {
        int64_t end_step = rand.from_range(0LL, 2LL);
        end += end_step;
        if (end > partition_length) {
          end = partition_length;
        }
        begins[partition_begin + i] = partition_begin + begin;
        ends[partition_begin + i] = partition_begin + end;
      }
    }
  }
}

// TODO: Enable treating multiple partitions as a single one with window
// frames cropped to boundaries of these partitions.
// This should help amortize the costs of data structure allocations done per
// partition among a group of them, in case when there are many small
// partitions.
//

class WindowFrameGenerator {
 public:
  // Sorted values for entire window
  struct InputValues {
    InputValues(int64_t num_rows, bool nulls_before, int64_t num_nulls,
                const int64_t* vals)
        : num_rows(num_rows),
          nulls_before(nulls_before),
          num_nulls(num_nulls),
          vals(vals) {}
    int64_t num_rows;
    bool nulls_before;
    int64_t num_nulls;
    const int64_t* vals;
  };

  // Bit vector representing ties (first row in every tie group has its bit set
  // to 1)
  struct InputTies {
    InputTies(int64_t num_rows, uint64_t* bitvec, uint64_t* popcounts)
        : num_rows(num_rows), bitvec(bitvec), popcounts(popcounts) {}
    int64_t num_rows;
    uint64_t* bitvec;
    uint64_t* popcounts;
  };

  struct InputBatch {
    InputBatch(int64_t begin, int64_t end, int64_t* frame_begins, int64_t* frame_ends)
        : begin(begin), end(end), frame_begins(frame_begins), frame_ends(frame_ends) {}

    // Range of row numbers that make input batch
    int64_t begin;
    int64_t end;

    // Output frames for input rows
    int64_t* frame_begins;
    int64_t* frame_ends;
  };

  // First row in every group of ties gets the corresponding bit set to 1.
  //
  // For the purpose of groups we consider null key being equal to null key.
  //
  static void GenTies(InputValues vals, InputTies* ties);

  // Deltas with respect to current row's tie group
  // for range of groups to include in its frame.
  // Range bounds specified by deltas are
  // inclusive. Null pointer means that the
  // corresponding side of the range is unbounded.
  //
  static void Groups(InputBatch* batch, bool constant_deltas, const int64_t* left_delta,
                     const int64_t* right_delta, InputTies ties, int64_t hardware_flags,
                     util::TempVectorStack* temp_vector_stack);

  // Constant value specifying delta between current  row number and row number
  // of the first row of its frame. Preceding can use kUnbounded constant.
  static void Rows(InputBatch* batch, bool constant_deltas, const int64_t* left_delta,
                   const int64_t* right_delta, int64_t num_rows);

  // Constant value specifying delta between current row value and inclusive
  // lower bound of its frame. Preceding can use kUnbounded constant.
  //
  // Only meaningful for a single column sort key.
  //
  static void Range(InputBatch* batch, bool constant_deltas, const int64_t* left_delta,
                    const int64_t* right_delta, InputValues vals);

 private:
  static void UnboundedRanges(InputBatch* batch, bool unbounded_left,
                              bool unbounded_right, int64_t num_rows);
};

class WindowFrameGeneratorBasic {
 public:
  static void Groups(WindowFrameGenerator::InputBatch* batch, bool constant_deltas,
                     const int64_t* left_delta, const int64_t* right_delta,
                     WindowFrameGenerator::InputValues vals);

  static void Rows(WindowFrameGenerator::InputBatch* batch, bool constant_deltas,
                   const int64_t* left_delta, const int64_t* right_delta,
                   int64_t num_rows);

  static void Range(WindowFrameGenerator::InputBatch* batch, bool constant_deltas,
                    const int64_t* left_delta, const int64_t* right_delta,
                    WindowFrameGenerator::InputValues vals);

 private:
  static int64_t GroupBegin(int64_t row_number, int64_t relative_group_number,
                            WindowFrameGenerator::InputValues vals);

  static int64_t RangeBegin(int64_t val, WindowFrameGenerator::InputValues vals);
};

}  // namespace compute
}  // namespace arrow
