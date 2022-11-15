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

#include <gmock/gmock-matchers.h>

#include <algorithm>
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec/window_functions/window_rank.h"

namespace arrow {
namespace compute {

class WindowFramesRandom {
 public:
  static void Generate(Random64Bit& rand, WindowFrameSequenceType frame_sequence_type,
                       int64_t num_rows, int num_ranges_per_frame,
                       std::vector<std::vector<int64_t>>* range_boundaries);

  static void GenerateSliding(Random64Bit& rand, int64_t num_rows,
                              int num_ranges_per_frame,
                              std::vector<std::vector<int64_t>>* range_boundaries,
                              int64_t suggested_frame_span, int64_t suggested_gap_length);

  static void GenerateCummulative(Random64Bit& rand, int64_t num_rows,
                                  int num_ranges_per_frame,
                                  std::vector<std::vector<int64_t>>* range_boundaries,
                                  int num_restarts);

  static void GenerateGeneric(Random64Bit& rand, int64_t num_rows,
                              int num_ranges_per_frame,
                              std::vector<std::vector<int64_t>>* range_boundaries,
                              int64_t max_frame_span, int64_t max_gap_length);

 private:
  static void CutHoles(Random64Bit& rand, int64_t frame_span, int64_t num_holes,
                       int64_t sum_hole_size, std::vector<int64_t>& result_boundaries);
};

void WindowFramesRandom::Generate(Random64Bit& rand,
                                  WindowFrameSequenceType frame_sequence_type,
                                  int64_t num_rows, int num_ranges_per_frame,
                                  std::vector<std::vector<int64_t>>* range_boundaries) {
  switch (frame_sequence_type) {
    case WindowFrameSequenceType::CUMMULATIVE:
      GenerateCummulative(rand, num_rows, num_ranges_per_frame, range_boundaries,
                          /*num_restarts=*/rand.from_range(0, 2));
      break;
    case WindowFrameSequenceType::SLIDING: {
      int64_t suggested_frame_span =
          rand.from_range(static_cast<int64_t>(0), num_rows / 4);
      int64_t suggested_gap_length =
          rand.from_range(static_cast<int64_t>(0), suggested_frame_span / 2);
      GenerateSliding(rand, num_rows, num_ranges_per_frame, range_boundaries,
                      suggested_frame_span, suggested_gap_length);
    } break;
    case WindowFrameSequenceType::GENERIC: {
      int64_t max_frame_span = rand.from_range(static_cast<int64_t>(0), num_rows / 4);
      int64_t max_gap_length =
          rand.from_range(static_cast<int64_t>(0), max_frame_span / 2);
      GenerateGeneric(rand, num_rows, num_ranges_per_frame, range_boundaries,
                      max_frame_span, max_gap_length);
    } break;
  }
}

void WindowFramesRandom::GenerateSliding(
    Random64Bit& rand, int64_t num_rows, int num_ranges_per_frame,
    std::vector<std::vector<int64_t>>* range_boundaries, int64_t suggested_frame_span,
    int64_t suggested_gap_length) {
  if (num_rows == 0) {
    return;
  }

  // Generate a sorted list of points that will serve as frame boundaries (for
  // all ranges in all frames).
  //
  std::vector<int64_t> boundaries(num_rows + suggested_frame_span);
  for (size_t i = 0; i < boundaries.size(); ++i) {
    boundaries[i] = rand.from_range(static_cast<int64_t>(0), num_rows);
  }
  std::sort(boundaries.begin(), boundaries.end());

  // Generate desired first frame (relative positions and sizes of ranges in
  // it).
  //
  // This will serve as a basis for distances between range boundary points.
  //
  std::vector<int64_t> desired_boundaries;
  CutHoles(rand, suggested_frame_span, num_ranges_per_frame - 1, suggested_gap_length,
           desired_boundaries);

  // Assign boundary points from the sorted random vector at predetermined
  // distances from each other to consecutive frames.
  //
  range_boundaries->resize(num_ranges_per_frame * 2);
  for (size_t i = 0; i < range_boundaries->size(); ++i) {
    (*range_boundaries)[i].clear();
  }
  for (int64_t i = 0; i < num_rows; ++i) {
    for (int boundary_index = 0; boundary_index < 2 * num_ranges_per_frame;
         ++boundary_index) {
      (*range_boundaries)[boundary_index].push_back(
          boundaries[i + desired_boundaries[boundary_index]]);
    }
  }
}

void WindowFramesRandom::GenerateCummulative(
    Random64Bit& rand, int64_t num_rows, int num_ranges_per_frame,
    std::vector<std::vector<int64_t>>* range_boundaries, int num_restarts) {
  int num_boundaries_per_frame = 2 * num_ranges_per_frame;
  range_boundaries->resize(num_boundaries_per_frame);
  for (int64_t i = 0; i < num_boundaries_per_frame; ++i) {
    (*range_boundaries)[i].clear();
  }

  // Divide rows into sections, each dedicated to a different range.
  //
  std::vector<int64_t> sections;
  sections.push_back(0);
  sections.push_back(num_rows);
  for (int i = 0; i < num_ranges_per_frame - 1; ++i) {
    sections.push_back(rand.from_range(static_cast<int64_t>(0), num_rows));
  }
  std::sort(sections.begin(), sections.end());

  // Process each section (range) separately.
  //
  for (int range_index = 0; range_index < num_ranges_per_frame; ++range_index) {
    std::vector<int64_t> boundaries(num_rows + num_restarts + 1);
    for (int64_t i = 0; i < num_rows + num_restarts + 1; ++i) {
      boundaries[i] = rand.from_range(sections[range_index], sections[range_index + 1]);
    }
    std::sort(boundaries.begin(), boundaries.end());

    // Mark restart points in the boundaries vector.
    //
    std::vector<bool> boundary_is_restart_point(boundaries.size());
    for (int64_t i = 0; i < num_rows + num_restarts + 1; ++i) {
      boundary_is_restart_point[i] = false;
    }
    boundary_is_restart_point[0] = true;
    for (int i = 0; i < num_restarts; ++i) {
      for (;;) {
        int64_t pos =
            rand.from_range(static_cast<int64_t>(0), num_rows + num_restarts - 1);
        if (!boundary_is_restart_point[pos]) {
          boundary_is_restart_point[pos] = true;
          break;
        }
      }
    }

    // Output results for next range.
    //
    int64_t current_begin = 0;
    for (int64_t i = 0; i < num_rows + num_restarts + 1; ++i) {
      if (boundary_is_restart_point[i]) {
        current_begin = boundaries[i];
      } else {
        (*range_boundaries)[2 * range_index + 0].push_back(current_begin);
        (*range_boundaries)[2 * range_index + 1].push_back(boundaries[i]);
      }
    }
  }
}

void WindowFramesRandom::GenerateGeneric(
    Random64Bit& rand, int64_t num_rows, int num_ranges_per_frame,
    std::vector<std::vector<int64_t>>* range_boundaries, int64_t max_frame_span,
    int64_t max_gap_length) {
  int num_boundaries_per_frame = 2 * num_ranges_per_frame;
  range_boundaries->resize(num_boundaries_per_frame);
  for (int64_t i = 0; i < num_boundaries_per_frame; ++i) {
    (*range_boundaries)[i].clear();
  }

  for (int64_t row_index = 0; row_index < num_rows; ++row_index) {
    int64_t frame_span =
        rand.from_range(static_cast<int64_t>(0), std::min(num_rows, max_frame_span));
    int64_t gap_length =
        rand.from_range(static_cast<int64_t>(0), std::min(frame_span, max_gap_length));
    int64_t frame_pos = rand.from_range(static_cast<int64_t>(0), num_rows - frame_span);
    std::vector<int64_t> frame_boundaries;
    CutHoles(rand, frame_span, num_ranges_per_frame - 1, gap_length, frame_boundaries);
    for (size_t i = 0; i < frame_boundaries.size(); ++i) {
      (*range_boundaries)[i].push_back(frame_boundaries[i] + frame_pos);
    }
  }
}

void WindowFramesRandom::CutHoles(Random64Bit& rand, int64_t frame_span,
                                  int64_t num_holes, int64_t sum_hole_size,
                                  std::vector<int64_t>& result_boundaries) {
  // Randomly pick size of each hole so that the sum is equal to the requested
  // total.
  //
  ARROW_DCHECK(sum_hole_size <= frame_span);
  std::vector<int64_t> cummulative_hole_sizes(num_holes + 1);
  cummulative_hole_sizes[0] = 0;
  for (int64_t i = 1; i < num_holes; ++i) {
    cummulative_hole_sizes[i] = rand.from_range(static_cast<int64_t>(0), sum_hole_size);
  }
  cummulative_hole_sizes[num_holes] = sum_hole_size;
  std::sort(cummulative_hole_sizes.begin(), cummulative_hole_sizes.end());

  // Randomly pick starting position for each hole.
  //
  std::vector<int64_t> hole_pos(num_holes);
  for (int64_t i = 0; i < num_holes; ++i) {
    hole_pos[i] = rand.from_range(static_cast<int64_t>(0), frame_span - sum_hole_size);
  }
  std::sort(hole_pos.begin(), hole_pos.end());
  for (int64_t i = 0; i < num_holes; ++i) {
    hole_pos[i] += cummulative_hole_sizes[i];
  }

  // Output result.
  //
  int64_t num_boundaries = (num_holes + 1) * 2;
  result_boundaries.resize(num_boundaries);
  result_boundaries[0] = 0;
  result_boundaries[num_boundaries - 1] = frame_span;
  for (int64_t i = 0; i < num_holes; ++i) {
    result_boundaries[1 + 2 * i] = hole_pos[i];
    result_boundaries[2 + 2 * i] =
        hole_pos[i] + cummulative_hole_sizes[i + 1] - cummulative_hole_sizes[i];
  }
}

void TestWindowRankVariant(RankType rank_type, bool use_frames, bool use_2D) {
  // TODO: Framed dense rank is not implemented yet:
  //
  ARROW_DCHECK(!(rank_type == RankType::DENSE_RANK && use_2D));

  Random64Bit rand(/*seed=*/0);

  // Preparing thread execution context
  //
  MemoryPool* pool = default_memory_pool();
  util::TempVectorStack temp_vector_stack;
  Status status = temp_vector_stack.Init(pool, 128 * util::MiniBatch::kMiniBatchLength);
  ARROW_DCHECK(status.ok());
  ThreadContext thread_context;
  thread_context.thread_index = 0;
  thread_context.temp_vector_stack = &temp_vector_stack;
  thread_context.hardware_flags = 0LL;

  // There will be: 24 small tests, 12 medium tests and 3 large tests.
  //
  constexpr int num_tests = 24 + 12 + 3;

  // When debugging a failed test case, setting this value allows to skip
  // execution of the first couple of test cases to go directly into the
  // interesting one, while at the same time making sure that the generated
  // random numbers are not affected.
  //
  const int num_tests_to_skip = 2;

  for (int test = 0; test < num_tests; ++test) {
    // Generate random values.
    //
    // There will be: 24 small tests, 12 medium tests and 3 large tests.
    //
    int64_t max_rows = (test < 24) ? 100 : (test < 36) ? 256 : 2500;
    int64_t num_rows = rand.from_range(static_cast<int64_t>(1), max_rows);
    std::vector<int64_t> vals(num_rows);
    int64_t max_val = num_rows;
    int tie_probability = rand.from_range(0, 256);
    for (int64_t i = 0; i < num_rows; ++i) {
      bool tie = rand.from_range(0, 255) < tie_probability;
      if (tie && i > 0) {
        vals[i] = vals[rand.from_range(static_cast<int64_t>(0), i - 1)];
      } else {
        vals[i] = rand.from_range(static_cast<int64_t>(0), max_val);
      }
    }

    // Generate random frames
    //
    int num_ranges_per_frame = rand.from_range(1, 3);
    std::vector<std::vector<int64_t>> range_boundaries;
    int frame_sequence_type_index = rand.from_range(0, 2);
    WindowFrameSequenceType frame_sequence_type =
        (frame_sequence_type_index == 0)   ? WindowFrameSequenceType::GENERIC
        : (frame_sequence_type_index == 1) ? WindowFrameSequenceType::SLIDING
                                           : WindowFrameSequenceType::CUMMULATIVE;
    WindowFramesRandom::Generate(rand, frame_sequence_type, num_rows,
                                 num_ranges_per_frame, &range_boundaries);
    WindowFrames frames;
    frames.first_row_index = 0;
    frames.num_frames = num_rows;
    frames.num_ranges_per_frame = num_ranges_per_frame;
    for (int range_index = 0; range_index < num_ranges_per_frame; ++range_index) {
      frames.begins[range_index] = range_boundaries[2 * range_index + 0].data();
      frames.ends[range_index] = range_boundaries[2 * range_index + 1].data();
    }

    // Random number generator is not used after this point in the test case,
    // so we can skip the rest of the test case if we try to fast forward to a
    // specific one.
    //
    if (test < num_tests_to_skip) {
      continue;
    }

    // Sort values and output permutation and bit vector of ties
    //
    BitVectorWithCounts tie_begins;
    tie_begins.Resize(num_rows);
    std::vector<int64_t> permutation(num_rows);
    std::vector<int64_t> vals_sorted(num_rows);
    {
      std::vector<std::pair<int64_t, int64_t>> val_row_pairs(num_rows);
      for (int64_t i = 0; i < num_rows; ++i) {
        val_row_pairs[i] = std::make_pair(vals[i], i);
      }
      std::sort(val_row_pairs.begin(), val_row_pairs.end());
      for (int64_t i = 0; i < num_rows; ++i) {
        permutation[i] = val_row_pairs[i].second;
        vals_sorted[i] = val_row_pairs[i].first;
      }
      tie_begins.GetNavigator().MarkTieBegins(num_rows, vals_sorted.data());
    }

    ARROW_SCOPED_TRACE(
        "num_rows = ", static_cast<int>(num_rows),
        "num_ranges_per_frame = ", num_ranges_per_frame, "window_frame_type = ",
        use_frames
            ? (frame_sequence_type == WindowFrameSequenceType::CUMMULATIVE ? "CUMMULATIVE"
               : frame_sequence_type == WindowFrameSequenceType::SLIDING   ? "SLIDING"
                                                                           : "GENERIC")
            : "NONE",
        "rank_type = ",
        rank_type == RankType::ROW_NUMBER       ? "ROW_NUMBER"
        : rank_type == RankType::RANK_TIES_LOW  ? "RANK_TIES_LOW"
        : rank_type == RankType::RANK_TIES_HIGH ? "RANK_TIES_HIGH"
                                                : "DENSE_RANK",
        "use_2D = ", use_2D);

    // At index 0 - reference results.
    // At index 1 - actual results from implementation we wish to verify.
    //
    std::vector<int64_t> output[2];
    output[0].resize(num_rows);
    output[1].resize(num_rows);

    // Execute reference implementation.
    //
    if (!use_frames) {
      WindowRank_Global_Ref::Eval(rank_type, tie_begins.GetNavigator(), output[0].data());
    } else if (!use_2D) {
      WindowRank_Framed_Ref::Eval(rank_type, tie_begins.GetNavigator(), nullptr, frames,
                                  output[0].data());
    } else {
      WindowRank_Framed_Ref::Eval(rank_type, tie_begins.GetNavigator(),
                                  permutation.data(), frames, output[0].data());
    }

    // Execute actual implementation.
    //
    if (!use_frames) {
      WindowRank_Global::Eval(rank_type, tie_begins.GetNavigator(), 0, num_rows,
                              output[1].data());
    } else if (!use_2D) {
      WindowRank_Framed1D::Eval(rank_type, tie_begins.GetNavigator(), frames,
                                output[1].data());
    } else {
      ASSERT_OK(WindowRank_Framed2D::Eval(rank_type, tie_begins.GetNavigator(),
                                          permutation.data(), frames, output[1].data(),
                                          thread_context));
    }

    bool ok = true;
    for (int64_t i = 0; i < num_rows; ++i) {
      if (output[0][i] != output[1][i]) {
        ARROW_DCHECK(false);
        ok = false;
      }
    }
    ASSERT_TRUE(ok);
  }
}

TEST(WindowFunctions, Rank) {
  // These flags are useful during debugging, to quickly restrict the set of
  // executed tests to just the failing one.
  //
  bool use_filter_framed = false;
  bool use_filter_rank_type = false;
  bool use_filter_2D = false;

  bool filter_framed_value = true;
  RankType filter_rank_type_value = RankType::RANK_TIES_HIGH;
  bool filter_2D_value = true;

  // Global rank
  //
  for (auto rank_type : {RankType::ROW_NUMBER, RankType::RANK_TIES_LOW,
                         RankType::RANK_TIES_HIGH, RankType::DENSE_RANK}) {
    if (use_filter_2D && filter_2D_value) {
      continue;
    }
    if (use_filter_framed && filter_framed_value) {
      continue;
    }
    if (use_filter_rank_type && filter_rank_type_value != rank_type) {
      continue;
    }
    TestWindowRankVariant(rank_type,
                          /*use_frames=*/false,
                          /*ignored*/ false);
  }

  // Framed rank
  //
  for (auto use_2D : {false, true}) {
    for (auto rank_type : {RankType::ROW_NUMBER, RankType::RANK_TIES_LOW,
                           RankType::RANK_TIES_HIGH, RankType::DENSE_RANK}) {
      if (use_filter_framed && !filter_framed_value) {
        continue;
      }
      if (use_filter_rank_type && filter_rank_type_value != rank_type) {
        continue;
      }
      if (use_filter_2D && filter_2D_value != use_2D) {
        continue;
      }
      if (rank_type == RankType::DENSE_RANK && use_2D) {
        continue;
      }
      TestWindowRankVariant(rank_type, /*use_frames=*/true, use_2D);
    }
  }
}

}  // namespace compute
}  // namespace arrow
