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

#include "arrow/compute/exec/window_functions/window_rank.h"

namespace arrow {
namespace compute {

void WindowRank_Global::Eval(RankType rank_type, const BitVectorNavigator& tie_begins,
                             int64_t batch_begin, int64_t batch_end, int64_t* results) {
  int64_t num_rows = tie_begins.bit_count();

  if (rank_type == RankType::ROW_NUMBER) {
    std::iota(results, results + batch_end - batch_begin, 1LL + batch_begin);
    return;
  }

  if (rank_type == RankType::DENSE_RANK) {
    for (int64_t i = batch_begin; i < batch_end; ++i) {
      results[i - batch_begin] = tie_begins.RankNext(i);
    }
    return;
  }

  if (rank_type == RankType::RANK_TIES_LOW) {
    int64_t rank = tie_begins.Select(tie_begins.RankNext(batch_begin) - 1);
    ARROW_DCHECK(
        tie_begins.RankNext(rank) == tie_begins.RankNext(batch_begin) &&
        (rank == 0 || tie_begins.RankNext(rank - 1) < tie_begins.RankNext(rank)));
    rank += 1;
    for (int64_t i = batch_begin; i < batch_end; ++i) {
      rank = (tie_begins.GetBit(i) != 0) ? i + 1 : rank;
      results[i - batch_begin] = rank;
    }
    return;
  }

  if (rank_type == RankType::RANK_TIES_HIGH) {
    int64_t rank_max = tie_begins.pop_count();
    int64_t rank_last = tie_begins.RankNext(batch_end - 1);
    int64_t rank = (rank_last == rank_max) ? num_rows : tie_begins.Select(rank_last);
    for (int64_t i = batch_end - 1; i >= batch_begin; --i) {
      results[i - batch_begin] = rank;
      rank = (tie_begins.GetBit(i) != 0) ? i : rank;
    }
    return;
  }
}

void WindowRank_Framed1D::Eval(RankType rank_type, const BitVectorNavigator& tie_begins,
                               const WindowFrames& frames, int64_t* results) {
  if (rank_type == RankType::RANK_TIES_LOW) {
    // We will compute global rank into the same array as the one provided for
    // the output (to avoid allocating another array).
    //
    // When computing rank for a given row we will only read the result
    // computed for that row (no access to other rows) and update the same
    // result array entry.
    //
    int64_t* global_ranks = results;
    WindowRank_Global::Eval(RankType::RANK_TIES_LOW, tie_begins, frames.first_row_index,
                            frames.first_row_index + frames.num_frames, global_ranks);

    // The rank is 1 + the number of rows with key strictly lower than the
    // current row's key.
    //
    for (int64_t frame_index = 0; frame_index < frames.num_frames; ++frame_index) {
      // If the frame does not contain current row it is still logically
      // considered as included in the frame (e.g. empty frame will yield rank
      // 1 since the set we look at consists of a single row, the current
      // row).
      //
      int64_t rank = 1;
      for (int range_index = 0; range_index < frames.num_ranges_per_frame;
           ++range_index) {
        int64_t global_rank = global_ranks[frame_index];
        int64_t range_begin = frames.begins[range_index][frame_index];
        int64_t range_end = frames.ends[range_index][frame_index];

        // The formula below takes care of the cases:
        // a) current row outside of the range to the left,
        // b) current row in the range and ties with the first row in the
        // range,
        // c) current row in the range and no tie with the first row in
        // the range,
        // d) current row outside of the range to the right and
        // ties with the last row in the range.
        // e) current row outside of the range to the right and does no tie
        // with the last row in the range.
        // f) empty frame range,
        //
        rank += std::max(static_cast<int64_t>(0),
                         std::min(global_rank, range_end + 1) - range_begin - 1);
      }

      results[frame_index] = rank;
    }
  }

  if (rank_type == RankType::RANK_TIES_HIGH) {
    // To compute TIES_HIGH variant, we can reverse boundaries,
    // global ranks by substracting their values from num_rows
    // and num_rows + 1 respectively, and we will get the same problem as
    // TIES_LOW, the result of which we can convert back using the same
    // method but this time using number of rows inside the frame instead of
    // global number of rows.
    //
    // That is how the formula used below was derived.
    //
    // Note that the number of rows considered to be in the frame depends
    // whether the current row is inside or outside of the ranges defining its
    // frame, because in the second case we need to add 1 to the total size of
    // ranges.
    //
    int64_t* global_ranks = results;
    WindowRank_Global::Eval(RankType::RANK_TIES_HIGH, tie_begins, frames.first_row_index,
                            frames.first_row_index + frames.num_frames, global_ranks);

    for (int64_t frame_index = 0; frame_index < frames.num_frames; ++frame_index) {
      int64_t rank = 0;

      for (int range_index = 0; range_index < frames.num_ranges_per_frame;
           ++range_index) {
        int64_t global_rank = global_ranks[frame_index];
        int64_t range_begin = frames.begins[range_index][frame_index];
        int64_t range_end = frames.ends[range_index][frame_index];

        rank += std::min(range_end, std::max(global_rank, range_begin)) - range_begin;
      }

      rank += frames.IsRowInsideItsFrame(frame_index) ? 0 : 1;

      results[frame_index] = rank;
    }
  }

  if (rank_type == RankType::ROW_NUMBER) {
    // Count rows inside the frame coming before the current row and add 1.
    //
    for (int64_t frame_index = 0; frame_index < frames.num_frames; ++frame_index) {
      int64_t row_index = frames.first_row_index + frame_index;
      int64_t rank = 1;
      for (int range_index = 0; range_index < frames.num_ranges_per_frame;
           ++range_index) {
        int64_t range_begin = frames.begins[range_index][frame_index];
        int64_t range_end = frames.ends[range_index][frame_index];

        rank += std::max(static_cast<int64_t>(0),
                         std::min(row_index, range_end) - range_begin);
      }

      results[frame_index] = rank;
    }
  }

  if (rank_type == RankType::DENSE_RANK) {
    for (int64_t frame_index = 0; frame_index < frames.num_frames; ++frame_index) {
      int64_t row_index = frames.first_row_index + frame_index;
      int64_t rank = 1;

      // gdr = global dense rank
      //
      // Note that computing global dense rank corresponds to calling
      // tie_begin.RankNext().
      //
      int64_t highest_gdr_seen = 0;
      int64_t gdr = tie_begins.RankNext(row_index);

      for (int range_index = 0; range_index < frames.num_ranges_per_frame;
           ++range_index) {
        int64_t range_begin = frames.begins[range_index][frame_index];
        int64_t range_end = frames.ends[range_index][frame_index];

        if (row_index < range_begin || range_end == range_begin) {
          // Empty frame and frame starting after the current row - nothing to
          // do.
          //
        } else {
          // Count how many NEW peer groups before the current row's peer
          // group are introduced by each range.
          //
          // Take into account when the last row of the previous range is in
          // the same peer group as the first row of the next range.
          //
          int64_t gdr_first = tie_begins.RankNext(range_begin);
          int64_t gdr_last = tie_begins.RankNext(range_end - 1);
          int64_t new_peer_groups = std::max(
              static_cast<int64_t>(0), std::min(gdr_last, gdr - 1) -
                                           std::max(highest_gdr_seen + 1, gdr_first) + 1);
          rank += new_peer_groups;
          highest_gdr_seen = gdr_last;
        }
      }

      results[frame_index] = rank;
    }
  }
}

Status WindowRank_Framed2D::Eval(RankType rank_type,
                                 const BitVectorNavigator& rank_key_tie_begins,
                                 const int64_t* order_by_rank_key,
                                 const WindowFrames& frames, int64_t* results,
                                 ThreadContext& thread_context) {
  int64_t num_rows = rank_key_tie_begins.bit_count();

  if (rank_type == RankType::DENSE_RANK) {
    if (frames.IsSliding()) {
      return DenseRankWithSplayTree();
    } else {
      return DenseRankWithRangeTree();
    }
  }

  ARROW_DCHECK(rank_type == RankType::ROW_NUMBER ||
               rank_type == RankType::RANK_TIES_LOW ||
               rank_type == RankType::RANK_TIES_HIGH);

  ParallelForStream exec_plan;

  // Build merge tree
  //
  MergeTree merge_tree;
  std::vector<int64_t> order_by_rank_key_copy(num_rows);
  memcpy(order_by_rank_key_copy.data(), order_by_rank_key,
         num_rows * sizeof(order_by_rank_key[0]));
  RETURN_NOT_OK(merge_tree.Build(num_rows,
                                 /*level_begin=*/bit_util::Log2(num_rows),
                                 order_by_rank_key_copy.data(), exec_plan));
  RETURN_NOT_OK(exec_plan.RunOnSingleThread(thread_context));

  // For each row compute the number of rows with the lower rank (lower or
  // equal in case of ties high).
  //
  // This will be used as an upper bound on rank attribute when querying
  // merge tree.
  //
  std::vector<int64_t> y_ends;
  std::swap(order_by_rank_key_copy, y_ends);
  auto temp_vector_stack = thread_context.temp_vector_stack;
  {
    TEMP_VECTOR(int64_t, global_ranks);
    BEGIN_MINI_BATCH_FOR(batch_begin, batch_length, num_rows)
    WindowRank_Global::Eval(rank_type, rank_key_tie_begins, batch_begin,
                            batch_begin + batch_length, global_ranks);
    if (rank_type == RankType::RANK_TIES_LOW || rank_type == RankType::ROW_NUMBER) {
      for (int64_t i = 0; i < batch_length; ++i) {
        --global_ranks[i];
      }
    }
    for (int64_t i = 0; i < batch_length; ++i) {
      int64_t row_index = order_by_rank_key[batch_begin + i];
      y_ends[row_index] = global_ranks[i];
    }
    END_MINI_BATCH_FOR
  }

  BEGIN_MINI_BATCH_FOR(batch_begin, batch_length, frames.num_frames)

  // Execute box count queries one batch of frames at a time.
  //
  const int64_t* x_begins_batch[WindowFrames::kMaxRangesInFrame];
  const int64_t* x_ends_batch[WindowFrames::kMaxRangesInFrame];
  for (int64_t range_index = 0; range_index < frames.num_ranges_per_frame;
       ++range_index) {
    x_begins_batch[range_index] = frames.begins[range_index] + batch_begin;
    x_ends_batch[range_index] = frames.ends[range_index] + batch_begin;
  }
  const int64_t* y_ends_batch = y_ends.data() + frames.first_row_index + batch_begin;
  int64_t* results_batch = results + batch_begin;
  merge_tree.BoxCountQuery(batch_length, frames.num_ranges_per_frame, x_begins_batch,
                           x_ends_batch, y_ends_batch, results_batch, thread_context);

  if (rank_type == RankType::RANK_TIES_LOW || rank_type == RankType::ROW_NUMBER) {
    // For TIES_LOW and ROW_NUMBER we need to add 1 to the output of box count
    // query to get the rank.
    //
    for (int64_t i = 0; i < batch_length; ++i) {
      ++results_batch[i];
    }
  } else {
    // For TIES_HIGH we need to add 1 to the output only
    // when the current row is outside of all the ranges defining its frame.
    //
    for (int64_t i = 0; i < batch_length; ++i) {
      results_batch[i] += frames.IsRowInsideItsFrame(batch_begin + i) ? 0 : 1;
    }
  }

  END_MINI_BATCH_FOR

  return Status::OK();
}

void WindowRank_Global_Ref::Eval(RankType rank_type, const BitVectorNavigator& tie_begins,
                                 int64_t* results) {
  int64_t num_rows = tie_begins.bit_count();
  const uint8_t* bit_vector = tie_begins.GetBytes();

  std::vector<int64_t> peer_group_offsets;
  for (int64_t i = 0; i < num_rows; ++i) {
    if (bit_util::GetBit(bit_vector, i)) {
      peer_group_offsets.push_back(i);
    }
  }
  int64_t num_peer_groups = static_cast<int64_t>(peer_group_offsets.size());
  peer_group_offsets.push_back(num_rows);

  for (int64_t peer_group = 0; peer_group < num_peer_groups; ++peer_group) {
    int64_t peer_group_begin = peer_group_offsets[peer_group];
    int64_t peer_group_end = peer_group_offsets[peer_group + 1];
    for (int64_t i = peer_group_begin; i < peer_group_end; ++i) {
      int64_t row_index = i;
      int64_t rank;
      switch (rank_type) {
        case RankType::ROW_NUMBER:
          rank = row_index + 1;
          break;
        case RankType::RANK_TIES_LOW:
          rank = peer_group_begin + 1;
          break;
        case RankType::RANK_TIES_HIGH:
          rank = peer_group_end;
          break;
        case RankType::DENSE_RANK:
          rank = peer_group + 1;
          break;
      }
      results[row_index] = rank;
    }
  }
}

void WindowRank_Framed_Ref::Eval(RankType rank_type,
                                 const BitVectorNavigator& rank_key_tie_begins,
                                 const int64_t* order_by_rank_key,
                                 const WindowFrames& frames, int64_t* results) {
  int64_t num_rows = rank_key_tie_begins.bit_count();

  std::vector<int64_t> global_ranks_order_by_rank_key(num_rows);
  WindowRank_Global_Ref::Eval(rank_type, rank_key_tie_begins,
                              global_ranks_order_by_rank_key.data());

  std::vector<int64_t> global_ranks(num_rows);
  if (!order_by_rank_key) {
    for (int64_t i = 0; i < num_rows; ++i) {
      global_ranks[i] = global_ranks_order_by_rank_key[i];
    }
  } else {
    for (int64_t i = 0; i < num_rows; ++i) {
      global_ranks[order_by_rank_key[i]] = global_ranks_order_by_rank_key[i];
    }
  }

  for (int64_t frame_index = 0; frame_index < frames.num_frames; ++frame_index) {
    int64_t current_row_index = frames.first_row_index + frame_index;

    // Compute list of global ranks for all rows within the frame.
    //
    // Make sure to include the current row in the frame, even if it lies
    // outside of the ranges defining its.
    //
    std::vector<int64_t> global_ranks_within_frame;
    bool current_row_included = false;
    for (int64_t range_index = 0; range_index < frames.num_ranges_per_frame;
         ++range_index) {
      int64_t begin = frames.begins[range_index][frame_index];
      int64_t end = frames.ends[range_index][frame_index];
      if (!current_row_included && current_row_index < begin) {
        global_ranks_within_frame.push_back(global_ranks[current_row_index]);
        current_row_included = true;
      }
      for (int64_t row_index = begin; row_index < end; ++row_index) {
        if (row_index == current_row_index) {
          current_row_included = true;
        }
        global_ranks_within_frame.push_back(global_ranks[row_index]);
      }
    }
    if (!current_row_included) {
      global_ranks_within_frame.push_back(global_ranks[current_row_index]);
      current_row_included = true;
    }

    int64_t rank = 0;
    for (int64_t frame_row_index = 0;
         frame_row_index < static_cast<int64_t>(global_ranks_within_frame.size());
         ++frame_row_index) {
      switch (rank_type) {
        case RankType::ROW_NUMBER:
          // Count the number of rows in the frame with lower global rank.
          //
          if (global_ranks_within_frame[frame_row_index] <
              global_ranks[current_row_index]) {
            ++rank;
          }
          break;
        case RankType::RANK_TIES_LOW:
          // Count the number of rows in the frame with lower global rank.
          //
          if (global_ranks_within_frame[frame_row_index] <
              global_ranks[current_row_index]) {
            ++rank;
          }
          break;
        case RankType::RANK_TIES_HIGH:
          // Count the number of rows in the frame with lower or equal global
          // rank.
          //
          if (global_ranks_within_frame[frame_row_index] <=
              global_ranks[current_row_index]) {
            ++rank;
          }
          break;
        case RankType::DENSE_RANK:
          // Count the number of rows in the frame with lower global rank that
          // have global rank different than the previous row.
          //
          bool global_rank_changed =
              (frame_row_index == 0) || (global_ranks_within_frame[frame_row_index] !=
                                         global_ranks_within_frame[frame_row_index - 1]);
          if (global_ranks_within_frame[frame_row_index] <
                  global_ranks[current_row_index] &&
              global_rank_changed) {
            ++rank;
          }
          break;
      }
    }
    // For all rank types except for RANK_TIES_HIGH increment obtained rank
    // value by 1.
    //
    if (rank_type != RankType::RANK_TIES_HIGH) {
      ++rank;
    }

    results[frame_index] = rank;
  }
}

}  // namespace compute
}  // namespace arrow
