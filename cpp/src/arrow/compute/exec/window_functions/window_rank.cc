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

class GroupPrevRankCalculator {
 public:
  GroupPrevRankCalculator(int64_t num_rows, const int64_t* group_ids_sorted,
                          const int64_t* permutation) {
    int64_t num_bit_words = bit_util::CeilDiv(num_rows, 64);

    last_in_group_bitvec_.resize(num_bit_words);
    memset(last_in_group_bitvec_.data(), 0, num_bit_words * sizeof(uint64_t));
    for (int64_t i = 0; i < num_rows; ++i) {
      bool last_in_group =
          (i == (num_rows - 1) || group_ids_sorted[i + 1] != group_ids_sorted[i]);
      if (last_in_group) {
        bit_util::SetBit(reinterpret_cast<uint8_t*>(last_in_group_bitvec_.data()),
                         permutation[i]);
      }
    }
    last_in_group_popcounts_.resize(num_bit_words);
    BitVectorNavigator::GenPopCounts(num_rows, last_in_group_bitvec_.data(),
                                     last_in_group_popcounts_.data());

    num_rows_ = num_rows;
    num_groups_ = BitVectorNavigator::PopCount(num_rows, last_in_group_bitvec_.data(),
                                               last_in_group_popcounts_.data());

    first_in_group_bitvec_.resize(num_bit_words);
    memset(first_in_group_bitvec_.data(), 0, num_bit_words * sizeof(uint64_t));
    for (int64_t i = 0; i < num_rows; ++i) {
      bool first_in_group = (i == 0 || group_ids_sorted[i - 1] != group_ids_sorted[i]);
      if (first_in_group) {
        bit_util::SetBit(reinterpret_cast<uint8_t*>(first_in_group_bitvec_.data()),
                         permutation[i]);
      }
    }
    first_in_group_popcounts_.resize(num_bit_words);
    BitVectorNavigator::GenPopCounts(num_rows, first_in_group_bitvec_.data(),
                                     first_in_group_popcounts_.data());
  }

  // Prev is 0-based row number
  // Returns 0-based row number
  int64_t Rank(int64_t row_number, int64_t prev) {
    if (bit_util::GetBit(reinterpret_cast<const uint8_t*>(first_in_group_bitvec_.data()),
                         row_number)) {
      return BitVectorNavigator::Rank(row_number, first_in_group_bitvec_.data(),
                                      first_in_group_popcounts_.data());
    } else {
      return num_groups_ + prev -
             BitVectorNavigator::Rank(prev, last_in_group_bitvec_.data(),
                                      last_in_group_popcounts_.data());
    }
  }
  // Prev is 0-based row number
  // Returns 0-based row number
  int64_t RankEnd(int64_t prev_end) {
    if (prev_end == num_rows_) {
      return num_rows_;
    }
    return num_groups_ + prev_end -
           BitVectorNavigator::Rank(prev_end, last_in_group_bitvec_.data(),
                                    last_in_group_popcounts_.data());
  }

 private:
  int64_t num_rows_;
  int64_t num_groups_;
  std::vector<uint64_t> last_in_group_bitvec_;
  std::vector<uint64_t> last_in_group_popcounts_;
  std::vector<uint64_t> first_in_group_bitvec_;
  std::vector<uint64_t> first_in_group_popcounts_;
};

void WindowRank::Global(RankType rank_type, int64_t num_rows, const uint64_t* ties_bitvec,
                        const uint64_t* ties_popcounts, int64_t* output,
                        int64_t hardware_flags,
                        util::TempVectorStack* temp_vector_stack) {
  switch (rank_type) {
    case RankType::RANK_TIES_LOW:
    case RankType::RANK_TIES_HIGH:
      GlobalRank(rank_type == RankType::RANK_TIES_LOW, num_rows, ties_bitvec,
                 ties_popcounts, output, hardware_flags, temp_vector_stack);
      break;
    case RankType::DENSE_RANK:
      GlobalDenseRank(num_rows, ties_bitvec, ties_popcounts, output);
      break;
    case RankType::ROW_NUMBER:
      GlobalRowNumber(num_rows, output);
      break;
  }
}

void WindowRank::WithinFrame(RankType rank_type, int64_t num_rows,
                             const uint64_t* ties_bitvec, const uint64_t* ties_popcounts,
                             const int64_t* frame_begins, const int64_t* frame_ends,
                             int64_t* output, int64_t hardware_flags,
                             util::TempVectorStack* temp_vector_stack) {
  switch (rank_type) {
    case RankType::RANK_TIES_LOW:
    case RankType::RANK_TIES_HIGH:
      RankWithinFrame(rank_type == RankType::RANK_TIES_LOW, num_rows, ties_bitvec,
                      ties_popcounts, frame_begins, frame_ends, output, hardware_flags,
                      temp_vector_stack);
      break;
    case RankType::DENSE_RANK:
      DenseRankWithinFrame(num_rows, ties_bitvec, ties_popcounts, frame_begins,
                           frame_ends, output);
      break;
    case RankType::ROW_NUMBER:
      RowNumberWithinFrame(num_rows, frame_begins, frame_ends, output);
      break;
  }
}

void WindowRank::OnSeparateAttribute(RankType rank_type, int64_t num_rows,
                                     const int64_t* global_ranks_sorted,
                                     const int64_t* permutation, bool progressive_frames,
                                     const int64_t* frame_begins,
                                     const int64_t* frame_ends, int64_t* output,
                                     int64_t hardware_flags,
                                     util::TempVectorStack* temp_vector_stack) {
  switch (rank_type) {
    case RankType::ROW_NUMBER:
    case RankType::RANK_TIES_LOW:
    case RankType::RANK_TIES_HIGH:
      if (!progressive_frames) {
        SeparateAttributeRank(rank_type == RankType::RANK_TIES_LOW, num_rows,
                              frame_begins, frame_ends, global_ranks_sorted, permutation,
                              output, hardware_flags, temp_vector_stack);
      } else {
        ProgressiveSeparateAttributeRank(
            /*dense_rank=*/false, rank_type == RankType::RANK_TIES_LOW, num_rows,
            frame_begins, frame_ends, global_ranks_sorted, output);
      }
      break;
    case RankType::DENSE_RANK:
      if (!progressive_frames) {
        SeparateAttributeDenseRank(num_rows, frame_begins, frame_ends,
                                   global_ranks_sorted, permutation, output,
                                   hardware_flags, temp_vector_stack);
      } else {
        ProgressiveSeparateAttributeRank(
            /*dense_rank=*/true, false, num_rows, frame_begins, frame_ends,
            global_ranks_sorted, output);
      }
      break;
  }
}

void WindowRank::GlobalRank(bool ties_low, int64_t num_rows, const uint64_t* bitvec,
                            const uint64_t* popcounts, int64_t* output,
                            int64_t hardware_flags,
                            util::TempVectorStack* temp_vector_stack) {
  // Range of indices for groups of ties in entire input
  int64_t rank_begin = 0;
  int64_t rank_end = BitVectorNavigator::PopCount(num_rows, bitvec, popcounts);

  // Break groups of ties into minibatches
  int64_t minibatch_length_max = util::MiniBatch::kMiniBatchLength - 1;
  auto selects_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(minibatch_length_max + 1));
  auto selects = selects_buf.mutable_data();
  for (int64_t minibatch_begin = rank_begin; minibatch_begin < rank_end;
       minibatch_begin += minibatch_length_max) {
    int64_t minibatch_end = std::min(rank_end, minibatch_begin + minibatch_length_max);

    // Get first (and last) row number for each group of ties in minibatch
    BitVectorNavigator::SelectsForRangeOfRanks(minibatch_begin, minibatch_end + 1,
                                               num_rows, bitvec, popcounts, selects,
                                               hardware_flags, temp_vector_stack);
    if (ties_low) {
      for (int64_t ties_group = 0; ties_group < minibatch_end - minibatch_begin;
           ++ties_group) {
        for (int64_t i = selects[ties_group]; i < selects[ties_group + 1]; ++i) {
          output[i] = selects[ties_group] + 1;
        }
      }
    } else {
      for (int64_t ties_group = 0; ties_group < minibatch_end - minibatch_begin;
           ++ties_group) {
        for (int64_t i = selects[ties_group]; i < selects[ties_group + 1]; ++i) {
          output[i] = selects[ties_group + 1];
        }
      }
    }
  }
}

void WindowRank::GlobalDenseRank(int64_t num_rows, const uint64_t* bitvec,
                                 const uint64_t* popcounts, int64_t* output) {
  for (int64_t i = 0; i < num_rows; ++i) {
    output[i] = BitVectorNavigator::RankNext(i, bitvec, popcounts);
  }
}

void WindowRank::GlobalRowNumber(int64_t num_rows, int64_t* output) {
  std::iota(output, output + num_rows, 1LL);
}

void WindowRank::RankWithinFrame(bool ties_low, int64_t num_rows, const uint64_t* bitvec,
                                 const uint64_t* popcounts, const int64_t* frame_begins,
                                 const int64_t* frame_ends, int64_t* output,
                                 int64_t hardware_flags,
                                 util::TempVectorStack* temp_vector_stack) {
  GlobalRank(ties_low, num_rows, bitvec, popcounts, output, hardware_flags,
             temp_vector_stack);
  for (int64_t i = 0; i < num_rows; ++i) {
    // If the frame does not contain current row it is still logically
    // considered as included in the frame (e.g. empty frame will yield rank
    // 1 since the set we look at consists of a single row - current row).

    // The case of an empty frame
    if (frame_begins[i] >= frame_ends[i]) {
      output[i] = 1;
      continue;
    }

    bool tie_with_first =
        BitVectorNavigator::RankNext(i, bitvec, popcounts) ==
        BitVectorNavigator::RankNext(frame_begins[i], bitvec, popcounts);
    bool tie_with_last =
        BitVectorNavigator::RankNext(i, bitvec, popcounts) ==
        BitVectorNavigator::RankNext(frame_ends[i] - 1, bitvec, popcounts);
    if (!tie_with_first) {
      if (i < frame_begins[i]) {
        output[i] = 1;
      } else if (i >= frame_ends[i]) {
        if (tie_with_last) {
          output[i] -= frame_begins[i];
        } else {
          output[i] = frame_ends[i] - frame_begins[i] + 1;
        }
      } else {
        output[i] -= frame_begins[i];
      }
    } else {
      if (tie_with_last) {
        output[i] = 1;
      } else {
        // Bit vector rank of current row is the same as the beginning of
        // the frame but different than for the last row of the frame, which
        // means that current row must appear before the last row of the
        // frame.
        //
        ARROW_DCHECK(i < frame_ends[i]);
        if (ties_low) {
          output[i] = 1;
        } else {
          if (i < frame_begins[i]) {
            output[i] -= frame_begins[i] - 1;
          } else {
            output[i] -= frame_begins[i];
          }
        }
      }
    }
  }
}

void WindowRank::DenseRankWithinFrame(int64_t num_rows, const uint64_t* bitvec,
                                      const uint64_t* popcounts,
                                      const int64_t* frame_begins,
                                      const int64_t* frame_ends, int64_t* output) {
  for (int64_t i = 0; i < num_rows; ++i) {
    if (frame_begins[i] >= frame_ends[i]) {
      output[i] = 1;
      continue;
    }

    if (i < frame_begins[i]) {
      output[i] = 1;
      continue;
    }
    if (i >= frame_ends[i]) {
      bool tie_with_last =
          BitVectorNavigator::RankNext(i, bitvec, popcounts) ==
          BitVectorNavigator::RankNext(frame_ends[i] - 1, bitvec, popcounts);
      output[i] = BitVectorNavigator::RankNext(frame_ends[i] - 1, bitvec, popcounts) -
                  BitVectorNavigator::RankNext(frame_begins[i], bitvec, popcounts) + 1 +
                  (tie_with_last ? 0 : 1);
      continue;
    }

    output[i] = BitVectorNavigator::RankNext(i, bitvec, popcounts) -
                BitVectorNavigator::RankNext(frame_begins[i], bitvec, popcounts) + 1;
  }
}

void WindowRank::RowNumberWithinFrame(int64_t num_rows, const int64_t* frame_begins,
                                      const int64_t* frame_ends, int64_t* output) {
  for (int64_t i = 0; i < num_rows; ++i) {
    if (frame_begins[i] >= frame_ends[i]) {
      output[i] = 1;
      continue;
    }

    if (i < frame_begins[i]) {
      output[i] = 1;
      continue;
    }

    if (i >= frame_ends[i]) {
      output[i] = frame_ends[i] - frame_begins[i] + 1;
      continue;
    }

    output[i] = i - frame_begins[i] + 1;
  }
}

void WindowRank::SeparateAttributeRank(
    bool ties_low,
    /* number of rows and number of frames */
    int64_t num_rows, const int64_t* begins, const int64_t* ends,
    /* Sorted (in ascending order) ranks (with respect to ranking attribute)
       for all rows */
    /* null can be passed if all ranks are distinct (in which case the
 sorted array would just contain sequence of integers from 1 to num_rows).
 Supplying null changes the semantics from rank to row number. */
    const int64_t* ranks_sorted,
    /* Permutation of row numbers that results in sortedness on ranking
       attribute */
    const int64_t* permutation, int64_t* output, int64_t hardware_flags,
    util::TempVectorStack* temp_vector_stack) {
  // Build merge tree
  //
  MergeTree merge_tree;
  merge_tree.Build(num_rows, permutation,
                   /* number of top levels to skip */ 0, hardware_flags,
                   temp_vector_stack);

  // Ties low means outputting the number of rows in window frame with rank
  // lower than current row plus 1. Initialize output counter accordingly.
  //
  int64_t delta = ties_low ? 1 : 0;
  std::fill_n(output, num_rows, delta);

  // For each row compute the number of rows with the lower rank (lower or
  // equal in case of ties high).
  //
  // This will be used as an upper bound on rank attribute when querying
  // merge tree.
  //
  std::vector<int64_t> y_ends(num_rows);
  for (int64_t i = 0; i < num_rows; ++i) {
    y_ends[permutation[i]] = (ranks_sorted ? ranks_sorted[i] : (i + 1)) + delta;
  }

  BEGIN_MINI_BATCH_FOR(batch_begin, batch_length, num_rows)

  merge_tree.MiniBatchRangeQuery(batch_length, begins + batch_begin, ends + batch_begin,
                                 y_ends.data() + batch_begin, temp_vector_stack,
                                 [&](int64_t iquery, int64_t node_begin, int64_t y_end) {
                                   output[batch_begin + iquery] += y_end - node_begin;
                                 });

  END_MINI_BATCH_FOR
}

void WindowRank::SeparateAttributeDenseRank(
    int64_t num_rows, const int64_t* begins, const int64_t* ends,
    /* The following two arrays must be the result of sorting rows on
      (global dense rank, row number within window) pairs. Within a group of
      peers with that same dense ranks rows must come in the order in which
      they appeared in the window. This could be accomplished by a stable
      sort of window on the dense rank value.
    */
    const int64_t* global_dense_ranks_sorted, const int64_t* permutation, int64_t* output,
    int64_t hardware_flags, util::TempVectorStack* temp_vector_stack) {
  // Mapping to the coordinates (x, y, z) used by range tree is described
  // below.
  //
  // Definitions are picked so that for each attribute every row has a
  // distinct coordinate in the range [0, num_rows).
  //
  // The coordinates correspond to position in the sorted array for
  // different sort orders: x - sorting on previous occurrence of the row
  // with the same global dense rank y - window ordering z - sorting on
  // global dense rank
  //
  // x:
  // - for the first row in each dense rank group, dense rank minus 1,
  // for other rows, number of rows preceding the previous row in the same
  // dense rank group that are not the last in their dense rank groups.
  // Alternative way of viewing this is the position in the array sorted on
  // the following function: 1 plus position in the window sort order of the
  // previous occurrence of the row with the same dense rank (current row's
  // peer in the global dense rank group) or 0 if there is no previous
  // occurrence.
  // - exclusive upper bound for this attribute in the range query for ith
  // frame is: number of dense rank groups plus the number of rows preceding
  // begins[i] in the window order that are not the last in their respective
  // dense rank groups.
  // Alternative way of viewing this is std::lower_bound for begins[i] + 1
  // in the sorted array introduced in the description of an alternative
  // view of x attribute.
  //
  // y:
  // - row number in the window sort order (sort order used to
  // compute frame boundaries)
  // - range query uses begins[i], ends[i] as a range filter on this
  // attribute for ith frame
  //
  // z:
  // - position in the array sorted on global dense rank
  // - range query uses number of rows with global dense rank less than that
  // of the current row of ith frame
  //
  GroupPrevRankCalculator x_calc(num_rows, global_dense_ranks_sorted, permutation);

  RangeTree tree;
  {
    std::vector<int64_t> x_sorted_on_z(num_rows);
    for (int64_t i = 0; i < num_rows; ++i) {
      bool has_next = (i < num_rows - 1) &&
                      (global_dense_ranks_sorted[i] == global_dense_ranks_sorted[i + 1]);
      if (has_next) {
        int64_t y = permutation[i + 1];
        int64_t z = i + 1;
        int64_t prev = permutation[i];
        int64_t x = x_calc.Rank(y, prev);
        x_sorted_on_z[z] = x;
      }
      bool has_prev =
          (i > 0) && (global_dense_ranks_sorted[i] == global_dense_ranks_sorted[i - 1]);
      if (!has_prev) {
        int64_t y = permutation[i];
        int64_t z = i;
        int64_t x = x_calc.Rank(y, -1);
        x_sorted_on_z[z] = x;
      }
    }

    const int64_t* y_sorted_on_z = permutation;
    tree.Build(num_rows, x_sorted_on_z.data(), y_sorted_on_z, hardware_flags,
               temp_vector_stack);
  }

  // For each frame compute upper bound on z coordinate
  //
  std::vector<int64_t> z_ends(num_rows);
  int64_t first_in_group;
  for (int64_t i = 0; i < num_rows; ++i) {
    bool is_first_in_group =
        (i == 0) || global_dense_ranks_sorted[i - 1] != global_dense_ranks_sorted[i];
    if (is_first_in_group) {
      first_in_group = i;
    }
    z_ends[permutation[i]] = first_in_group;
  }

  TEMP_VECTOR(int64_t, x_ends);

  BEGIN_MINI_BATCH_FOR(batch_begin, batch_length, num_rows)

  for (int64_t i = batch_begin; i < batch_begin + batch_length; ++i) {
    x_ends[i - batch_begin] = x_calc.RankEnd(begins[i]);
  }

  tree.BoxCount(batch_length, x_ends, begins + batch_begin, ends + batch_begin,
                z_ends.data() + batch_begin, output + batch_begin, hardware_flags,
                temp_vector_stack);

  // Output is 1 plus the number of rows satisfying range query
  //
  for (int64_t i = batch_begin; i < batch_begin + batch_length; ++i) {
    ++output[i];
  }

  END_MINI_BATCH_FOR
}

void WindowRank::ProgressiveSeparateAttributeRank(bool dense_rank, bool ties_low,
                                                  int64_t num_rows, const int64_t* begins,
                                                  const int64_t* ends,
                                                  const int64_t* global_ranks,
                                                  int64_t* output) {
  if (dense_rank) {
    ProgressiveSeparateAttributeRankImp<true>(false, num_rows, begins, ends, global_ranks,
                                              output);
  } else {
    ProgressiveSeparateAttributeRankImp<false>(ties_low, num_rows, begins, ends,
                                               global_ranks, output);
  }
}

template <bool T_DENSE_RANK>
void WindowRank::ProgressiveSeparateAttributeRankImp(bool ties_low, int64_t num_rows,
                                                     const int64_t* begins,
                                                     const int64_t* ends,
                                                     const int64_t* global_ranks,
                                                     int64_t* output) {
  SplayTree tree;
  int64_t begin = begins[0];
  int64_t end = begin;

  for (int64_t iframe = 0; iframe < num_rows; ++iframe) {
    int64_t frame_begin = begins[iframe];
    int64_t frame_end = ends[iframe];
    ARROW_DCHECK(frame_begin >= begin && frame_end >= end);

    if (end <= frame_begin) {
      tree.Clear();
      begin = end = frame_begin;
    }

    while (begin < frame_begin) {
      tree.Remove(global_ranks[begin++]);
      ARROW_DCHECK(begin <= end);
    }
    while (frame_end > end) {
      tree.Insert(global_ranks[end++]);
    }

    if (T_DENSE_RANK) {
      output[iframe] = tree.DenseRank(global_ranks[iframe]);
    } else {
      output[iframe] = tree.Rank(ties_low, global_ranks[iframe]);
    }
  }
}

void WindowRankBasic::Global(RankType rank_type, int64_t num_rows, const uint64_t* bitvec,
                             int64_t* output) {
  int64_t current_group_id;
  int64_t first_in_group;
  int64_t num_in_group;
  for (int64_t i = 0; i < num_rows; ++i) {
    if (i == 0) {
      current_group_id = 0;
      first_in_group = 0;
      num_in_group = 1;
      for (num_in_group = 1; first_in_group + num_in_group < num_rows; ++num_in_group) {
      }
    } else {
      if (bit_util::GetBit(reinterpret_cast<const uint8_t*>(bitvec), i)) {
        ++current_group_id;
        first_in_group = i;
      }
    }
    if (first_in_group == i) {
      while (first_in_group + num_in_group < num_rows &&
             !bit_util::GetBit(reinterpret_cast<const uint8_t*>(bitvec),
                               first_in_group + num_in_group)) {
        ++num_in_group;
      }
    }

    switch (rank_type) {
      case RankType::ROW_NUMBER:
        output[i] = i + 1;
        break;
      case RankType::RANK_TIES_LOW:
        output[i] = first_in_group + 1;
        break;
      case RankType::RANK_TIES_HIGH:
        output[i] = first_in_group + num_in_group;
        break;
      case RankType::DENSE_RANK:
        output[i] = current_group_id + 1;
        break;
    }
  }
}

void WindowRankBasic::WithinFrame(RankType rank_type, int64_t num_rows,
                                  const uint64_t* bitvec, const int64_t* frame_begins,
                                  const int64_t* frame_ends, int64_t* output) {
  for (int64_t i = 0; i < num_rows; ++i) {
    int64_t begin = frame_begins[i];
    int64_t end = frame_ends[i];
    if (end == begin) {
      output[i] = 1;
      continue;
    }
    int64_t num_words = bit_util::CeilDiv(end - begin + 1, 64);
    std::vector<uint64_t> frame_bitvec(num_words);
    memset(frame_bitvec.data(), 0, num_words * sizeof(uint64_t));
    if (i < begin) {
      output[i] = 1;
      continue;
    }
    for (int64_t j = 0; j < end - begin; ++j) {
      if (bit_util::GetBit(reinterpret_cast<const uint8_t*>(bitvec), j)) {
        bit_util::SetBit(reinterpret_cast<uint8_t*>(frame_bitvec.data()), j);
      }
    }
    bool one_more_group = false;
    if (i >= end) {
      for (int64_t j = end; j <= i; ++j) {
        if (bit_util::GetBit(reinterpret_cast<const uint8_t*>(bitvec), j)) {
          one_more_group = true;
          bit_util::SetBit(reinterpret_cast<uint8_t*>(frame_bitvec.data()), end - begin);
          break;
        }
      }
    }
    std::vector<int64_t> frame_output(end - begin + 1);
    Global(rank_type, end - begin + (one_more_group ? 1 : 0), frame_bitvec.data(),
           frame_output.data());
    output[i] = frame_output[std::min(end, i) - begin];
  }
}

void WindowRankBasic::SeparateAttribute(RankType rank_type, int64_t num_rows,
                                        const int64_t* begins, const int64_t* ends,
                                        const int64_t* global_ranks_sorted,
                                        const int64_t* permutation, int64_t* output) {
  if (num_rows == 0) {
    return;
  }

  std::vector<int64_t> inverse_permutation(num_rows);
  for (int64_t i = 0; i < num_rows; ++i) {
    inverse_permutation[permutation[i]] = i;
  }

  for (int64_t i = 0; i < num_rows; ++i) {
    int64_t begin = begins[i];
    int64_t end = ends[i];
    if (end == begin) {
      output[i] = 1;
      continue;
    }

    // position in the array of sorted global ranks and row number
    std::vector<std::pair<int64_t, int64_t>> rank_row;
    for (int64_t j = begin; j < end; ++j) {
      rank_row.push_back(std::make_pair(inverse_permutation[j], j));
    }
    bool one_more_group = false;
    if (i >= end) {
      rank_row.push_back(std::make_pair(inverse_permutation[i], i));
      if (global_ranks_sorted[inverse_permutation[i]] >
          global_ranks_sorted[inverse_permutation[end - 1]]) {
        one_more_group = true;
      }
    }

    std::sort(rank_row.begin(), rank_row.end());

    int64_t num_words = bit_util::CeilDiv(end - begin + 1, 64);
    std::vector<uint64_t> frame_bitvec(num_words);
    memset(frame_bitvec.data(), 0, num_words * sizeof(uint64_t));
    if (i < begin) {
      output[i] = 1;
      continue;
    }
    for (int64_t j = 0; j < end - begin + (one_more_group ? 1 : 0); ++j) {
      if (j == 0 || global_ranks_sorted[rank_row[j - 1].first] !=
                        global_ranks_sorted[rank_row[j].first]) {
        bit_util::SetBit(reinterpret_cast<uint8_t*>(frame_bitvec.data()), j);
      }
    }
    std::vector<int64_t> frame_output(end - begin + 1);
    Global(rank_type, end - begin + (one_more_group ? 1 : 0), frame_bitvec.data(),
           frame_output.data());
    for (int64_t j = 0; j < end - begin + (one_more_group ? 1 : 0); ++j) {
      if (rank_row[j].second == i) {
        output[i] = frame_output[j];
        break;
      }
    }
  }
}

void WindowRankTest::TestRank(RankType rank_type, bool separate_ranking_attribute,
                              bool use_frames, bool use_progressive_frames) {
  Random64BitCopy rand;
  MemoryPool* pool = default_memory_pool();
  util::TempVectorStack temp_vector_stack;
  Status status = temp_vector_stack.Init(pool, 128 * util::MiniBatch::kMiniBatchLength);
  ARROW_DCHECK(status.ok());
  int64_t hardware_flags = 0LL;

  constexpr int num_tests = 100;
  const int num_tests_to_skip = 0;
  for (int test = 0; test < num_tests; ++test) {
    // Generate random values
    //
    constexpr int64_t max_rows = 1100;
    int64_t num_rows = rand.from_range(static_cast<int64_t>(1LL), max_rows);
    std::vector<int64_t> vals(num_rows);
    constexpr int64_t max_val = 65535;
    int tie_probability = rand.from_range(0, 256);
    for (int64_t i = 0; i < num_rows; ++i) {
      bool tie = rand.from_range(0, 255) < tie_probability;
      if (tie && i > 0) {
        vals[i] = vals[rand.from_range(static_cast<int64_t>(0LL), i - 1)];
      } else {
        vals[i] = rand.from_range(static_cast<int64_t>(0LL), max_val);
      }
    }

    // Generate random frames
    //
    std::vector<int64_t> begins;
    std::vector<int64_t> ends;
    GenerateTestFrames(rand, num_rows, begins, ends,
                       /*progressive=*/use_progressive_frames,
                       /*expansive=*/false);

    if (test < num_tests_to_skip) {
      continue;
    }

    // Sort values and output permutation and bit vector of ties
    //
    int64_t num_bit_words = bit_util::CeilDiv(num_rows, 64);
    std::vector<uint64_t> ties_bitvec(num_bit_words);
    std::vector<uint64_t> ties_popcounts(num_bit_words);
    std::vector<int64_t> permutation(num_rows);
    {
      std::vector<std::pair<int64_t, int64_t>> val_row_pairs(num_rows);
      for (int64_t i = 0; i < num_rows; ++i) {
        val_row_pairs[i] = std::make_pair(vals[i], i);
      }
      std::sort(val_row_pairs.begin(), val_row_pairs.end());
      for (int64_t i = 0; i < num_rows; ++i) {
        permutation[i] = val_row_pairs[i].second;
      }
      memset(ties_bitvec.data(), 0, num_bit_words * sizeof(uint64_t));
      for (int64_t i = 0; i < num_rows; ++i) {
        bool is_first_in_group =
            (i == 0 || val_row_pairs[i - 1].first != val_row_pairs[i].first);
        if (is_first_in_group) {
          bit_util::SetBit(reinterpret_cast<uint8_t*>(ties_bitvec.data()), i);
        }
      }
      BitVectorNavigator::GenPopCounts(num_rows, ties_bitvec.data(),
                                       ties_popcounts.data());
    }

    // Generate global ranks for the case when window frames use different
    // row order
    //
    std::vector<int64_t> global_ranks(num_rows);
    WindowRankBasic::Global(rank_type, num_rows, ties_bitvec.data(), global_ranks.data());

    printf("num_rows %d ", static_cast<int>(num_rows));

    std::vector<int64_t> output[2];
    output[0].resize(num_rows);
    output[1].resize(num_rows);

    int64_t num_repeats;
#ifndef NDEBUG
    num_repeats = 1;
#else
    num_repeats = std::max(1LL, 1024 * 1024LL / num_rows);
#endif
    printf("num_repeats %d ", static_cast<int>(num_repeats));

    // int64_t start = __rdtsc();
    for (int repeat = 0; repeat < num_repeats; ++repeat) {
      if (!use_frames) {
        WindowRankBasic::Global(rank_type, num_rows, ties_bitvec.data(),
                                output[0].data());
      } else if (!separate_ranking_attribute) {
        WindowRankBasic::WithinFrame(rank_type, num_rows, ties_bitvec.data(),
                                     begins.data(), ends.data(), output[0].data());
      } else {
        WindowRankBasic::SeparateAttribute(rank_type, num_rows, begins.data(),
                                           ends.data(), global_ranks.data(),
                                           permutation.data(), output[0].data());
      }
    }
    // int64_t end = __rdtsc();
    // printf("cpr basic %.1f ",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));
    // start = __rdtsc();
    for (int repeat = 0; repeat < num_repeats; ++repeat) {
      if (!use_frames) {
        WindowRank::Global(rank_type, num_rows, ties_bitvec.data(), ties_popcounts.data(),
                           output[1].data(), hardware_flags, &temp_vector_stack);
      } else if (!separate_ranking_attribute) {
        WindowRank::WithinFrame(rank_type, num_rows, ties_bitvec.data(),
                                ties_popcounts.data(), begins.data(), ends.data(),
                                output[1].data(), hardware_flags, &temp_vector_stack);
      } else {
        WindowRank::OnSeparateAttribute(rank_type, num_rows, global_ranks.data(),
                                        permutation.data(), use_progressive_frames,
                                        begins.data(), ends.data(), output[1].data(),
                                        hardware_flags, &temp_vector_stack);
      }
    }
    // end = __rdtsc();
    // printf("cpr normal %.1f ",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));

    bool ok = true;
    for (int64_t i = 0; i < num_rows; ++i) {
    }
    printf("%s\n", ok ? "correct" : "wrong");
  }
}

}  // namespace compute
}  // namespace arrow
