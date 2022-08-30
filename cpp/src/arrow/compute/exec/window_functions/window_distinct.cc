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

#include "arrow/compute/exec/window_functions/window_distinct.h"
#include <cstdint>
#include <unordered_set>  // For test
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec/window_functions/bit_vector_navigator.h"
#include "arrow/compute/exec/window_functions/merge_tree.h"
#include "arrow/compute/exec/window_functions/radix_sort.h"

namespace arrow {
namespace compute {

void WindowDistinct::Init(int64_t num_rows, int64_t num_groups,
                          const int64_t* group_ids, /*const WindowFrames &frames,*/
                          int64_t hardware_flags,
                          util::TempVectorStack* temp_vector_stack) {
  num_rows_ = num_rows;
  num_groups_ = num_groups;

  std::vector<int64_t> group_ids_sorted(num_rows);
  std::vector<int64_t> row_numbers_sorted_on_group_id(num_rows);
  RadixSort::Sort(num_rows, num_groups - 1, group_ids, group_ids_sorted.data(),
                  row_numbers_sorted_on_group_id.data());

  std::vector<int64_t> prev(num_rows);
  GenListsForGroups(num_rows, num_groups, group_ids_sorted.data(),
                    row_numbers_sorted_on_group_id.data(), prev.data(), nullptr);

  int64_t num_bit_words = bit_util::CeilDiv(num_rows_, 64);
  std::vector<uint64_t> has_prev_bitvec(num_bit_words);
  std::vector<uint64_t> has_prev_popcounts(num_bit_words);
  has_next_bitvec_.resize(num_bit_words);
  has_next_popcounts_.resize(num_bit_words);
  GenHasPrevNextBitvecs(num_rows_, group_ids_sorted.data(),
                        row_numbers_sorted_on_group_id.data(), has_prev_bitvec.data(),
                        has_next_bitvec_.data());
  BitVectorNavigator::GenPopCounts(num_rows_, has_prev_bitvec.data(),
                                   has_prev_popcounts.data());
  BitVectorNavigator::GenPopCounts(num_rows_, has_next_bitvec_.data(),
                                   has_next_popcounts_.data());

  row_numbers_sorted_on_prev_.resize(num_rows);
  GenRowNumbersSortedOnPrev(prev.data(), has_prev_bitvec.data(),
                            has_prev_popcounts.data(),
                            row_numbers_sorted_on_prev_.data());

  // std::vector<int64_t> row_numbers_sorted_on_prev1(num_rows);
  // count_prev_LE_.resize(num_rows);
  // GenCountLE(num_rows, num_rows - 1, prev.data(), count_prev_LE_.data(),
  //            row_numbers_sorted_on_prev1.data());

  // for (int64_t i = 0; i < num_rows_; ++i) {
  //   ARROW_DCHECK(row_numbers_sorted_on_prev[i] ==
  //                row_numbers_sorted_on_prev1[i]);
  // }

  merge_tree_.Build(num_rows_, row_numbers_sorted_on_prev_.data(),
                    /*num_levels_to_skip=*/0, hardware_flags, temp_vector_stack);
}

void WindowDistinct::Count(const WindowFrames& frames, int64_t* output,
                           int64_t hardware_flags,
                           util::TempVectorStack* temp_vector_stack) const {
  memset(output, 0, frames.num_frames * sizeof(int64_t));

  int64_t batch_length_max = util::MiniBatch::kMiniBatchLength;
  auto query_states_buf = util::TempVectorHolder<MergeTree::RangeQueryState>(
      temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  MergeTree::RangeQueryState* query_states = query_states_buf.mutable_data();
  auto query_outputs_buf = util::TempVectorHolder<MergeTree::RangeQueryState>(
      temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  MergeTree::RangeQueryState* query_outputs = query_outputs_buf.mutable_data();

  int64_t num_frames = frames.num_frames;
  const int64_t* begins = frames.begins[0];
  const int64_t* ends = frames.ends[0];

  int tree_height = merge_tree_.get_height();

  if (tree_height == 1) {
    // Special case when there is at most one row
    //
    ARROW_DCHECK(num_rows_ < 2);
    if (num_rows_) {
      output[0] = (frames.ends[0][0] > frames.begins[0][0]) ? 1 : 0;
    }
    return;
  }

  for (int64_t batch_begin = 0; batch_begin < num_frames;
       batch_begin += batch_length_max) {
    int64_t batch_length_next = std::min(num_frames - batch_begin, batch_length_max);

    for (int64_t i = batch_begin; i < batch_begin + batch_length_next; ++i) {
      int64_t top_level_rank = CountPrevLE(begins[i]);
      // int64_t top_level_rank1 = count_prev_LE_[begins[i]];
      // ARROW_DCHECK(top_level_rank == top_level_rank1);
      query_states[i - batch_begin].pos[0] =
          MergeTree::RangeQueryState::PosFromNodeAndLength(tree_height - 1, 0LL,
                                                           top_level_rank);
      query_states[i - batch_begin].pos[1] = MergeTree::RangeQueryState::kEmpty;
    }

    for (int level = tree_height - 1; level > 0; --level) {
      merge_tree_.RangeQueryStep(level, batch_length_next, begins + batch_begin,
                                 ends + batch_begin, query_states, query_outputs);
      for (int64_t i = batch_begin; i < batch_begin + batch_length_next; ++i) {
        for (int ioutput_pos = 0; ioutput_pos < 2; ++ioutput_pos) {
          int64_t output_pos = query_outputs[i - batch_begin].pos[ioutput_pos];
          if (output_pos != MergeTree::RangeQueryState::kEmpty) {
            int64_t output_node;
            int64_t output_length;
            MergeTree::RangeQueryState::NodeAndLengthFromPos(
                level - 1, output_pos, &output_node, &output_length);
            output[i] += output_length;
          }
        }
      }
    }
  }
}

void WindowDistinct::Sum(const WindowFrames& frames, WindowAggregateFunc* func,
                         WindowAggregateFunc::Vector* results, uint8_t* results_validity,
                         int64_t hardware_flags,
                         util::TempVectorStack* temp_vector_stack) const {
  int64_t num_frames = frames.num_frames;
  const int64_t* begins = frames.begins[0];
  const int64_t* ends = frames.ends[0];

  int64_t batch_length_max = util::MiniBatch::kMiniBatchLength;

  int tree_height = merge_tree_.get_height();

  // Zero out the result
  //
  func->Zero(num_frames, results, 0);
  memset(results_validity, 0xff, bit_util::BytesForBits(num_frames));

  // Allocate temporary buffers
  //

  WindowAggregateFunc::Vector* sum_buf[2];
  sum_buf[0] = func->Alloc(num_frames);
  sum_buf[1] = func->Alloc(num_frames);

  if (tree_height == 1) {
    // Special case when there is at most one row
    //
    ARROW_DCHECK(num_rows_ < 2);
    if (num_rows_) {
      if (ends[0] > begins[0]) {
        func->Zero(1, results, 0);
        int64_t out_id = 0, in_id = 0;
        func->Sum(1, results, &out_id, nullptr, &in_id);
      } else {
        bit_util::ClearBit(results_validity, 0);
      }
    }
    return;
  }

  // Initialize query states
  //
  std::vector<MergeTree::RangeQueryState> query_states(num_frames);
  for (int64_t i = 0; i < num_frames; ++i) {
    if (ends[i] == begins[i]) {
      bit_util::ClearBit(results_validity, i);
    }
    int64_t top_level_rank = CountPrevLE(begins[i]);
    query_states[i].pos[0] = MergeTree::RangeQueryState::PosFromNodeAndLength(
        tree_height - 1, 0LL, top_level_rank);
    query_states[i].pos[1] = MergeTree::RangeQueryState::kEmpty;
  }

  auto query_outputs_buf = util::TempVectorHolder<MergeTree::RangeQueryState>(
      temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  MergeTree::RangeQueryState* query_outputs = query_outputs_buf.mutable_data();

  // Two elements per batch row in these temp buffers
  //
  auto add_dst_ids_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(2 * batch_length_max));
  int64_t* add_dst_ids = add_dst_ids_buf.mutable_data();

  auto add_src_ids_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(2 * batch_length_max));
  int64_t* add_src_ids = add_src_ids_buf.mutable_data();

  int64_t num_adds;

  for (int level = tree_height - 1; level > 0; --level) {
    WindowAggregateFunc::Vector* sum_in = sum_buf[level & 1];
    WindowAggregateFunc::Vector* sum_out = sum_buf[1 - (level & 1)];

    if (level == tree_height - 1) {
      // Reorder source values based on row_numbers_sorted_on_prev_
      //
      std::vector<int64_t> out_ids(num_frames);
      std::iota(out_ids.begin(), out_ids.end(), 0);
      func->Zero(num_frames, sum_in, 0);
      func->Sum(num_frames, sum_in, out_ids.data(), nullptr,
                row_numbers_sorted_on_prev_.data());
    }

    // Split values related to the current tree level
    //
    func->Split(num_frames, sum_out, sum_in, merge_tree_, level, hardware_flags,
                temp_vector_stack);

    // Compute prefix sum of values related to the tree level below
    //
    WindowAggregateFunc::Vector* segmented_prefix_sum = sum_in;
    func->PrefixSum(num_frames, segmented_prefix_sum, sum_out, level - 1);

    // Break into mini-batches
    //
    for (int64_t batch_begin = 0; batch_begin < num_frames;
         batch_begin += batch_length_max) {
      int64_t batch_length_next = std::min(num_frames - batch_begin, batch_length_max);

      merge_tree_.RangeQueryStep(level, batch_length_next, begins + batch_begin,
                                 ends + batch_begin, query_states.data() + batch_begin,
                                 query_outputs);

      num_adds = 0;
      for (int64_t i = 0; i < batch_length_next; ++i) {
        for (int ioutput_pos = 0; ioutput_pos < 2; ++ioutput_pos) {
          int64_t output_pos = query_outputs[i].pos[ioutput_pos];
          if (output_pos != MergeTree::RangeQueryState::kEmpty) {
            int64_t output_node;
            int64_t output_length;
            MergeTree::RangeQueryState::NodeAndLengthFromPos(
                level - 1, output_pos, &output_node, &output_length);

            add_dst_ids[num_adds] = batch_begin + i;
            add_src_ids[num_adds] = (output_node << (level - 1)) + output_length - 1;
            ++num_adds;
          }
        }
      }
      func->Sum(num_adds, results, add_dst_ids, segmented_prefix_sum, add_src_ids);
    }
  }

  delete sum_buf[0];
  delete sum_buf[1];
}

// Output 0 for the first row with each group id.
// Otherwise output 1 plus row number of the last preceding row with the same
// group id.
// Symmetrically 1 plus number of rows is output when there is no following
// row within the same group.
//
// Starting from 0 instead of -1 makes radix sorting easier.
//
void WindowDistinct::GenListsForGroups(int64_t num_rows, int64_t num_groups,
                                       const int64_t* group_ids_sorted,
                                       const int64_t* row_numbers_sorted_on_group_id,
                                       int64_t* opt_prev, int64_t* opt_next) {
  for (int64_t i = 0; i < num_rows; ++i) {
    int64_t row_number = row_numbers_sorted_on_group_id[i];
    if (opt_prev) {
      bool group_first = (i == 0 || group_ids_sorted[i] != group_ids_sorted[i - 1]);
      int64_t prev = group_first ? 0LL : 1LL + row_numbers_sorted_on_group_id[i - 1];
      opt_prev[row_number] = prev;
    }
    if (opt_next) {
      bool group_last =
          (i == num_rows - 1 || group_ids_sorted[i] != group_ids_sorted[i + 1]);
      int64_t next =
          group_last ? 1LL + num_rows : 1LL + row_numbers_sorted_on_group_id[i + 1];
      opt_next[row_number] = next;
    }
  }
}

void WindowDistinct::GenHasPrevNextBitvecs(int64_t num_rows,
                                           const int64_t* group_ids_sorted,
                                           const int64_t* row_numbers_sorted_on_group_ids,
                                           uint64_t* has_prev_bitvec,
                                           uint64_t* has_next_bitvec) {
  if (has_prev_bitvec) {
    memset(has_prev_bitvec, 0xff, bit_util::BytesForBits(num_rows));
    for (int64_t i = 0; i < num_rows; ++i) {
      bool group_first = (i == 0 || group_ids_sorted[i] != group_ids_sorted[i - 1]);
      if (group_first) {
        bit_util::ClearBit(reinterpret_cast<uint8_t*>(has_prev_bitvec),
                           row_numbers_sorted_on_group_ids[i]);
      }
    }
  }
  if (has_next_bitvec) {
    memset(has_next_bitvec, 0xff, bit_util::BytesForBits(num_rows));
    for (int64_t i = 0; i < num_rows; ++i) {
      bool group_last =
          (i == num_rows - 1 || group_ids_sorted[i] != group_ids_sorted[i + 1]);
      if (group_last) {
        bit_util::ClearBit(reinterpret_cast<uint8_t*>(has_next_bitvec),
                           row_numbers_sorted_on_group_ids[i]);
      }
    }
  }
}

void WindowDistinct::GenRowNumbersSortedOnPrev(
    const int64_t* prev, const uint64_t* has_prev_bitvec,
    const uint64_t* has_prev_popcounts, int64_t* row_numbers_sorted_on_prev) const {
  for (int64_t i = 0; i < num_rows_; ++i) {
    if (!bit_util::GetBit(reinterpret_cast<const uint8_t*>(has_prev_bitvec), i)) {
      int64_t rank = i - BitVectorNavigator::Rank(i, has_prev_bitvec, has_prev_popcounts);
      row_numbers_sorted_on_prev[rank] = i;
    } else {
      int64_t rank = CountPrevLE(prev[i] - 1);
      row_numbers_sorted_on_prev[rank] = i;
    }
  }
}

// Return the number of rows with prev attribute that is less than or equal to
// the given value. This is used to initialize top-down merge tree traversal.
//
int64_t WindowDistinct::CountPrevLE(int64_t upper_bound) const {
  return num_groups_ + BitVectorNavigator::Rank(upper_bound, has_next_bitvec_.data(),
                                                has_next_popcounts_.data());
}

// void WindowDistinct::GenCountLE(int64_t num_rows, int64_t max_value,
//                                 const int64_t *values,
//                                 /* num_rows elements */
//                                 int64_t *count_LE,
//                                 int64_t *row_numbers_sorted_on_values) {
//   std::vector<int64_t> values_sorted(num_rows);
//   // Sort all the rows on values.
//   // This is sorting of a dense set of integers. We will use radix sort.
//   //
//   RadixSort::Sort(num_rows, max_value, values, values_sorted.data(),
//                   row_numbers_sorted_on_values);

//   int64_t output_cursor = 0LL;
//   int64_t last_output_value = 0LL;
//   for (int64_t i = 0; i < num_rows; ++i) {
//     int64_t value = values_sorted[i];
//     bool last_in_tie_group =
//         (i == (num_rows - 1)) || values_sorted[i + 1] != values_sorted[i];
//     if (last_in_tie_group) {
//       while (output_cursor < value) {
//         count_LE[output_cursor++] = last_output_value;
//       }
//       last_output_value = i + 1;
//       count_LE[output_cursor++] = last_output_value;
//     }
//   }
//   while (output_cursor <= max_value) {
//     count_LE[output_cursor++] = last_output_value;
//   }
// }

void WindowDistinctBasic::Count(int64_t num_rows, const int64_t* group_ids,
                                const WindowFrames& frames, int64_t* output) {
  if (num_rows == 0) {
    return;
  }

  const int64_t* begins = frames.begins[0];
  const int64_t* ends = frames.ends[0];

  int64_t max_group_id = *std::max_element(group_ids, group_ids + num_rows);
  std::vector<bool> group_id_set(max_group_id + 1);

  for (int64_t ioutput = 0; ioutput < num_rows; ++ioutput) {
    int64_t begin = begins[ioutput];
    int64_t end = ends[ioutput];

    for (int64_t i = 0; i < max_group_id + 1; ++i) {
      group_id_set[i] = false;
    }
    int64_t result = 0LL;
    for (int64_t irow = begin; irow < end; ++irow) {
      int64_t group_id = group_ids[irow];
      if (!group_id_set[group_id]) {
        group_id_set[group_id] = true;
        ++result;
      }
    }
    output[ioutput] = result;
  }
}

void WindowDistinctBasic::Sum(int64_t num_rows, const WindowFrames& frames,
                              const int64_t* group_ids, const int64_t* vals,
                              std::vector<std::pair<uint64_t, uint64_t>>& results,
                              uint8_t* results_validity) {
  if (num_rows == 0) {
    return;
  }

  int64_t num_frames = frames.num_frames;
  const int64_t* begins = frames.begins[0];
  const int64_t* ends = frames.ends[0];

  int64_t max_group_id = *std::max_element(group_ids, group_ids + num_rows);
  std::vector<bool> group_id_set(max_group_id + 1);

  results.resize(num_frames);
  for (int64_t i = 0; i < num_frames; ++i) {
    int64_t begin = begins[i];
    int64_t end = ends[i];

    if (begin == end) {
      bit_util::ClearBit(results_validity, i);
      results[i] = std::make_pair(0ULL, 0ULL);
      continue;
    }
    bit_util::SetBit(results_validity, i);

    for (int64_t i = 0; i < max_group_id + 1; ++i) {
      group_id_set[i] = false;
    }

    uint64_t overflow = 0ULL;
    uint64_t result = 0ULL;
    for (int64_t j = begin; j < end; ++j) {
      int64_t group_id = group_ids[j];
      if (!group_id_set[group_id]) {
        group_id_set[group_id] = true;
        result += static_cast<uint64_t>(vals[j]);
        if (result < static_cast<uint64_t>(vals[j])) {
          ++overflow;
        }
        if (vals[j] < 0LL) {
          overflow += ~0ULL;
        }
      }
    }

    results[i] = std::make_pair(result, overflow);
  }
}

void WindowDistinctTest::TestCountOrSum(bool test_sum) {
  Random64BitCopy rand;
  MemoryPool* pool = default_memory_pool();
  int64_t hardware_flags = 0LL;
  util::TempVectorStack temp_vector_stack;
  Status status = temp_vector_stack.Init(pool, 128 * util::MiniBatch::kMiniBatchLength);
  ARROW_DCHECK(status.ok());

  constexpr int64_t num_groups_max = 32;
  constexpr int64_t num_rows_max = 8 * 1024;  // 64;
  constexpr int64_t num_tests = 30;

  int num_tests_to_skip = 0;
  for (int64_t itest = 0; itest < num_tests; ++itest) {
    int64_t num_rows = rand.from_range(static_cast<int64_t>(1), num_rows_max);
    int64_t num_groups =
        std::min(num_rows, rand.from_range(static_cast<int64_t>(1), num_groups_max));
    int64_t num_repeats = 1;
#ifdef NDEBUG
    num_repeats = bit_util::CeilDiv(128 * 1024, num_rows);
#endif
    printf("num_repeats = %d ", static_cast<int>(num_repeats));
    std::vector<int64_t> values;
    std::vector<int64_t> group_ids;
    values.resize(num_rows);
    group_ids.resize(num_rows);
    std::unordered_set<int64_t> uniques;
    std::vector<int64_t> group_keys;
    for (int64_t i = 0; i < num_groups; ++i) {
      for (;;) {
        int64_t group_key = rand.next();
        if (uniques.find(group_key) == uniques.end()) {
          uniques.insert(group_key);
          group_keys.push_back(group_key);
          break;
        }
      }
    }
    for (int64_t i = 0; i < num_rows; ++i) {
      int64_t group_id =
          i < num_groups ? i : rand.from_range(static_cast<int64_t>(0), num_groups - 1);
      values[i] = group_keys[group_id];
      group_ids[i] = group_id;
    }

    // Generate random frames
    //
    std::vector<int64_t> begins;
    std::vector<int64_t> ends;
    GenerateTestFrames(rand, num_rows, begins, ends, /*progressive=*/false,
                       /*expansive=*/false);
    WindowFrames frames;
    frames.num_ranges_in_frame = 1;
    frames.num_frames = num_rows;
    frames.begins[0] = begins.data();
    frames.ends[0] = ends.data();

    // Generate random values for sum if needed
    //
    std::vector<int64_t> vals;
    WinAggFun_SumInt64 func;
    if (test_sum) {
      vals.resize(num_rows);
      constexpr int64_t max_val = 65535;
      for (int64_t i = 0; i < num_rows; ++i) {
        vals[i] = rand.from_range(static_cast<int64_t>(0), max_val);
      }
      func.Init(num_rows, vals.data());
    }

    if (itest < num_tests_to_skip) {
      continue;
    }

    std::vector<int64_t> output, output_basic;
    if (!test_sum) {
      output.resize(num_rows);
      output_basic.resize(num_rows);
    }

    std::vector<std::pair<uint64_t, uint64_t>> output_sum_basic;
    std::vector<uint8_t> output_sum_validity, output_sum_validity_basic;
    WinAggFun_SumInt64::VectorUint128* output_sum = nullptr;

    if (test_sum) {
      output_sum = func.Alloc(num_rows);
      output_sum_basic.resize(num_rows);
      output_sum_validity.resize(bit_util::BytesForBits(num_rows));
      output_sum_validity_basic.resize(bit_util::BytesForBits(num_rows));
    }

    // int64_t start = __rdtsc();
    for (int64_t irepeat = 0; irepeat < num_repeats; ++irepeat) {
      if (test_sum) {
        WindowDistinctBasic::Sum(num_rows, frames, group_ids.data(), vals.data(),
                                 output_sum_basic, output_sum_validity_basic.data());
      } else {
        WindowDistinctBasic::Count(num_rows, group_ids.data(), frames,
                                   output_basic.data());
      }
    }
    // int64_t end = __rdtsc();
    // printf("CountDistinct::Run cycles per frame %.1f\n",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));

    // start = __rdtsc();
    for (int64_t irepeat = 0; irepeat < num_repeats; ++irepeat) {
      WindowDistinct eval;
      eval.Init(num_rows, num_groups, group_ids.data(), hardware_flags,
                &temp_vector_stack);
      if (test_sum) {
        eval.Sum(frames, &func, output_sum, output_sum_validity.data(), hardware_flags,
                 &temp_vector_stack);
      } else {
        eval.Count(frames, output.data(), hardware_flags, &temp_vector_stack);
      }
    }
    // end = __rdtsc();
    // printf("CountDistinct::RunBasic cycles per frame %.1f\n",
    //        static_cast<float>(end - start) / static_cast<float>(num_rows *
    //        num_repeats));

    bool ok = true;
    if (test_sum) {
      for (int64_t i = 0; i < num_rows; ++i) {
        bool l_valid = bit_util::GetBit(output_sum_validity_basic.data(), i);
        bool r_valid = bit_util::GetBit(output_sum_validity.data(), i);
        if (l_valid != r_valid) {
          ARROW_DCHECK(false);
          ok = false;
          break;
        }
        if (l_valid && r_valid) {
          if (output_sum_basic[i].first != output_sum->vals_[i].first ||
              output_sum_basic[i].second != output_sum->vals_[i].second) {
            ARROW_DCHECK(false);
            ok = false;
            break;
          }
        }
      }
    } else {
      for (int64_t i = 0; i < num_rows; ++i) {
        if (output[i] != output_basic[i]) {
          ARROW_DCHECK(false);
          ok = false;
          break;
        }
      }
    }
    printf("%s\n", ok ? "correct" : "wrong");

    if (output_sum) {
      delete output_sum;
    }
  }
}

}  // namespace compute
}  // namespace arrow
