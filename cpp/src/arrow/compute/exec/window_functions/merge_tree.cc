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

#include "arrow/compute/exec/window_functions/merge_tree.h"

namespace arrow {
namespace compute {

void MergeTree::Build(int64_t num_rows, const int64_t* permutation,
                      int num_levels_to_skip, int64_t hardware_flags,
                      util::TempVectorStack* temp_vector_stack) {
  num_rows_ = num_rows;
  if (num_rows == 0) {
    return;
  }

  int height = 1 + arrow::bit_util::Log2(num_rows);
  level_bitvecs_.resize(height);
  level_popcounts_.resize(height);

  int64_t num_bit_words = arrow::bit_util::CeilDiv(num_rows, 64);

  // We skip level 0 on purpose - it is not used.
  // We also skip num_levels_to_skip from the top.
  //
  for (int level = 1; level < height - num_levels_to_skip; ++level) {
    level_bitvecs_[level].resize(num_bit_words);
    level_popcounts_[level].resize(num_bit_words);
  }

  std::vector<int64_t> permutation_temp[2];
  permutation_temp[0].resize(num_rows);
  permutation_temp[1].resize(num_rows);
  int64_t* permutation_pingpong[2];
  permutation_pingpong[0] = permutation_temp[0].data();
  permutation_pingpong[1] = permutation_temp[1].data();

  // Generate tree layers top-down
  //
  int top_level = height - num_levels_to_skip - 1;
  for (int target_level = top_level; target_level > 0; --target_level) {
    int flip = target_level % 2;
    const int64_t* permutation_up =
        (target_level == top_level - 1) ? permutation : permutation_pingpong[flip];
    if (target_level < top_level) {
      int64_t* permutation_this = permutation_pingpong[1 - flip];
      Split(target_level + 1, permutation_up, permutation_this, hardware_flags,
            temp_vector_stack);
    }
    const int64_t* permutation_this =
        (target_level == top_level) ? permutation : permutation_pingpong[1 - flip];
    GenBitvec(target_level, permutation_this);
  }
}

void MergeTree::RangeQueryStep(int level, int64_t num_queries, const int64_t* begins,
                               const int64_t* ends, RangeQueryState* query_states,
                               RangeQueryState* query_outputs) const {
  for (int64_t iquery = 0; iquery < num_queries; ++iquery) {
    int64_t begin = begins[iquery];
    int64_t end = ends[iquery];
    RangeQueryState& state = query_states[iquery];
    RangeQueryState& output = query_outputs[iquery];
    ARROW_DCHECK(begin <= end && begin >= 0 && end <= num_rows_);

    RangeQueryState parent_state;
    parent_state.pos[0] = state.pos[0];
    parent_state.pos[1] = state.pos[1];
    state.pos[0] = state.pos[1] = output.pos[0] = output.pos[1] = RangeQueryState::kEmpty;

    for (int iparent_pos = 0; iparent_pos < 2; ++iparent_pos) {
      int64_t parent_pos = parent_state.pos[iparent_pos];
      if (parent_pos != RangeQueryState::kEmpty) {
        RangeQueryState child_state;
        Cascade(level, parent_pos, &child_state);
        for (int ichild_pos = 0; ichild_pos < 2; ++ichild_pos) {
          int64_t child_pos = child_state.pos[ichild_pos];
          if (child_pos != RangeQueryState::kEmpty) {
            int64_t child_node;
            int64_t child_length;
            RangeQueryState::NodeAndLengthFromPos(level - 1, child_pos, &child_node,
                                                  &child_length);
            if (NodeFullyInsideRange(level - 1, child_node, begin, end)) {
              output.AppendPos(child_pos);
            } else if (NodePartiallyInsideRange(level - 1, child_node, begin, end)) {
              state.AppendPos(child_pos);
            }
          }
        }
      }
    }
  }
}

void MergeTree::NthElement(int64_t num_queries, const uint16_t* opt_ids,
                           const int64_t* begins, const int64_t* ends,
                           /* ns[i] must be in the range [0; ends[i] - begins[i]) */
                           const int64_t* ns, int64_t* row_numbers,
                           util::TempVectorStack* temp_vector_stack) const {
  int64_t batch_length_max = util::MiniBatch::kMiniBatchLength;

  // Allocate temporary buffers
  //
  auto temp_begins_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  int64_t* temp_begins = temp_begins_buf.mutable_data();

  auto temp_ends_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  int64_t* temp_ends = temp_ends_buf.mutable_data();

  auto temp_ns_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(batch_length_max));
  int64_t* temp_ns = temp_ns_buf.mutable_data();

  for (int64_t batch_begin = 0; batch_begin < num_queries;
       batch_begin += batch_length_max) {
    int64_t batch_length = std::min(num_queries - batch_begin, batch_length_max);

    // Initialize tree cursors (begin and end of a range of some top level
    // node for each query/frame).
    //
    if (opt_ids) {
      for (int64_t i = 0; i < batch_length; ++i) {
        uint16_t id = opt_ids[batch_begin + i];
        temp_begins[i] = begins[id];
        temp_ends[i] = ends[id];
        temp_ns[i] = ns[id];
        ARROW_DCHECK(temp_ns[i] >= 0 && temp_ns[i] < temp_ends[i] - temp_begins[i]);
      }
    } else {
      memcpy(temp_begins, begins + batch_begin, batch_length * sizeof(temp_begins[0]));
      memcpy(temp_ends, ends + batch_begin, batch_length * sizeof(temp_ends[0]));
      memcpy(temp_ns, ns + batch_begin, batch_length * sizeof(temp_ns[0]));
    }

    // Traverse the tree top-down
    //
    int top_level = static_cast<int>(level_bitvecs_.size()) - 1;
    for (int level = top_level; level > 0; --level) {
      for (int64_t i = 0; i < batch_length; ++i) {
        NthElementStep(level, temp_begins + i, temp_ends + i, temp_ns + i);
      }
    }

    // Output results
    //
    if (opt_ids) {
      for (int64_t i = 0; i < batch_length; ++i) {
        uint16_t id = opt_ids[batch_begin + i];
        row_numbers[id] = temp_begins[i];
      }
    } else {
      for (int64_t i = 0; i < batch_length; ++i) {
        row_numbers[batch_begin + i] = temp_begins[i];
      }
    }
  }
}

void MergeTree::GenBitvec(
    /* level to generate for */ int level, const int64_t* permutation) {
  uint64_t result = 0ULL;
  for (int64_t base = 0; base < num_rows_; base += 64) {
    for (int64_t i = base; i < std::min(base + 64, num_rows_); ++i) {
      int64_t bit = (permutation[i] >> (level - 1)) & 1;
      result |= static_cast<uint64_t>(bit) << (i & 63);
    }
    level_bitvecs_[level][base / 64] = result;
    result = 0ULL;
  }

  BitVectorNavigator::GenPopCounts(num_rows_, level_bitvecs_[level].data(),
                                   level_popcounts_[level].data());
}

void MergeTree::Cascade(int level, int64_t pos, RangeQueryState* result) const {
  ARROW_DCHECK(level > 0);

  int64_t node;
  int64_t length;
  RangeQueryState::NodeAndLengthFromPos(level, pos, &node, &length);

  int64_t node_begin = node << level;
  // We use RankNext for node_begin + length - 1 instead of Rank for node_begin
  // + length, because the latter one may be equal to num_rows_ which is an
  // index out of range for bitvector.
  //
  int64_t rank =
      BitVectorNavigator::RankNext(node_begin + length - 1, level_bitvecs_[level].data(),
                                   level_popcounts_[level].data());
  int64_t local_rank = rank - (node_begin / 2);
  result->pos[0] =
      RangeQueryState::PosFromNodeAndLength(level - 1, node * 2, length - local_rank);
  bool has_right_child =
      (node_begin + (static_cast<int64_t>(1) << (level - 1))) < num_rows_;
  result->pos[1] = has_right_child ? RangeQueryState::PosFromNodeAndLength(
                                         level - 1, node * 2 + 1, local_rank)
                                   : RangeQueryState::kEmpty;
}

bool MergeTree::NodeFullyInsideRange(int level, int64_t node, int64_t begin,
                                     int64_t end) const {
  int64_t node_begin = node << level;
  int64_t node_end = std::min(num_rows_, node_begin + (static_cast<int64_t>(1) << level));
  return node_begin >= begin && node_end <= end;
}

bool MergeTree::NodePartiallyInsideRange(int level, int64_t node, int64_t begin,
                                         int64_t end) const {
  int64_t node_begin = node << level;
  int64_t node_end = std::min(num_rows_, node_begin + (static_cast<int64_t>(1) << level));
  return node_begin < end && node_end > begin;
}

}  // namespace compute
}  // namespace arrow
