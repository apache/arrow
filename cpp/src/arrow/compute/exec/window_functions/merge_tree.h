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
#include "arrow/compute/exec/util.h"
#include "arrow/util/bit_util.h"
#include "bit_vector_navigator.h"

namespace arrow {
namespace compute {

// TODO: Support multiple [begin, end) ranges in range and nth_element queries.
//

// One way to think about MergeTree is that, when we traverse top down, we
// switch to sortedness on X axis, and when we traverse bottom up, we switch to
// sortedness on Y axis. At the lowest level of MergeTree rows are sorted on X
// and the highest level they are sorted on Y.
//
class MergeTree {
 public:
  MergeTree() : num_rows_(0) {}

  void Build(int64_t num_rows, const int64_t* permutation, int num_levels_to_skip,
             int64_t hardware_flags, util::TempVectorStack* temp_vector_stack);

  int get_height() const { return num_rows_ ? 1 + arrow::bit_util::Log2(num_rows_) : 0; }

  template <typename S>
  void Split(
      /* upper level */ int level, const S* in, S* out, int64_t hardware_flags,
      util::TempVectorStack* temp_vector_stack) const {
    int64_t lower_node_length = 1LL << (level - 1);
    int64_t lower_node_mask = lower_node_length - 1LL;

    int64_t batch_length_max = util::MiniBatch::kMiniBatchLength;
    int num_ids;
    auto ids_buf = util::TempVectorHolder<uint16_t>(
        temp_vector_stack, static_cast<uint32_t>(batch_length_max));
    uint16_t* ids = ids_buf.mutable_data();

    // Break into mini-batches
    int64_t rank_batch_begin[2];
    rank_batch_begin[0] = 0;
    rank_batch_begin[1] = 0;
    for (int64_t batch_begin = 0; batch_begin < num_rows_;
         batch_begin += batch_length_max) {
      int64_t batch_length = std::min(num_rows_ - batch_begin, batch_length_max);

      for (int child = 0; child <= 1; ++child) {
        // Get parent node positions (relative to the batch) for all elements
        // coming from left child
        util::bit_util::bits_to_indexes(
            child, hardware_flags, static_cast<int>(batch_length),
            reinterpret_cast<const uint8_t*>(level_bitvecs_[level].data() +
                                             batch_begin / 64),
            &num_ids, ids);

        for (int i = 0; i < num_ids; ++i) {
          int64_t upper_pos = batch_begin + ids[i];
          int64_t rank = rank_batch_begin[child] + i;
          int64_t lower_pos = (rank & ~lower_node_mask) * 2 + child * lower_node_length +
                              (rank & lower_node_mask);
          out[lower_pos] = in[upper_pos];
        }
        rank_batch_begin[child] += num_ids;
      }
    }
  }

  // State or output for range query.
  //
  // Represents between zero and two different nodes from a single level of the
  // tree.
  //
  // For each node remembers the length of its prefix, which represents a
  // subrange of selected elements of that node.
  //
  // Length is between 1 and the number of node elements at this level (both
  // bounds inclusive), because empty set of selected elements is represented by
  // a special constant kEmpty.
  //
  struct RangeQueryState {
    static constexpr int64_t kEmpty = -1LL;

    static int64_t PosFromNodeAndLength(int level, int64_t node, int64_t length) {
      if (length == 0) {
        return kEmpty;
      }
      return (node << level) + length - 1;
    }

    static void NodeAndLengthFromPos(int level, int64_t pos, int64_t* node,
                                     int64_t* length) {
      ARROW_DCHECK(pos != kEmpty);
      *node = pos >> level;
      *length = 1 + pos - (*node << level);
    }

    void AppendPos(int64_t new_pos) {
      // One of the two positions must be set to null
      //
      if (pos[0] == kEmpty) {
        pos[0] = new_pos;
      } else {
        ARROW_DCHECK(pos[1] == kEmpty);
        pos[1] = new_pos;
      }
    }

    int64_t pos[2];
  };

  // Visiting each level updates state cursor pair and outputs state cursor
  // pair.
  //
  void RangeQueryStep(int level, int64_t num_queries, const int64_t* begins,
                      const int64_t* ends, RangeQueryState* query_states,
                      RangeQueryState* query_outputs) const;

  int64_t NthElement(int64_t begin, int64_t end, int64_t n) const {
    ARROW_DCHECK(n >= 0 && n < end - begin);
    int64_t temp_begin = begin;
    int64_t temp_end = end;
    int64_t temp_n = n;

    // Traverse the tree top-down
    //
    int top_level = static_cast<int>(level_bitvecs_.size()) - 1;
    for (int level = top_level; level > 0; --level) {
      NthElementStep(level, &temp_begin, &temp_end, &temp_n);
    }

    return temp_begin;
  }

  void NthElement(int64_t num_queries, const uint16_t* opt_ids, const int64_t* begins,
                  const int64_t* ends,
                  /* ns[i] must be in the range [0; ends[i] - begins[i]) */
                  const int64_t* ns, int64_t* row_numbers,
                  util::TempVectorStack* temp_vector_stack) const;

 private:
  /* output 0 if value comes from left child and 1 otherwise */
  void GenBitvec(
      /* level to generate for */ int level,
      /* source permutation of rows for elements in this level */
      const int64_t* permutation);

  void Cascade(int level, int64_t pos, RangeQueryState* result) const;

  bool NodeFullyInsideRange(int level, int64_t node, int64_t begin, int64_t end) const;

  bool NodePartiallyInsideRange(int level, int64_t node, int64_t begin,
                                int64_t end) const;

  void NthElementStep(int level, int64_t* begin, int64_t* end, int64_t* n) const {
    int64_t node_length = 1LL << level;
    uint64_t node_mask = node_length - 1;
    int64_t node_begin = (*begin & ~node_mask);

    int64_t rank_begin = BitVectorNavigator::Rank(*begin, level_bitvecs_[level].data(),
                                                  level_popcounts_[level].data());
    int64_t rank_end = BitVectorNavigator::RankNext(
        *end - 1, level_bitvecs_[level].data(), level_popcounts_[level].data());
    int64_t length_left = (*end - *begin) - (rank_end - rank_begin);
    int64_t child_mask = (length_left <= *n ? ~0LL : 0LL);

    *begin = node_begin + ((node_length / 2 + rank_begin - node_begin / 2) & child_mask) +
             (((*begin - node_begin) - (rank_begin - node_begin / 2)) & ~child_mask);
    *end = *begin + ((rank_end - rank_begin) & child_mask) + (length_left & ~child_mask);
    *n -= (length_left & child_mask);
  }

  int64_t num_rows_;
  std::vector<std::vector<uint64_t>> level_bitvecs_;
  std::vector<std::vector<uint64_t>> level_popcounts_;
};

}  // namespace compute
}  // namespace arrow
