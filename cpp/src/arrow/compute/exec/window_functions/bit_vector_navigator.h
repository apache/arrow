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

namespace arrow {
namespace compute {

// Bit-vector allocated size must be multiple of 64-bits.
// There is exactly ceil(num_bits / 64) 64-bit population counters.
//
class BitVectorNavigator {
 public:
  static uint64_t GenPopCounts(int64_t num_bits, const uint64_t* bits,
                               uint64_t* pop_counts) {
    int64_t num_pop_counts = (num_bits + 63) / 64;
    uint64_t sum = 0;
    for (int64_t i = 0; i < num_pop_counts; ++i) {
      pop_counts[i] = sum;
      sum += ARROW_POPCOUNT64(bits[i]);
    }
    return sum;
  }

  // O(1)
  static inline int64_t PopCount(int64_t num_bits, const uint64_t* bitvec,
                                 const uint64_t* popcounts) {
    int64_t last_word = (num_bits - 1) / 64;
    return popcounts[last_word] + ARROW_POPCOUNT64(bitvec[last_word]);
  }

  // O(log(N))
  // The output is set to -1 if rank is below zero and to num_bits if
  // rank is above the maximum rank of any row in the represented range.
  static inline int64_t Select(int64_t rank, int64_t num_bits, const uint64_t* bits,
                               const uint64_t* pop_counts) {
    if (rank < 0) {
      return -1LL;
    }
    int64_t max_rank = PopCount(num_bits, bits, pop_counts) - 1LL;
    if (rank > max_rank) {
      return num_bits;
    }

    int64_t num_pop_counts = (num_bits + 63) / 64;
    // Find index of 64-bit block that contains the nth set bit.
    int64_t block_id = (std::upper_bound(pop_counts, pop_counts + num_pop_counts,
                                         static_cast<uint64_t>(rank)) -
                        pop_counts) -
                       1;
    // Binary search position of (n - pop_count + 1)th bit set in the 64-bit
    // block.
    uint64_t block = bits[block_id];
    int64_t bit_rank = rank - pop_counts[block_id];
    int bit_id = 0;
    for (int half_bits = 32; half_bits >= 1; half_bits /= 2) {
      uint64_t mask = ((1ULL << half_bits) - 1ULL);
      int64_t lower_half_pop_count = ARROW_POPCOUNT64(block & mask);
      if (bit_rank >= lower_half_pop_count) {
        block >>= half_bits;
        bit_rank -= lower_half_pop_count;
        bit_id += half_bits;
      }
    }
    return block_id * 64 + bit_id;
  }

  // TODO: We could implement BitVectorNavigator::Select that works on batches
  // instead of single rows. Then it could use precomputed static B-tree to
  // speed up binary search.
  //

  // O(1)
  // Input row number must be valid (between 0 and number of rows less 1).
  static inline int64_t Rank(int64_t pos, const uint64_t* bits,
                             const uint64_t* pop_counts) {
    int64_t block = pos >> 6;
    int offset = static_cast<int>(pos & 63LL);
    uint64_t mask = (1ULL << offset) - 1ULL;
    int64_t rank1 =
        static_cast<int64_t>(pop_counts[block]) + ARROW_POPCOUNT64(bits[block] & mask);
    return rank1;
  }

  // O(1)
  // Rank of the next row (also valid for the last row when the next row would
  // be outside of the range of rows).
  static inline int64_t RankNext(int64_t pos, const uint64_t* bits,
                                 const uint64_t* pop_counts) {
    int64_t block = pos >> 6;
    int offset = static_cast<int>(pos & 63LL);
    uint64_t mask = ~0ULL >> (63 - offset);
    int64_t rank1 =
        static_cast<int64_t>(pop_counts[block]) + ARROW_POPCOUNT64(bits[block] & mask);
    return rank1;
  }

  // Input ranks may be outside of range of ranks present in the input bit
  // vector.
  //
  static void SelectsForRangeOfRanks(int64_t rank_begin, int64_t rank_end,
                                     int64_t num_bits, const uint64_t* bitvec,
                                     const uint64_t* popcounts, int64_t* outputs,
                                     int64_t hardware_flags,
                                     util::TempVectorStack* temp_vector_stack);

  static void SelectsForRelativeRanksForRangeOfRows(
      int64_t batch_begin, int64_t batch_end, int64_t rank_delta, int64_t num_rows,
      const uint64_t* ties_bitvec, const uint64_t* ties_popcounts, int64_t* outputs,
      int64_t hardware_flags, util::TempVectorStack* temp_vector_stack);

  template <typename INDEX_T>
  static void GenSelectedIds(int64_t num_rows, const uint64_t* bitvec, INDEX_T* ids,
                             int64_t hardware_flags,
                             util::TempVectorStack* temp_vector_stack) {
    // Break into mini-batches.
    //
    int64_t batch_length_max = util::MiniBatch::kMiniBatchLength;
    auto batch_ids_buf =
        util::TempVectorHolder<uint16_t>(temp_vector_stack, batch_length_max);
    auto batch_ids = batch_ids_buf.mutable_data();
    int batch_num_ids;
    int64_t num_ids = 0;
    for (int64_t batch_begin = 0; batch_begin < num_rows;
         batch_begin += batch_length_max) {
      int64_t batch_length = std::min(num_rows - batch_begin, batch_length_max);
      util::bit_util::bits_to_indexes(
          /*bit_to_search=*/1, hardware_flags, batch_length,
          reinterpret_cast<const uint8_t*>(bitvec + (batch_begin / 64)), &batch_num_ids,
          batch_ids);
      for (int i = 0; i < batch_num_ids; ++i) {
        ids[num_ids + i] = static_cast<INDEX_T>(batch_begin + batch_ids[i]);
      }
      num_ids += batch_num_ids;
    }
  }
};

}  // namespace compute
}  // namespace arrow
