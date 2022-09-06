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

#include "arrow/compute/exec/window_functions/bit_vector_navigator.h"
#include <cstdint>
#include "arrow/compute/exec/util.h"

namespace arrow {
namespace compute {

void BitVectorNavigator::SelectsForRangeOfRanks(
    int64_t rank_begin, int64_t rank_end, int64_t num_bits, const uint64_t* bitvec,
    const uint64_t* popcounts, int64_t* outputs, int64_t hardware_flags,
    util::TempVectorStack* temp_vector_stack) {
  ARROW_DCHECK(rank_begin <= rank_end);
  if (rank_begin == rank_end) {
    return;
  }
  int64_t popcount_all = PopCount(num_bits, bitvec, popcounts);
  if (rank_end <= 0LL) {
    for (int64_t i = 0LL; i < rank_end - rank_begin; ++i) {
      outputs[i] = -1LL;
    }
    return;
  }
  if (rank_begin >= popcount_all) {
    for (int64_t i = 0LL; i < rank_end - rank_begin; ++i) {
      outputs[i] = num_bits;
    }
    return;
  }
  if (rank_begin < 0LL) {
    for (int64_t i = 0LL; i < -rank_begin; ++i) {
      outputs[i] = -1LL;
    }
    outputs += -rank_begin;
    rank_begin = 0LL;
  }
  if (rank_end > popcount_all) {
    for (int64_t i = popcount_all - rank_begin; i < rank_end - rank_begin; ++i) {
      outputs[i] = num_bits;
    }
    rank_end = popcount_all;
  }

  int64_t minibatch_length_max = util::MiniBatch::kMiniBatchLength;
  auto indexes = util::TempVectorHolder<uint16_t>(
      temp_vector_stack, static_cast<uint32_t>(minibatch_length_max));
  int num_indexes;

  int64_t first_select =
      BitVectorNavigator::Select(rank_begin, num_bits, bitvec, popcounts);
  int64_t last_select =
      BitVectorNavigator::Select(rank_begin, num_bits, bitvec, popcounts);

  for (int64_t minibatch_begin = first_select; minibatch_begin < last_select + 1;
       minibatch_begin += minibatch_length_max) {
    int64_t minibatch_end =
        std::min(last_select + 1, minibatch_begin + minibatch_length_max);
    util::bit_util::bits_to_indexes(
        /*bit_to_search=*/1, hardware_flags,
        static_cast<int>(minibatch_end - minibatch_begin),
        reinterpret_cast<const uint8_t*>(bitvec), &num_indexes, indexes.mutable_data(),
        static_cast<int>(minibatch_begin));
    for (int i = 0; i < num_indexes; ++i) {
      outputs[i] = minibatch_begin + indexes.mutable_data()[i];
    }
    outputs += num_indexes;
  }
}

void BitVectorNavigator::SelectsForRelativeRanksForRangeOfRows(
    int64_t batch_begin, int64_t batch_end, int64_t rank_delta, int64_t num_rows,
    const uint64_t* ties_bitvec, const uint64_t* ties_popcounts, int64_t* outputs,
    int64_t hardware_flags, util::TempVectorStack* temp_vector_stack) {
  // Break into mini-batches
  int64_t minibatch_length_max = util::MiniBatch::kMiniBatchLength;
  auto selects_for_ranks_buf = util::TempVectorHolder<int64_t>(
      temp_vector_stack, static_cast<uint32_t>(minibatch_length_max));
  auto selects_for_ranks = selects_for_ranks_buf.mutable_data();
  for (int64_t minibatch_begin = batch_begin; minibatch_begin < batch_end;
       minibatch_begin += minibatch_length_max) {
    int64_t minibatch_end = std::min(batch_end, minibatch_begin + minibatch_length_max);

    // First and last rank that we are interested in
    int64_t first_rank =
        BitVectorNavigator::RankNext(minibatch_begin, ties_bitvec, ties_popcounts) - 1LL;
    int64_t last_rank =
        BitVectorNavigator::RankNext(minibatch_end - 1, ties_bitvec, ties_popcounts) -
        1LL;

    // Do select for each rank in the calculated range.
    //
    BitVectorNavigator::SelectsForRangeOfRanks(
        first_rank + rank_delta, last_rank + rank_delta + 1, num_rows, ties_bitvec,
        ties_popcounts, selects_for_ranks, hardware_flags, temp_vector_stack);

    int irank = 0;
    outputs[minibatch_begin - batch_begin] = selects_for_ranks[irank];
    for (int64_t i = minibatch_begin + 1; i < minibatch_end; ++i) {
      irank += bit_util::GetBit(reinterpret_cast<const uint8_t*>(ties_bitvec), i) ? 1 : 0;
      outputs[minibatch_begin - batch_begin] = selects_for_ranks[irank];
    }
  }
}

}  // namespace compute
}  // namespace arrow
