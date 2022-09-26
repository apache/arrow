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
#include <numeric>  // for std::iota
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec/window_functions/bit_vector_navigator.h"
#include "arrow/compute/exec/window_functions/merge_tree.h"
#include "arrow/compute/exec/window_functions/range_tree.h"
#include "arrow/compute/exec/window_functions/splay_tree.h"
#include "arrow/compute/exec/window_functions/window_frame.h"

namespace arrow {
namespace compute {

// TODO: Current row does not have to be inside its frame.
// Make sure that ranking functions behave well in that case.
//

// TODO: Scale ranks to achieve CUME_DIST and NTILE values.

enum class RankType : int {
  ROW_NUMBER = 0,
  RANK_TIES_LOW = 1,
  RANK_TIES_HIGH = 2,
  DENSE_RANK = 3
};

class WindowRank {
 public:
  static void Global(RankType rank_type, int64_t num_rows, const uint64_t* ties_bitvec,
                     const uint64_t* ties_popcounts, int64_t* output,
                     int64_t hardware_flags, util::TempVectorStack* temp_vector_stack);

  static void WithinFrame(RankType rank_type, int64_t num_rows,
                          const uint64_t* ties_bitvec, const uint64_t* ties_popcounts,
                          const int64_t* frame_begins, const int64_t* frame_ends,
                          int64_t* output, int64_t hardware_flags,
                          util::TempVectorStack* temp_vector_stack);

  static void OnSeparateAttribute(RankType rank_type, int64_t num_rows,
                                  const int64_t* global_ranks_sorted,
                                  const int64_t* permutation, bool progressive_frames,
                                  const int64_t* frame_begins, const int64_t* frame_ends,
                                  int64_t* output, int64_t hardware_flags,
                                  util::TempVectorStack* temp_vector_stack);

 private:
  static void GlobalRank(bool ties_low, int64_t num_rows, const uint64_t* bitvec,
                         const uint64_t* popcounts, int64_t* output,
                         int64_t hardware_flags,
                         util::TempVectorStack* temp_vector_stack);

  static void GlobalDenseRank(int64_t num_rows, const uint64_t* bitvec,
                              const uint64_t* popcounts, int64_t* output);

  static void GlobalRowNumber(int64_t num_rows, int64_t* output);

  static void RankWithinFrame(bool ties_low, int64_t num_rows, const uint64_t* bitvec,
                              const uint64_t* popcounts, const int64_t* frame_begins,
                              const int64_t* frame_ends, int64_t* output,
                              int64_t hardware_flags,
                              util::TempVectorStack* temp_vector_stack);

  static void DenseRankWithinFrame(int64_t num_rows, const uint64_t* bitvec,
                                   const uint64_t* popcounts, const int64_t* frame_begins,
                                   const int64_t* frame_ends, int64_t* output);

  static void RowNumberWithinFrame(int64_t num_rows, const int64_t* frame_begins,
                                   const int64_t* frame_ends, int64_t* output);

  static void SeparateAttributeRank(
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
      util::TempVectorStack* temp_vector_stack);

  static void SeparateAttributeDenseRank(
      int64_t num_rows, const int64_t* begins, const int64_t* ends,
      /* The following two arrays must be the result of sorting rows on
        (global dense rank, row number within window) pairs. Within a group of
        peers with that same dense ranks rows must come in the order in which
        they appeared in the window. This could be accomplished by a stable
        sort of window on the dense rank value.
      */
      const int64_t* global_dense_ranks_sorted, const int64_t* permutation,
      int64_t* output, int64_t hardware_flags, util::TempVectorStack* temp_vector_stack);

  static void ProgressiveSeparateAttributeRank(bool dense_rank, bool ties_low,
                                               int64_t num_rows, const int64_t* begins,
                                               const int64_t* ends,
                                               const int64_t* global_ranks_sorted,
                                               const int64_t* permutation,
                                               int64_t* output);

  template <bool T_DENSE_RANK>
  static void ProgressiveSeparateAttributeRankImp(
      bool ties_low, int64_t num_rows, const int64_t* begins, const int64_t* ends,
      const int64_t* global_ranks_sorted, const int64_t* permutation, int64_t* output);
};

class WindowRankBasic {
 public:
  static void Global(RankType rank_type, int64_t num_rows, const uint64_t* bitvec,
                     int64_t* output);

  static void WithinFrame(RankType rank_type, int64_t num_rows, const uint64_t* bitvec,
                          const int64_t* frame_begins, const int64_t* frame_ends,
                          int64_t* output);

  static void SeparateAttribute(RankType rank_type, int64_t num_rows,
                                const int64_t* begins, const int64_t* ends,
                                const int64_t* global_ranks_sorted,
                                const int64_t* permutation, int64_t* output);
};

class WindowRankTest {
 public:
  static void TestRank(RankType rank_type, bool separate_ranking_attribute,
                       bool use_frames, bool use_progressive_frames);
};

}  // namespace compute
}  // namespace arrow
