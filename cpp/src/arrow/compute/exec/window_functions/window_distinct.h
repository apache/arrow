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
#include "arrow/compute/exec/window_functions/merge_tree.h"
#include "arrow/compute/exec/window_functions/window_expr.h"
#include "arrow/compute/exec/window_functions/window_frame.h"

namespace arrow {
namespace compute {

class WindowDistinct {
 public:
  void Init(int64_t num_rows, int64_t num_groups, const int64_t* group_ids,
            /*const WindowFrames &frames,*/ int64_t hardware_flags,
            util::TempVectorStack* stack);
  void Count(const WindowFrames& frames, int64_t* output, int64_t hardware_flags,
             util::TempVectorStack* temp_vector_stack) const;
  void Sum(const WindowFrames& frames, WindowAggregateFunc* func,
           WindowAggregateFunc::Vector* results, uint8_t* results_validity,
           int64_t hardware_flags, util::TempVectorStack* temp_vector_stack) const;

 private:
  static void GenListsForGroups(int64_t num_rows, int64_t num_groups,
                                const int64_t* group_ids_sorted,
                                const int64_t* row_numbers_sorted_on_group_id,
                                int64_t* opt_prev, int64_t* opt_next);

  // static void GenCountLE(int64_t num_rows, int64_t max_value,
  //                        const int64_t *values,
  //                        /* num_rows elements */
  //                        int64_t *count_LE,
  //                        int64_t *row_numbers_sorted_on_values);

  static void GenHasPrevNextBitvecs(int64_t num_rows, const int64_t* group_ids_sorted,
                                    const int64_t* row_numbers_sorted_on_group_ids,
                                    uint64_t* has_prev_bitvec, uint64_t* has_next_bitvec);

  void GenRowNumbersSortedOnPrev(const int64_t* prev, const uint64_t* has_prev_bitvec,
                                 const uint64_t* has_prev_popcounts,
                                 int64_t* row_numbers_sorted_on_prev) const;

  int64_t CountPrevLE(int64_t upper_bound) const;

  int64_t num_rows_;
  int64_t num_groups_;
  MergeTree merge_tree_;
  // std::vector<int64_t> prev_;
  // std::vector<int64_t> next_;
  std::vector<uint64_t> has_next_bitvec_;
  std::vector<uint64_t> has_next_popcounts_;
  // std::vector<int64_t> count_prev_LE_;
  std::vector<int64_t> row_numbers_sorted_on_prev_;
};

class WindowDistinctBasic {
 public:
  static void Count(int64_t num_rows, const int64_t* group_ids,
                    const WindowFrames& frames, int64_t* output);
  static void Sum(int64_t num_rows, const WindowFrames& frames, const int64_t* group_ids,
                  const int64_t* vals,
                  std::vector<std::pair<uint64_t, uint64_t>>& results,
                  uint8_t* results_validity);
};

class WindowDistinctTest {
 public:
  static void TestCountOrSum(bool test_sum);
};

}  // namespace compute
}  // namespace arrow