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

#include <numeric>

#include "arrow/compute/exec.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

// Specialized case for GH-40997
TEST(Grouper, ResortedColumnsWithLargeNullRows) {
  const uint64_t num_rows = 1024;

  // construct random array with plenty of null values
  const int32_t kSeed = 42;
  const int32_t min = 0;
  const int32_t max = 100;
  const double null_probability = 0.3;
  const double true_probability = 0.5;
  auto rng = random::RandomArrayGenerator(kSeed);
  auto b_arr = rng.Boolean(num_rows, true_probability, null_probability);
  auto i32_arr = rng.Int32(num_rows, min, max, null_probability);
  auto i64_arr = rng.Int64(num_rows, min, max * 10, null_probability);

  // construct batches with columns which will be resorted in the grouper make
  std::vector<ExecBatch> exec_batches = {ExecBatch({i64_arr, i32_arr, b_arr}, num_rows),
                                         ExecBatch({i32_arr, i64_arr, b_arr}, num_rows),
                                         ExecBatch({i64_arr, b_arr, i32_arr}, num_rows),
                                         ExecBatch({i32_arr, b_arr, i64_arr}, num_rows),
                                         ExecBatch({b_arr, i32_arr, i64_arr}, num_rows),
                                         ExecBatch({b_arr, i64_arr, i32_arr}, num_rows)};

  const int num_batches = static_cast<int>(exec_batches.size());
  std::vector<uint32_t> group_num_vec;
  group_num_vec.reserve(num_batches);

  for (const auto& exec_batch : exec_batches) {
    ExecSpan span(exec_batch);
    ASSERT_OK_AND_ASSIGN(auto grouper, Grouper::Make(span.GetTypes()));
    ASSERT_OK_AND_ASSIGN(Datum group_ids, grouper->Consume(span));
    group_num_vec.emplace_back(grouper->num_groups());
  }

  for (int i = 1; i < num_batches; i++) {
    ASSERT_EQ(group_num_vec[i - 1], group_num_vec[i]);
  }
}

}  // namespace compute
}  // namespace arrow
