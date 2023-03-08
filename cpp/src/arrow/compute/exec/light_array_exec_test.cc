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

#include "arrow/compute/light_array.h"

#include <gtest/gtest.h>

#include "arrow/compute/exec/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace compute {

TEST(KeyColumnArray, FromExecBatch) {
  ExecBatch batch =
      ExecBatchFromJSON({int64(), boolean()}, "[[1, true], [2, false], [null, null]]");
  std::vector<KeyColumnArray> arrays;
  ASSERT_OK(ColumnArraysFromExecBatch(batch, &arrays));

  ASSERT_EQ(2, arrays.size());
  ASSERT_EQ(8, arrays[0].metadata().fixed_length);
  ASSERT_EQ(0, arrays[1].metadata().fixed_length);
  ASSERT_EQ(3, arrays[0].length());
  ASSERT_EQ(3, arrays[1].length());

  ASSERT_OK(ColumnArraysFromExecBatch(batch, 1, 1, &arrays));

  ASSERT_EQ(2, arrays.size());
  ASSERT_EQ(8, arrays[0].metadata().fixed_length);
  ASSERT_EQ(0, arrays[1].metadata().fixed_length);
  ASSERT_EQ(1, arrays[0].length());
  ASSERT_EQ(1, arrays[1].length());
}

TEST(ExecBatchBuilder, AppendBatches) {
  std::unique_ptr<MemoryPool> owned_pool = MemoryPool::CreateDefault();
  MemoryPool* pool = owned_pool.get();
  ExecBatch batch_one =
      ExecBatchFromJSON({int64(), boolean()}, "[[1, true], [2, false], [null, null]]");
  ExecBatch batch_two =
      ExecBatchFromJSON({int64(), boolean()}, "[[null, true], [5, true], [6, false]]");
  ExecBatch combined = ExecBatchFromJSON(
      {int64(), boolean()},
      "[[1, true], [2, false], [null, null], [null, true], [5, true], [6, false]]");
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[3] = {0, 1, 2};
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 3, row_ids, /*num_cols=*/2));
    ASSERT_OK(builder.AppendSelected(pool, batch_two, 3, row_ids, /*num_cols=*/2));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(combined, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  ASSERT_EQ(0, pool->bytes_allocated());
}

TEST(ExecBatchBuilder, AppendBatchesSomeRows) {
  std::unique_ptr<MemoryPool> owned_pool = MemoryPool::CreateDefault();
  MemoryPool* pool = owned_pool.get();
  ExecBatch batch_one =
      ExecBatchFromJSON({int64(), boolean()}, "[[1, true], [2, false], [null, null]]");
  ExecBatch batch_two =
      ExecBatchFromJSON({int64(), boolean()}, "[[null, true], [5, true], [6, false]]");
  ExecBatch combined = ExecBatchFromJSON(
      {int64(), boolean()}, "[[1, true], [2, false], [null, true], [5, true]]");
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[2] = {0, 1};
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 2, row_ids, /*num_cols=*/2));
    ASSERT_OK(builder.AppendSelected(pool, batch_two, 2, row_ids, /*num_cols=*/2));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(combined, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  ASSERT_EQ(0, pool->bytes_allocated());
}

TEST(ExecBatchBuilder, AppendBatchesSomeCols) {
  std::unique_ptr<MemoryPool> owned_pool = MemoryPool::CreateDefault();
  MemoryPool* pool = owned_pool.get();
  ExecBatch batch_one =
      ExecBatchFromJSON({int64(), boolean()}, "[[1, true], [2, false], [null, null]]");
  ExecBatch batch_two =
      ExecBatchFromJSON({int64(), boolean()}, "[[null, true], [5, true], [6, false]]");
  ExecBatch first_col_only =
      ExecBatchFromJSON({int64()}, "[[1], [2], [null], [null], [5], [6]]");
  ExecBatch last_col_only = ExecBatchFromJSON(
      {boolean()}, "[[true], [false], [null], [true], [true], [false]]");
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[3] = {0, 1, 2};
    int first_col_ids[1] = {0};
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 3, row_ids, /*num_cols=*/1,
                                     first_col_ids));
    ASSERT_OK(builder.AppendSelected(pool, batch_two, 3, row_ids, /*num_cols=*/1,
                                     first_col_ids));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(first_col_only, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[3] = {0, 1, 2};
    // If we don't specify col_ids and num_cols is 1 it is implicitly the first col
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 3, row_ids, /*num_cols=*/1));
    ASSERT_OK(builder.AppendSelected(pool, batch_two, 3, row_ids, /*num_cols=*/1));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(first_col_only, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[3] = {0, 1, 2};
    int last_col_ids[1] = {1};
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 3, row_ids, /*num_cols=*/1,
                                     last_col_ids));
    ASSERT_OK(builder.AppendSelected(pool, batch_two, 3, row_ids, /*num_cols=*/1,
                                     last_col_ids));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(last_col_only, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  ASSERT_EQ(0, pool->bytes_allocated());
}

TEST(ExecBatchBuilder, AppendNulls) {
  std::unique_ptr<MemoryPool> owned_pool = MemoryPool::CreateDefault();
  MemoryPool* pool = owned_pool.get();
  ExecBatch batch_one =
      ExecBatchFromJSON({int64(), boolean()}, "[[1, true], [2, false], [null, null]]");
  ExecBatch combined = ExecBatchFromJSON(
      {int64(), boolean()},
      "[[1, true], [2, false], [null, null], [null, null], [null, null]]");
  ExecBatch just_nulls =
      ExecBatchFromJSON({int64(), boolean()}, "[[null, null], [null, null]]");
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[3] = {0, 1, 2};
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 3, row_ids, /*num_cols=*/2));
    ASSERT_OK(builder.AppendNulls(pool, {int64(), boolean()}, 2));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(combined, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  {
    ExecBatchBuilder builder;
    ASSERT_OK(builder.AppendNulls(pool, {int64(), boolean()}, 2));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(just_nulls, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  ASSERT_EQ(0, pool->bytes_allocated());
}

}  // namespace compute
}  // namespace arrow
