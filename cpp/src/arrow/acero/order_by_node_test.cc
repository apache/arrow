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

#include <gtest/gtest.h>

#include <gmock/gmock-matchers.h>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/test_nodes.h"
#include "arrow/table.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_pointer_cast;

using compute::SortKey;
using compute::SortOrder;

namespace acero {

// Sorting is slow, don't use too many rows
static constexpr int kRowsPerBatch = 4;
static constexpr int kNumBatches = 32;

std::shared_ptr<Table> TestTable() {
  return gen::Gen({{"up", gen::Step()},
                   {"down", gen::Step(/*start=*/0, /*step=*/-1, /*signed_int=*/true)}})
      ->FailOnError()
      ->Table(kRowsPerBatch, kNumBatches);
}

void CheckOrderBy(OrderByNodeOptions options) {
  constexpr random::SeedType kSeed = 42;
  constexpr int kJitterMod = 4;
  RegisterTestNodes();
  std::shared_ptr<Table> input = TestTable();
  Declaration plan =
      Declaration::Sequence({{"table_source", TableSourceNodeOptions(input)},
                             {"jitter", JitterNodeOptions(kSeed, kJitterMod)},
                             {"order_by", options}});
  for (bool use_threads : {false, true}) {
    QueryOptions query_options;
    query_options.sequence_output = true;
    query_options.use_threads = use_threads;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> actual,
                         DeclarationToTable(plan, query_options));

    AssertTablesEqual(*input, *actual, /*same_chunk_layout=*/false);
  }
}

void CheckOrderByInvalid(OrderByNodeOptions options, const std::string& message) {
  std::shared_ptr<Table> input = TestTable();
  Declaration plan = Declaration::Sequence(
      {{"table_source", TableSourceNodeOptions(input)}, {"order_by", options}});
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr(message),
                                  DeclarationToStatus(std::move(plan)));
}

TEST(OrderByNode, Basic) {
  CheckOrderBy(OrderByNodeOptions({{SortKey("up")}}));
  CheckOrderBy(OrderByNodeOptions({{SortKey("down", SortOrder::Descending)}}));
  CheckOrderBy(
      OrderByNodeOptions({{SortKey("up"), SortKey("down", SortOrder::Descending)}}));
}

TEST(OrderByNode, Large) {
  constexpr random::SeedType kSeed = 42;
  constexpr int kJitterMod = 4;
  static constexpr int kSmallNumBatches = 8;

  RegisterTestNodes();
  // Confirm that the order by node chunks output for parallel processing
  std::shared_ptr<Table> input = gen::Gen({{"up", gen::Step()}})
                                     ->FailOnError()
                                     ->Table(ExecPlan::kMaxBatchSize, kSmallNumBatches);
  Declaration plan = Declaration::Sequence({
      {"table_source", TableSourceNodeOptions(input)},
      {"order_by", OrderByNodeOptions({{SortKey("up", SortOrder::Descending)}})},
      {"jitter", JitterNodeOptions(kSeed, kJitterMod)},
  });
  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema batches_and_schema,
                       DeclarationToExecBatches(std::move(plan)));
  ASSERT_EQ(kSmallNumBatches, static_cast<int>(batches_and_schema.batches.size()));
  // Jitter simulates parallel processing after the order by node.  However, the batch
  // index should be correct, regardless of the jitter
  for (const auto& batch : batches_and_schema.batches) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Scalar> last_row,
                         batch.values[0].make_array()->GetScalar(batch.length - 1));
    uint32_t last_val = checked_pointer_cast<UInt32Scalar>(last_row)->value;
    ASSERT_EQ(last_val, ExecPlan::kMaxBatchSize * (kSmallNumBatches - batch.index - 1));
  }
}

TEST(OrderByNode, Invalid) {
  CheckOrderByInvalid(OrderByNodeOptions(Ordering::Implicit()),
                      "`ordering` must be an explicit non-empty ordering");
  CheckOrderByInvalid(OrderByNodeOptions(Ordering::Unordered()),
                      "`ordering` must be an explicit non-empty ordering");
}

}  // namespace acero
}  // namespace arrow
