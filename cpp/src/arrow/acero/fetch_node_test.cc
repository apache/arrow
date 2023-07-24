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

namespace arrow {
namespace acero {

static constexpr int kRowsPerBatch = 16;
static constexpr int kNumBatches = 32;

std::shared_ptr<Table> TestTable() {
  return gen::Gen({gen::Step()})->FailOnError()->Table(kRowsPerBatch, kNumBatches);
}

void CheckFetch(FetchNodeOptions options) {
  constexpr random::SeedType kSeed = 42;
  constexpr int kJitterMod = 4;
  RegisterTestNodes();
  std::shared_ptr<Table> input = TestTable();
  Declaration plan =
      Declaration::Sequence({{"table_source", TableSourceNodeOptions(input)},
                             {"jitter", JitterNodeOptions(kSeed, kJitterMod)},
                             {"fetch", options}});
  for (bool use_threads : {false, true}) {
    QueryOptions query_options;
    query_options.use_threads = use_threads;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> actual,
                         DeclarationToTable(plan, query_options));

    if (options.offset >= input->num_rows() || options.count == 0) {
      // In these cases, Table::Slice would fail or give us a table with 1 chunk while
      // the fetch node gives us a table with 0 chunks
      ASSERT_EQ(0, actual->num_rows());
    } else {
      std::shared_ptr<Table> expected = input->Slice(options.offset, options.count);
      AssertTablesEqual(*expected, *actual);
    }
  }
}

void CheckFetchInvalid(FetchNodeOptions options, const std::string& message) {
  std::shared_ptr<Table> input = TestTable();
  Declaration plan = Declaration::Sequence(
      {{"table_source", TableSourceNodeOptions(input)}, {"fetch", options}});
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr(message),
                                  DeclarationToStatus(std::move(plan)));
}

TEST(FetchNode, Basic) {
  CheckFetch({0, 20});
  CheckFetch({20, 20});
  CheckFetch({0, 1000});
  CheckFetch({1000, 20});
  CheckFetch({50, 50});
  CheckFetch({0, 0});
}

TEST(FetchNode, Invalid) {
  CheckFetchInvalid({-1, 10}, "`offset` must be non-negative");
  CheckFetchInvalid({10, -1}, "`count` must be non-negative");
}

}  // namespace acero
}  // namespace arrow
