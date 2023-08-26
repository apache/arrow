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

#include <gmock/gmock-matchers.h>

#include "arrow/acero/options.h"
#include "arrow/acero/test_util_internal.h"
#include "arrow/acero/util.h"
#include "arrow/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using compute::call;
using compute::field_ref;
using compute::SortIndices;
using compute::SortKey;
using compute::Take;

namespace acero {

BatchesWithSchema GenerateBatches(std::vector<ExecBatch> batches,
                                  const std::shared_ptr<Schema>& schema) {
  return BatchesWithSchema{batches, schema};
}

void CheckRunOutput(BatchesWithSchema left_input, BatchesWithSchema right_input,
                    JoinType join_type, Expression filter, BatchesWithSchema exp_batches,
                    bool parallel = false) {
  Declaration left{"source",
                   SourceNodeOptions{left_input.schema, left_input.gen(parallel,
                                                                       /*slow=*/false)}};
  Declaration right{
      "source", SourceNodeOptions{right_input.schema, right_input.gen(parallel,
                                                                      /*slow=*/false)}};
  NestedLoopJoinNodeOptions join_options{join_type, filter};
  Declaration join{"nestedloopjoin", {std::move(left), std::move(right)}, join_options};

  ASSERT_OK_AND_ASSIGN(auto out_table, DeclarationToTable(std::move(join), parallel));

  ASSERT_OK_AND_ASSIGN(auto exp_table,
                       TableFromExecBatches(exp_batches.schema, exp_batches.batches));

  if (exp_table->num_rows() == 0) {
    ASSERT_EQ(exp_table->num_rows(), out_table->num_rows());
  } else {
    std::vector<SortKey> sort_keys;
    for (auto&& f : exp_batches.schema->fields()) {
      sort_keys.emplace_back(f->name());
    }
    ASSERT_OK_AND_ASSIGN(auto exp_table_sort_ids,
                         SortIndices(exp_table, SortOptions(sort_keys)));
    ASSERT_OK_AND_ASSIGN(auto exp_table_sorted, Take(exp_table, exp_table_sort_ids));
    ASSERT_OK_AND_ASSIGN(auto out_table_sort_ids,
                         SortIndices(out_table, SortOptions(sort_keys)));
    ASSERT_OK_AND_ASSIGN(auto out_table_sorted, Take(out_table, out_table_sort_ids));

    AssertTablesEqual(*exp_table_sorted.table(), *out_table_sorted.table(),
                      /*same_chunk_layout=*/false, /*flatten=*/true);
  }
}

void RunNormalBatchesTest(JoinType type, bool parallel) {
  auto l_schema = schema({field("i", int32()), field("j", boolean())});
  auto r_schema = schema({field("k", int32()), field("l", boolean())});
  std::shared_ptr<Schema> exp_schema;
  BatchesWithSchema l_batches, r_batches, exp_batches;
  Expression filter = greater(call("add", {field_ref("i"), field_ref("k")}), literal(12));

  l_batches = GenerateBatches(
      {
          ExecBatchFromJSON({int32(), boolean()}, "[[null, true], [4, false]]"),
          ExecBatchFromJSON({int32(), boolean()}, "[[5, true], [6, false], [7, false]]"),
      },
      l_schema);
  r_batches = GenerateBatches(
      {
          ExecBatchFromJSON({int32(), boolean()}, "[[null, true], [4, false]]"),
          ExecBatchFromJSON({int32(), boolean()}, "[[5, true], [6, false], [7, false]]"),
      },
      r_schema);

  switch (type) {
    case JoinType::INNER: {
      exp_schema = schema({field("i", int32()), field("j", boolean()),
                           field("k", int32()), field("l", boolean())});
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[6, false, 7, false]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[7, false, 6, false], [7, false, 7, false]])"),
          },
          exp_schema);
      break;
    }
    case JoinType::LEFT_OUTER: {
      exp_schema = schema({field("i", int32()), field("j", boolean()),
                           field("k", int32()), field("l", boolean())});
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[null, true, null, null], [4, false, null, null]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[6, false, 7, false]])"),
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[7, false, 6, false], [7, false, 7, false], [5, true, null, null]])"),
          },
          exp_schema);
      break;
    }
    case JoinType::LEFT_SEMI:
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean()}, R"([[6, false]])"),
              ExecBatchFromJSON({int32(), boolean()}, R"([[7, false]])"),
          },
          l_schema);
      break;
    case JoinType::LEFT_ANTI:
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean()}, R"([[null, true], [4, false]])"),
              ExecBatchFromJSON({int32(), boolean()}, R"([[5, true]])"),
          },
          l_schema);
      break;
    default:
      FAIL() << "join type not implemented!";
  }
  CheckRunOutput(l_batches, r_batches, type, filter, exp_batches);
}

void RunInnerEmptyBatchesTest(JoinType type, bool parallel) {
  auto l_schema = schema({field("i", int32()), field("j", boolean())});
  auto r_schema = schema({field("k", int32()), field("l", boolean())});
  std::shared_ptr<Schema> exp_schema;
  BatchesWithSchema l_batches, r_batches, exp_batches;
  Expression filter = greater(call("add", {field_ref("i"), field_ref("k")}), literal(12));

  l_batches = GenerateBatches(
      {
          ExecBatchFromJSON({int32(), boolean()}, "[[null, true], [4, false]]"),
          ExecBatchFromJSON({int32(), boolean()}, "[[5, true], [6, false], [7, false]]"),
      },
      l_schema);
  r_batches = GenerateBatches(
      {
          ExecBatchFromJSON({int32(), boolean()}, "[]"),
          ExecBatchFromJSON({int32(), boolean()}, "[]"),
      },
      r_schema);

  switch (type) {
    case JoinType::INNER: {
      exp_schema = schema({field("i", int32()), field("j", boolean()),
                           field("k", int32()), field("l", boolean())});
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()}, R"([])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()}, R"([])"),
          },
          exp_schema);
      break;
    }
    case JoinType::LEFT_OUTER: {
      exp_schema = schema({field("i", int32()), field("j", boolean()),
                           field("k", int32()), field("l", boolean())});
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[null, true, null, null], [4, false, null, null], [5, true, null, null], [6, false, null, null], [7, false, null, null]])"),
          },
          exp_schema);
      break;
    }
    case JoinType::LEFT_SEMI:
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean()}, R"([])"),
              ExecBatchFromJSON({int32(), boolean()}, R"([])"),
          },
          l_schema);
      break;
    case JoinType::LEFT_ANTI:
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON(
                  {int32(), boolean()},
                  R"([[null, true], [4, false], [5, true], [6, false], [7, false]])"),
          },
          l_schema);
      break;
    default:
      FAIL() << "join type not implemented!";
  }
  CheckRunOutput(l_batches, r_batches, type, filter, exp_batches);
}

void RunMaybeEmptyBatchesTest(JoinType type, bool parallel) {
  auto l_schema = schema({field("i", int32()), field("j", boolean())});
  auto r_schema = schema({field("k", int32()), field("l", boolean())});
  std::shared_ptr<Schema> exp_schema;
  BatchesWithSchema l_batches, r_batches, exp_batches;
  Expression filter = greater(call("add", {field_ref("i"), field_ref("k")}), literal(12));

  l_batches = GenerateBatches(
      {
          ExecBatchFromJSON({int32(), boolean()}, "[[null, true], [4, false]]"),
          ExecBatchFromJSON({int32(), boolean()}, "[[5, true], [6, false], [7, false]]"),
      },
      l_schema);
  r_batches = GenerateBatches(
      {
          ExecBatchFromJSON({int32(), boolean()},
                            "[[5, true], [null, true], [7, false]]"),
          ExecBatchFromJSON({int32(), boolean()}, "[]"),
      },
      r_schema);

  switch (type) {
    case JoinType::INNER: {
      exp_schema = schema({field("i", int32()), field("j", boolean()),
                           field("k", int32()), field("l", boolean())});
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[6, false, 7, false]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[7, false, 7, false]])"),
          },
          exp_schema);
      break;
    }
    case JoinType::LEFT_OUTER: {
      exp_schema = schema({field("i", int32()), field("j", boolean()),
                           field("k", int32()), field("l", boolean())});
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[null, true, null, null], [4, false, null, null]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[6, false, 7, false]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[7, false, 7, false], [5, true, null, null]])"),
          },
          exp_schema);
      break;
    }
    case JoinType::LEFT_SEMI:
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean()}, R"([[6, false]])"),
              ExecBatchFromJSON({int32(), boolean()}, R"([[7, false]])"),
          },
          l_schema);
      break;
    case JoinType::LEFT_ANTI:
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean()}, R"([[null, true], [4, false]])"),
              ExecBatchFromJSON({int32(), boolean()}, R"([[5, true]])"),
          },
          l_schema);
      break;
    default:
      FAIL() << "join type not implemented!";
  }
  CheckRunOutput(l_batches, r_batches, type, filter, exp_batches);
}

void RunLiteralTrueTest(JoinType type, bool parallel) {
  auto l_schema = schema({field("i", int32()), field("j", boolean())});
  auto r_schema = schema({field("k", int32()), field("l", boolean())});
  std::shared_ptr<Schema> exp_schema;
  BatchesWithSchema l_batches, r_batches, exp_batches;
  Expression filter = literal(true);

  l_batches = GenerateBatches(
      {
          ExecBatchFromJSON({int32(), boolean()}, "[[null, true], [4, false]]"),
          ExecBatchFromJSON({int32(), boolean()}, "[[5, true], [6, false], [7, false]]"),
      },
      l_schema);
  r_batches = GenerateBatches(
      {
          ExecBatchFromJSON({int32(), boolean()}, "[[null, true], [4, false]]"),
          ExecBatchFromJSON({int32(), boolean()}, "[[5, true], [6, false], [7, false]]"),
      },
      r_schema);

  switch (type) {
    case JoinType::INNER: {
      exp_schema = schema({field("i", int32()), field("j", boolean()),
                           field("k", int32()), field("l", boolean())});
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[null, true, null, true], [null, true, 4, false]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[4, false, null, true], [4, false, 4, false]])"),
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[null, true, 5, true], [null, true, 6, false], [null, true, 7, false]])"),
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[4, false, 5, true], [4, false, 6, false], [4, false, 7, false]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[5, true, null, true], [5, true, 4, false]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[6, false, null, true], [6, false, 4, false]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[7, false, null, true], [7, false, 4, false]])"),
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[5, true, 5, true], [5, true, 6, false], [5, true, 7, false]])"),
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[6, false, 5, true], [6, false, 6, false], [6, false, 7, false]])"),
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[7, false, 5, true], [7, false, 6, false], [7, false, 7, false]])"),
          },
          exp_schema);
      break;
    }
    case JoinType::LEFT_OUTER: {
      exp_schema = schema({field("i", int32()), field("j", boolean()),
                           field("k", int32()), field("l", boolean())});
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[null, true, null, true], [null, true, 4, false]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[4, false, null, true], [4, false, 4, false]])"),
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[null, true, 5, true], [null, true, 6, false], [null, true, 7, false]])"),
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[4, false, 5, true], [4, false, 6, false], [4, false, 7, false]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[5, true, null, true], [5, true, 4, false]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[6, false, null, true], [6, false, 4, false]])"),
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()},
                                R"([[7, false, null, true], [7, false, 4, false]])"),
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[5, true, 5, true], [5, true, 6, false], [5, true, 7, false]])"),
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[6, false, 5, true], [6, false, 6, false], [6, false, 7, false]])"),
              ExecBatchFromJSON(
                  {int32(), boolean(), int32(), boolean()},
                  R"([[7, false, 5, true], [7, false, 6, false], [7, false, 7, false]])"),
          },
          exp_schema);
      break;
    }
    case JoinType::LEFT_SEMI:
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean()}, R"([[null, true]])"),
              ExecBatchFromJSON({int32(), boolean()}, R"([[4, false]])"),
              ExecBatchFromJSON({int32(), boolean()}, R"([[5, true]])"),
              ExecBatchFromJSON({int32(), boolean()}, R"([[6, false]])"),
              ExecBatchFromJSON({int32(), boolean()}, R"([[7, false]])"),
          },
          l_schema);
      break;
    case JoinType::LEFT_ANTI:
      exp_batches = GenerateBatches(
          {
              ExecBatchFromJSON({int32(), boolean(), int32(), boolean()}, R"([])"),
              ExecBatchFromJSON({int32(), boolean()}, R"([])"),
          },
          l_schema);
      break;
    default:
      FAIL() << "join type not implemented!";
  }
  CheckRunOutput(l_batches, r_batches, type, filter, exp_batches);
}

class NestedLoopJoinTest : public testing::TestWithParam<std::tuple<JoinType, bool>> {};

INSTANTIATE_TEST_SUITE_P(
    NestedLoopJoinTest, NestedLoopJoinTest,
    ::testing::Combine(::testing::Values(JoinType::INNER, JoinType::LEFT_OUTER,
                                         JoinType::LEFT_SEMI, JoinType::LEFT_ANTI),
                       ::testing::Values(false)));

TEST_P(NestedLoopJoinTest, TestNormalBatchesJoins) {
  RunNormalBatchesTest(std::get<0>(GetParam()), std::get<1>(GetParam()));
}

TEST_P(NestedLoopJoinTest, TestInnerEmptyBatches) {
  RunInnerEmptyBatchesTest(std::get<0>(GetParam()), std::get<1>(GetParam()));
}

TEST_P(NestedLoopJoinTest, TestMaybeEmptyBatches) {
  RunMaybeEmptyBatchesTest(std::get<0>(GetParam()), std::get<1>(GetParam()));
}

TEST_P(NestedLoopJoinTest, TestFilterTrue) {
  RunLiteralTrueTest(std::get<0>(GetParam()), std::get<1>(GetParam()));
}

}  // namespace acero
}  // namespace arrow
