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
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <memory>

#include "arrow/acero/test_util_internal.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/string.h"

namespace arrow {

namespace acero {

Result<std::shared_ptr<Table>> TableGroupBy(
    std::shared_ptr<Table> table, std::vector<Aggregate> aggregates,
    std::vector<FieldRef> keys, bool use_threads = false,
    MemoryPool* memory_pool = default_memory_pool()) {
  Declaration plan = Declaration::Sequence(
      {{"table_source", TableSourceNodeOptions(std::move(table))},
       {"aggregate", AggregateNodeOptions(std::move(aggregates), std::move(keys))}});
  return DeclarationToTable(std::move(plan), use_threads, memory_pool);
}

TEST(GroupByConvenienceFunc, Basic) {
  std::shared_ptr<Schema> in_schema =
      schema({field("key1", utf8()), field("key2", int32()), field("value", int32())});
  std::shared_ptr<Table> in_table = TableFromJSON(in_schema, {R"([
    ["x", 1, 1],
    ["y", 1, 2],
    ["y", 2, 3],
    ["z", 2, 4],
    ["z", 2, 5]
  ])"});

  // One key, two aggregates, same values array
  std::shared_ptr<Table> expected = TableFromJSON(schema({
                                                      field("key1", utf8()),
                                                      field("value_sum", int64()),
                                                      field("value_count", int64()),
                                                  }),
                                                  {R"([
        ["x", 1, 1],
        ["y", 5, 2],
        ["z", 9, 2]
    ])"});
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> actual,
                       TableGroupBy(in_table,
                                    {{"hash_sum", {"value"}, "value_sum"},
                                     {"hash_count", {"value"}, "value_count"}},
                                    {"key1"}));
  AssertTablesEqual(*expected, *actual);

  // Two keys, one aggregate
  expected = TableFromJSON(schema({field("key1", utf8()), field("key2", int32()),
                                   field("value_sum", int64())}),
                           {
                               R"([
        ["x", 1, 1],
        ["y", 1, 2],
        ["y", 2, 3],
        ["z", 2, 9]
      ])"});

  ASSERT_OK_AND_ASSIGN(actual,
                       TableGroupBy(in_table, {{"hash_sum", {"value"}, "value_sum"}},
                                    {{"key1"}, {"key2"}}));
  AssertTablesEqual(*expected, *actual);

  // No keys (whole table aggregate)
  expected =
      TableFromJSON(schema({field("sum", int64()), field("count", int64())}), {
                                                                                  R"([
      [15, 5]
    ])"});
  ASSERT_OK_AND_ASSIGN(actual, TableGroupBy(in_table,
                                            {{"sum", {"value"}, "value_sum"},
                                             {"count", {"value"}, "value_count"}},
                                            {}));

  // No aggregates (used to get distinct key values)
  expected =
      TableFromJSON(schema({field("key_0", utf8()), field("key_1", int32())}), {
                                                                                   R"([
      ["x", 1],
      ["y", 1],
      ["y", 2],
      ["z", 2]
    ])"});
  ASSERT_OK_AND_ASSIGN(actual, TableGroupBy(in_table, {}, {{"key1"}, {"key2"}}));
  AssertTablesEqual(*expected, *actual);
}

TEST(GroupByConvenienceFunc, Invalid) {
  std::shared_ptr<Schema> in_schema =
      schema({field("key1", utf8()), field("key2", int32()), field("value", int32())});
  std::shared_ptr<Table> in_table = TableFromJSON(in_schema, {R"([
    ["x", 1, 1],
    ["y", 1, 2],
    ["y", 2, 3],
    ["z", 2, 4],
    ["z", 2, 5]
  ])"});

  // Scalar/hash mismatch
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("a hash aggregate function was expected"),
      TableGroupBy(in_table, {{"count", {"value"}, "value_count"}}, {"key1"}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("a scalar aggregate function was expected"),
      TableGroupBy(in_table, {{"hash_count", {"value"}, "value_count"}}, {}));
  // Not an aggregate function
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("is not an aggregate function"),
      TableGroupBy(in_table, {{"add", {"value"}, "value_add"}}, {"key1"}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("is not an aggregate function"),
      TableGroupBy(in_table, {{"add", {"value"}, "value_add"}}, {}));
}

void TestVarStdMultiBatch(const std::string& var_std_func_name) {
  std::shared_ptr<Schema> in_schema = schema({field("value", float64())});
  std::shared_ptr<Table> in_table = TableFromJSON(in_schema, {R"([
    [1],
    [2],
    [3]
  ])",
                                                              R"([
    [4],
    [4],
    [4]
  ])"});

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> actual,
                       TableGroupBy(in_table, {{var_std_func_name, {"value"}, "x"}}, {},
                                    /*use_threads=*/false));

  ASSERT_OK_AND_ASSIGN(auto var_scalar, actual->column(0)->GetScalar(0));
  // the next assertion will fail if only the second batch affects the result
  ASSERT_NE(0, std::dynamic_pointer_cast<DoubleScalar>(var_scalar)->value);
}

TEST(GroupByConvenienceFunc, VarianceMultiBatch) { TestVarStdMultiBatch("variance"); }

TEST(GroupByConvenienceFunc, StdDevMultiBatch) { TestVarStdMultiBatch("stddev"); }

TEST(GroupByNode, NoSkipNulls) {
  constexpr int kNumBatches = 128;

  std::shared_ptr<Schema> in_schema =
      schema({field("key", int32()), field("value", int32())});

  // This regresses GH-36053.  Some groups have nulls and other groups do not.  The
  // "does this group have nulls" field needs to merge correctly between different
  // aggregate states.  We use 128 batches to encourage multiple thread states to
  // be used.
  ExecBatch nulls_batch =
      ExecBatchFromJSON({int32(), int32()}, "[[1, null], [1, null], [1, null]]");
  ExecBatch no_nulls_batch =
      ExecBatchFromJSON({int32(), int32()}, "[[2, 1], [2, 1], [2, 1]]");

  std::vector<ExecBatch> batches;
  batches.reserve(kNumBatches);
  for (int i = 0; i < kNumBatches; i += 2) {
    batches.push_back(nulls_batch);
    batches.push_back(no_nulls_batch);
  }

  std::vector<Aggregate> aggregates = {Aggregate(
      "hash_sum", std::make_shared<compute::ScalarAggregateOptions>(/*skip_nulls=*/false),
      FieldRef("value"))};
  std::vector<FieldRef> keys = {"key"};

  Declaration plan = Declaration::Sequence(
      {{"exec_batch_source", ExecBatchSourceNodeOptions(in_schema, std::move(batches))},
       {"aggregate", AggregateNodeOptions(aggregates, keys)}});

  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema out_batches,
                       DeclarationToExecBatches(plan));

  std::shared_ptr<Schema> out_schema =
      schema({field("key", int32()), field("sum_value", int64())});
  int32_t expected_sum = static_cast<int32_t>(no_nulls_batch.length) *
                         static_cast<int32_t>(bit_util::CeilDiv(kNumBatches, 2));
  ExecBatch expected_batch = ExecBatchFromJSON(
      {int32(), int64()}, "[[1, null], [2, " + internal::ToChars(expected_sum) + "]]");

  AssertExecBatchesEqualIgnoringOrder(out_schema, {expected_batch}, out_batches.batches);
}

}  // namespace acero
}  // namespace arrow
