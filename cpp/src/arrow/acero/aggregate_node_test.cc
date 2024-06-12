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
#include "arrow/testing/matchers.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/string.h"
using testing::UnorderedElementsAreArray;

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

TEST(ScalarAggregate, FilterBeforeAgg) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;

  BatchesWithSchema basic_data;
  basic_data.batches = {
      ExecBatchFromJSON({int32(), int32(), boolean()},
                        "[[1, 3, false], [2, 2, true], [3, 3, true], [4, 15, false]]"),
      ExecBatchFromJSON({int32(), int32(), boolean()},
                        "[[5, 1, false], [6, 8, true], [7, 20, false], [8, 11, false]]"),
  };
  basic_data.schema =
      schema({field("i", int32()), field("j", int32()), field("k", boolean())});

  auto count_filter = greater(compute::field_ref("i"), literal(3));
  auto scalar_filter = greater(compute::field_ref("j"), literal(10));
  auto count_options = std::make_shared<compute::CountOptions>(
      compute::CountOptions::ONLY_VALID, count_filter);
  auto scalar_options =
      std::make_shared<compute::ScalarAggregateOptions>(true, 1, scalar_filter);
  auto tdigest_options =
      std::make_shared<compute::TDigestOptions>(0.5, 100, 500, true, 0, scalar_filter);
  auto variance_options =
      std::make_shared<compute::VarianceOptions>(0, true, 0, scalar_filter);
  ASSERT_OK(
      Declaration::Sequence(
          {
              {"source",
               SourceNodeOptions{basic_data.schema, basic_data.gen(/*parallel=*/false,
                                                                   /*slow=*/false)}},
              {"aggregate", AggregateNodeOptions{/*aggregates=*/{
                                {"all", scalar_options, "k", "all(k)"},
                                {"any", scalar_options, "k", "any(k)"},
                                {"count", count_options, "i", "count(i)"},
                                {"count_all", "count(*)", count_options},
                                {"mean", scalar_options, "i", "mean(i)"},
                                {"product", scalar_options, "i", "product(i)"},
                                {"stddev", variance_options, "i", "stddev(i)"},
                                {"sum", scalar_options, "j", "sum(j)"},
                                {"tdigest", tdigest_options, "i", "tdigest(i)"},
                                {"variance", variance_options, "i", "variance(i)"}}}},
              {"sink", SinkNodeOptions{&sink_gen}},
          })
          .AddToPlan(plan.get()));

  ASSERT_THAT(
      StartAndCollect(plan.get(), sink_gen),
      Finishes(ResultWith(UnorderedElementsAreArray({
          ExecBatchFromJSON(
              {boolean(), boolean(), int64(), int64(), float64(), int64(), float64(),
               int64(), float64(), float64()},
              {ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR,
               ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR,
               ArgShape::ARRAY, ArgShape::SCALAR},
              R"([[false, false, 5, 5, 6.333333333333333, 224, 1.699673171197595, 46, 7, 2.888888888888889]])"),
      }))));
}

TEST(GroupByNode, FilterBeforeHashAgg) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;

  BatchesWithSchema basic_data;
  basic_data.batches = {
      ExecBatchFromJSON({int32(), int32(), boolean()},
                        "[[1, 3, false], [2, 2, true], [3, 3, true], [4, 15, false]]"),
      ExecBatchFromJSON({int32(), int32(), boolean()},
                        "[[5, 1, false], [6, 8, true], [7, 20, false], [8, 11, false]]"),
  };
  basic_data.schema =
      schema({field("i", int32()), field("j", int32()), field("k", boolean())});

  auto count_filter = greater(compute::field_ref("i"), literal(1));
  auto scalar_filter = greater(compute::field_ref("j"), literal(2));
  auto count_options = std::make_shared<compute::CountOptions>(
      compute::CountOptions::ONLY_VALID, count_filter);
  auto scalar_options =
      std::make_shared<compute::ScalarAggregateOptions>(true, 1, scalar_filter);
  auto tdigest_options =
      std::make_shared<compute::TDigestOptions>(0.5, 100, 500, true, 0, scalar_filter);
  auto variance_options =
      std::make_shared<compute::VarianceOptions>(0, true, 0, scalar_filter);
  SortOptions options({compute::SortKey("j", compute::SortOrder::Ascending)});
  ASSERT_OK(
      Declaration::Sequence(
          {
              {"source",
               SourceNodeOptions{basic_data.schema, basic_data.gen(/*parallel=*/false,
                                                                   /*slow=*/false)}},
              {
                  "aggregate",
                  AggregateNodeOptions{
                      /*aggregates=*/{
                          {"hash_all", scalar_options, "k", "hash_all(k)"},
                          {"hash_any", scalar_options, "k", "hash_any(k)"},
                          {"hash_count", count_options, "i", "hash_count(i)"},
                          {"hash_count_all", "hash_count(*)", count_options},
                          {"hash_mean", scalar_options, "i", "hash_mean(i)"},
                          {"hash_product", scalar_options, "i", "hash_product(i)"},
                          {"hash_stddev", variance_options, "i", "hash_stddev(i)"},
                          {"hash_sum", scalar_options, "j", "hash_sum(j)"},
                          {"hash_tdigest", tdigest_options, "i", "hash_tdigest(i)"},
                          {"hash_variance", variance_options, "i", "hash_variance(i)"}},
                      /*keys=*/{"j"}},
              },
              {"order_by_sink", OrderBySinkNodeOptions{options, &sink_gen}},
          })
          .AddToPlan(plan.get()));

  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(ResultWith(UnorderedElementsAreArray({ExecBatchFromJSON(
                  {int32(), boolean(), boolean(), int64(), int64(), float64(), int64(),
                   float64(), int64(), fixed_size_list(float64(), 1), float64()},
                  R"([[1, null, null, 1, 1, null, null, null, null, [null], null], 
                      [2, null, null, 1, 1, null, null, null, null, [null], null], 
                      [3, false, true, 1, 1, 2, 3, 1, 6, [1], 1], 
                      [8, true, true, 1, 1, 6, 6, 0, 8, [6], 0], 
                      [11, false, false, 1, 1, 8, 8, 0, 11, [8], 0],
                      [15, false, false, 1, 1, 4, 4, 0, 15, [4], 0], 
                      [20, false, false, 1, 1, 7, 7, 0, 20, [7], 0]])")}))));
}
}  // namespace acero
}  // namespace arrow
