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
#include "arrow/compute/api.h"
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

TEST(GroupBy, VarArgKernel) {
  // test hash aggregate should be able to resolve function with var args inputs
  // group
  struct EmptyKernel : compute::KernelState {};
  auto Init = [](compute::KernelContext *, const compute::KernelInitArgs&) ->
      arrow::Result<std::unique_ptr<compute::KernelState>> {
    return std::make_unique<EmptyKernel>();
  };
  auto Resize = [](compute::KernelContext*, int64_t) -> Status {
    return Status::OK();
  };
  auto Consume = [](compute::KernelContext*, const compute::ExecSpan&) -> Status {
    return Status::OK();
  };
  auto Merge = [](compute::KernelContext*,compute::KernelState&&,const ArrayData&)
  -> Status {
    return Status::OK();
  };
  auto Finalize = [](compute::KernelContext*, Datum*) -> Status {
    return Status::OK();
  };
  compute::FunctionDoc doc {"udf_var_arg", "a udf with var arg",
                           {"group_id", "z", "x..."},
                           "ScalarAggregateOptions"};
  auto func = std::make_shared<compute::HashAggregateFunction>(
      "udf_var_arg",
      compute::Arity(2,true),
      std::move(doc)
      );
  // add first kernel for args = {bool, i64...}
  auto sig_i64 = compute::KernelSignature::Make({
                                                    arrow::uint32(),
                                                    arrow::uint8(),
                                                    arrow::int64()},
                                                arrow::int64(),
                                                /*var_arg*/true);
  compute::HashAggregateKernel ker_i64 {std::move(sig_i64), Init, Resize, Consume,
                                       Merge, Finalize, false};
  ASSERT_OK(func->AddKernel(std::move(ker_i64)));

  // add second kernel for args = {bool, f64...}
  auto sig_f64 = compute::KernelSignature::Make({
                                                    arrow::uint32(),
                                                    arrow::uint8(),
                                                 arrow::float64()},
                                                arrow::float64(),
                                                /*var_arg*/true);
  compute::HashAggregateKernel ker_f64 {std::move(sig_f64), Init, Resize, Consume,
                                       Merge, Finalize, false};
  ASSERT_OK(func->AddKernel(std::move(ker_f64)));
  ASSERT_OK(compute::GetFunctionRegistry()->AddFunction(std::move(func)));

  // declare plan and should validate
  auto input_schema = arrow::schema({field("key", utf8()),
                                     field("z", uint8()),
                                     field("i_1", int64()),
                                     field("i_2", int64()),
                                     field("f_1", float64())});

  // aggr1, aggr2 should resolve to the same function
  std::vector<FieldRef> var_arg_i64_input;
  var_arg_i64_input.emplace_back("z");
  var_arg_i64_input.emplace_back("i_1");
  var_arg_i64_input.emplace_back("i_2");
  Aggregate agg_1 {"udf_var_arg", std::make_shared<compute::ScalarAggregateOptions>(),
      std::move(var_arg_i64_input), "o_i64"};

  std::vector<FieldRef> var_arg_f64_input;
  var_arg_f64_input.emplace_back("z");
  var_arg_f64_input.emplace_back("f_1");
  Aggregate agg_2 {"udf_var_arg", std::make_shared<compute::ScalarAggregateOptions>(),
                  std::move(var_arg_f64_input), "o_f64"};
  std::vector<arrow::FieldRef> gr_keys;
  gr_keys.emplace_back("key");

  AsyncGenerator<std::optional<compute::ExecBatch>> source_gen, sink_gen;
  auto decl = acero::Declaration::Sequence({
      {"source", acero::SourceNodeOptions{input_schema, source_gen}},
      {"aggregate", acero::AggregateNodeOptions{{agg_1, agg_2}, std::move(gr_keys)}},
      {"sink", acero::SinkNodeOptions{&sink_gen}}
  });

  ASSERT_OK_AND_ASSIGN(auto plan, acero::ExecPlan::Make())
  ASSERT_OK(decl.AddToPlan(plan.get()));
  ASSERT_OK(plan->Validate());
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
