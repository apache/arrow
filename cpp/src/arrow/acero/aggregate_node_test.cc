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
#include <gtest/gtest.h>
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/status.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/acero/aggregate_internal.h"
#include "arrow/acero/test_nodes.h"
#include "arrow/acero/test_util_internal.h"
#include "arrow/compute/ordering.h"
#include "arrow/compute/test_util_internal.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/string.h"
namespace arrow {

using compute::ExecBatchFromJSON;

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

std::vector<ExecBatch> TestExecBatches(const std::vector<TypeHolder>& types,
                                       std::vector<std::string> json_batches,
                                       bool implicit_ordering = true) {
  std::vector<ExecBatch> batches;
  int64_t index = 0;
  for (auto&& el : json_batches) {
    batches.push_back(ExecBatchFromJSON(types, el));
    if (implicit_ordering) batches.back().index = index++;
  }
  return batches;
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

TEST(GroupByNode, BasicParallel) {
  const int64_t num_batches = 8;

  std::vector<ExecBatch> batches(num_batches, ExecBatchFromJSON({int32()}, "[[42]]"));

  Declaration plan = Declaration::Sequence(
      {{"exec_batch_source",
        ExecBatchSourceNodeOptions(schema({field("key", int32())}), batches)},
       {"aggregate", AggregateNodeOptions{/*aggregates=*/{{"hash_count_all", "count(*)"}},
                                          /*keys=*/{"key"}}}});

  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema out_batches,
                       DeclarationToExecBatches(plan));

  ExecBatch expected_batch = ExecBatchFromJSON(
      {int32(), int64()}, "[[42, " + std::to_string(num_batches) + "]]");
  AssertExecBatchesEqualIgnoringOrder(out_batches.schema, {expected_batch},
                                      out_batches.batches);
}

TEST(GroupByNode, ParallelOrderedAggregator) {
  // const int64_t num_batches = 8;

  std::vector<ExecBatch> batches = TestExecBatches({int64(), int64()}, {R"([
    [0,999],
    [0,1]
  ])",
                                                                        R"([
    [1,333],
    [1,1]
  ])",
                                                                        R"([
    [2,111],
    [2,1]
  ])"});
  static constexpr random::SeedType kTestSeed = 42;
  static constexpr int kMaxJitterMod = 3;
  RegisterTestNodes();
  Declaration plan = Declaration::Sequence(
      {{"exec_batch_source",
        ExecBatchSourceNodeOptions(
            schema({field("key1", int64()), field("value", int64())}), batches)},
       {"jitter", JitterNodeOptions(kTestSeed, kMaxJitterMod)},
       {"aggregate",
        AggregateNodeOptions{/*aggregates=*/
                             {{"hash_first", nullptr, "value", "mean(value)"}},
                             {"key1"},
                             {}}}});
  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema out_batches,
                       DeclarationToExecBatches(plan));
  ExecBatch expected_batch = ExecBatchFromJSON({int64(), int64()},
                                               R"([
    [0, 999],
    [1, 333],
    [2, 111] 
    ])");

  AssertExecBatchesEqual(out_batches.schema, {expected_batch}, out_batches.batches);
}

TEST(GroupByNode, ParallelSegmentedAggregatorGroupBy) {
  std::vector<ExecBatch> batches =
      TestExecBatches({utf8(), int64(), int64(), int64()}, {R"([
    ["x",0,1,1],
    ["y",0,1,2],
    ["z",0,1,3]
  ])",
                                                            R"([
    ["x",1,2,1],
    ["y",1,2,2],
    ["z",1,3,3]
  ])",
                                                            R"([
    ["x",2,3,1],
    ["y",2,3,2],
    ["z",2,3,3]
  ])"});

  Ordering order({compute::SortKey("key2", compute::SortOrder::Ascending),
                  compute::SortKey("key1", compute::SortOrder::Ascending)});
  static constexpr random::SeedType kTestSeed = 42;
  static constexpr int kMaxJitterMod = 3;
  RegisterTestNodes();
  Declaration plan = Declaration::Sequence(
      {{"exec_batch_source",
        ExecBatchSourceNodeOptions(
            schema({field("key1", utf8()), field("key2", int64()), field("key3", int64()),
                    field("value", int64())}),
            batches)},
       {"order_by", OrderByNodeOptions(order)},
       {"jitter", JitterNodeOptions(kTestSeed, kMaxJitterMod)},
       {"aggregate",
        AggregateNodeOptions{/*aggregates=*/
                             {{"hash_mean", nullptr, "value", "mean(value)"}},
                             {"key1"},
                             {"key2"}}}});

  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema out_batches,
                       DeclarationToExecBatches(plan));
  std::vector<ExecBatch> batches_ex = TestExecBatches({int64(), utf8(), float64()}, {R"([
    [0,"x" , 1],[0,"y" , 2],[0,"z" , 3]
    ])",
                                                                                     R"([ 
    [1,"x" , 1],[1,"y" , 2],[1,"z" , 3]
    ])",
                                                                                     R"([
    [2,"x" , 1],[2,"y" , 2],[2,"z" , 3]
    ])"});

  AssertExecBatchesEqual(out_batches.schema, batches_ex, out_batches.batches);
}

TEST(ScalarAggregateNode, AnyAll) {
  // GH-43768: boolean_any and boolean_all with constant input should work well
  // when min_count != 0.
  std::shared_ptr<Schema> in_schema = schema({field("not_used", int32())});
  std::shared_ptr<Schema> out_schema = schema({field("agg_out", boolean())});
  struct AnyAllCase {
    std::string batches_json;
    Expression literal;
    std::string expected_json;
    bool skip_nulls = false;
    uint32_t min_count = 2;
  };
  std::vector<AnyAllCase> cases{
      {"[[42], [42], [42], [42]]", literal(true), "[[true]]"},
      {"[[42], [42], [42], [42]]", literal(false), "[[false]]"},
      {"[[42], [42], [42], [42]]", literal(BooleanScalar{}), "[[null]]"},
      {"[[42]]", literal(true), "[[null]]"},
      {"[[42], [42], [42]]", literal(true), "[[true]]"},
      {"[[42], [42], [42]]", literal(true), "[[null]]", /*skip_nulls=*/false,
       /*min_count=*/4},
      {"[[42], [42], [42], [42]]", literal(BooleanScalar{}), "[[null]]",
       /*skip_nulls=*/true},
  };
  for (const AnyAllCase& any_all_case : cases) {
    for (auto func_name : {"any", "all"}) {
      std::vector<ExecBatch> batches{
          ExecBatchFromJSON({int32()}, any_all_case.batches_json)};
      std::vector<Aggregate> aggregates = {
          Aggregate(func_name,
                    std::make_shared<compute::ScalarAggregateOptions>(
                        /*skip_nulls=*/any_all_case.skip_nulls,
                        /*min_count=*/any_all_case.min_count),
                    FieldRef("literal"))};

      // And a projection to make the input including a Scalar Boolean
      Declaration plan = Declaration::Sequence(
          {{"exec_batch_source", ExecBatchSourceNodeOptions(in_schema, batches)},
           {"project", ProjectNodeOptions({any_all_case.literal}, {"literal"})},
           {"aggregate", AggregateNodeOptions(aggregates)}});

      ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema out_batches,
                           DeclarationToExecBatches(plan));

      ExecBatch expected_batch =
          ExecBatchFromJSON({boolean()}, any_all_case.expected_json);

      AssertExecBatchesEqualIgnoringOrder(out_schema, {expected_batch},
                                          out_batches.batches);
    }
  }
}

TEST(ScalarAggregateNode, BasicParallel) {
  const int64_t num_batches = 8;

  std::vector<ExecBatch> batches(num_batches, ExecBatchFromJSON({int32()}, "[[42]]"));

  Declaration plan = Declaration::Sequence(
      {{"exec_batch_source",
        ExecBatchSourceNodeOptions(schema({field("", int32())}), batches)},
       {"aggregate", AggregateNodeOptions{/*aggregates=*/{{"count_all", "count(*)"}}}}});

  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema out_batches,
                       DeclarationToExecBatches(plan));

  ExecBatch expected_batch =
      ExecBatchFromJSON({int64()}, "[[" + std::to_string(num_batches) + "]]");
  AssertExecBatchesEqualIgnoringOrder(out_batches.schema, {expected_batch},
                                      out_batches.batches);
}

TEST(ScalarAggregateNode, ParallelOrderedAggregator) {
  std::vector<ExecBatch> batches = TestExecBatches({int64(), int64()}, {R"([
    [0,999],
    [0,1]
  ])",
                                                                        R"([
    [1,999],
    [1,1]
  ])",
                                                                        R"([
    [2,999],
    [2,1]
  ])"});

  static constexpr random::SeedType kTestSeed = 42;
  static constexpr int kMaxJitterMod = 3;
  RegisterTestNodes();

  Declaration plan = Declaration::Sequence(
      {{"exec_batch_source",
        ExecBatchSourceNodeOptions(
            schema({field("key1", int64()), field("value", int64())}), batches)},
       {"jitter", JitterNodeOptions(kTestSeed, kMaxJitterMod)},
       {"aggregate",
        AggregateNodeOptions{/*aggregates=*/
                             {{"first", nullptr, "value", "mean(value)"}}}}});

  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema out_batches,
                       DeclarationToExecBatches(plan));

  ExecBatch expected_batch = ExecBatchFromJSON({int64()}, R"([ [999] ])");
  AssertExecBatchesEqual(out_batches.schema, {expected_batch}, out_batches.batches);
}

TEST(ScalarAggregateNode, ParallelSegmentedAggregator) {
  std::vector<ExecBatch> batches = TestExecBatches({int64(), int64(), int64()},
                                                   {R"([
    [0,1,999],
    [0,2,1]
  ])",
                                                    R"([
    [1,11,499],
    [1,12,1]
  ])",
                                                    R"([
    [2,21,299],
    [2,22,1]
  ])"},
                                                   true);

  static constexpr random::SeedType kTestSeed = 42;
  static constexpr int kMaxJitterMod = 3;
  RegisterTestNodes();

  Declaration plan = Declaration::Sequence(
      {{"exec_batch_source",
        ExecBatchSourceNodeOptions(schema({field("key1", int64()), field("key2", int64()),
                                           field("value", int64())}),
                                   batches)},
       {"jitter", JitterNodeOptions(kTestSeed, kMaxJitterMod)},
       {"aggregate", AggregateNodeOptions{/*aggregates=*/
                                          {{"mean", nullptr, "value", "mean(value)"}},
                                          {},
                                          {FieldRef("key1")}}}});

  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema out_batches,
                       DeclarationToExecBatches(plan));
  auto expected_batch = TestExecBatches(
      {int64(), float64()}, {R"([[0 , 500]])", R"([[1 , 250]])", R"([[2 , 150]])"});
  AssertExecBatchesEqual(out_batches.schema, expected_batch, out_batches.batches);
}

TEST(ScalarAggregateNode, ParallelSegmentedAggregatorKeySorted) {
  std::vector<ExecBatch> batches = TestExecBatches({int64(), int64(), int64(), int64()},
                                                   {R"([
    [0,1,1,999],
    [0,2,1,1]
  ])",
                                                    R"([
    [1,11,2,499],
    [1,12,2,1]
  ])",
                                                    R"([
    [2,21,3,299],
    [2,22,3,1]
  ])"},
                                                   true);

  static constexpr random::SeedType kTestSeed = 42;
  static constexpr int kMaxJitterMod = 3;
  RegisterTestNodes();
  Ordering order({compute::SortKey("key1", compute::SortOrder::Ascending),
                  compute::SortKey("key3", compute::SortOrder::Ascending)});

  Declaration plan = Declaration::Sequence(
      {{"exec_batch_source",
        ExecBatchSourceNodeOptions(
            schema({field("key1", int64()), field("key2", int64()),
                    field("key3", int64()), field("value", int64())}),
            batches)},
       {"order_by", OrderByNodeOptions{order}},
       {"jitter", JitterNodeOptions(kTestSeed, kMaxJitterMod)},
       {"aggregate", AggregateNodeOptions{/*aggregates=*/
                                          {{"mean", nullptr, "value", "mean(value)"}},
                                          {},
                                          {FieldRef("key1")}}}});

  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema out_batches,
                       DeclarationToExecBatches(plan));
  auto expected_batch = TestExecBatches(
      {int64(), float64()}, {R"([[0 , 500]])", R"([[1 , 250]])", R"([[2 , 150]])"});
  AssertExecBatchesEqual(out_batches.schema, expected_batch, out_batches.batches);
}

class BackpressureTestExecNode;

class BackpressureTestNodeOptions : public ExecNodeOptions {
 public:
  explicit BackpressureTestNodeOptions(Ordering order) : order_(std::move(order)) {
    is_paused_ = std::make_shared<bool>(false);
  }
  std::shared_ptr<bool> is_paused_;
  Ordering order_;
  static constexpr std::string_view kName = "backpressure";
  bool is_paused() { return *is_paused_; }
};

class BackpressureTestExecNode : public ExecNode {
 public:
  BackpressureTestExecNode(ExecPlan* plan, NodeVector inputs,
                           std::shared_ptr<Schema> output_schema,
                           const BackpressureTestNodeOptions& _options)
      : ExecNode(plan, inputs, {"something"}, output_schema),
        is_paused_(_options.is_paused_),
        order_(std::move(_options.order_)),
        options(_options) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    const BackpressureTestNodeOptions& bb =
        static_cast<const BackpressureTestNodeOptions&>(options);
    auto input = inputs[0];
    return input->plan()->EmplaceNode<BackpressureTestExecNode>(
        plan, inputs, inputs[0]->output_schema(), bb);
  }

  const char* kind_name() const override { return "BackpressureTestNode"; }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    ++total_batches_;
    ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(batch)));
    if (total_batches_ == total_batches_received)
      return output_->InputFinished(this, total_batches_);
    return Status::OK();
  }
  Status InputFinished(ExecNode* input, int total_batches) override {
    (void)input;
    total_batches_received = total_batches;
    return Status::OK();
  }
  Status StartProducing() override { return Status::OK(); }

  Status Init() { return Status::OK(); }

  const Ordering& ordering() const override { return order_; }

  Status DoConsume(const ExecSpan& batch, size_t thread_index) { return Status::OK(); }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->PauseProducing(this, counter);
    *is_paused_ = true;
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->ResumeProducing(this, counter);
    *is_paused_ = false;
  }

 protected:
  Status StopProducingImpl() override { return Status::OK(); }

 public:
  int64_t total_batches_ = 0;
  int64_t total_batches_received = 0;
  std::shared_ptr<bool> is_paused_;
  Ordering order_;
  const BackpressureTestNodeOptions& options;
};

TEST(ExecPlanExecution, SequenceQueueBackpressure) {
  // static std::once_flag registered;
  // std::call_once(registered, [] {
  //   ExecFactoryRegistry* registry = default_exec_factory_registry();
  //   (void)registry->AddFactory(std::string(BackpressureTestNodeOptions::kName),
  //                              BackpressureTestExecNode::Make);
  // });

  // BackpressureCountingNode::Register();
  RegisterTestNodes();
  std::vector<ExecBatch> batches =
      gen::Gen({{"key1", gen::Step(0, 1)}, {"value", gen::Random(int32())}})
          ->FailOnError()
          ->ExecBatches(5, 30);

  constexpr uint32_t kPauseIfAbove = 8;
  constexpr uint32_t kResumeIfBelow = 4;
  uint32_t pause_if_above_bytes =
      kPauseIfAbove * static_cast<uint32_t>(batches[0].TotalBufferSize());
  uint32_t resume_if_below_bytes =
      kResumeIfBelow * static_cast<uint32_t>(batches[0].TotalBufferSize());

  EXPECT_OK_AND_ASSIGN(std::shared_ptr<ExecPlan> plan, ExecPlan::Make());
  PushGenerator<std::optional<ExecBatch>> batch_producer;
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;
  Ordering order({compute::SortKey("key1", compute::SortOrder::Ascending)});

  BackpressureCounters options;
  BackpressureCountingNodeOptions opc(&options);

  // BackpressureTestNodeOptions options(order);

  BackpressureMonitor* backpressure_monitor;
  BackpressureOptions backpressure_options(resume_if_below_bytes, pause_if_above_bytes);
  std::shared_ptr<Schema> schema_ =
      schema({field("key1", int32()), field("value", int32())});
  ARROW_EXPECT_OK(
      acero::Declaration::Sequence(
          {
              {"source", SourceNodeOptions(schema_, batch_producer, order)},
              {"backpressure_count", opc},
              {"aggregate",
               AggregateNodeOptions{/*aggregates=*/
                                    {{"mean", nullptr, "value", "mean(value)"}},
                                    {},
                                    {FieldRef("key1")}}},
              {"sink", SinkNodeOptions{&sink_gen,
                                       /*schema=*/nullptr, backpressure_options,
                                       &backpressure_monitor}},
          })
          .AddToPlan(plan.get()));
  plan->StartProducing();

  // set indexes to match the order
  for (size_t i = 0; i < batches.size(); ++i) {
    batches[i].index = i;
  }
  // lets reverse first indexes to fill the SequenceQueue with
  // enough batches to trigger the Backpressure when all the required indexes will be
  // present in the SequenceQueue to emit the batches
  std::reverse(batches.begin(), batches.begin() + kPauseIfAbove + 1);

  // lets push few first batches
  uint32_t count = 0;
  for (; count < kPauseIfAbove; count++) {
    batch_producer.producer().Push(batches[count]);
  }
  SleepABit();
  // at this point there isn't a idex 0 in the queue so nothing happens.
  ASSERT_FALSE(options.is_paused());
  // we push the batch with 0 index to be processed
  batch_producer.producer().Push(batches[count++]);
  SleepABit();
  // we let the precesses run
  BusyWait(5, [&] { return options.is_paused(); });
  SleepABit();
  //  at this point Node shoule trigger backpressure Pause signal.
  ASSERT_TRUE(options.is_paused());

  // we give it some time to process the batches and wait for the backPressure release
  // signal.
  BusyWait(5, [&] { return !options.is_paused(); });
  SleepABit();
  ASSERT_FALSE(options.is_paused());

  // we push the rest of the batches in order
  for (uint32_t i = count; i < batches.size(); i++) {
    batch_producer.producer().Push(batches[i]);
  }
  SleepABit();
  BusyWait(5, [&] { return options.is_paused(); });
  SleepABit();
  // BackPressure should also be triggered since we will push like 20+ batches which
  // will tiger the back pressure
  ASSERT_TRUE(options.is_paused());

  ASSERT_FINISHES_OK(sink_gen());
  BusyWait(10, [&] { return !options.is_paused(); });
  //  the BackPressure shoule be released here
  ASSERT_FALSE(options.is_paused());

  // Cleanup
  batch_producer.producer().Push(IterationEnd<std::optional<ExecBatch>>());
  plan->StopProducing();

  auto fut = plan->finished();
  ASSERT_TRUE(fut.Wait(kDefaultAssertFinishesWaitSeconds));
  if (!fut.status().ok()) {
    ASSERT_TRUE(fut.status().IsCancelled());
  }
}

}  // namespace acero
}  // namespace arrow
