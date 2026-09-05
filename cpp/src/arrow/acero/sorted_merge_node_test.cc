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

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/map_node.h"
#include "arrow/acero/options.h"
#include "arrow/acero/test_nodes.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/ordering.h"
#include "arrow/compute/test_util_internal.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging_internal.h"

namespace arrow::acero {

std::shared_ptr<Table> TestTable(int start, int step, int rows_per_batch,
                                 int num_batches) {
  return gen::Gen({{"timestamp", gen::Step(start, step)}, {"str", gen::Random(utf8())}})
      ->FailOnError()
      ->Table(rows_per_batch, num_batches);
}

TEST(SortedMergeNode, Basic) {
  auto table1 = TestTable(
      /*start=*/0,
      /*step=*/2,
      /*rows_per_batch=*/2,
      /*num_batches=*/3);
  auto table2 = TestTable(
      /*start=*/1,
      /*step=*/2,
      /*rows_per_batch=*/3,
      /*num_batches=*/2);
  auto table3 = TestTable(
      /*start=*/3,
      /*step=*/3,
      /*rows_per_batch=*/6,
      /*num_batches=*/1);
  std::vector<Declaration::Input> src_decls;
  src_decls.emplace_back(Declaration("table_source", TableSourceNodeOptions(table1)));
  src_decls.emplace_back(Declaration("table_source", TableSourceNodeOptions(table2)));
  src_decls.emplace_back(Declaration("table_source", TableSourceNodeOptions(table3)));

  auto ops = OrderByNodeOptions(compute::Ordering({compute::SortKey("timestamp")}));

  Declaration sorted_merge{"sorted_merge", src_decls, ops};
  ASSERT_OK_AND_ASSIGN(auto output,
                       DeclarationToTable(sorted_merge, /*use_threads=*/false));
  ASSERT_EQ(output->num_rows(), 18);

  ASSERT_OK_AND_ASSIGN(auto expected_ts_builder,
                       MakeBuilder(int32(), default_memory_pool()));
  for (auto i : {0, 1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 9, 9, 10, 11, 12, 15, 18}) {
    ASSERT_OK(expected_ts_builder->AppendScalar(*MakeScalar(i)));
  }
  ASSERT_OK_AND_ASSIGN(auto expected_ts, expected_ts_builder->Finish());
  auto output_col = output->column(0);
  ASSERT_OK_AND_ASSIGN(auto output_ts, Concatenate(output_col->chunks()));

  AssertArraysEqual(*expected_ts, *output_ts);
}

TEST(SortedMergeNode, SignedValuesCrossZero) {
  auto table1 = TestTable(
      /*start=*/-4,
      /*step=*/2,
      /*rows_per_batch=*/3,
      /*num_batches=*/1);
  auto table2 = TestTable(
      /*start=*/-3,
      /*step=*/2,
      /*rows_per_batch=*/4,
      /*num_batches=*/1);
  std::vector<Declaration::Input> src_decls;
  src_decls.emplace_back(Declaration("table_source", TableSourceNodeOptions(table1)));
  src_decls.emplace_back(Declaration("table_source", TableSourceNodeOptions(table2)));

  auto options = OrderByNodeOptions(compute::Ordering({compute::SortKey("timestamp")}));
  Declaration sorted_merge{"sorted_merge", src_decls, options};
  ASSERT_OK_AND_ASSIGN(auto output,
                       DeclarationToTable(sorted_merge, /*use_threads=*/false));

  auto expected = ArrayFromJSON(int32(), "[-4, -3, -2, -1, 0, 1, 3]");
  ASSERT_OK_AND_ASSIGN(auto actual, Concatenate(output->column(0)->chunks()));
  AssertArraysEqual(*expected, *actual);
}

TEST(SortedMergeNode, MergesScalarPayload) {
  auto input_schema = schema({field("timestamp", int32()), field("source", utf8())});
  ExecBatch left(
      {ArrayFromJSON(int32(), "[0, 2]"), std::make_shared<StringScalar>("left")}, 2);
  ExecBatch right(
      {ArrayFromJSON(int32(), "[1, 3]"), std::make_shared<StringScalar>("right")}, 2);

  Declaration merge{
      "sorted_merge",
      {Declaration{"exec_batch_source",
                   ExecBatchSourceNodeOptions(input_schema, {std::move(left)})},
       Declaration{"exec_batch_source",
                   ExecBatchSourceNodeOptions(input_schema, {std::move(right)})}},
      OrderByNodeOptions(compute::Ordering({compute::SortKey("timestamp")}))};
  ASSERT_OK_AND_ASSIGN(auto output,
                       DeclarationToTable(std::move(merge), /*use_threads=*/false));

  ASSERT_OK_AND_ASSIGN(auto timestamps, Concatenate(output->column(0)->chunks()));
  AssertArraysEqual(*ArrayFromJSON(int32(), "[0, 1, 2, 3]"), *timestamps);
  ASSERT_OK_AND_ASSIGN(auto sources, Concatenate(output->column(1)->chunks()));
  AssertArraysEqual(*ArrayFromJSON(utf8(), R"(["left", "right", "left", "right"])"),
                    *sources);
}

TEST(SortedMergeNode, ProducesSortedOutputFromJitteredInputsOnBothExecutors) {
  // Use enough small batches that delivery and flow control overlap.  Correctness must
  // depend on logical batch indices rather than physical arrival order.
  constexpr int kBatchesPerInput = 64;
  RegisterTestNodes();
  auto table0 = TestTable(0, 3, /*rows_per_batch=*/1, kBatchesPerInput);
  auto table1 = TestTable(1, 3, /*rows_per_batch=*/1, kBatchesPerInput);
  auto table2 = TestTable(2, 3, /*rows_per_batch=*/1, kBatchesPerInput);

  for (bool use_threads : {false, true}) {
    SCOPED_TRACE(use_threads ? "threaded" : "serial");
    std::vector<Declaration::Input> inputs;
    inputs.emplace_back(Declaration::Sequence(
        {{"table_source", TableSourceNodeOptions(table0)},
         {"jitter", JitterNodeOptions(/*seed=*/42, /*max_jitter_modifier=*/4)}}));
    inputs.emplace_back(Declaration::Sequence(
        {{"table_source", TableSourceNodeOptions(table1)},
         {"jitter", JitterNodeOptions(/*seed=*/84, /*max_jitter_modifier=*/4)}}));
    inputs.emplace_back(Declaration::Sequence(
        {{"table_source", TableSourceNodeOptions(table2)},
         {"jitter", JitterNodeOptions(/*seed=*/126, /*max_jitter_modifier=*/4)}}));

    QueryOptions query_options;
    query_options.use_threads = use_threads;
    Declaration merge{
        "sorted_merge", std::move(inputs),
        OrderByNodeOptions(compute::Ordering({compute::SortKey("timestamp")}))};
    ASSERT_OK_AND_ASSIGN(auto output,
                         DeclarationToTable(std::move(merge), query_options));

    ASSERT_EQ(output->num_rows(), 3 * kBatchesPerInput);
    ASSERT_OK_AND_ASSIGN(auto actual, Concatenate(output->column(0)->chunks()));
    ASSERT_OK_AND_ASSIGN(auto expected_builder,
                         MakeBuilder(int32(), default_memory_pool()));
    for (int value = 0; value < 3 * kBatchesPerInput; ++value) {
      ASSERT_OK(expected_builder->AppendScalar(*MakeScalar(value)));
    }
    ASSERT_OK_AND_ASSIGN(auto expected, expected_builder->Finish());
    AssertArraysEqual(*expected, *actual);
  }
}

TEST(SortedMergeNode, DownstreamBackpressureAndStop) {
  for (bool stop_while_paused : {false, true}) {
    SCOPED_TRACE(stop_while_paused ? "stop" : "resume");
    auto input_schema = schema({field("timestamp", int32())});
    PushGenerator<std::optional<ExecBatch>> left_generator;
    PushGenerator<std::optional<ExecBatch>> right_generator;
    AsyncGenerator<std::optional<ExecBatch>> sink_generator;
    BackpressureMonitor* backpressure_monitor = nullptr;

    Declaration left{
        "source", SourceNodeOptions(input_schema, left_generator, Ordering::Implicit())};
    Declaration right{
        "source", SourceNodeOptions(input_schema, right_generator, Ordering::Implicit())};
    Declaration merge{
        "sorted_merge",
        {std::move(left), std::move(right)},
        OrderByNodeOptions(compute::Ordering({compute::SortKey("timestamp")}))};
    Declaration sink{"sink",
                     {std::move(merge)},
                     SinkNodeOptions(&sink_generator, /*schema=*/nullptr,
                                     BackpressureOptions(/*resume_if_below=*/1,
                                                         /*pause_if_above=*/1),
                                     &backpressure_monitor, /*sequence_output=*/false)};

    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(*threaded_exec_context()));
    ASSERT_OK(sink.AddToPlan(plan.get()));
    ASSERT_OK(plan->Validate());
    ASSERT_NE(backpressure_monitor, nullptr);
    plan->StartProducing();

    left_generator.producer().Push(compute::ExecBatchFromJSON({int32()}, "[[0]]"));
    right_generator.producer().Push(compute::ExecBatchFromJSON({int32()}, "[[1]]"));
    BusyWait(10.0, [&] { return backpressure_monitor->is_paused(); });
    ASSERT_TRUE(backpressure_monitor->is_paused());

    if (stop_while_paused) {
      plan->StopProducing();
      left_generator.producer().Push(IterationEnd<std::optional<ExecBatch>>());
      right_generator.producer().Push(IterationEnd<std::optional<ExecBatch>>());
      ASSERT_TRUE(plan->finished().Wait(kDefaultAssertFinishesWaitSeconds));
      ASSERT_TRUE(plan->finished().status().IsCancelled());
      continue;
    }

    const uint64_t paused_bytes = backpressure_monitor->bytes_in_use();
    left_generator.producer().Push(compute::ExecBatchFromJSON({int32()}, "[[2]]"));
    right_generator.producer().Push(compute::ExecBatchFromJSON({int32()}, "[[3]]"));
    arrow::internal::GetCpuThreadPool()->WaitForIdle();
    EXPECT_EQ(backpressure_monitor->bytes_in_use(), paused_bytes);

    ASSERT_FINISHES_OK_AND_ASSIGN(auto first_output, sink_generator());
    ASSERT_TRUE(first_output.has_value());
    BusyWait(10.0, [&] { return backpressure_monitor->bytes_in_use() > 0; });
    const bool resumed = backpressure_monitor->bytes_in_use() > 0;

    left_generator.producer().Push(IterationEnd<std::optional<ExecBatch>>());
    right_generator.producer().Push(IterationEnd<std::optional<ExecBatch>>());
    for (;;) {
      ASSERT_FINISHES_OK_AND_ASSIGN(auto output, sink_generator());
      if (!output) break;
    }
    ASSERT_FINISHES_OK(plan->finished());
    EXPECT_TRUE(resumed);
  }
}

}  // namespace arrow::acero
