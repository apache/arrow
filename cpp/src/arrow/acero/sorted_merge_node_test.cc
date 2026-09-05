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
#include "arrow/compute/cast.h"
#include "arrow/compute/ordering.h"
#include "arrow/compute/row/row_encoder_internal.h"
#include "arrow/compute/test_util_internal.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

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
  // Now We can use threads for sorted merging since now it uses sequencer to assure
  // deterministic order of timestamps, but for the sake of not modifying this test we
  // won't use threads
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

TEST(SortedMergeNode, BasicThreaded) {
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
  static constexpr random::SeedType kTestSeed = 42;
  static constexpr int kMaxJitterMod = 2;
  RegisterTestNodes();
  std::vector<Declaration::Input> src_decls;
  src_decls.emplace_back(
      Declaration("jitter", {Declaration("table_source", TableSourceNodeOptions(table1))},
                  JitterNodeOptions(kTestSeed, kMaxJitterMod)));
  src_decls.emplace_back(
      Declaration("jitter", {Declaration("table_source", TableSourceNodeOptions(table2))},
                  JitterNodeOptions(kTestSeed, kMaxJitterMod)));
  src_decls.emplace_back(
      Declaration("jitter", {Declaration("table_source", TableSourceNodeOptions(table3))},
                  JitterNodeOptions(kTestSeed, kMaxJitterMod)));

  auto ops = OrderByNodeOptions(compute::Ordering({compute::SortKey("timestamp")}));
  Declaration sorted_merge{"sorted_merge", src_decls, ops};
  // Now We can use threads for sorted merging since it uses sequencer to assure
  // deterministic order of timestamps.
  // To simulate wrong order usually caused by enabling threads we use jitter node which
  // would previously break the output without using sequencer.
  ASSERT_OK_AND_ASSIGN(auto output,
                       DeclarationToTable(sorted_merge, /*use_threads=*/true));
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

TEST(SortedMergeNode, PauseProducingSortedMerge) {
#ifndef ARROW_ENABLE_THREADING
  GTEST_SKIP() << "Test requires threading support";
#endif

  RegisterTestNodes();

  int batch_size = 1;
  auto make_shift = [batch_size](int num_batches, const std::shared_ptr<Schema>& schema,
                                 int shift) {
    return MakeIntegerBatches(
        {[](int row) -> int64_t { return row; },
         [num_batches](int row) -> int64_t { return row / num_batches; },
         [shift](int row) -> int64_t { return row * 10 + shift; }},
        schema, num_batches, batch_size);
  };
  auto l_schema =
      schema({field("time", int64()), field("key", int64()), field("l_value", int64())});
  auto r_schema =
      schema({field("time", int64()), field("key", int64()), field("l_value", int64())});

  auto output_schema =
      schema({field("time", int64()), field("key", int64()), field("l_value", int64()),
              field("key", int64()), field("l_value", int64())});

  ASSERT_OK_AND_ASSIGN(auto out_batch,
                       MakeIntegerBatches({[](int row) -> int64_t { return row; },
                                           [](int row) -> int64_t { return row; },
                                           [](int row) -> int64_t { return row / 20; },
                                           [](int row) -> int64_t { return row / 20; },
                                           [](int row) -> int64_t { return row * 10; }},
                                          output_schema, 20, batch_size))

  ASSERT_OK_AND_ASSIGN(auto l_batches, make_shift(50, l_schema, 2));
  ASSERT_OK_AND_ASSIGN(auto r0_batches, make_shift(50, r_schema, 1));
  std::optional<ExecBatch> out = out_batch.batches[0];

  constexpr uint32_t thresholdOfBackpressureSmn = 8;

  EXPECT_OK_AND_ASSIGN(std::shared_ptr<ExecPlan> plan, ExecPlan::Make());
  PushGenerator<std::optional<ExecBatch>> batch_producer_left;
  PushGenerator<std::optional<ExecBatch>> batch_producer_right;

  auto ordering = compute::Ordering({compute::SortKey("time")});

  AsyncGenerator<std::optional<ExecBatch>> sink_gen;
  BackpressureMonitor* backpressure_monitor;
  BackpressureOptions backpressure_options(1, 2);

  Declaration left{"source", SourceNodeOptions(l_schema, batch_producer_left, ordering)};
  Declaration right{"source",
                    SourceNodeOptions(r_schema, batch_producer_right, ordering)};

  auto opt = OrderByNodeOptions(ordering);

  BackpressureCounters bp_countersl, bp_countersr;

  Declaration left_count{"backpressure_count",
                         {std::move(left)},
                         BackpressureCountingNodeOptions(&bp_countersl)};

  Declaration right_count{"backpressure_count",
                          {std::move(right)},
                          BackpressureCountingNodeOptions(&bp_countersr)};

  Declaration asof_join{
      "sorted_merge", {std::move(left_count), std::move(right_count)}, std::move(opt)};

  ARROW_EXPECT_OK(
      acero::Declaration::Sequence(
          {
              std::move(asof_join),
              {"sink", SinkNodeOptions{&sink_gen, /*schema=*/nullptr,
                                       backpressure_options, &backpressure_monitor}},
          })
          .AddToPlan(plan.get()));

  ASSERT_TRUE(backpressure_monitor);
  plan->StartProducing();
  auto fut = plan->finished();

  EXPECT_FALSE(backpressure_monitor->is_paused());

  // Should be able to push kPauseIfAbove batches without triggering back pressure
  int64_t l_cnt = 0;
  int64_t r_cnt = 0;

  EXPECT_FALSE(bp_countersl.is_paused());
  EXPECT_FALSE(bp_countersr.is_paused());
  EXPECT_FALSE(backpressure_monitor->is_paused());
  batch_producer_left.producer().Push(l_batches.batches[l_cnt++]);
  batch_producer_right.producer().Push(r0_batches.batches[r_cnt++]);
  // this should trigger pause on sink
  BusyWait(3.0, [&]() { return backpressure_monitor->is_paused(); });
  arrow::io::internal::GetIOThreadPool()->WaitForIdle();
  arrow::internal::GetCpuThreadPool()->WaitForIdle();

  // Fill up the inputs of the asof join node
  for (uint32_t i = 0; i < thresholdOfBackpressureSmn; i++) {
    SleepABit();
    // EXPECT_FALSE(bp_countersl.is_paused());
    // EXPECT_FALSE(bp_countersr.is_paused());
    EXPECT_TRUE(backpressure_monitor->is_paused());
    batch_producer_left.producer().Push(l_batches.batches[l_cnt++]);
    batch_producer_right.producer().Push(r0_batches.batches[r_cnt++]);
  }
  SleepABit();
  BusyWait(3.0, [&]() { return bp_countersl.is_paused(); });
  BusyWait(3.0, [&]() { return bp_countersr.is_paused(); });
  arrow::io::internal::GetIOThreadPool()->WaitForIdle();
  arrow::internal::GetCpuThreadPool()->WaitForIdle();
  SleepABit();
  // Verify pause propagates
  EXPECT_TRUE(bp_countersl.is_paused());
  EXPECT_TRUE(bp_countersr.is_paused());

  std::optional<ExecBatch> opt_batch;

  do {
    ASSERT_FINISHES_OK_AND_ASSIGN(opt_batch, sink_gen());
    ASSERT_TRUE(opt_batch);
    l_cnt -= opt_batch->length;
  } while (l_cnt > 0);

  BusyWait(3.0, [&]() { return !bp_countersl.is_paused(); });
  BusyWait(3.0, [&]() { return !bp_countersr.is_paused(); });
  arrow::io::internal::GetIOThreadPool()->WaitForIdle();
  arrow::internal::GetCpuThreadPool()->WaitForIdle();
  EXPECT_FALSE(bp_countersl.is_paused());
  EXPECT_FALSE(bp_countersr.is_paused());
  EXPECT_FALSE(backpressure_monitor->is_paused());

  batch_producer_left.producer().Push(IterationEnd<std::optional<ExecBatch>>());
  batch_producer_right.producer().Push(IterationEnd<std::optional<ExecBatch>>());

  ASSERT_THAT(fut, Finishes(Ok()));
}

}  // namespace arrow::acero