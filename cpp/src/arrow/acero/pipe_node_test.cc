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

#include <functional>
#include <memory>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/test_nodes.h"
#include "arrow/acero/test_util_internal.h"
#include "arrow/acero/util.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/test_util_internal.h"
#include "arrow/io/util_internal.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/config.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/vector.h"

using testing::HasSubstr;

namespace arrow {

using compute::call;
using compute::ExecBatchFromJSON;
using compute::field_ref;

namespace acero {

void CheckFinishesCancelledOrOk(const Future<>& fut) {
  // There is a race condition with most tests that cancel plans.  If the
  // cancel call comes in too slowly then the plan might have already finished
  // ok.
  ASSERT_TRUE(fut.Wait(kDefaultAssertFinishesWaitSeconds));
  if (!fut.status().ok()) {
    ASSERT_TRUE(fut.status().IsCancelled());
  }
}

TEST(ExecPlanExecution, PipeErrorSink) {
  auto basic_data = MakeBasicBatches();

  AsyncGenerator<std::optional<ExecBatch>> main_sink_gen;
  AsyncGenerator<std::optional<ExecBatch>> dup_sink_gen;
  AsyncGenerator<std::optional<ExecBatch>> dup1_sink_gen;
  Declaration decl = Declaration::Sequence(
      {{"source", SourceNodeOptions{basic_data.schema, basic_data.gen(/*parallel=*/false,
                                                                      /*slow=*/false)}},
       {"pipe_tee", PipeSinkNodeOptions{"named_pipe_1"}},
       {"sink", SinkNodeOptions{&main_sink_gen}}});

  Declaration dup = Declaration::Sequence(
      {{"pipe_source", PipeSourceNodeOptions{"named_pipe_1", basic_data.schema}},
       {"sink", SinkNodeOptions{&dup_sink_gen}}});

  Declaration dup1 = Declaration::Sequence(
      {{"pipe_source", PipeSourceNodeOptions{"named_pipe_1", basic_data.schema}},
       {"filter", FilterNodeOptions{equal(field_ref("i32"), literal<std::string>("6"))}},
       {"sink", SinkNodeOptions{&dup1_sink_gen}}});

  auto bad_schema = schema({field("i32", uint32()), field("bool", boolean())});
  Declaration dup2 = Declaration::Sequence(
      {{"pipe_source", PipeSourceNodeOptions{"named_pipe_1", bad_schema}},
       {"filter", FilterNodeOptions{equal(field_ref("i32"), literal(6))}},
       {"sink", SinkNodeOptions{&dup1_sink_gen}}});
  dup1.label = "dup1";

  // fail on planning
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  ASSERT_OK(decl.AddToPlan(plan.get()));
  ASSERT_OK(dup.AddToPlan(plan.get()));
  ASSERT_THAT(
      dup1.AddToPlan(plan.get()).status(),
      Raises(StatusCode::NotImplemented,
             HasSubstr(
                 "Function 'equal' has no kernel matching input types (int32, string)")));
  // fail on exec
  ASSERT_OK_AND_ASSIGN(plan, ExecPlan::Make());
  ASSERT_OK(decl.AddToPlan(plan.get()));
  ASSERT_OK(dup.AddToPlan(plan.get()));
  ASSERT_OK(dup2.AddToPlan(plan.get()));
  ASSERT_OK(plan->Validate());
  std::vector<AsyncGenerator<std::optional<ExecBatch>>> gens = {
      main_sink_gen, dup_sink_gen, dup1_sink_gen};
  plan->StartProducing();
  ASSERT_THAT(plan->finished().result().status(),
              Raises(StatusCode::Invalid,
                     HasSubstr("Pipe schema does not match for named_pipe_1")));

  Declaration dup3 = Declaration::Sequence(
      {{"pipe_source",
        PipeSourceNodeOptions{"named_pipe_missing_sink", basic_data.schema}},
       {"sink", SinkNodeOptions{&dup_sink_gen}}});

  ASSERT_OK_AND_ASSIGN(plan, ExecPlan::Make());
  ASSERT_OK(decl.AddToPlan(plan.get()));
  ASSERT_OK(dup.AddToPlan(plan.get()));
  ASSERT_OK(dup3.AddToPlan(plan.get()));
  ASSERT_OK(plan->Validate());
  gens = {main_sink_gen, dup_sink_gen, dup1_sink_gen};
  plan->StartProducing();
  ASSERT_THAT(
      plan->finished().result().status(),
      Raises(StatusCode::Invalid,
             HasSubstr("Pipe 'named_pipe_missing_sink' error: Pipe does not have sink")));
}

TEST(ExecPlanExecution, PipeErrorDuplicateSink) {
  auto basic_data = MakeBasicBatches();

  AsyncGenerator<std::optional<ExecBatch>> main_sink_gen;
  AsyncGenerator<std::optional<ExecBatch>> dup_sink_gen;
  Declaration decl = Declaration::Sequence(
      {{"source", SourceNodeOptions{basic_data.schema, basic_data.gen(/*parallel=*/false,
                                                                      /*slow=*/false)}},
       {"pipe_tee", PipeSinkNodeOptions{"named_pipe_1"}},
       {"pipe_tee", PipeSinkNodeOptions{"named_pipe_1"}},
       {"sink", SinkNodeOptions{&main_sink_gen}}});

  Declaration dup = Declaration::Sequence(
      {{"pipe_source", PipeSourceNodeOptions{"named_pipe_1", basic_data.schema}},
       {"sink", SinkNodeOptions{&dup_sink_gen}}});

  // fail on planning
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  ASSERT_OK(decl.AddToPlan(plan.get()));
  ASSERT_OK(dup.AddToPlan(plan.get()));
  plan->StartProducing();
  ASSERT_THAT(
      plan->finished().result().status(),
      Raises(StatusCode::Invalid, HasSubstr("Pipe:named_pipe_1 has multiple sinks")));
}

TEST(ExecPlanExecution, PipeFilterSink) {
  auto basic_data = MakeBasicBatches();
  AsyncGenerator<std::optional<ExecBatch>> main_sink_gen;
  AsyncGenerator<std::optional<ExecBatch>> dup_sink_gen;
  AsyncGenerator<std::optional<ExecBatch>> dup1_sink_gen;
  Declaration decl = Declaration::Sequence(
      {{"source", SourceNodeOptions{basic_data.schema, basic_data.gen(/*parallel=*/false,
                                                                      /*slow=*/false)}},
       {"pipe_tee", PipeSinkNodeOptions{"named_pipe_1"}},
       {"filter", FilterNodeOptions{equal(field_ref("i32"), literal(6))}},
       {"sink", SinkNodeOptions{&main_sink_gen}}});

  Declaration dup = Declaration::Sequence(
      {{"pipe_source", PipeSourceNodeOptions{"named_pipe_1", basic_data.schema}},
       {"sink", SinkNodeOptions{&dup_sink_gen}}});

  Declaration dup1 = Declaration::Sequence(
      {{"pipe_source", PipeSourceNodeOptions{"named_pipe_1", basic_data.schema}},
       {"filter", FilterNodeOptions{and_(greater(field_ref("i32"), literal(3)),
                                         less(field_ref("i32"), literal(6)))}},
       {"sink", SinkNodeOptions{&dup1_sink_gen}}});
  dup1.label = "dup1";

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  ASSERT_OK(decl.AddToPlan(plan.get()));
  ASSERT_OK(dup.AddToPlan(plan.get()));
  ASSERT_OK(dup1.AddToPlan(plan.get()));
  ASSERT_OK(plan->Validate());
  std::vector<AsyncGenerator<std::optional<ExecBatch>>> gens = {
      main_sink_gen, dup_sink_gen, dup1_sink_gen};
  ASSERT_FINISHES_OK_AND_ASSIGN(auto exec_batches_vec, StartAndCollect(plan.get(), gens));

  ASSERT_EQ(exec_batches_vec.size(), 3);

  auto exp_batches = {ExecBatchFromJSON({int32(), boolean()}, "[]"),
                      ExecBatchFromJSON({int32(), boolean()}, "[[6, false]]")};
  auto exp_dup1 = {ExecBatchFromJSON({int32(), boolean()}, "[[4, false]]"),
                   ExecBatchFromJSON({int32(), boolean()}, "[[5, null]]")};
  AssertExecBatchesEqualIgnoringOrder(basic_data.schema, exp_batches,
                                      exec_batches_vec[0]);
  AssertExecBatchesEqualIgnoringOrder(basic_data.schema, basic_data.batches,
                                      exec_batches_vec[1]);
  AssertExecBatchesEqualIgnoringOrder(basic_data.schema, exp_dup1, exec_batches_vec[2]);
}

TEST(ExecPlanExecution, PipeMultiSchemaSink) {
  auto basic_data = MakeBasicBatches();
  AsyncGenerator<std::optional<ExecBatch>> main_sink_gen;
  AsyncGenerator<std::optional<ExecBatch>> dup_sink_gen;
  AsyncGenerator<std::optional<ExecBatch>> dup1_sink_gen;

  Expression strbool =
      call("if_else", {field_ref("bool"), literal("true"), literal("false")});

  Declaration decl = Declaration::Sequence(
      {{"source", SourceNodeOptions{basic_data.schema, basic_data.gen(/*parallel=*/false,
                                                                      /*slow=*/false)}},
       {"pipe_tee", PipeSinkNodeOptions{"named_pipe_1"}},
       {"project", ProjectNodeOptions({strbool}, {"str"})},
       {"pipe_tee", PipeSinkNodeOptions{"named_pipe_2"}},
       {"sink", SinkNodeOptions{&main_sink_gen}}});

  std::shared_ptr<Schema> schema_2 = schema({field("str", utf8())});
  Declaration dup = Declaration::Sequence(
      {{"pipe_source", PipeSourceNodeOptions{"named_pipe_1", basic_data.schema}},
       {"sink", SinkNodeOptions{&dup_sink_gen}}});

  Declaration dup1 = Declaration::Sequence(
      {{"pipe_source", PipeSourceNodeOptions{"named_pipe_2", schema_2}},
       {"sink", SinkNodeOptions{&dup1_sink_gen}}});
  dup1.label = "dup1";

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  ASSERT_OK(decl.AddToPlan(plan.get()));
  ASSERT_OK(dup.AddToPlan(plan.get()));
  ASSERT_OK(dup1.AddToPlan(plan.get()));
  ASSERT_OK(plan->Validate());

  std::vector<AsyncGenerator<std::optional<ExecBatch>>> gens = {
      main_sink_gen, dup_sink_gen, dup1_sink_gen};
  ASSERT_FINISHES_OK_AND_ASSIGN(auto exec_batches_vec, StartAndCollect(plan.get(), gens));

  ASSERT_EQ(exec_batches_vec.size(), 3);

  // ASSERT_OK_AND_ASSIGN(auto result, DeclarationToExecBatches(std::move(plan)));
  auto exp_batches = {ExecBatchFromJSON({utf8()}, "[[\"true\"],[\"false\"]]"),
                      ExecBatchFromJSON({utf8()}, "[[\"false\"],[\"false\"],[null]]")};
  AssertExecBatchesEqualIgnoringOrder(schema_2, exp_batches, exec_batches_vec[0]);
  AssertExecBatchesEqualIgnoringOrder(basic_data.schema, basic_data.batches,
                                      exec_batches_vec[1]);
  AssertExecBatchesEqualIgnoringOrder(schema_2, exp_batches, exec_batches_vec[2]);
}

TEST(ExecPlanExecution, PipeBackpressure) {
  std::optional<ExecBatch> batch =
      ExecBatchFromJSON({int32(), boolean()},
                        "[[4, false], [5, null], [6, false], [7, false], [null, true]]");
  constexpr uint32_t kPauseIfAbove = 4;
  constexpr uint32_t kResumeIfBelow = 2;
  uint32_t pause_if_above_bytes =
      kPauseIfAbove * static_cast<uint32_t>(batch->TotalBufferSize());
  uint32_t resume_if_below_bytes =
      kResumeIfBelow * static_cast<uint32_t>(batch->TotalBufferSize());
  EXPECT_OK_AND_ASSIGN(std::shared_ptr<ExecPlan> plan, ExecPlan::Make());
  PushGenerator<std::optional<ExecBatch>> batch_producer;
  BackpressureOptions backpressure_options(resume_if_below_bytes, pause_if_above_bytes);
  struct SinkDesc {
    AsyncGenerator<std::optional<ExecBatch>> sink_gen;
    BackpressureMonitor* backpressure_monitor;
  };
  SinkDesc mainSink;
  std::shared_ptr<Schema> schema_ = schema({field("data", int32())});
  ARROW_EXPECT_OK(acero::Declaration::Sequence(
                      {
                          {"source", SourceNodeOptions(schema_, batch_producer)},
                          {"pipe_tee", PipeSinkNodeOptions{"named_pipe_1"}},
                          {"sink", SinkNodeOptions{&mainSink.sink_gen, /*schema=*/nullptr,
                                                   backpressure_options,
                                                   &mainSink.backpressure_monitor}},
                      })
                      .AddToPlan(plan.get()));

  SinkDesc dupSink;
  ARROW_EXPECT_OK(acero::Declaration::Sequence(
                      {
                          {"pipe_source", PipeSourceNodeOptions{"named_pipe_1", schema_}},
                          {"sink", SinkNodeOptions{&dupSink.sink_gen, /*schema=*/nullptr,
                                                   backpressure_options,
                                                   &dupSink.backpressure_monitor}},
                      })
                      .AddToPlan(plan.get()));

  SinkDesc dup1Sink;
  ARROW_EXPECT_OK(
      acero::Declaration::Sequence(
          {
              {"pipe_source", PipeSourceNodeOptions{"named_pipe_1", schema_}},
              //{"filter", FilterNodeOptions{equal(field_ref("data"),
              // literal<std::string>("6"))}},
              {"filter", FilterNodeOptions{equal(field_ref("data"), literal(6))}},
              {"sink",
               SinkNodeOptions{&dup1Sink.sink_gen, /*schema=*/nullptr,
                               backpressure_options, &dup1Sink.backpressure_monitor}},
          })
          .AddToPlan(plan.get()));

  ASSERT_TRUE(mainSink.backpressure_monitor);
  ASSERT_TRUE(dupSink.backpressure_monitor);
  ASSERT_TRUE(dup1Sink.backpressure_monitor);
  plan->StartProducing();
  auto plan_finished = plan->finished();

  ASSERT_FALSE(mainSink.backpressure_monitor->is_paused());
  ASSERT_FALSE(dupSink.backpressure_monitor->is_paused());
  ASSERT_FALSE(dup1Sink.backpressure_monitor->is_paused());

  // Should be able to push kPauseIfAbove batches without triggering back pressure
  for (uint32_t i = 0; i < kPauseIfAbove; i++) {
    batch_producer.producer().Push(batch);
  }

  SleepABit();
  ASSERT_FALSE(mainSink.backpressure_monitor->is_paused());
  ASSERT_FALSE(dupSink.backpressure_monitor->is_paused());
  ASSERT_FALSE(dup1Sink.backpressure_monitor->is_paused());

  // One more batch should trigger back pressure
  batch_producer.producer().Push(batch);
  BusyWait(10, [&] { return mainSink.backpressure_monitor->is_paused(); });
  ASSERT_TRUE(mainSink.backpressure_monitor->is_paused());
  BusyWait(10, [&] { return dupSink.backpressure_monitor->is_paused(); });
  ASSERT_TRUE(dupSink.backpressure_monitor->is_paused());
  ASSERT_FALSE(dup1Sink.backpressure_monitor->is_paused());

  // Reading as much as we can while keeping it paused
  for (uint32_t i = kPauseIfAbove; i >= kResumeIfBelow; i--) {
    ASSERT_FINISHES_OK(mainSink.sink_gen());
    ASSERT_FINISHES_OK(dupSink.sink_gen());
  }
  SleepABit();
  ASSERT_TRUE(mainSink.backpressure_monitor->is_paused());
  ASSERT_TRUE(dupSink.backpressure_monitor->is_paused());

  // Reading one more item should open up backpressure
  ASSERT_FINISHES_OK(mainSink.sink_gen());
  BusyWait(10, [&] { return !mainSink.backpressure_monitor->is_paused(); });
  ASSERT_FALSE(mainSink.backpressure_monitor->is_paused());

  ASSERT_FINISHES_OK(dupSink.sink_gen());
  BusyWait(10, [&] { return !dupSink.backpressure_monitor->is_paused(); });
  ASSERT_FALSE(dupSink.backpressure_monitor->is_paused());

  for (uint32_t i = 0; i < kResumeIfBelow - 1; i++) {
    ASSERT_FINISHES_OK(mainSink.sink_gen());
    ASSERT_FINISHES_OK(dupSink.sink_gen());
  }
  SleepABit();
  ASSERT_EQ(mainSink.backpressure_monitor->bytes_in_use(), 0);
  ASSERT_EQ(dupSink.backpressure_monitor->bytes_in_use(), 0);

  for (uint32_t i = 0; i < kPauseIfAbove * 2; i++) {
    batch_producer.producer().Push(batch);
    ASSERT_FINISHES_OK(mainSink.sink_gen());
    ASSERT_FINISHES_OK(dupSink.sink_gen());
  }
  SleepABit();
  ASSERT_FALSE(mainSink.backpressure_monitor->is_paused());
  ASSERT_FALSE(dupSink.backpressure_monitor->is_paused());
  ASSERT_FALSE(dup1Sink.backpressure_monitor->is_paused());

  batch_producer.producer().Push(batch);
  ASSERT_FINISHES_OK(mainSink.sink_gen());
  ASSERT_FINISHES_OK(dupSink.sink_gen());
  SleepABit();

  ASSERT_FALSE(mainSink.backpressure_monitor->is_paused());
  ASSERT_FALSE(dupSink.backpressure_monitor->is_paused());
  ASSERT_TRUE(dup1Sink.backpressure_monitor->is_paused());

  // Cleanup
  batch_producer.producer().Push(IterationEnd<std::optional<ExecBatch>>());
  plan->StopProducing();
  CheckFinishesCancelledOrOk(plan->finished());
}

}  // namespace acero
}  // namespace arrow
