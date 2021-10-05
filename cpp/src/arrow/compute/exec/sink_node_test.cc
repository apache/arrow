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

#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/util/async_generator.h"

namespace arrow {
namespace compute {

TEST(SinkNode, Backpressure) {
  constexpr uint32_t kPauseIfAbove = 4;
  constexpr uint32_t kResumeIfBelow = 2;
  EXPECT_OK_AND_ASSIGN(std::shared_ptr<ExecPlan> plan, ExecPlan::Make());
  PushGenerator<util::optional<ExecBatch>> batch_producer;
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;
  util::BackpressureOptions backpressure_options =
      util::BackpressureOptions::Make(kResumeIfBelow, kPauseIfAbove);
  std::shared_ptr<Schema> schema_ = schema({field("data", uint32())});
  ARROW_EXPECT_OK(compute::Declaration::Sequence(
                      {
                          {"source", SourceNodeOptions(schema_, batch_producer)},
                          {"sink", SinkNodeOptions{&sink_gen, backpressure_options}},
                      })
                      .AddToPlan(plan.get()));
  ARROW_EXPECT_OK(plan->StartProducing());

  EXPECT_OK_AND_ASSIGN(util::optional<ExecBatch> batch, ExecBatch::Make({MakeScalar(0)}));
  ASSERT_TRUE(backpressure_options.toggle->IsOpen());

  // Should be able to push kPauseIfAbove batches without triggering back pressure
  for (uint32_t i = 0; i < kPauseIfAbove; i++) {
    batch_producer.producer().Push(batch);
  }
  SleepABit();
  ASSERT_TRUE(backpressure_options.toggle->IsOpen());

  // One more batch should trigger back pressure
  batch_producer.producer().Push(batch);
  BusyWait(10, [&] { return !backpressure_options.toggle->IsOpen(); });
  ASSERT_FALSE(backpressure_options.toggle->IsOpen());

  // Reading as much as we can while keeping it paused
  for (uint32_t i = kPauseIfAbove; i >= kResumeIfBelow; i--) {
    ASSERT_FINISHES_OK(sink_gen());
  }
  SleepABit();
  ASSERT_FALSE(backpressure_options.toggle->IsOpen());

  // Reading one more item should open up backpressure
  ASSERT_FINISHES_OK(sink_gen());
  BusyWait(10, [&] { return backpressure_options.toggle->IsOpen(); });
  ASSERT_TRUE(backpressure_options.toggle->IsOpen());

  // Cleanup
  batch_producer.producer().Push(IterationEnd<util::optional<ExecBatch>>());
  plan->StopProducing();
  ASSERT_FINISHES_OK(plan->finished());
}

}  // namespace compute
}  // namespace arrow
