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

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/record_batch.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::Executor;

namespace compute {

void AssertBatchesEqual(const RecordBatchVector& expected,
                        const RecordBatchVector& actual) {
  ASSERT_EQ(expected.size(), actual.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    AssertBatchesEqual(*expected[i], *actual[i]);
  }
}

TEST(ExecPlanConstruction, Empty) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

  ASSERT_RAISES(Invalid, plan->Validate());
}

TEST(ExecPlanConstruction, SingleNode) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto node = MakeDummyNode(plan.get(), "dummy", /*num_inputs=*/0, /*num_outputs=*/0);
  ASSERT_OK(plan->Validate());
  ASSERT_THAT(plan->sources(), ::testing::ElementsAre(node));
  ASSERT_THAT(plan->sinks(), ::testing::ElementsAre(node));

  ASSERT_OK_AND_ASSIGN(plan, ExecPlan::Make());
  node = MakeDummyNode(plan.get(), "dummy", /*num_inputs=*/1, /*num_outputs=*/0);
  // Input not bound
  ASSERT_RAISES(Invalid, plan->Validate());

  ASSERT_OK_AND_ASSIGN(plan, ExecPlan::Make());
  node = MakeDummyNode(plan.get(), "dummy", /*num_inputs=*/0, /*num_outputs=*/1);
  // Output not bound
  ASSERT_RAISES(Invalid, plan->Validate());
}

TEST(ExecPlanConstruction, SourceSink) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto source = MakeDummyNode(plan.get(), "source", /*num_inputs=*/0, /*num_outputs=*/1);
  auto sink = MakeDummyNode(plan.get(), "sink", /*num_inputs=*/1, /*num_outputs=*/0);
  // Input / output not bound
  ASSERT_RAISES(Invalid, plan->Validate());

  sink->AddInput(source);
  ASSERT_OK(plan->Validate());
  EXPECT_THAT(plan->sources(), ::testing::ElementsAre(source));
  EXPECT_THAT(plan->sinks(), ::testing::ElementsAre(sink));
}

TEST(ExecPlanConstruction, MultipleNode) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

  auto source1 =
      MakeDummyNode(plan.get(), "source1", /*num_inputs=*/0, /*num_outputs=*/2);

  auto source2 =
      MakeDummyNode(plan.get(), "source2", /*num_inputs=*/0, /*num_outputs=*/1);

  auto process1 =
      MakeDummyNode(plan.get(), "process1", /*num_inputs=*/1, /*num_outputs=*/2);

  auto process2 =
      MakeDummyNode(plan.get(), "process1", /*num_inputs=*/2, /*num_outputs=*/1);

  auto process3 =
      MakeDummyNode(plan.get(), "process3", /*num_inputs=*/3, /*num_outputs=*/1);

  auto sink = MakeDummyNode(plan.get(), "sink", /*num_inputs=*/1, /*num_outputs=*/0);

  sink->AddInput(process3);

  process3->AddInput(process1);
  process3->AddInput(process2);
  process3->AddInput(process1);

  process2->AddInput(source1);
  process2->AddInput(source2);

  process1->AddInput(source1);

  ASSERT_OK(plan->Validate());
  ASSERT_THAT(plan->sources(), ::testing::ElementsAre(source1, source2));
  ASSERT_THAT(plan->sinks(), ::testing::ElementsAre(sink));
}

struct StartStopTracker {
  std::vector<std::string> started, stopped;

  StartProducingFunc start_producing_func(Status st = Status::OK()) {
    return [this, st](ExecNode* node) {
      started.push_back(node->label());
      return st;
    };
  }

  StopProducingFunc stop_producing_func() {
    return [this](ExecNode* node) { stopped.push_back(node->label()); };
  }
};

TEST(ExecPlan, DummyStartProducing) {
  StartStopTracker t;

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto source1 = MakeDummyNode(plan.get(), "source1", /*num_inputs=*/0, /*num_outputs=*/2,
                               t.start_producing_func(), t.stop_producing_func());
  auto source2 = MakeDummyNode(plan.get(), "source2", /*num_inputs=*/0, /*num_outputs=*/1,
                               t.start_producing_func(), t.stop_producing_func());
  auto process1 =
      MakeDummyNode(plan.get(), "process1", /*num_inputs=*/1, /*num_outputs=*/2,
                    t.start_producing_func(), t.stop_producing_func());
  auto process2 =
      MakeDummyNode(plan.get(), "process2", /*num_inputs=*/2, /*num_outputs=*/1,
                    t.start_producing_func(), t.stop_producing_func());
  auto process3 =
      MakeDummyNode(plan.get(), "process3", /*num_inputs=*/3, /*num_outputs=*/1,
                    t.start_producing_func(), t.stop_producing_func());

  auto sink = MakeDummyNode(plan.get(), "sink", /*num_inputs=*/1, /*num_outputs=*/0,
                            t.start_producing_func(), t.stop_producing_func());

  process1->AddInput(source1);
  process2->AddInput(process1);
  process2->AddInput(source2);
  process3->AddInput(process1);
  process3->AddInput(source1);
  process3->AddInput(process2);
  sink->AddInput(process3);

  ASSERT_OK(plan->Validate());
  ASSERT_EQ(t.started.size(), 0);
  ASSERT_EQ(t.stopped.size(), 0);

  ASSERT_OK(plan->StartProducing());
  // Note that any correct reverse topological order may do
  ASSERT_THAT(t.started, ::testing::ElementsAre("sink", "process3", "process2",
                                                "process1", "source2", "source1"));
  ASSERT_EQ(t.stopped.size(), 0);
}

TEST(ExecPlan, DummyStartProducingCycle) {
  // A trivial cycle
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto node = MakeDummyNode(plan.get(), "dummy", /*num_inputs=*/1, /*num_outputs=*/1);
  node->AddInput(node);
  ASSERT_OK(plan->Validate());
  ASSERT_RAISES(Invalid, plan->StartProducing());

  // A less trivial one
  ASSERT_OK_AND_ASSIGN(plan, ExecPlan::Make());
  auto source = MakeDummyNode(plan.get(), "source", /*num_inputs=*/0, /*num_outputs=*/1);
  auto process1 =
      MakeDummyNode(plan.get(), "process1", /*num_inputs=*/2, /*num_outputs=*/2);
  auto process2 =
      MakeDummyNode(plan.get(), "process2", /*num_inputs=*/1, /*num_outputs=*/1);
  auto process3 =
      MakeDummyNode(plan.get(), "process3", /*num_inputs=*/2, /*num_outputs=*/2);
  auto sink = MakeDummyNode(plan.get(), "sink", /*num_inputs=*/1, /*num_outputs=*/0);

  process1->AddInput(source);
  process2->AddInput(process1);
  process3->AddInput(process2);
  process3->AddInput(process1);
  process1->AddInput(process3);
  sink->AddInput(process3);

  ASSERT_OK(plan->Validate());
  ASSERT_RAISES(Invalid, plan->StartProducing());
}

TEST(ExecPlan, DummyStartProducingError) {
  StartStopTracker t;

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto source1 = MakeDummyNode(plan.get(), "source1", /*num_inputs=*/0, /*num_outputs=*/2,
                               t.start_producing_func(Status::NotImplemented("zzz")),
                               t.stop_producing_func());
  auto source2 = MakeDummyNode(plan.get(), "source2", /*num_inputs=*/0, /*num_outputs=*/1,
                               t.start_producing_func(), t.stop_producing_func());
  auto process1 = MakeDummyNode(
      plan.get(), "process1", /*num_inputs=*/1, /*num_outputs=*/2,
      t.start_producing_func(Status::IOError("xxx")), t.stop_producing_func());
  auto process2 =
      MakeDummyNode(plan.get(), "process2", /*num_inputs=*/2, /*num_outputs=*/1,
                    t.start_producing_func(), t.stop_producing_func());
  process1->AddInput(source1);
  process2->AddInput(process1);
  process2->AddInput(source2);
  auto process3 =
      MakeDummyNode(plan.get(), "process3", /*num_inputs=*/3, /*num_outputs=*/1,
                    t.start_producing_func(), t.stop_producing_func());
  process3->AddInput(process1);
  process3->AddInput(source1);
  process3->AddInput(process2);
  auto sink = MakeDummyNode(plan.get(), "sink", /*num_inputs=*/1, /*num_outputs=*/0,
                            t.start_producing_func(), t.stop_producing_func());
  sink->AddInput(process3);

  ASSERT_OK(plan->Validate());
  ASSERT_EQ(t.started.size(), 0);
  ASSERT_EQ(t.stopped.size(), 0);

  // `process1` raises IOError
  ASSERT_RAISES(IOError, plan->StartProducing());
  ASSERT_THAT(t.started,
              ::testing::ElementsAre("sink", "process3", "process2", "process1"));
  // Nodes that started successfully were stopped in reverse order
  ASSERT_THAT(t.stopped, ::testing::ElementsAre("process2", "process3", "sink"));
}

// TODO move this to gtest_util.h?

class SlowRecordBatchReader : public RecordBatchReader {
 public:
  explicit SlowRecordBatchReader(std::shared_ptr<RecordBatchReader> reader)
      : reader_(std::move(reader)) {}

  std::shared_ptr<Schema> schema() const override { return reader_->schema(); }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    SleepABit();
    return reader_->ReadNext(batch);
  }

  static Result<std::shared_ptr<RecordBatchReader>> Make(
      RecordBatchVector batches, std::shared_ptr<Schema> schema = nullptr) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          RecordBatchReader::Make(std::move(batches), std::move(schema)));
    return std::make_shared<SlowRecordBatchReader>(std::move(reader));
  }

 protected:
  std::shared_ptr<RecordBatchReader> reader_;
};

static Result<RecordBatchGenerator> MakeSlowRecordBatchGenerator(
    RecordBatchVector batches, std::shared_ptr<Schema> schema) {
  auto gen = MakeVectorGenerator(batches);
  // TODO move this into testing/async_generator_util.h?
  auto delayed_gen = MakeMappedGenerator<std::shared_ptr<RecordBatch>>(
      std::move(gen), [](const std::shared_ptr<RecordBatch>& batch) {
        auto fut = Future<std::shared_ptr<RecordBatch>>::Make();
        SleepABitAsync().AddCallback(
            [fut, batch](const Status& status) mutable { fut.MarkFinished(batch); });
        return fut;
      });
  // Adding readahead implicitly adds parallelism by pulling reentrantly from
  // the delayed generator
  return MakeReadaheadGenerator(std::move(delayed_gen), /*max_readahead=*/64);
}

class TestExecPlanExecution : public ::testing::Test {
 public:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(io_executor_, internal::ThreadPool::Make(8));
  }

  RecordBatchVector MakeRandomBatches(const std::shared_ptr<Schema>& schema,
                                      int num_batches = 10, int batch_size = 4) {
    random::RandomArrayGenerator rng(42);
    RecordBatchVector batches;
    batches.reserve(num_batches);
    for (int i = 0; i < num_batches; ++i) {
      batches.push_back(rng.BatchOf(schema->fields(), batch_size));
    }
    return batches;
  }

  struct CollectorPlan {
    std::shared_ptr<ExecPlan> plan;
    RecordBatchCollectNode* sink;
  };

  Result<CollectorPlan> MakeSourceSink(std::shared_ptr<RecordBatchReader> reader,
                                       const std::shared_ptr<Schema>& schema) {
    ARROW_ASSIGN_OR_RAISE(auto plan, ExecPlan::Make());
    auto source =
        MakeRecordBatchReaderNode(plan.get(), "source", reader, io_executor_.get());
    auto sink = MakeRecordBatchCollectNode(plan.get(), "sink", schema);
    sink->AddInput(source);
    return CollectorPlan{plan, sink};
  }

  Result<CollectorPlan> MakeSourceSink(RecordBatchGenerator generator,
                                       const std::shared_ptr<Schema>& schema) {
    ARROW_ASSIGN_OR_RAISE(auto plan, ExecPlan::Make());
    auto source = MakeRecordBatchReaderNode(plan.get(), "source", schema, generator,
                                            io_executor_.get());
    auto sink = MakeRecordBatchCollectNode(plan.get(), "sink", schema);
    sink->AddInput(source);
    return CollectorPlan{plan, sink};
  }

  Result<CollectorPlan> MakeSourceSink(const RecordBatchVector& batches,
                                       const std::shared_ptr<Schema>& schema) {
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make(batches, schema));
    return MakeSourceSink(std::move(reader), schema);
  }

  Result<RecordBatchVector> StartAndCollect(ExecPlan* plan,
                                            RecordBatchCollectNode* sink) {
    RETURN_NOT_OK(plan->StartProducing());
    auto fut = CollectAsyncGenerator(sink->generator());
    return fut.result();
  }

  template <typename RecordBatchReaderFactory>
  void TestSourceSink(RecordBatchReaderFactory reader_factory) {
    auto schema = ::arrow::schema({field("a", int32()), field("b", boolean())});
    RecordBatchVector batches{
        RecordBatchFromJSON(schema, R"([{"a": null, "b": true},
                                        {"a": 4,    "b": false}])"),
        RecordBatchFromJSON(schema, R"([{"a": 5,    "b": null},
                                        {"a": 6,    "b": false},
                                        {"a": 7,    "b": false}])"),
    };

    ASSERT_OK_AND_ASSIGN(auto reader, reader_factory(batches, schema));
    ASSERT_OK_AND_ASSIGN(auto cp, MakeSourceSink(reader, schema));
    ASSERT_OK(cp.plan->Validate());

    ASSERT_OK_AND_ASSIGN(auto got_batches, StartAndCollect(cp.plan.get(), cp.sink));
    AssertBatchesEqual(batches, got_batches);
  }

  template <typename RecordBatchReaderFactory>
  void TestStressSourceSink(int num_batches, RecordBatchReaderFactory batch_factory) {
    auto schema = ::arrow::schema({field("a", int32()), field("b", boolean())});
    auto batches = MakeRandomBatches(schema, num_batches);

    ASSERT_OK_AND_ASSIGN(auto reader, batch_factory(batches, schema));
    ASSERT_OK_AND_ASSIGN(auto cp, MakeSourceSink(reader, schema));
    ASSERT_OK(cp.plan->Validate());

    ASSERT_OK_AND_ASSIGN(auto got_batches, StartAndCollect(cp.plan.get(), cp.sink));
    AssertBatchesEqual(batches, got_batches);
  }

 protected:
  std::shared_ptr<Executor> io_executor_;
};

TEST_F(TestExecPlanExecution, SourceSink) { TestSourceSink(RecordBatchReader::Make); }

TEST_F(TestExecPlanExecution, SlowSourceSink) {
  TestSourceSink(SlowRecordBatchReader::Make);
}

TEST_F(TestExecPlanExecution, SlowSourceSinkParallel) {
  TestSourceSink(MakeSlowRecordBatchGenerator);
}

TEST_F(TestExecPlanExecution, StressSourceSink) {
  TestStressSourceSink(/*num_batches=*/200, RecordBatchReader::Make);
}

TEST_F(TestExecPlanExecution, StressSlowSourceSink) {
  // This doesn't create parallelism as the RecordBatchReader is iterated serially.
  TestStressSourceSink(/*num_batches=*/30, SlowRecordBatchReader::Make);
}

TEST_F(TestExecPlanExecution, StressSlowSourceSinkParallel) {
  TestStressSourceSink(/*num_batches=*/300, MakeSlowRecordBatchGenerator);
}

}  // namespace compute
}  // namespace arrow
