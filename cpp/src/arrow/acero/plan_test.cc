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
#include "arrow/io/util_internal.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/vector.h"

using testing::Contains;
using testing::ElementsAre;
using testing::ElementsAreArray;
using testing::HasSubstr;
using testing::Optional;
using testing::UnorderedElementsAreArray;

namespace arrow {

using compute::call;
using compute::CountOptions;
using compute::field_ref;
using compute::ScalarAggregateOptions;
using compute::SortKey;
using compute::SortOrder;
using compute::Take;
using compute::TDigestOptions;

namespace acero {

TEST(ExecPlanConstruction, Empty) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

  ASSERT_THAT(plan->Validate(), Raises(StatusCode::Invalid));
}

TEST(ExecPlanConstruction, SingleNode) {
  // Single node that is both source and sink
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto node = MakeDummyNode(plan.get(), "dummy", /*inputs=*/{}, /*is_sink=*/true);
  ASSERT_OK(plan->Validate());
  ASSERT_THAT(plan->nodes(), ElementsAre(node));

  // Single source node that is not supposed to be a sink (invalid)
  ASSERT_OK_AND_ASSIGN(plan, ExecPlan::Make());
  node = MakeDummyNode(plan.get(), "dummy", /*inputs=*/{});
  // Output not bound
  ASSERT_THAT(plan->Validate(), Raises(StatusCode::Invalid));
}

TEST(ExecPlanConstruction, SourceSink) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto source = MakeDummyNode(plan.get(), "source", /*inputs=*/{});
  auto sink = MakeDummyNode(plan.get(), "sink", /*inputs=*/{source}, /*is_sink=*/true);

  ASSERT_OK(plan->Validate());
  EXPECT_THAT(plan->nodes(), ElementsAre(source, sink));
}

TEST(ExecPlanConstruction, AutoLabel) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto source1 = MakeDummyNode(plan.get(), "", /*inputs=*/{});
  auto source2 = MakeDummyNode(plan.get(), "some_label", /*inputs=*/{});
  auto source3 = MakeDummyNode(plan.get(), "", /*inputs=*/{});

  ASSERT_EQ("0", source1->label());
  ASSERT_EQ("some_label", source2->label());
  ASSERT_EQ("2", source3->label());
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

  auto source1 = MakeDummyNode(plan.get(), "source1", /*inputs=*/{}, /*is_sink=*/false,
                               t.start_producing_func(), t.stop_producing_func());

  auto source2 = MakeDummyNode(plan.get(), "source2", /*inputs=*/{}, /*is_sink=*/false,
                               t.start_producing_func(), t.stop_producing_func());

  auto process1 =
      MakeDummyNode(plan.get(), "process1", /*inputs=*/{source1}, /*is_sink=*/false,
                    t.start_producing_func(), t.stop_producing_func());

  auto process2 =
      MakeDummyNode(plan.get(), "process2", /*inputs=*/{process1, source2},
                    /*is_sink=*/false, t.start_producing_func(), t.stop_producing_func());

  auto process3 =
      MakeDummyNode(plan.get(), "process3", /*inputs=*/{process2},
                    /*is_sink=*/false, t.start_producing_func(), t.stop_producing_func());

  MakeDummyNode(plan.get(), "sink", /*inputs=*/{process3}, /*is_sink=*/true,
                t.start_producing_func(), t.stop_producing_func());

  ASSERT_OK(plan->Validate());
  ASSERT_EQ(t.started.size(), 0);
  ASSERT_EQ(t.stopped.size(), 0);

  plan->StartProducing();
  // Note that any correct reverse topological order may do
  ASSERT_THAT(t.started, ElementsAre("sink", "process3", "process2", "process1",
                                     "source2", "source1"));

  plan->StopProducing();
  ASSERT_THAT(plan->finished(), Finishes(Ok()));
  // Note that any correct topological order may do
  ASSERT_THAT(t.stopped, ElementsAre("source1", "source2", "process1", "process2",
                                     "process3", "sink"));

  plan->StartProducing();
  ASSERT_THAT(plan->finished(), Finishes(Raises(StatusCode::Invalid,
                                                HasSubstr("plan had already finished"))));
}

TEST(ExecPlan, DummyStartProducingError) {
  StartStopTracker t;

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto source1 = MakeDummyNode(plan.get(), "source1", /*inputs=*/{}, /*is_sink=*/false,
                               t.start_producing_func(Status::NotImplemented("zzz")),
                               t.stop_producing_func());

  auto source2 = MakeDummyNode(plan.get(), "source2", /*inputs=*/{}, /*is_sink=*/false,
                               t.start_producing_func(), t.stop_producing_func());

  auto process1 = MakeDummyNode(
      plan.get(), "process1", /*inputs=*/{source1}, /*is_sink=*/false,
      t.start_producing_func(Status::IOError("xxx")), t.stop_producing_func());

  auto process2 =
      MakeDummyNode(plan.get(), "process2", /*inputs=*/{process1, source2},
                    /*is_sink=*/false, t.start_producing_func(), t.stop_producing_func());

  auto process3 =
      MakeDummyNode(plan.get(), "process3", /*inputs=*/{process2},
                    /*is_sink=*/false, t.start_producing_func(), t.stop_producing_func());

  MakeDummyNode(plan.get(), "sink", /*inputs=*/{process3}, /*is_sink=*/true,
                t.start_producing_func(), t.stop_producing_func());

  ASSERT_OK(plan->Validate());
  ASSERT_EQ(t.started.size(), 0);
  ASSERT_EQ(t.stopped.size(), 0);

  // `process1` raises IOError
  plan->StartProducing();
  ASSERT_THAT(plan->finished(), Finishes(Raises(StatusCode::IOError)));
  ASSERT_THAT(t.started, ElementsAre("sink", "process3", "process2", "process1"));
  // All nodes will be stopped when an abort happens
  ASSERT_THAT(t.stopped, ElementsAre("process2", "process1", "source1", "source2",
                                     "process3", "sink"));
}

TEST(ExecPlanExecution, SourceSink) {
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");

      auto basic_data = MakeBasicBatches();

      Declaration plan(
          "source", SourceNodeOptions{basic_data.schema, basic_data.gen(parallel, slow)});
      ASSERT_OK_AND_ASSIGN(auto result,
                           DeclarationToExecBatches(std::move(plan), parallel));
      AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches,
                                          basic_data.batches);
    }
  }
}

TEST(ExecPlanExecution, UseSinkAfterExecution) {
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;
  {
    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
    auto basic_data = MakeBasicBatches();
    ASSERT_OK(Declaration::Sequence(
                  {
                      {"source", SourceNodeOptions{basic_data.schema,
                                                   basic_data.gen(/*parallel=*/false,
                                                                  /*slow=*/false)}},
                      {"sink", SinkNodeOptions{&sink_gen}},
                  })
                  .AddToPlan(plan.get()));
    plan->StartProducing();
    ASSERT_FINISHES_OK(plan->finished());
  }
  ASSERT_FINISHES_AND_RAISES(Invalid, sink_gen());
}

TEST(ExecPlanExecution, TableSourceSink) {
  for (int batch_size : {1, 4}) {
    auto exp_batches = MakeBasicBatches();
    ASSERT_OK_AND_ASSIGN(auto table,
                         TableFromExecBatches(exp_batches.schema, exp_batches.batches));
    Declaration plan("table_source", TableSourceNodeOptions{table, batch_size});

    ASSERT_OK_AND_ASSIGN(auto result_table,
                         DeclarationToTable(std::move(plan), /*use_threads=*/false));
    AssertTablesEqualIgnoringOrder(table, result_table);
  }
}

TEST(ExecPlanExecution, TableSourceSinkError) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;

  auto exp_batches = MakeBasicBatches();
  ASSERT_OK_AND_ASSIGN(auto table,
                       TableFromExecBatches(exp_batches.schema, exp_batches.batches));

  auto null_table_options = TableSourceNodeOptions{NULLPTR, 1};
  ASSERT_THAT(MakeExecNode("table_source", plan.get(), {}, null_table_options),
              Raises(StatusCode::Invalid, HasSubstr("not null")));

  auto negative_batch_size_options = TableSourceNodeOptions{table, -1};
  ASSERT_THAT(MakeExecNode("table_source", plan.get(), {}, negative_batch_size_options),
              Raises(StatusCode::Invalid, HasSubstr("batch_size > 0")));
}

template <typename ElementType, typename OptionsType>
void TestSourceSinkError(
    std::string source_factory_name,
    std::function<Result<std::vector<ElementType>>(const BatchesWithSchema&)>
        to_elements) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  std::shared_ptr<Schema> no_schema;

  auto exp_batches = MakeBasicBatches();
  ASSERT_OK_AND_ASSIGN(auto elements, to_elements(exp_batches));
  auto element_it_maker = [&elements]() {
    return MakeVectorIterator<ElementType>(elements);
  };

  auto null_executor_options = OptionsType{exp_batches.schema, element_it_maker};
  ASSERT_OK(MakeExecNode(source_factory_name, plan.get(), {}, null_executor_options));

  auto null_schema_options = OptionsType{no_schema, element_it_maker};
  ASSERT_THAT(MakeExecNode(source_factory_name, plan.get(), {}, null_schema_options),
              Raises(StatusCode::Invalid, HasSubstr("not null")));

  auto no_io_but_executor = OptionsType{exp_batches.schema, element_it_maker,
                                        io::default_io_context().executor()};
  no_io_but_executor.requires_io = false;
  ASSERT_THAT(MakeExecNode(source_factory_name, plan.get(), {}, no_io_but_executor),
              Raises(StatusCode::Invalid, HasSubstr("io_executor was not nullptr")));
}

template <typename ElementType, typename OptionsType>
void TestSourceSink(
    std::string source_factory_name,
    std::function<Result<std::vector<ElementType>>(const BatchesWithSchema&)>
        to_elements) {
  auto exp_batches = MakeBasicBatches();
  ASSERT_OK_AND_ASSIGN(auto elements, to_elements(exp_batches));
  auto element_it_maker = [&elements]() {
    return MakeVectorIterator<ElementType>(elements);
  };
  for (bool requires_io : {false, true}) {
    for (bool use_threads : {false, true}) {
      Declaration plan(source_factory_name,
                       OptionsType{exp_batches.schema, element_it_maker, requires_io});
      QueryOptions query_options;
      query_options.use_threads = use_threads;
      ASSERT_OK_AND_ASSIGN(auto result,
                           DeclarationToExecBatches(std::move(plan), query_options));
      // Should not need to ignore order since sink should sequence by implicit order
      AssertExecBatchesEqual(result.schema, result.batches, exp_batches.batches);
      AssertExecBatchesSequenced(result.batches);
    }
  }
}

void TestRecordBatchReaderSourceSink(
    std::function<Result<std::shared_ptr<RecordBatchReader>>(const BatchesWithSchema&)>
        to_reader) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");
    auto exp_batches = MakeBasicBatches();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RecordBatchReader> reader,
                         to_reader(exp_batches));
    RecordBatchReaderSourceNodeOptions options{reader};
    Declaration plan("record_batch_reader_source", std::move(options));
    ASSERT_OK_AND_ASSIGN(auto result, DeclarationToExecBatches(plan, parallel));
    AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches,
                                        exp_batches.batches);
  }
}

void TestRecordBatchReaderSourceSinkError(
    std::function<Result<std::shared_ptr<RecordBatchReader>>(const BatchesWithSchema&)>
        to_reader) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto source_factory_name = "record_batch_reader_source";
  auto exp_batches = MakeBasicBatches();
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<RecordBatchReader> reader, to_reader(exp_batches));

  auto null_executor_options = RecordBatchReaderSourceNodeOptions{reader};
  ASSERT_OK(MakeExecNode(source_factory_name, plan.get(), {}, null_executor_options));

  std::shared_ptr<RecordBatchReader> no_reader;
  auto null_reader_options = RecordBatchReaderSourceNodeOptions{no_reader};
  ASSERT_THAT(MakeExecNode(source_factory_name, plan.get(), {}, null_reader_options),
              Raises(StatusCode::Invalid, HasSubstr("not null")));
}

TEST(ExecPlanExecution, ArrayVectorSourceSink) {
  TestSourceSink<std::shared_ptr<ArrayVector>, ArrayVectorSourceNodeOptions>(
      "array_vector_source", ToArrayVectors);
}

TEST(ExecPlanExecution, ArrayVectorSourceSinkError) {
  TestSourceSinkError<std::shared_ptr<ArrayVector>, ArrayVectorSourceNodeOptions>(
      "array_vector_source", ToArrayVectors);
}

TEST(ExecPlanExecution, ExecBatchSourceSink) {
  TestSourceSink<std::shared_ptr<ExecBatch>, ExecBatchSourceNodeOptions>(
      "exec_batch_source", ToExecBatches);
}

TEST(ExecPlanExecution, ExecBatchSourceSinkError) {
  TestSourceSinkError<std::shared_ptr<ExecBatch>, ExecBatchSourceNodeOptions>(
      "exec_batch_source", ToExecBatches);
}

TEST(ExecPlanExecution, RecordBatchSourceSink) {
  TestSourceSink<std::shared_ptr<RecordBatch>, RecordBatchSourceNodeOptions>(
      "record_batch_source", ToRecordBatches);
}

TEST(ExecPlanExecution, RecordBatchSourceSinkError) {
  TestSourceSinkError<std::shared_ptr<RecordBatch>, RecordBatchSourceNodeOptions>(
      "record_batch_source", ToRecordBatches);
}

TEST(ExecPlanExecution, RecordBatchReaderSourceSink) {
  TestRecordBatchReaderSourceSink(ToRecordBatchReader);
}

TEST(ExecPlanExecution, RecordBatchReaderSourceSinkError) {
  TestRecordBatchReaderSourceSinkError(ToRecordBatchReader);
}

void CheckFinishesCancelledOrOk(const Future<>& fut) {
  // There is a race condition with most tests that cancel plans.  If the
  // cancel call comes in too slowly then the plan might have already finished
  // ok.
  ASSERT_TRUE(fut.Wait(kDefaultAssertFinishesWaitSeconds));
  if (!fut.status().ok()) {
    ASSERT_TRUE(fut.status().IsCancelled());
  }
}

TEST(ExecPlanExecution, SinkNodeBackpressure) {
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
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;
  BackpressureMonitor* backpressure_monitor;
  BackpressureOptions backpressure_options(resume_if_below_bytes, pause_if_above_bytes);
  std::shared_ptr<Schema> schema_ = schema({field("data", uint32())});
  ARROW_EXPECT_OK(
      acero::Declaration::Sequence(
          {
              {"source", SourceNodeOptions(schema_, batch_producer)},
              {"sink", SinkNodeOptions{&sink_gen, /*schema=*/nullptr,
                                       backpressure_options, &backpressure_monitor}},
          })
          .AddToPlan(plan.get()));
  ASSERT_TRUE(backpressure_monitor);
  plan->StartProducing();

  ASSERT_FALSE(backpressure_monitor->is_paused());

  // Should be able to push kPauseIfAbove batches without triggering back pressure
  for (uint32_t i = 0; i < kPauseIfAbove; i++) {
    batch_producer.producer().Push(batch);
  }
  SleepABit();
  ASSERT_FALSE(backpressure_monitor->is_paused());

  // One more batch should trigger back pressure
  batch_producer.producer().Push(batch);
  BusyWait(10, [&] { return backpressure_monitor->is_paused(); });
  ASSERT_TRUE(backpressure_monitor->is_paused());

  // Reading as much as we can while keeping it paused
  for (uint32_t i = kPauseIfAbove; i >= kResumeIfBelow; i--) {
    ASSERT_FINISHES_OK(sink_gen());
  }
  SleepABit();
  ASSERT_TRUE(backpressure_monitor->is_paused());

  // Reading one more item should open up backpressure
  ASSERT_FINISHES_OK(sink_gen());
  BusyWait(10, [&] { return !backpressure_monitor->is_paused(); });
  ASSERT_FALSE(backpressure_monitor->is_paused());

  // Cleanup
  batch_producer.producer().Push(IterationEnd<std::optional<ExecBatch>>());
  plan->StopProducing();
  CheckFinishesCancelledOrOk(plan->finished());
}

TEST(ExecPlan, ToString) {
  auto basic_data = MakeBasicBatches();
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;

  // Cannot test the following mini-plans with DeclarationToString since validation
  // would fail (no sink)
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  ASSERT_OK(Declaration::Sequence(
                {
                    {"source", SourceNodeOptions{basic_data.schema,
                                                 basic_data.gen(/*parallel=*/false,
                                                                /*slow=*/false)}},
                    {"sink", SinkNodeOptions{&sink_gen}},
                })
                .AddToPlan(plan.get()));
  EXPECT_EQ(plan->nodes()[0]->ToString(), R"(:SourceNode{})");
  EXPECT_EQ(plan->nodes()[1]->ToString(), R"(:SinkNode{})");
  EXPECT_EQ(plan->ToString(), R"(ExecPlan with 2 nodes:
:SinkNode{}
  :SourceNode{}
)");

  std::shared_ptr<CountOptions> options =
      std::make_shared<CountOptions>(CountOptions::ONLY_VALID);
  Declaration declaration = Declaration::Sequence({
      {"source",
       SourceNodeOptions{basic_data.schema,
                         basic_data.gen(/*parallel=*/false, /*slow=*/false)},
       "custom_source_label"},
      {"filter", FilterNodeOptions{greater_equal(field_ref("i32"), literal(0))}},
      {"project", ProjectNodeOptions{{
                      field_ref("bool"),
                      call("multiply", {field_ref("i32"), literal(2)}),
                  }}},
      {"aggregate",
       AggregateNodeOptions{
           /*aggregates=*/{
               {"hash_sum", nullptr, "multiply(i32, 2)", "sum(multiply(i32, 2))"},
               {"hash_count", options, "multiply(i32, 2)", "count(multiply(i32, 2))"},
               {"hash_count_all", "count(*)"},
           },
           /*keys=*/{"bool"}}},
      {"filter",
       FilterNodeOptions{greater(field_ref("sum(multiply(i32, 2))"), literal(10))}},
      {"order_by_sink",
       OrderBySinkNodeOptions{
           SortOptions({SortKey{"sum(multiply(i32, 2))", SortOrder::Ascending}}),
           &sink_gen},
       "custom_sink_label"},
  });
  ASSERT_OK_AND_ASSIGN(std::string plan_str, DeclarationToString(declaration));
  EXPECT_EQ(plan_str, R"a(ExecPlan with 6 nodes:
custom_sink_label:OrderBySinkNode{by={sort_keys=[FieldRef.Name(sum(multiply(i32, 2))) ASC], null_placement=AtEnd}}
  :FilterNode{filter=(sum(multiply(i32, 2)) > 10)}
    :GroupByNode{keys=["bool"], aggregates=[
    	hash_sum(multiply(i32, 2)),
    	hash_count(multiply(i32, 2), {mode=NON_NULL}),
    	hash_count_all(*),
    ]}
      :ProjectNode{projection=[bool, multiply(i32, 2)]}
        :FilterNode{filter=(i32 >= 0)}
          custom_source_label:SourceNode{}
)a");

  Declaration union_node{"union", ExecNodeOptions{}};
  Declaration lhs{"source",
                  SourceNodeOptions{basic_data.schema,
                                    basic_data.gen(/*parallel=*/false, /*slow=*/false)}};
  lhs.label = "lhs";
  Declaration rhs{"source",
                  SourceNodeOptions{basic_data.schema,
                                    basic_data.gen(/*parallel=*/false, /*slow=*/false)}};
  rhs.label = "rhs";
  union_node.inputs.emplace_back(lhs);
  union_node.inputs.emplace_back(rhs);
  declaration = Declaration::Sequence({
      union_node,
      {"aggregate", AggregateNodeOptions{/*aggregates=*/{
                                             {"count", options, "i32", "count(i32)"},
                                             {"count_all", "count(*)"},
                                         },
                                         /*keys=*/{}}},
      {"sink", SinkNodeOptions{&sink_gen}},
  });
  ASSERT_OK_AND_ASSIGN(plan_str, DeclarationToString(declaration));
  EXPECT_EQ(plan_str, R"a(ExecPlan with 5 nodes:
:SinkNode{}
  :ScalarAggregateNode{aggregates=[
	count(i32, {mode=NON_NULL}),
	count_all(*),
]}
    :UnionNode{}
      rhs:SourceNode{}
      lhs:SourceNode{}
)a");
}

TEST(ExecPlanExecution, CustomFieldNames) {
  auto generator = gen::Gen({{"x", gen::Step()}})->FailOnError();
  std::vector<::arrow::compute::ExecBatch> ebatches =
      generator->ExecBatches(/*rows_per_batch=*/1, /*num_batches=*/1);
  Declaration source =
      Declaration("exec_batch_source", ::arrow::acero::ExecBatchSourceNodeOptions(
                                           generator->Schema(), std::move(ebatches)));
  QueryOptions opts;
  opts.field_names = {"y"};

  ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<RecordBatch>> batches,
                       DeclarationToBatches(source, opts));

  std::shared_ptr<Schema> expected_schema = schema({field("y", uint32())});

  for (const auto& batch : batches) {
    AssertSchemaEqual(*expected_schema, *batch->schema());
  }

  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema batches_with_schema,
                       DeclarationToExecBatches(source, opts));

  AssertSchemaEqual(*expected_schema, *batches_with_schema.schema);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, DeclarationToTable(source, opts));
  AssertSchemaEqual(*expected_schema, *table->schema());

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatchReader> reader,
                       DeclarationToReader(source, opts));
  AssertSchemaEqual(*expected_schema, *reader->schema());
}

TEST(ExecPlanExecution, SourceOrderBy) {
  std::vector<ExecBatch> expected = {
      ExecBatchFromJSON({int32(), boolean()},
                        "[[4, false], [5, null], [6, false], [7, false], [null, true]]")};
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");

      ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
      AsyncGenerator<std::optional<ExecBatch>> sink_gen;

      auto basic_data = MakeBasicBatches();

      SortOptions options({SortKey("i32", SortOrder::Ascending)});
      ASSERT_OK(Declaration::Sequence(
                    {
                        {"source", SourceNodeOptions{basic_data.schema,
                                                     basic_data.gen(parallel, slow)}},
                        {"order_by_sink", OrderBySinkNodeOptions{options, &sink_gen}},
                    })
                    .AddToPlan(plan.get()));

      ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
                  Finishes(ResultWith(ElementsAreArray(expected))));
    }
  }
}

TEST(ExecPlanExecution, SourceSinkError) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;

  auto basic_data = MakeBasicBatches();
  auto it = basic_data.batches.begin();
  AsyncGenerator<std::optional<ExecBatch>> error_source_gen =
      [&]() -> Result<std::optional<ExecBatch>> {
    if (it == basic_data.batches.end()) {
      return Status::Invalid("Artificial error");
    }
    return std::make_optional(*it++);
  };

  ASSERT_OK(Declaration::Sequence(
                {
                    {"source", SourceNodeOptions{basic_data.schema, error_source_gen}},
                    {"sink", SinkNodeOptions{&sink_gen}},
                })
                .AddToPlan(plan.get()));

  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(Raises(StatusCode::Invalid, HasSubstr("Artificial"))));
}

TEST(ExecPlanExecution, InvalidSequencing) {
  auto basic_data = MakeBasicBatches();
  Declaration plan = Declaration::Sequence(
      {{"source", SourceNodeOptions{basic_data.schema, basic_data.gen(false, false)}},
       {"aggregate", AggregateNodeOptions({}, {"i32"})}});

  QueryOptions query_options;
  ASSERT_OK(DeclarationToStatus(plan, query_options));
  query_options.sequence_output = true;
  ASSERT_THAT(DeclarationToStatus(plan, query_options),
              Raises(StatusCode::Invalid, HasSubstr("no meaningful ordering")));
}

TEST(ExecPlanExecution, SourceConsumingSink) {
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");
      ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
      std::atomic<uint32_t> batches_seen{0};
      Future<> finish = Future<>::Make();
      struct TestConsumer : public SinkNodeConsumer {
        TestConsumer(std::atomic<uint32_t>* batches_seen, Future<> finish)
            : batches_seen(batches_seen), finish(std::move(finish)) {}

        Status Init(const std::shared_ptr<Schema>& schema,
                    BackpressureControl* backpressure_control, ExecPlan* plan) override {
          return Status::OK();
        }

        Status Consume(ExecBatch batch) override {
          (*batches_seen)++;
          return Status::OK();
        }

        Future<> Finish() override { return finish; }

        std::atomic<uint32_t>* batches_seen;
        Future<> finish;
      };
      std::shared_ptr<TestConsumer> consumer =
          std::make_shared<TestConsumer>(&batches_seen, finish);

      auto basic_data = MakeBasicBatches();
      ASSERT_OK_AND_ASSIGN(
          auto source, MakeExecNode("source", plan.get(), {},
                                    SourceNodeOptions(basic_data.schema,
                                                      basic_data.gen(parallel, slow))));
      ASSERT_OK(MakeExecNode("consuming_sink", plan.get(), {source},
                             ConsumingSinkNodeOptions(consumer)));
      plan->StartProducing();
      SleepABit();
      // Consumer isn't finished and so plan shouldn't have finished
      AssertNotFinished(plan->finished());
      // Mark consumption complete, plan should finish
      finish.MarkFinished();
      ASSERT_FINISHES_OK(plan->finished());
      ASSERT_EQ(2, batches_seen);
    }
  }
}

TEST(ExecPlanExecution, SourceTableConsumingSink) {
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");
      ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

      std::shared_ptr<Table> out = nullptr;

      auto basic_data = MakeBasicBatches();

      TableSinkNodeOptions options{&out};

      ASSERT_OK_AND_ASSIGN(
          auto source, MakeExecNode("source", plan.get(), {},
                                    SourceNodeOptions(basic_data.schema,
                                                      basic_data.gen(parallel, slow))));
      ASSERT_OK(MakeExecNode("table_sink", plan.get(), {source}, options));
      plan->StartProducing();
      SleepABit();
      ASSERT_OK_AND_ASSIGN(auto expected,
                           TableFromExecBatches(basic_data.schema, basic_data.batches));
      ASSERT_FINISHES_OK(plan->finished());
      ASSERT_EQ(5, out->num_rows());
      AssertTablesEqualIgnoringOrder(expected, out);
    }
  }
}

TEST(ExecPlanExecution, DeclarationToSchema) {
  auto basic_data = MakeBasicBatches();
  auto plan = Declaration::Sequence(
      {{"source", SourceNodeOptions(basic_data.schema, basic_data.gen(false, false))},
       {"aggregate", AggregateNodeOptions({{"hash_sum", "i32", "int32_sum"}}, {"bool"})},
       {"project",
        ProjectNodeOptions({field_ref("int32_sum"),
                            call("multiply", {field_ref("int32_sum"), literal(2)})})}});
  auto expected_out_schema =
      schema({field("int32_sum", int64()), field("multiply(int32_sum, 2)", int64())});
  ASSERT_OK_AND_ASSIGN(auto actual_out_schema, DeclarationToSchema(std::move(plan)));
  AssertSchemaEqual(expected_out_schema, actual_out_schema);
}

TEST(ExecPlanExecution, DeclarationToReader) {
  auto basic_data = MakeBasicBatches();
  auto plan = Declaration::Sequence(
      {{"source", SourceNodeOptions(basic_data.schema, basic_data.gen(false, false))}});
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatchReader> reader,
                       DeclarationToReader(plan));

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> out, reader->ToTable());
  ASSERT_EQ(5, out->num_rows());
  ASSERT_OK(reader->Close());
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("already closed reader"),
                                  reader->Next());
}

TEST(ExecPlanExecution, ConsumingSinkNames) {
  struct SchemaKeepingConsumer : public SinkNodeConsumer {
    std::shared_ptr<Schema> schema_;
    Status Init(const std::shared_ptr<Schema>& schema,
                BackpressureControl* backpressure_control, ExecPlan* plan) override {
      schema_ = schema;
      return Status::OK();
    }
    Status Consume(ExecBatch batch) override { return Status::OK(); }
    Future<> Finish() override { return Future<>::MakeFinished(); }
  };
  std::vector<std::vector<std::string>> names_data = {{}, {"a", "b"}, {"a", "b", "c"}};
  for (const auto& names : names_data) {
    auto consumer = std::make_shared<SchemaKeepingConsumer>();
    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
    auto basic_data = MakeBasicBatches();
    ASSERT_OK(Declaration::Sequence(
                  {{"source",
                    SourceNodeOptions(basic_data.schema, basic_data.gen(false, false))},
                   {"consuming_sink", ConsumingSinkNodeOptions(consumer, names)}})
                  .AddToPlan(plan.get()));
    ASSERT_OK_AND_ASSIGN(
        auto source,
        MakeExecNode("source", plan.get(), {},
                     SourceNodeOptions(basic_data.schema, basic_data.gen(false, false))));
    ASSERT_OK(MakeExecNode("consuming_sink", plan.get(), {source},
                           ConsumingSinkNodeOptions(consumer, names)));
    if (names.size() != 0 &&
        names.size() != static_cast<size_t>(basic_data.batches[0].num_values())) {
      plan->StartProducing();
      ASSERT_FINISHES_AND_RAISES(Invalid, plan->finished());
    } else {
      auto expected_names = names.size() == 0 ? basic_data.schema->field_names() : names;
      plan->StartProducing();
      ASSERT_FINISHES_OK(plan->finished());
      ASSERT_EQ(expected_names, consumer->schema_->field_names());
    }
  }
}

TEST(ExecPlanExecution, ConsumingSinkError) {
  struct InitErrorConsumer : public SinkNodeConsumer {
    Status Init(const std::shared_ptr<Schema>& schema,
                BackpressureControl* backpressure_control, ExecPlan* plan) override {
      return Status::Invalid("XYZ");
    }
    Status Consume(ExecBatch batch) override { return Status::OK(); }
    Future<> Finish() override { return Future<>::MakeFinished(); }
  };
  struct ConsumeErrorConsumer : public SinkNodeConsumer {
    Status Init(const std::shared_ptr<Schema>& schema,
                BackpressureControl* backpressure_control, ExecPlan* plan) override {
      return Status::OK();
    }
    Status Consume(ExecBatch batch) override { return Status::Invalid("XYZ"); }
    Future<> Finish() override { return Future<>::MakeFinished(); }
  };
  struct FinishErrorConsumer : public SinkNodeConsumer {
    Status Init(const std::shared_ptr<Schema>& schema,
                BackpressureControl* backpressure_control, ExecPlan* plan) override {
      return Status::OK();
    }
    Status Consume(ExecBatch batch) override { return Status::OK(); }
    Future<> Finish() override { return Future<>::MakeFinished(Status::Invalid("XYZ")); }
  };
  std::vector<std::shared_ptr<SinkNodeConsumer>> consumers{
      std::make_shared<InitErrorConsumer>(), std::make_shared<ConsumeErrorConsumer>(),
      std::make_shared<FinishErrorConsumer>()};

  for (auto& consumer : consumers) {
    auto basic_data = MakeBasicBatches();
    Declaration plan = Declaration::Sequence(
        {{"source", SourceNodeOptions(basic_data.schema, basic_data.gen(false, false))},
         {"consuming_sink", ConsumingSinkNodeOptions(consumer)}});
    // Since the source node is not parallel the entire plan is run during
    // StartProducing
    ASSERT_RAISES(Invalid, DeclarationToStatus(std::move(plan)));
  }
}

TEST(ExecPlanExecution, StressSourceSink) {
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");

      int num_batches = (slow && !parallel) ? 30 : 300;

      auto random_data = MakeRandomBatches(
          schema({field("a", int32()), field("b", boolean())}), num_batches);
      Declaration plan("source", SourceNodeOptions{random_data.schema,
                                                   random_data.gen(parallel, slow)});
      ASSERT_OK_AND_ASSIGN(auto result,
                           DeclarationToExecBatches(std::move(plan), parallel));
      AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches,
                                          random_data.batches);
    }
  }
}

TEST(ExecPlanExecution, StressSourceOrderBy) {
  auto input_schema = schema({field("a", int32()), field("b", boolean())});
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");

      int num_batches = (slow && !parallel) ? 30 : 300;

      ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
      AsyncGenerator<std::optional<ExecBatch>> sink_gen;

      auto random_data = MakeRandomBatches(input_schema, num_batches);

      SortOptions options({SortKey("a", SortOrder::Ascending)});
      ASSERT_OK(Declaration::Sequence(
                    {
                        {"source", SourceNodeOptions{random_data.schema,
                                                     random_data.gen(parallel, slow)}},
                        {"order_by_sink", OrderBySinkNodeOptions{options, &sink_gen}},
                    })
                    .AddToPlan(plan.get()));

      // Check that data is sorted appropriately
      ASSERT_FINISHES_OK_AND_ASSIGN(auto exec_batches,
                                    StartAndCollect(plan.get(), sink_gen));
      ASSERT_OK_AND_ASSIGN(auto actual, TableFromExecBatches(input_schema, exec_batches));
      ASSERT_OK_AND_ASSIGN(auto original,
                           TableFromExecBatches(input_schema, random_data.batches));
      ASSERT_OK_AND_ASSIGN(auto sort_indices, SortIndices(original, options));
      ASSERT_OK_AND_ASSIGN(auto expected, Take(original, sort_indices));
      AssertSchemaEqual(actual->schema(), expected.table()->schema());
      AssertArraysEqual(*actual->column(0)->chunk(0),
                        *expected.table()->column(0)->chunk(0));
    }
  }
}

TEST(ExecPlanExecution, StressSourceGroupedSumStop) {
  auto input_schema = schema({field("a", int32()), field("b", boolean())});
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");

      int num_batches = (slow && !parallel) ? 30 : 300;

      ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
      AsyncGenerator<std::optional<ExecBatch>> sink_gen;

      auto random_data = MakeRandomBatches(input_schema, num_batches);

      SortOptions options({SortKey("a", SortOrder::Ascending)});
      ASSERT_OK(
          Declaration::Sequence(
              {
                  {"source", SourceNodeOptions{random_data.schema,
                                               random_data.gen(parallel, slow)}},
                  {"aggregate", AggregateNodeOptions{
                                    /*aggregates=*/{{"hash_sum", nullptr, "a", "sum(a)"}},
                                    /*keys=*/{"b"}}},
                  {"sink", SinkNodeOptions{&sink_gen}},
              })
              .AddToPlan(plan.get()));

      ASSERT_OK(plan->Validate());
      plan->StartProducing();
      plan->StopProducing();
      CheckFinishesCancelledOrOk(plan->finished());
    }
  }
}

TEST(ExecPlanExecution, StressSourceSinkStopped) {
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");

      int num_batches = (slow && !parallel) ? 30 : 300;

      AsyncGenerator<std::optional<ExecBatch>> sink_gen;

      auto random_data = MakeRandomBatches(
          schema({field("a", int32()), field("b", boolean())}), num_batches);

      Declaration decl = Declaration::Sequence({
          {"source",
           SourceNodeOptions{random_data.schema, random_data.gen(parallel, slow)}},
          {"sink", SinkNodeOptions{&sink_gen}},
      });

      std::shared_ptr<ExecPlan> plan;
      Future<std::optional<ExecBatch>> first_batch_fut =
          ::arrow::internal::RunSynchronously<Future<std::optional<ExecBatch>>>(
              [&](::arrow::internal::Executor* executor)
                  -> Future<std::optional<ExecBatch>> {
                ExecContext ctx = ExecContext(default_memory_pool(), executor);
                ARROW_ASSIGN_OR_RAISE(plan, ExecPlan::Make(ctx));
                ARROW_RETURN_NOT_OK(decl.AddToPlan(plan.get()));
                ARROW_RETURN_NOT_OK(plan->Validate());
                plan->StartProducing();
                return sink_gen();
              },
              parallel);

      if (parallel) {
        ASSERT_FINISHES_OK_AND_ASSIGN(std::optional<ExecBatch> batch, first_batch_fut);
        ASSERT_TRUE(batch.has_value());
        ASSERT_THAT(random_data.batches, Contains(*batch));
      } else {
        EXPECT_THAT(first_batch_fut,
                    Finishes(ResultWith(Optional(random_data.batches[0]))));
      }

      plan->StopProducing();
      Future<> finished = plan->finished();
      CheckFinishesCancelledOrOk(plan->finished());
    }
  }
}

TEST(ExecPlanExecution, SourceFilterSink) {
  auto basic_data = MakeBasicBatches();
  Declaration plan = Declaration::Sequence(
      {{"source", SourceNodeOptions{basic_data.schema, basic_data.gen(/*parallel=*/false,
                                                                      /*slow=*/false)}},
       {"filter", FilterNodeOptions{equal(field_ref("i32"), literal(6))}}});
  ASSERT_OK_AND_ASSIGN(auto result, DeclarationToExecBatches(std::move(plan)));
  auto exp_batches = {ExecBatchFromJSON({int32(), boolean()}, "[]"),
                      ExecBatchFromJSON({int32(), boolean()}, "[[6, false]]")};
  AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches, exp_batches);
}

TEST(ExecPlanExecution, SourceProjectSink) {
  auto basic_data = MakeBasicBatches();
  Declaration plan = Declaration::Sequence(
      {{"source", SourceNodeOptions{basic_data.schema, basic_data.gen(/*parallel=*/false,
                                                                      /*slow=*/false)}},
       {"project", ProjectNodeOptions{{
                                          not_(field_ref("bool")),
                                          call("add", {field_ref("i32"), literal(1)}),
                                      },
                                      {"!bool", "i32 + 1"}}}});

  auto exp_batches = {
      ExecBatchFromJSON({boolean(), int32()}, "[[false, null], [true, 5]]"),
      ExecBatchFromJSON({boolean(), int32()}, "[[null, 6], [true, 7], [true, 8]]")};
  ASSERT_OK_AND_ASSIGN(auto result, DeclarationToExecBatches(std::move(plan)));
  AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches, exp_batches);
}

TEST(ExecPlanExecution, ProjectMaintainsOrder) {
  RegisterTestNodes();
  constexpr int kRandomSeed = 42;
  constexpr int64_t kRowsPerBatch = 1;
  constexpr int kNumBatches = 16;

  auto generator = gen::Gen({{"x", gen::Step()}})->FailOnError();
  std::vector<::arrow::compute::ExecBatch> ebatches =
      generator->ExecBatches(kRowsPerBatch, kNumBatches);
  Declaration source_node =
      Declaration("exec_batch_source", ::arrow::acero::ExecBatchSourceNodeOptions(
                                           generator->Schema(), std::move(ebatches)));

  Declaration plan =
      Declaration::Sequence({source_node,
                             {"jitter", JitterNodeOptions(kRandomSeed)},
                             {"project", ProjectNodeOptions({field_ref("x")})}});

  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema batches,
                       DeclarationToExecBatches(std::move(plan)));

  AssertExecBatchesSequenced(batches.batches);
}

TEST(ExecPlanExecution, FilterMaintainsOrder) {
  RegisterTestNodes();
  constexpr int kRandomSeed = 42;
  constexpr int64_t kRowsPerBatch = 1;
  constexpr int kNumBatches = 16;

  auto generator = gen::Gen({{"x", gen::Step()}})->FailOnError();
  std::vector<::arrow::compute::ExecBatch> ebatches =
      generator->ExecBatches(kRowsPerBatch, kNumBatches);
  Declaration source_node =
      Declaration("exec_batch_source", ::arrow::acero::ExecBatchSourceNodeOptions(
                                           generator->Schema(), std::move(ebatches)));

  Declaration plan = Declaration::Sequence(
      {source_node,
       {"jitter", JitterNodeOptions(kRandomSeed)},
       {"filter",
        // The filter x > 50 should result in a few empty batches
        FilterNodeOptions({call("greater", {field_ref("x"), literal(50)})})}});

  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema batches,
                       DeclarationToExecBatches(std::move(plan)));

  // Sanity check that some filtering took place
  ASSERT_EQ(0, batches.batches[0].length);

  AssertExecBatchesSequenced(batches.batches);
}

namespace {

BatchesWithSchema MakeGroupableBatches(int multiplicity = 1) {
  BatchesWithSchema out;

  out.batches = {ExecBatchFromJSON({int32(), utf8()}, R"([
                   [12, "alfa"],
                   [7,  "beta"],
                   [3,  "alfa"]
                 ])"),
                 ExecBatchFromJSON({int32(), utf8()}, R"([
                   [-2, "alfa"],
                   [-1, "gama"],
                   [3,  "alfa"]
                 ])"),
                 ExecBatchFromJSON({int32(), utf8()}, R"([
                   [5,  "gama"],
                   [3,  "beta"],
                   [-8, "alfa"]
                 ])")};

  size_t batch_count = out.batches.size();
  for (int repeat = 1; repeat < multiplicity; ++repeat) {
    for (size_t i = 0; i < batch_count; ++i) {
      out.batches.push_back(out.batches[i]);
    }
  }

  out.schema = schema({field("i32", int32()), field("str", utf8())});

  return out;
}

}  // namespace

TEST(ExecPlanExecution, SourceGroupedSum) {
  std::shared_ptr<Schema> out_schema =
      schema({field("str", utf8()), field("sum(i32)", int64())});
  const std::shared_ptr<Table> expected_parallel =
      TableFromJSON(out_schema, {R"([["alfa", 800], ["beta", 1000], ["gama", 400]])"});
  const std::shared_ptr<Table> expected_single =
      TableFromJSON(out_schema, {R"([["alfa", 8], ["beta", 10], ["gama", 4]])"});

  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    auto input = MakeGroupableBatches(/*multiplicity=*/parallel ? 100 : 1);

    Declaration plan = Declaration::Sequence(
        {{"source", SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
         {"aggregate",
          AggregateNodeOptions{/*aggregates=*/{{"hash_sum", nullptr, "i32", "sum(i32)"}},
                               /*keys=*/{"str"}}}});

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> actual,
                         DeclarationToTable(std::move(plan), parallel));

    auto expected = parallel ? expected_parallel : expected_single;

    AssertTablesEqualIgnoringOrder(expected, actual);
  }
}

TEST(ExecPlanExecution, SourceMinMaxScalar) {
  // Regression test for ARROW-16904
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    auto input = MakeGroupableBatches(/*multiplicity=*/parallel ? 100 : 1);
    auto minmax_opts = std::make_shared<ScalarAggregateOptions>();
    auto min_max_type = struct_({field("min", int32()), field("max", int32())});
    auto expected_table = TableFromJSON(schema({field("struct", min_max_type)}), {R"([
      [{"min": -8, "max": 12}]
    ])"});

    // NOTE: Test `ScalarAggregateNode` by omitting `keys` attribute
    Declaration plan = Declaration::Sequence(
        {{"source", SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
         {"aggregate",
          AggregateNodeOptions{
              /*aggregates=*/{{"min_max", std::move(minmax_opts), "i32", "min_max"}},
              /*keys=*/{}}}});
    ASSERT_OK_AND_ASSIGN(auto result_table,
                         DeclarationToTable(std::move(plan), parallel));
    // No need to ignore order since there is only 1 row
    AssertTablesEqual(*result_table, *expected_table);
  }
}

TEST(ExecPlanExecution, NestedSourceFilter) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    auto input = MakeNestedBatches();
    auto expected_table = TableFromJSON(input.schema, {R"([])",
                                                       R"([
      [{"i32": 5, "bool": null}],
      [{"i32": 6, "bool": false}],
      [{"i32": 7, "bool": false}]
    ])"});

    Declaration plan = Declaration::Sequence(
        {{"source", SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
         {"filter", FilterNodeOptions{greater_equal(field_ref(FieldRef("struct", "i32")),
                                                    literal(5))}}});
    ASSERT_OK_AND_ASSIGN(auto result_table,
                         DeclarationToTable(std::move(plan), parallel));
    AssertTablesEqual(*result_table, *expected_table);
  }
}

TEST(ExecPlanExecution, NestedSourceProjectGroupedSum) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    auto input = MakeNestedBatches();
    auto expected =
        TableFromJSON(schema({field("bool", boolean()), field("i32", int64())}), {R"([
      [true, null],
      [false, 17],
      [null, 5]
])"});

    Declaration plan = Declaration::Sequence(
        {{"source", SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
         {"project", ProjectNodeOptions{{
                                            field_ref(FieldRef("struct", "i32")),
                                            field_ref(FieldRef("struct", "bool")),
                                        },
                                        {"i32", "bool"}}},
         {"aggregate",
          AggregateNodeOptions{/*aggregates=*/{{"hash_sum", nullptr, "i32", "sum(i32)"}},
                               /*keys=*/{"bool"}}}});

    ASSERT_OK_AND_ASSIGN(auto actual, DeclarationToTable(std::move(plan), parallel));
    AssertTablesEqualIgnoringOrder(expected, actual);
  }
}

TEST(ExecPlanExecution, SourceFilterProjectGroupedSumFilter) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    int batch_multiplicity = parallel ? 100 : 1;
    auto input = MakeGroupableBatches(/*multiplicity=*/batch_multiplicity);

    Declaration plan = Declaration::Sequence(
        {{"source", SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
         {"filter", FilterNodeOptions{greater_equal(field_ref("i32"), literal(0))}},
         {"project", ProjectNodeOptions{{
                         field_ref("str"),
                         call("multiply", {field_ref("i32"), literal(2)}),
                     }}},
         {"aggregate",
          AggregateNodeOptions{/*aggregates=*/{{"hash_sum", nullptr, "multiply(i32, 2)",
                                                "sum(multiply(i32, 2))"}},
                               /*keys=*/{"str"}}},
         {"filter", FilterNodeOptions{greater(field_ref("sum(multiply(i32, 2))"),
                                              literal(10 * batch_multiplicity))}}});

    auto expected = TableFromJSON(
        schema({field("str", utf8()), field("sum(multiply(i32, 2))", int64())}),
        {parallel ? R"([["alfa", 3600], ["beta", 2000]])"
                  : R"([["alfa", 36], ["beta", 20]])"});
    ASSERT_OK_AND_ASSIGN(auto actual, DeclarationToTable(std::move(plan), parallel));
    AssertTablesEqualIgnoringOrder(expected, actual);
  }
}

TEST(ExecPlanExecution, SourceFilterProjectGroupedSumOrderBy) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    int batch_multiplicity = parallel ? 100 : 1;
    auto input = MakeGroupableBatches(/*multiplicity=*/batch_multiplicity);

    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
    AsyncGenerator<std::optional<ExecBatch>> sink_gen;

    SortOptions options({SortKey("str", SortOrder::Descending)});
    ASSERT_OK(
        Declaration::Sequence(
            {
                {"source",
                 SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
                {"filter",
                 FilterNodeOptions{greater_equal(field_ref("i32"), literal(0))}},
                {"project", ProjectNodeOptions{{
                                field_ref("str"),
                                call("multiply", {field_ref("i32"), literal(2)}),
                            }}},
                {"aggregate",
                 AggregateNodeOptions{
                     /*aggregates=*/{{"hash_sum", nullptr, "multiply(i32, 2)",
                                      "sum(multiply(i32, 2))"}},
                     /*keys=*/{"str"}}},
                {"filter", FilterNodeOptions{greater(field_ref("sum(multiply(i32, 2))"),
                                                     literal(10 * batch_multiplicity))}},
                {"order_by_sink", OrderBySinkNodeOptions{options, &sink_gen}},
            })
            .AddToPlan(plan.get()));

    ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
                Finishes(ResultWith(ElementsAreArray({ExecBatchFromJSON(
                    {utf8(), int64()}, parallel ? R"([["beta", 2000], ["alfa", 3600]])"
                                                : R"([["beta", 20], ["alfa", 36]])")}))));
  }
}

TEST(ExecPlanExecution, SourceFilterProjectGroupedSumTopK) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    int batch_multiplicity = parallel ? 100 : 1;
    auto input = MakeGroupableBatches(/*multiplicity=*/batch_multiplicity);

    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
    AsyncGenerator<std::optional<ExecBatch>> sink_gen;

    SelectKOptions options = SelectKOptions::TopKDefault(/*k=*/1, {"str"});
    ASSERT_OK(Declaration::Sequence(
                  {
                      {"source", SourceNodeOptions{input.schema,
                                                   input.gen(parallel, /*slow=*/false)}},
                      {"project", ProjectNodeOptions{{
                                      field_ref("str"),
                                      call("multiply", {field_ref("i32"), literal(2)}),
                                  }}},
                      {"aggregate",
                       AggregateNodeOptions{
                           /*aggregates=*/{{"hash_sum", nullptr, "multiply(i32, 2)",
                                            "sum(multiply(i32, 2))"}},
                           /*keys=*/{"str"}}},
                      {"select_k_sink", SelectKSinkNodeOptions{options, &sink_gen}},
                  })
                  .AddToPlan(plan.get()));

    ASSERT_THAT(
        StartAndCollect(plan.get(), sink_gen),
        Finishes(ResultWith(ElementsAreArray({ExecBatchFromJSON(
            {utf8(), int64()}, parallel ? R"([["gama", 800]])" : R"([["gama", 8]])")}))));
  }
}

TEST(ExecPlanExecution, SourceScalarAggSink) {
  auto basic_data = MakeBasicBatches();

  Declaration plan = Declaration::Sequence(
      {{"source", SourceNodeOptions{basic_data.schema,
                                    basic_data.gen(/*parallel=*/false, /*slow=*/false)}},
       {"aggregate", AggregateNodeOptions{
                         /*aggregates=*/{{"sum", nullptr, "i32", "sum(i32)"},
                                         {"any", nullptr, "bool", "any(bool)"}},
                     }}});
  auto exp_batches = {ExecBatchFromJSON(
      {int64(), boolean()}, {ArgShape::SCALAR, ArgShape::SCALAR}, "[[22, true]]")};
  ASSERT_OK_AND_ASSIGN(auto result, DeclarationToExecBatches(std::move(plan)));
  AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches, exp_batches);
}

TEST(ExecPlanExecution, AggregationPreservesOptions) {
  // ARROW-13638: aggregation nodes initialize per-thread kernel state lazily
  // and need to keep a copy/strong reference to function options
  {
    auto basic_data = MakeBasicBatches();
    Future<std::shared_ptr<Table>> table_future;
    {
      auto options = std::make_shared<TDigestOptions>(TDigestOptions::Defaults());
      Declaration plan = Declaration::Sequence(
          {{"source",
            SourceNodeOptions{basic_data.schema, basic_data.gen(/*parallel=*/false,
                                                                /*slow=*/false)}},
           {"aggregate", AggregateNodeOptions{
                             /*aggregates=*/{{"tdigest", options, "i32", "tdigest(i32)"}},
                         }}});
      table_future = DeclarationToTableAsync(std::move(plan));
    }

    std::shared_ptr<Table> expected =
        TableFromJSON(schema({field("tdigest(i32)", float64())}), {"[[5.5]]"});

    ASSERT_FINISHES_OK_AND_ASSIGN(std::shared_ptr<Table> actual, table_future);
    AssertTablesEqualIgnoringOrder(expected, actual);
  }
  {
    auto data = MakeGroupableBatches(/*multiplicity=*/100);
    Future<std::shared_ptr<Table>> table_future;
    {
      auto options = std::make_shared<CountOptions>(CountOptions::Defaults());
      Declaration plan = Declaration::Sequence(
          {{"source", SourceNodeOptions{data.schema, data.gen(/*parallel=*/false,
                                                              /*slow=*/false)}},
           {"aggregate", AggregateNodeOptions{/*aggregates=*/{{"hash_count", options,
                                                               "i32", "count(i32)"}},
                                              /*keys=*/{"str"}}}});
      table_future = DeclarationToTableAsync(std::move(plan));
    }

    std::shared_ptr<Table> expected =
        TableFromJSON(schema({field("str", utf8()), field("count(i32)", int64())}),
                      {R"([["alfa", 500], ["beta", 200], ["gama", 200]])"});

    ASSERT_FINISHES_OK_AND_ASSIGN(std::shared_ptr<Table> actual, table_future);
    AssertTablesEqualIgnoringOrder(expected, actual);
  }
}

TEST(ExecPlanExecution, ScalarSourceScalarAggSink) {
  // ARROW-9056: scalar aggregation can be done over scalars, taking
  // into account batch.length > 1 (e.g. a partition column)
  BatchesWithSchema scalar_data;
  scalar_data.batches = {
      ExecBatchFromJSON({int32(), boolean()}, {ArgShape::SCALAR, ArgShape::SCALAR},
                        "[[5, false], [5, false], [5, false]]"),
      ExecBatchFromJSON({int32(), boolean()}, "[[5, true], [6, false], [7, true]]")};
  scalar_data.schema = schema({field("a", int32()), field("b", boolean())});

  // index can't be tested as it's order-dependent
  // mode/quantile can't be tested as they're technically vector kernels
  Declaration plan = Declaration::Sequence(
      {{"source", SourceNodeOptions{scalar_data.schema,
                                    scalar_data.gen(/*parallel=*/false, /*slow=*/false)}},
       {"aggregate", AggregateNodeOptions{
                         /*aggregates=*/{{"all", nullptr, "b", "all(b)"},
                                         {"any", nullptr, "b", "any(b)"},
                                         {"count", nullptr, "a", "count(a)"},
                                         {"count_all", "count(*)"},
                                         {"mean", nullptr, "a", "mean(a)"},
                                         {"product", nullptr, "a", "product(a)"},
                                         {"stddev", nullptr, "a", "stddev(a)"},
                                         {"sum", nullptr, "a", "sum(a)"},
                                         {"tdigest", nullptr, "a", "tdigest(a)"},
                                         {"variance", nullptr, "a", "variance(a)"}}}}});

  auto exp_batches = {
      ExecBatchFromJSON(
          {boolean(), boolean(), int64(), int64(), float64(), int64(), float64(), int64(),
           float64(), float64()},
          {ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR,
           ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR,
           ArgShape::ARRAY, ArgShape::SCALAR},
          R"([[false, true, 6, 6, 5.5, 26250, 0.7637626158259734, 33, 5.0, 0.5833333333333334]])"),
  };
  ASSERT_OK_AND_ASSIGN(auto result, DeclarationToExecBatches(std::move(plan)));
  AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches, exp_batches);
}

TEST(ExecPlanExecution, ScalarSourceStandaloneNullaryScalarAggSink) {
  BatchesWithSchema scalar_data;
  scalar_data.batches = {
      ExecBatchFromJSON({int32(), boolean()}, {ArgShape::SCALAR, ArgShape::SCALAR},
                        "[[5, null], [5, false], [5, false]]"),
      ExecBatchFromJSON({int32(), boolean()}, "[[5, true], [null, false], [7, true]]")};
  scalar_data.schema = schema({
      field("a", int32()),
      field("b", boolean()),
  });

  Declaration plan = Declaration::Sequence(
      {{"source",
        SourceNodeOptions{scalar_data.schema, scalar_data.gen(/*parallel=*/false,
                                                              /*slow=*/false)}},
       {"aggregate", AggregateNodeOptions{/*aggregates=*/{
                         {"count_all", "count(*)"},
                     }}}});
  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema actual_batches,
                       DeclarationToExecBatches(std::move(plan)));

  auto expected = ExecBatchFromJSON({int64()}, {ArgShape::SCALAR}, R"([[6]])");
  AssertExecBatchesEqualIgnoringOrder(actual_batches.schema, actual_batches.batches,
                                      {expected});
}

TEST(ExecPlanExecution, ScalarSourceGroupedSum) {
  // ARROW-14630: ensure grouped aggregation with a scalar key/array input doesn't
  // error
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;

  BatchesWithSchema scalar_data;
  scalar_data.batches = {
      ExecBatchFromJSON({int32(), boolean()}, {ArgShape::ARRAY, ArgShape::SCALAR},
                        "[[5, false], [6, false], [7, false]]"),
      ExecBatchFromJSON({int32(), boolean()}, {ArgShape::ARRAY, ArgShape::SCALAR},
                        "[[1, true], [2, true], [3, true]]"),
  };
  scalar_data.schema = schema({field("a", int32()), field("b", boolean())});

  SortOptions options({SortKey("b", SortOrder::Descending)});
  ASSERT_OK(
      Declaration::Sequence(
          {
              {"source",
               SourceNodeOptions{scalar_data.schema, scalar_data.gen(/*parallel=*/false,
                                                                     /*slow=*/false)}},
              {"aggregate", AggregateNodeOptions{/*aggregates=*/{{"hash_sum", nullptr,
                                                                  "a", "hash_sum(a)"}},
                                                 /*keys=*/{"b"}}},
              {"order_by_sink", OrderBySinkNodeOptions{options, &sink_gen}},
          })
          .AddToPlan(plan.get()));

  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(ResultWith(UnorderedElementsAreArray({
                  ExecBatchFromJSON({boolean(), int64()}, R"([[true, 6], [false, 18]])"),
              }))));
}

TEST(ExecPlanExecution, SelfInnerHashJoinSink) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    auto input = MakeGroupableBatches();

    auto left = Declaration::Sequence(
        {{"source", SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
         {"filter", FilterNodeOptions{greater_equal(field_ref("i32"), literal(-1))}}});

    auto right = Declaration::Sequence(
        {{"source", SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
         {"filter", FilterNodeOptions{less_equal(field_ref("i32"), literal(2))}}});

    // left side: [3,  "alfa"], [3,  "alfa"], [12, "alfa"], [3,  "beta"], [7,  "beta"],
    // [-1, "gama"], [5,  "gama"]
    // right side: [-2, "alfa"], [-8, "alfa"], [-1, "gama"]

    HashJoinNodeOptions join_opts{JoinType::INNER,
                                  /*left_keys=*/{"str"},
                                  /*right_keys=*/{"str"}, literal(true), "l_", "r_"};

    auto plan = Declaration("hashjoin", {left, right}, std::move(join_opts));

    ASSERT_OK_AND_ASSIGN(auto result,
                         DeclarationToExecBatches(std::move(plan), parallel));

    std::vector<ExecBatch> expected = {
        ExecBatchFromJSON({int32(), utf8(), int32(), utf8()}, R"([
            [3, "alfa", -2, "alfa"], [3, "alfa", -8, "alfa"],
            [3, "alfa", -2, "alfa"], [3, "alfa", -8, "alfa"],
            [12, "alfa", -2, "alfa"], [12, "alfa", -8, "alfa"],
            [-1, "gama", -1, "gama"], [5, "gama", -1, "gama"]])")};

    AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches, expected);
  }
}

TEST(ExecPlanExecution, SelfOuterHashJoinSink) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    auto input = MakeGroupableBatches();

    auto left = Declaration::Sequence(
        {{"source", SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
         {"filter", FilterNodeOptions{greater_equal(field_ref("i32"), literal(-1))}}});

    auto right = Declaration::Sequence(
        {{"source", SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
         {"filter", FilterNodeOptions{less_equal(field_ref("i32"), literal(2))}}});

    // left side: [3,  "alfa"], [3,  "alfa"], [12, "alfa"], [3,  "beta"], [7,  "beta"],
    // [-1, "gama"], [5,  "gama"]
    // right side: [-2, "alfa"], [-8, "alfa"], [-1, "gama"]

    HashJoinNodeOptions join_opts{JoinType::FULL_OUTER,
                                  /*left_keys=*/{"str"},
                                  /*right_keys=*/{"str"}, literal(true), "l_", "r_"};

    auto plan = Declaration("hashjoin", {left, right}, std::move(join_opts));

    ASSERT_OK_AND_ASSIGN(auto result,
                         DeclarationToExecBatches(std::move(plan), parallel));

    std::vector<ExecBatch> expected = {
        ExecBatchFromJSON({int32(), utf8(), int32(), utf8()}, R"([
            [3, "alfa", -2, "alfa"], [3, "alfa", -8, "alfa"],
            [3, "alfa", -2, "alfa"], [3, "alfa", -8, "alfa"],
            [12, "alfa", -2, "alfa"], [12, "alfa", -8, "alfa"],
            [3,  "beta", null, null], [7,  "beta", null, null],
            [-1, "gama", -1, "gama"], [5, "gama", -1, "gama"]])")};

    AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches, expected);
  }
}

TEST(ExecPlan, RecordBatchReaderSourceSink) {
  // set up a RecordBatchReader:
  auto input = MakeBasicBatches();

  RecordBatchVector batches;
  for (const ExecBatch& exec_batch : input.batches) {
    ASSERT_OK_AND_ASSIGN(auto batch, exec_batch.ToRecordBatch(input.schema));
    batches.push_back(std::move(batch));
  }

  ASSERT_OK_AND_ASSIGN(auto table, Table::FromRecordBatches(batches));
  std::shared_ptr<RecordBatchReader> reader = std::make_shared<TableBatchReader>(*table);

  // Map the RecordBatchReader to a SourceNode
  ASSERT_OK_AND_ASSIGN(
      auto batch_gen,
      MakeReaderGenerator(std::move(reader), arrow::io::internal::GetIOThreadPool()));

  Declaration plan =
      Declaration::Sequence({{"source", SourceNodeOptions{table->schema(), batch_gen}}});
  ASSERT_OK_AND_ASSIGN(auto result, DeclarationToExecBatches(std::move(plan)));
  AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches, input.batches);
}

TEST(ExecPlan, SourceEnforcesBatchLimit) {
  auto random_data = MakeRandomBatches(
      schema({field("a", int32()), field("b", boolean())}), /*num_batches=*/3,
      /*batch_size=*/static_cast<int32_t>(std::floor(ExecPlan::kMaxBatchSize * 3.5)));

  Declaration plan = Declaration::Sequence(
      {{"source",
        SourceNodeOptions{random_data.schema,
                          random_data.gen(/*parallel=*/false, /*slow=*/false)}}});
  ASSERT_OK_AND_ASSIGN(auto result, DeclarationToExecBatches(std::move(plan)));
  AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches, random_data.batches);
  for (const auto& batch : result.batches) {
    ASSERT_LE(batch.length, ExecPlan::kMaxBatchSize);
  }
}

TEST(ExecPlanExecution, SegmentedAggregationWithMultiThreading) {
  BatchesWithSchema data;
  data.batches = {ExecBatchFromJSON({int32()}, "[[1]]")};
  data.schema = schema({field("i32", int32())});
  Declaration plan = Declaration::Sequence(
      {{"source",
        SourceNodeOptions{data.schema, data.gen(/*parallel=*/false, /*slow=*/false)}},
       {"aggregate", AggregateNodeOptions{/*aggregates=*/{
                                              {"count", nullptr, "i32", "count(i32)"},
                                          },
                                          /*keys=*/{}, /*segment_keys=*/{"i32"}}}});
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented, HasSubstr("multi-threaded"),
                                  DeclarationToExecBatches(std::move(plan)));
}

TEST(ExecPlanExecution, SegmentedAggregationWithOneSegment) {
  BatchesWithSchema data;
  data.batches = {
      ExecBatchFromJSON({int32(), int32(), int32()}, "[[1, 1, 1], [1, 2, 1], [1, 1, 2]]"),
      ExecBatchFromJSON({int32(), int32(), int32()},
                        "[[1, 2, 2], [1, 1, 3], [1, 2, 3]]")};
  data.schema = schema({
      field("a", int32()),
      field("b", int32()),
      field("c", int32()),
  });

  Declaration plan = Declaration::Sequence(
      {{"source",
        SourceNodeOptions{data.schema, data.gen(/*parallel=*/false, /*slow=*/false)}},
       {"aggregate", AggregateNodeOptions{/*aggregates=*/{
                                              {"hash_sum", nullptr, "c", "sum(c)"},
                                              {"hash_mean", nullptr, "c", "mean(c)"},
                                          },
                                          /*keys=*/{"b"}, /*segment_leys=*/{"a"}}}});
  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema actual_batches,
                       DeclarationToExecBatches(std::move(plan), /*use_threads=*/false));

  auto expected = ExecBatchFromJSON({int32(), int32(), int64(), float64()},
                                    R"([[1, 1, 6, 2], [1, 2, 6, 2]])");
  AssertExecBatchesEqualIgnoringOrder(actual_batches.schema, actual_batches.batches,
                                      {expected});
}

TEST(ExecPlanExecution, SegmentedAggregationWithTwoSegments) {
  BatchesWithSchema data;
  data.batches = {
      ExecBatchFromJSON({int32(), int32(), int32()}, "[[1, 1, 1], [1, 2, 1], [1, 1, 2]]"),
      ExecBatchFromJSON({int32(), int32(), int32()},
                        "[[2, 2, 2], [2, 1, 3], [2, 2, 3]]")};
  data.schema = schema({
      field("a", int32()),
      field("b", int32()),
      field("c", int32()),
  });

  Declaration plan = Declaration::Sequence(
      {{"source",
        SourceNodeOptions{data.schema, data.gen(/*parallel=*/false, /*slow=*/false)}},
       {"aggregate", AggregateNodeOptions{/*aggregates=*/{
                                              {"hash_sum", nullptr, "c", "sum(c)"},
                                              {"hash_mean", nullptr, "c", "mean(c)"},
                                          },
                                          /*keys=*/{"b"}, /*segment_keys=*/{"a"}}}});
  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema actual_batches,
                       DeclarationToExecBatches(std::move(plan), /*use_threads=*/false));

  auto expected = ExecBatchFromJSON(
      {int32(), int32(), int64(), float64()},
      R"([[1, 1, 3, 1.5], [1, 2, 1, 1], [2, 1, 3, 3], [2, 2, 5, 2.5]])");
  AssertExecBatchesEqualIgnoringOrder(actual_batches.schema, actual_batches.batches,
                                      {expected});
}

TEST(ExecPlanExecution, SegmentedAggregationWithBatchCrossingSegment) {
  BatchesWithSchema data;
  data.batches = {
      ExecBatchFromJSON({int32(), int32(), int32()}, "[[1, 1, 1], [1, 1, 1], [2, 2, 2]]"),
      ExecBatchFromJSON({int32(), int32(), int32()},
                        "[[2, 2, 2], [3, 3, 3], [3, 3, 3]]")};
  data.schema = schema({
      field("a", int32()),
      field("b", int32()),
      field("c", int32()),
  });

  Declaration plan = Declaration::Sequence(
      {{"source",
        SourceNodeOptions{data.schema, data.gen(/*parallel=*/false, /*slow=*/false)}},
       {"aggregate", AggregateNodeOptions{/*aggregates=*/{
                                              {"hash_sum", nullptr, "c", "sum(c)"},
                                              {"hash_mean", nullptr, "c", "mean(c)"},
                                          },
                                          /*keys=*/{"b"}, /*segment_leys=*/{"a"}}}});
  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema actual_batches,
                       DeclarationToExecBatches(std::move(plan), /*use_threads=*/false));

  auto expected = ExecBatchFromJSON({int32(), int32(), int64(), float64()},
                                    R"([[1, 1, 2, 1], [2, 2, 4, 2], [3, 3, 6, 3]])");
  AssertExecBatchesEqualIgnoringOrder(actual_batches.schema, actual_batches.batches,
                                      {expected});
}

TEST(ExecPlanExecution, UnalignedInput) {
  std::shared_ptr<Array> array = ArrayFromJSON(int32(), "[1, 2, 3]");
  std::shared_ptr<Array> unaligned = UnalignBuffers(*array);
  ASSERT_OK_AND_ASSIGN(ExecBatch sample_batch,
                       ExecBatch::Make({unaligned}, array->length()));

  BatchesWithSchema data;
  data.batches = {std::move(sample_batch)};
  data.schema = schema({field("i32", int32())});

  Declaration plan = Declaration::Sequence({
      {"exec_batch_source", ExecBatchSourceNodeOptions(data.schema, data.batches)},
  });

  int64_t initial_bytes_allocated = default_memory_pool()->total_bytes_allocated();

  // By default we should warn and so the plan should finish ok
  ASSERT_OK(DeclarationToStatus(plan));
  ASSERT_EQ(initial_bytes_allocated, default_memory_pool()->total_bytes_allocated());

  QueryOptions query_options;

#ifndef ARROW_UBSAN
  // Nothing should happen if we ignore alignment
  query_options.unaligned_buffer_handling = UnalignedBufferHandling::kIgnore;
  ASSERT_OK(DeclarationToStatus(plan, query_options));
  ASSERT_EQ(initial_bytes_allocated, default_memory_pool()->total_bytes_allocated());
#endif

  query_options.unaligned_buffer_handling = UnalignedBufferHandling::kError;
  ASSERT_THAT(DeclarationToStatus(plan, query_options),
              Raises(StatusCode::Invalid,
                     testing::HasSubstr("An input buffer was poorly aligned")));
  ASSERT_EQ(initial_bytes_allocated, default_memory_pool()->total_bytes_allocated());

  query_options.unaligned_buffer_handling = UnalignedBufferHandling::kReallocate;
  ASSERT_OK(DeclarationToStatus(plan, query_options));
  ASSERT_LT(initial_bytes_allocated, default_memory_pool()->total_bytes_allocated());
}

}  // namespace acero
}  // namespace arrow
