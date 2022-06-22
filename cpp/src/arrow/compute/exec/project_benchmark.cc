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

#include <condition_variable>
#include <mutex>

#include "benchmark/benchmark.h"

#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/task_util.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/dataset/partition.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

static constexpr int64_t kTotalBatchSize = 1000000;

static void ProjectionOverhead(benchmark::State& state, Expression expr) {
  const int32_t batch_size = static_cast<int32_t>(state.range(0));
  const int32_t num_batches = kTotalBatchSize / batch_size;

  arrow::compute::BatchesWithSchema data = MakeRandomBatches(
      schema({field("i64", int64()), field("bool", boolean())}), num_batches, batch_size);
  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  for (auto _ : state) {
    state.PauseTiming();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::compute::ExecPlan> plan,
                         ExecPlan::Make(&ctx));
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    ASSERT_OK(Declaration::Sequence(
                  {
                      {"source",
                       SourceNodeOptions{data.schema,
                                         data.gen(/*parallel=*/true, /*slow=*/false)},
                       "custom_source_label"},
                      {"project", ProjectNodeOptions{{
                                      expr,
                                  }}},
                      {"sink", SinkNodeOptions{&sink_gen}, "custom_sink_label"},
                  })
                  .AddToPlan(plan.get()));
    state.ResumeTiming();
    ASSERT_FINISHES_OK(StartAndCollect(plan.get(), sink_gen));
  }

  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches * batch_size),
      benchmark::Counter::kIsRate);

  state.counters["batches_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches), benchmark::Counter::kIsRate);
}

static void ProjectionOverheadIsolated(benchmark::State& state, Expression expr) {
  const int32_t batch_size = static_cast<int32_t>(state.range(0));
  const int32_t num_batches = kTotalBatchSize / batch_size;

  arrow::compute::BatchesWithSchema data = MakeRandomBatches(
      schema({field("i64", int64()), field("bool", boolean())}), num_batches, batch_size);
  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  for (auto _ : state) {
    state.PauseTiming();

    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::compute::ExecPlan> plan,
                         ExecPlan::Make(&ctx));
    // Source and sink nodes have no effect on the benchmark.
    // Used for dummy purposes as they are referenced in InputReceived and InputFinished.
    ASSERT_OK_AND_ASSIGN(
        arrow::compute::ExecNode * source_node,
        MakeExecNode("source", plan.get(), {},
                     SourceNodeOptions{data.schema, data.gen(/*parallel=*/true,
                                                             /*slow=*/false)}));
    ASSERT_OK_AND_ASSIGN(arrow::compute::ExecNode * project_node,
                         MakeExecNode("project", plan.get(), {source_node},
                                      ProjectNodeOptions{{
                                          expr,
                                      }}));
    ASSERT_OK(
        MakeExecNode("sink", plan.get(), {project_node}, SinkNodeOptions{&sink_gen}));

    std::unique_ptr<arrow::compute::TaskScheduler> scheduler = TaskScheduler::Make();
    std::condition_variable all_tasks_finished_cv;
    std::mutex mutex;
    int task_group_id = scheduler->RegisterTaskGroup(
        [&](size_t thread_id, int64_t task_id) {
          project_node->InputReceived(source_node, data.batches[task_id]);
          return Status::OK();
        },
        [&](size_t thread_id) {
          project_node->InputFinished(source_node, static_cast<int>(data.batches.size()));
          std::unique_lock<std::mutex> lk(mutex);
          all_tasks_finished_cv.notify_one();
          return Status::OK();
        });
    scheduler->RegisterEnd();
    ThreadIndexer thread_indexer;

    state.ResumeTiming();
    arrow::internal::ThreadPool* thread_pool = arrow::internal::GetCpuThreadPool();
    ASSERT_OK(scheduler->StartScheduling(
        thread_indexer(),
        [&](std::function<Status(size_t)> task) -> Status {
          return thread_pool->Spawn([&, task]() {
            size_t tid = thread_indexer();
            ARROW_DCHECK_OK(task(tid));
          });
        },
        thread_pool->GetCapacity(),
        /*use_sync_execution=*/false));
    std::unique_lock<std::mutex> lk(mutex);
    ASSERT_OK(scheduler->StartTaskGroup(thread_indexer(), task_group_id, num_batches));
    all_tasks_finished_cv.wait(lk);
    ASSERT_TRUE(project_node->finished().is_finished());
  }

  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches * batch_size),
      benchmark::Counter::kIsRate);

  state.counters["batches_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches), benchmark::Counter::kIsRate);
}

arrow::compute::Expression complex_expression =
    and_(less(field_ref("i64"), literal(20)), greater(field_ref("i64"), literal(0)));
arrow::compute::Expression simple_expression = call("negate", {field_ref("i64")});
arrow::compute::Expression zero_copy_expression = call(
    "cast", {field_ref("i64")}, compute::CastOptions::Safe(timestamp(TimeUnit::NANO)));
arrow::compute::Expression ref_only_expression = field_ref("i64");

void SetArgs(benchmark::internal::Benchmark* bench) {
  bench->ArgNames({"batch_size"})
      ->RangeMultiplier(10)
      ->Range(1000, kTotalBatchSize)
      ->UseRealTime();
}

BENCHMARK_CAPTURE(ProjectionOverheadIsolated, complex_expression, complex_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverheadIsolated, simple_expression, simple_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverheadIsolated, zero_copy_expression, zero_copy_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverheadIsolated, ref_only_expression, ref_only_expression)
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(ProjectionOverhead, complex_expression, complex_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverhead, simple_expression, simple_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverhead, zero_copy_expression, zero_copy_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverhead, ref_only_expression, ref_only_expression)
    ->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow
