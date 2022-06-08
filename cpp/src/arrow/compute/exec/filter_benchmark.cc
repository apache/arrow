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
#include <mutex>
#include <condition_variable>

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

static constexpr int64_t total_batch_size = 1e6;

static void FilterOverhead(benchmark::State& state, Expression expr) {
  const auto batch_size = static_cast<int32_t>(state.range(0));
  const auto num_batches = total_batch_size / batch_size;

  auto data = MakeRandomBatches(schema({field("i64", int64()), field("bool", boolean())}),
                                num_batches, batch_size);
  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  for (auto _ : state) {
    state.PauseTiming();
    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(&ctx));
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    ASSERT_OK(Declaration::Sequence(
                  {
                      {"source",
                       SourceNodeOptions{data.schema,
                                         data.gen(/*parallel=*/true, /*slow=*/false)},
                       "custom_source_label"},
                      {"filter",
                       FilterNodeOptions{
                           expr,
                       }},
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

static void FilterOverheadIsolated(benchmark::State& state, Expression expr) {
  const auto batch_size = static_cast<int32_t>(state.range(0));
  const auto num_batches = total_batch_size / batch_size;

  auto data = MakeRandomBatches(schema({field("i64", int64()), field("bool", boolean())}),
                                num_batches, batch_size);
  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  for (auto _ : state) {
    state.PauseTiming();
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;

    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(&ctx));
    auto source_node_options = SourceNodeOptions{data.schema, data.gen(/*parallel=*/true,
                                                                       /*slow=*/false)};
    ASSERT_OK_AND_ASSIGN(auto sn,
                         MakeExecNode("source", plan.get(), {}, source_node_options));
    ASSERT_OK_AND_ASSIGN(auto pn, MakeExecNode("filter", plan.get(), {sn},
                                               FilterNodeOptions{
                                                   expr,
                                               }));
    MakeExecNode("sink", plan.get(), {pn}, SinkNodeOptions{&sink_gen});

    auto scheduler = TaskScheduler::Make();
    std::condition_variable cv;
    std::mutex mutex;
    int task_group_id = scheduler->RegisterTaskGroup(
        [&](size_t thread_id, int64_t task_id) {
          pn->InputReceived(sn, data.batches[task_id]);
          return Status::OK();
        },
        [&](size_t thread_id) {
          pn->InputFinished(sn, data.batches.size());
          std::unique_lock<std::mutex> lk(mutex);
          cv.notify_one();
          return Status::OK();
        });
    scheduler->RegisterEnd();
    ThreadIndexer thread_indexer;

    state.ResumeTiming();
    auto tp = arrow::internal::GetCpuThreadPool();
    ASSERT_OK(scheduler->StartScheduling(
        thread_indexer(),
        [&](std::function<Status(size_t)> task) -> Status {
          return tp->Spawn([&, task]() {
            size_t tid = thread_indexer();
            ARROW_DCHECK_OK(task(tid));
          });
        },
        tp->GetCapacity(),
        /*use_sync_execution=*/false));
    std::unique_lock<std::mutex> lk(mutex);
    ASSERT_OK(scheduler->StartTaskGroup(thread_indexer(), task_group_id, num_batches));
    cv.wait(lk);
    ASSERT_TRUE(pn->finished().is_finished());
  }
  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches * batch_size),
      benchmark::Counter::kIsRate);

  state.counters["batches_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches), benchmark::Counter::kIsRate);
}

auto complex_expression =
    less(less(field_ref("i64"), literal(20)), greater(field_ref("i64"), literal(0)));
auto simple_expression = less(call("negate", {field_ref("i64")}), literal(0));
auto zero_copy_expression = is_valid((call(
    "cast", {field_ref("i64")}, compute::CastOptions::Safe(timestamp(TimeUnit::NANO)))));
auto ref_only_expression = less(field_ref("i64"), literal(0));

void SetArgs(benchmark::internal::Benchmark* bench) {
  bench->ArgNames({"batch_size"})
      ->RangeMultiplier(10)
      ->Range(1000, total_batch_size)
      ->UseRealTime();
}

BENCHMARK_CAPTURE(FilterOverheadIsolated, complex_expression, complex_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverheadIsolated, simple_expression, simple_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverheadIsolated, zero_copy_expression, zero_copy_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverheadIsolated, ref_only_expression, ref_only_expression)
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(FilterOverhead, complex_expression, complex_expression)->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverhead, simple_expression, simple_expression)->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverhead, zero_copy_expression, zero_copy_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverhead, ref_only_expression, ref_only_expression)
    ->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow
