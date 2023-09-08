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

#include "arrow/acero/benchmark_util.h"

#include <condition_variable>
#include <memory>
#include <mutex>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/task_util.h"
#include "arrow/acero/util.h"
#include "arrow/util/macros.h"

namespace arrow {

namespace acero {

// Generates batches from data, then benchmark rows_per_second and batches_per_second for
// an isolated node. We do this by passing in batches through a task scheduler, and
// calling InputFinished and InputReceived.

Status BenchmarkIsolatedNodeOverhead(
    benchmark::State& state, arrow::compute::Expression expr, int32_t num_batches,
    int32_t batch_size, arrow::acero::BatchesWithSchema data, std::string factory_name,
    arrow::acero::ExecNodeOptions& options, arrow::MemoryPool* pool) {
  for (auto _ : state) {
    state.PauseTiming();
    AsyncGenerator<std::optional<arrow::compute::ExecBatch>> sink_gen;

    ExecContext ctx(pool, ::arrow::internal::GetCpuThreadPool());

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::acero::ExecPlan> plan,
                          arrow::acero::ExecPlan::Make(ctx));
    // Source and sink nodes have no effect on the benchmark.
    // Used for dummy purposes as they are referenced in InputReceived and InputFinished.
    ARROW_ASSIGN_OR_RAISE(arrow::acero::ExecNode * source_node,
                          MakeExecNode("source", plan.get(), {},
                                       arrow::acero::SourceNodeOptions{
                                           data.schema, data.gen(/*parallel=*/true,
                                                                 /*slow=*/false)}));

    ARROW_ASSIGN_OR_RAISE(arrow::acero::ExecNode * node,
                          MakeExecNode(factory_name, plan.get(), {source_node}, options));
    ARROW_RETURN_NOT_OK(MakeExecNode("sink", plan.get(), {node},
                                     arrow::acero::SinkNodeOptions{&sink_gen}));

    std::unique_ptr<arrow::acero::TaskScheduler> scheduler =
        arrow::acero::TaskScheduler::Make();
    std::condition_variable all_tasks_finished_cv;
    std::mutex mutex;

    int task_group_id = scheduler->RegisterTaskGroup(
        [&](size_t thread_id, int64_t task_id) {
          return node->InputReceived(source_node, data.batches[task_id]);
        },
        [&](size_t thread_id) {
          RETURN_NOT_OK(
              node->InputFinished(source_node, static_cast<int>(data.batches.size())));
          std::unique_lock<std::mutex> lk(mutex);
          all_tasks_finished_cv.notify_one();
          return Status::OK();
        });
    scheduler->RegisterEnd();

    arrow::acero::ThreadIndexer thread_indexer;

    state.ResumeTiming();
    arrow::internal::ThreadPool* thread_pool = arrow::internal::GetCpuThreadPool();
    ARROW_RETURN_NOT_OK(scheduler->StartScheduling(
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
    ARROW_RETURN_NOT_OK(
        scheduler->StartTaskGroup(thread_indexer(), task_group_id, num_batches));
    all_tasks_finished_cv.wait(lk);
  }
  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches * batch_size),
      benchmark::Counter::kIsRate);

  state.counters["batches_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches), benchmark::Counter::kIsRate);
  return Status::OK();
}

// Generates batches from data, then benchmark rows_per_second and batches_per_second for
// a source -> node_declarations -> sink sequence.

Status BenchmarkNodeOverhead(benchmark::State& state, int32_t num_batches,
                             int32_t batch_size, arrow::acero::BatchesWithSchema data,
                             std::vector<arrow::acero::Declaration>& node_declarations,
                             MemoryPool* pool) {
  ExecContext ctx(pool, ::arrow::internal::GetCpuThreadPool());
  for (auto _ : state) {
    state.PauseTiming();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::acero::ExecPlan> plan,
                          arrow::acero::ExecPlan::Make(ctx));
    AsyncGenerator<std::optional<arrow::compute::ExecBatch>> sink_gen;
    arrow::acero::Declaration source = arrow::acero::Declaration(
        {"source",
         arrow::acero::SourceNodeOptions{data.schema,
                                         data.gen(/*parallel=*/true, /*slow=*/false)},
         "custom_source_label"});
    arrow::acero::Declaration sink = arrow::acero::Declaration(
        {"sink", arrow::acero::SinkNodeOptions{&sink_gen}, "custom_sink_label"});
    std::vector<arrow::acero::Declaration> sequence = {source};
    sequence.insert(sequence.end(), node_declarations.begin(), node_declarations.end());
    sequence.push_back(sink);
    ARROW_RETURN_NOT_OK(
        arrow::acero::Declaration::Sequence(sequence).AddToPlan(plan.get()));
    state.ResumeTiming();
    ARROW_RETURN_NOT_OK(StartAndCollect(plan.get(), sink_gen).status());
  }

  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches * batch_size),
      benchmark::Counter::kIsRate);

  state.counters["batches_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches), benchmark::Counter::kIsRate);
  return Status::OK();
}

}  // namespace acero
}  // namespace arrow
