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


#include "arrow/util/benchmark_util.h"

#include <condition_variable>
#include <mutex>

namespace arrow {

// Generates batches from data, then benchmark rows_per_second and batches_per_second for
// an isolated node. We do this by passing in batches through a task scheduler, and
// calling InputFinished and InputReceived.

void BenchmarkIsolatedNodeOverhead(benchmark::State& state,
                                   arrow::compute::ExecContext ctx,
                                   arrow::compute::Expression expr, int32_t num_batches,
                                   int32_t batch_size,
                                   arrow::compute::BatchesWithSchema data,
                                   std::string factory_name,
                                   arrow::compute::ExecNodeOptions& options) {
  for (auto _ : state) {
    state.PauseTiming();
    AsyncGenerator<util::optional<arrow::compute::ExecBatch>> sink_gen;

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::compute::ExecPlan> plan,
                         arrow::compute::ExecPlan::Make(&ctx));
    // Source and sink nodes have no effect on the benchmark.
    // Used for dummy purposes as they are referenced in InputReceived and InputFinished.
    ASSERT_OK_AND_ASSIGN(arrow::compute::ExecNode * source_node,
                         MakeExecNode("source", plan.get(), {},
                                      arrow::compute::SourceNodeOptions{
                                          data.schema, data.gen(/*parallel=*/true,
                                                                /*slow=*/false)}));

    ASSERT_OK_AND_ASSIGN(arrow::compute::ExecNode * node,
                         MakeExecNode(factory_name, plan.get(), {source_node}, options));
    MakeExecNode("sink", plan.get(), {node}, arrow::compute::SinkNodeOptions{&sink_gen});

    std::unique_ptr<arrow::compute::TaskScheduler> scheduler =
        arrow::compute::TaskScheduler::Make();
    std::condition_variable all_tasks_finished_cv;
    std::mutex mutex;

    int task_group_id = scheduler->RegisterTaskGroup(
        [&](size_t thread_id, int64_t task_id) {
          node->InputReceived(source_node, data.batches[task_id]);
          return Status::OK();
        },
        [&](size_t thread_id) {
          node->InputFinished(source_node, static_cast<int>(data.batches.size()));
          std::unique_lock<std::mutex> lk(mutex);
          all_tasks_finished_cv.notify_one();
          return Status::OK();
        });
    scheduler->RegisterEnd();

    arrow::compute::ThreadIndexer thread_indexer;

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
    ASSERT_TRUE(node->finished().is_finished());
  }
  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches * batch_size),
      benchmark::Counter::kIsRate);

  state.counters["batches_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches), benchmark::Counter::kIsRate);
}

// Generates batches from data, then benchmark rows_per_second and batches_per_second for
// a source -> node_declarations -> sink sequence.

void BenchmarkNodeOverhead(benchmark::State& state, arrow::compute::ExecContext ctx,
                           int32_t num_batches, int32_t batch_size,
                           arrow::compute::BatchesWithSchema data,
                           std::vector<arrow::compute::Declaration>& node_declarations) {
  for (auto _ : state) {
    state.PauseTiming();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::compute::ExecPlan> plan,
                         arrow::compute::ExecPlan::Make(&ctx));
    AsyncGenerator<util::optional<arrow::compute::ExecBatch>> sink_gen;
    arrow::compute::Declaration source = arrow::compute::Declaration(
        {"source",
         arrow::compute::SourceNodeOptions{data.schema,
                                           data.gen(/*parallel=*/true, /*slow=*/false)},
         "custom_source_label"});
    arrow::compute::Declaration sink = arrow::compute::Declaration(
        {"sink", arrow::compute::SinkNodeOptions{&sink_gen}, "custom_sink_label"});
    std::vector<arrow::compute::Declaration> sequence = {source};
    sequence.insert(sequence.end(), node_declarations.begin(), node_declarations.end());
    sequence.push_back(sink);
    ASSERT_OK(arrow::compute::Declaration::Sequence(sequence).AddToPlan(plan.get()));
    state.ResumeTiming();
    ASSERT_FINISHES_OK(StartAndCollect(plan.get(), sink_gen));
  }

  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches * batch_size),
      benchmark::Counter::kIsRate);

  state.counters["batches_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches), benchmark::Counter::kIsRate);
}

};

