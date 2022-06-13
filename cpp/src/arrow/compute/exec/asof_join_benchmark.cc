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
#include "arrow/filesystem/api.h"
#include "arrow/ipc/api.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

static constexpr int64_t kTotalBatchSize = 1e2;
static const char* time_col = "time_ns";
static const char* key_col = "id";

// Wrapper to enable the use of RecordBatchFileReaders as RecordBatchReaders
class RecordBatchFileReaderWrapper : public arrow::ipc::RecordBatchReader {
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> _reader;
  int _next;

 public:
  virtual ~RecordBatchFileReaderWrapper() {}
  explicit RecordBatchFileReaderWrapper(
      std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader)
      : _reader(reader), _next(0) {}

  virtual arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) {
    // cerr << "ReadNext _next=" << _next << "\n";
    if (_next < _reader->num_record_batches()) {
      ARROW_ASSIGN_OR_RAISE(*batch, _reader->ReadRecordBatch(_next++));
      // cerr << "\t --> " << (*batch)->num_rows() << "\n";
    } else {
      batch->reset();
      // cerr << "\t --> EOF\n";
    }

    return arrow::Status::OK();
  }

  virtual std::shared_ptr<arrow::Schema> schema() const { return _reader->schema(); }
};

static arrow::compute::ExecNode* make_arrow_ipc_reader_node(
    std::shared_ptr<arrow::compute::ExecPlan>& plan,
    std::shared_ptr<arrow::fs::FileSystem>& fs, const std::string& filename) {
  // TODO: error checking
  std::shared_ptr<arrow::io::RandomAccessFile> input = *fs->OpenInputFile(filename);
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> in_reader =
      *arrow::ipc::RecordBatchFileReader::Open(input);
  std::shared_ptr<RecordBatchFileReaderWrapper> reader(
      new RecordBatchFileReaderWrapper(in_reader));

  auto schema = reader->schema();
  auto batch_gen = *arrow::compute::MakeReaderGenerator(
      std::move(reader), arrow::internal::GetCpuThreadPool());

  // cerr << "create source("<<filename<<")\n";
  return *arrow::compute::MakeExecNode(
      "source",    // registered type
      plan.get(),  // execution plan
      {},          // inputs
      arrow::compute::SourceNodeOptions(std::make_shared<arrow::Schema>(*schema),
                                        batch_gen));  // options
}

static void AsOfJoinOverhead(benchmark::State& state, Expression expr,
                             int64_t tolerance) {
  const int32_t batch_size = static_cast<int32_t>(state.range(0));
  const int64_t num_batches = kTotalBatchSize / batch_size;
  const std::string data_directory = "../../../bamboo-streaming/";

  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());

  for (auto _ : state) {
    state.PauseTiming();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::compute::ExecPlan> plan,
                         ExecPlan::Make(&ctx));
    std::shared_ptr<arrow::fs::FileSystem> fs =
        std::make_shared<arrow::fs::LocalFileSystem>();
    arrow::compute::ExecNode* left_table_source =
        make_arrow_ipc_reader_node(plan, fs, data_directory + "left_table.feather");
    arrow::compute::ExecNode* right_table0_source =
        make_arrow_ipc_reader_node(plan, fs, data_directory + "right_table.feather");
    arrow::compute::ExecNode* right_table1_source =
        make_arrow_ipc_reader_node(plan, fs, data_directory + "right_table2.feather");
    ASSERT_OK_AND_ASSIGN(
        arrow::compute::ExecNode * asof_join_node,
        MakeExecNode("asofjoin", plan.get(),
                     {left_table_source, right_table0_source, right_table1_source},
                     AsofJoinNodeOptions{time_col, key_col, tolerance}));
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    MakeExecNode("sink", plan.get(), {asof_join_node}, SinkNodeOptions{&sink_gen});
    state.ResumeTiming();
    ASSERT_FINISHES_OK(StartAndCollect(plan.get(), sink_gen));
  }

  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches * batch_size),
      benchmark::Counter::kIsRate);

  state.counters["batches_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches), benchmark::Counter::kIsRate);
}

static void AsOfJoinOverheadIsolated(benchmark::State& state, Expression expr,
                                     int64_t tolerance) {
  const int32_t batch_size = 100;  // static_cast<int32_t>(state.range(0));
  const int64_t num_batches = 1;   // kTotalBatchSize / batch_size;

  arrow::compute::BatchesWithSchema left_table =
      MakeRandomBatches(schema({field(time_col, int64()), field(key_col, int32()),
                                field("l_v0", float64())}),
                        num_batches, batch_size);
  arrow::compute::BatchesWithSchema right_table0 =
      MakeRandomBatches(schema({field(time_col, int64()), field(key_col, int32()),
                                field("r0_v0", float64())}),
                        num_batches, batch_size);
  arrow::compute::BatchesWithSchema right_table1 =
      MakeRandomBatches(schema({field(time_col, int64()), field(key_col, int32()),
                                field("r1_v0", float32())}),
                        num_batches, batch_size);

  arrow::compute::BatchesWithSchema data = MakeRandomBatches(
      schema({field("i64", int64()), field("bool", boolean())}), num_batches, batch_size);
  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  for (auto _ : state) {
    state.PauseTiming();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::compute::ExecPlan> plan,
                         ExecPlan::Make(&ctx));
    // Source and sink nodes have no effect on the benchmark.
    // Used for dummy purposes as they are referenced in InputReceived and InputFinished.
    AsofJoinNodeOptions join_options(time_col, key_col, tolerance);
    Declaration join{"asofjoin", join_options};
    ASSERT_OK_AND_ASSIGN(arrow::compute::ExecNode * left_table_source,
                         MakeExecNode("source", plan.get(), {},
                                      SourceNodeOptions{left_table.schema,
                                                        left_table.gen(/*parallel=*/true,
                                                                       /*slow=*/false)}));
    ASSERT_OK_AND_ASSIGN(
        arrow::compute::ExecNode * right_table0_source,
        MakeExecNode(
            "source", plan.get(), {},
            SourceNodeOptions{right_table0.schema, right_table0.gen(/*parallel=*/true,
                                                                    /*slow=*/false)}));
    ASSERT_OK_AND_ASSIGN(
        arrow::compute::ExecNode * right_table1_source,
        MakeExecNode(
            "source", plan.get(), {},
            SourceNodeOptions{right_table1.schema, right_table1.gen(/*parallel=*/true,
                                                                    /*slow=*/false)}));
    ASSERT_OK_AND_ASSIGN(
        arrow::compute::ExecNode * asof_join_node,
        MakeExecNode("asofjoin", plan.get(),
                     {left_table_source, right_table0_source, right_table1_source},
                     AsofJoinNodeOptions{time_col, key_col, tolerance}));
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    MakeExecNode("sink", plan.get(), {asof_join_node}, SinkNodeOptions{&sink_gen});

    std::unique_ptr<arrow::compute::TaskScheduler> scheduler = TaskScheduler::Make();

    std::condition_variable left_table_batch_finished_cv;
    std::condition_variable right_table0_batch_finished_cv;
    std::condition_variable right_table1_batch_finished_cv;
    std::mutex mutex_left_table;
    std::mutex mutex_right_table0;
    std::mutex mutex_right_table1;

    auto register_task_group = [&](ExecNode* node, arrow::compute::BatchesWithSchema data,
                                   std::condition_variable& tasks_finished_cv,
                                   std::mutex& mutex) {
      return scheduler->RegisterTaskGroup(
          [&](size_t thread_id, int64_t task_id) {
            std::cout << task_id << " batch wanted with size : " << data.batches.size()
                      << std::endl;
            asof_join_node->InputReceived(node, data.batches[task_id]);
            return Status::OK();
          },
          [&](size_t thread_id) {
            asof_join_node->InputFinished(node, static_cast<int>(data.batches.size()));
            std::unique_lock<std::mutex> lk(mutex);
            tasks_finished_cv.notify_one();
            return Status::OK();
          });
    };

    int task_group_left_table = register_task_group(
        left_table_source, left_table, left_table_batch_finished_cv, mutex_left_table);
    int task_group_right_table0 =
        register_task_group(right_table0_source, right_table0,
                            right_table0_batch_finished_cv, mutex_right_table0);
    int task_group_right_table1 =
        register_task_group(right_table1_source, right_table1,
                            right_table1_batch_finished_cv, mutex_right_table1);

    std::cout << task_group_left_table << " registered" << std::endl;
    std::cout << task_group_right_table0 << " registered" << std::endl;
    std::cout << task_group_right_table1 << " registered" << std::endl;

    scheduler->RegisterEnd();
    ThreadIndexer thread_indexer;

    state.ResumeTiming();
    arrow::internal::ThreadPool* thread_pool = arrow::internal::GetCpuThreadPool();
    ASSERT_OK(scheduler->StartScheduling(
        thread_indexer(),
        [&](std::function<Status(size_t)> task) -> Status {
          std::cout << "attempting to spawn task" << std::endl;
          return thread_pool->Spawn([&, task]() {
            size_t tid = thread_indexer();
            ARROW_DCHECK_OK(task(tid));
          });
        },
        thread_pool->GetCapacity(),
        /*use_sync_execution=*/false));
    std::unique_lock<std::mutex> lk(mutex_left_table);
    std::unique_lock<std::mutex> lk1(mutex_right_table0);
    std::unique_lock<std::mutex> lk2(mutex_right_table1);

    std::cout << "reached mutex" << std::endl;

    ASSERT_OK(scheduler->StartTaskGroup(0, task_group_left_table, num_batches));
    std::cout << "start tg 0" << std::endl;
    ASSERT_OK(scheduler->StartTaskGroup(1, task_group_right_table0, num_batches));
    std::cout << "start tg 1" << std::endl;
    ASSERT_OK(scheduler->StartTaskGroup(2, task_group_right_table1, num_batches));
    std::cout << "start tg 2" << std::endl;

    left_table_batch_finished_cv.wait(lk);
    right_table0_batch_finished_cv.wait(lk1);
    right_table1_batch_finished_cv.wait(lk2);

    ASSERT_TRUE(asof_join_node->finished().is_finished());
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
      ->Range(10, kTotalBatchSize)
      ->UseRealTime();
}
/*
BENCHMARK_CAPTURE(AsOfJoinOverheadIsolated, complex_expression, complex_expression, 1000)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverheadIsolated, simple_expression, simple_expression, 1000)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverheadIsolated, zero_copy_expression, zero_copy_expression,
                  1000)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverheadIsolated, ref_only_expression, ref_only_expression,
                  1000)
    ->Apply(SetArgs);
*/
BENCHMARK_CAPTURE(AsOfJoinOverhead, complex_expression, complex_expression, 1000)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, simple_expression, simple_expression, 1000)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, zero_copy_expression, zero_copy_expression, 1000)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, ref_only_expression, ref_only_expression, 1000)
    ->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow
