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
#include <string>

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

static const char* time_col = "time";
static const char* key_col = "id";

static std::vector<std::string> generateRightHandTables(std::string freq, int width_index,
                                                        int num_tables,
                                                        int num_ids_index) {
  auto const generate_file_name = [](std::string freq, std::string is_wide,
                                     std::string num_ids, std::string num) {
    return freq + "_" + is_wide + "_" + num_ids + num + ".feather";
  };

  std::string width_table[] = {"1_cols",  "10_cols", "20_cols",
                               "40_cols", "80_cols", "100_cols"};   // 0 - 5
  std::string num_ids_table[] = {"100_ids", "2000_ids", "5k_ids"};  // 0 - 2

  std::string wide_string = width_table[width_index];
  std::string ids = num_ids_table[num_ids_index];

  std::vector<std::string> right_hand_tables;
  for (int j = 1; j <= num_tables; j++) {
    right_hand_tables.push_back(
        generate_file_name(freq, wide_string, ids, std::to_string(j)));
  }
  return right_hand_tables;
}

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
    // cout << "ReadNext _next=" << _next << "\n";
    if (_next < _reader->num_record_batches()) {
      ARROW_ASSIGN_OR_RAISE(*batch, _reader->ReadRecordBatch(_next++));
      // cout << "\t --> " << (*batch)->num_rows() << "\n";
    } else {
      batch->reset();
      // cout << "\t --> EOF\n";
    }

    return arrow::Status::OK();
  }

  virtual std::shared_ptr<arrow::Schema> schema() const { return _reader->schema(); }
};

static std::tuple<arrow::compute::ExecNode*, int64_t, int, size_t>
make_arrow_ipc_reader_node(std::shared_ptr<arrow::compute::ExecPlan>& plan,
                           std::shared_ptr<arrow::fs::FileSystem>& fs,
                           const std::string& filename) {
  // TODO: error checking
  std::shared_ptr<arrow::io::RandomAccessFile> input = *fs->OpenInputFile(filename);
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> in_reader =
      *arrow::ipc::RecordBatchFileReader::Open(input);
  std::shared_ptr<RecordBatchFileReaderWrapper> reader(
      new RecordBatchFileReaderWrapper(in_reader));

  auto schema = reader->schema();
  // we assume there is a time field represented in uint64, a key field of int32, and the
  // remaining fields are float64.
  size_t row_size =
      sizeof(_Float64) * (schema->num_fields() - 2) + sizeof(int64_t) + sizeof(int32_t);
  auto batch_gen = *arrow::compute::MakeReaderGenerator(
      std::move(reader), arrow::internal::GetCpuThreadPool());
  int64_t rows = in_reader->CountRows().ValueOrDie();
  // cout << "create source("<<filename<<")\n";
  return {*arrow::compute::MakeExecNode(
              "source",    // registered type
              plan.get(),  // execution plan
              {},          // inputs
              arrow::compute::SourceNodeOptions(
                  std::make_shared<arrow::Schema>(*schema),  // options, )
                  batch_gen)),
          rows, in_reader->num_record_batches(), row_size * rows};
}

static void AsOfJoinOverhead(benchmark::State& state, std::string left_table,
                             std::vector<std::string> right_tables) {
  int64_t tolerance = 0;
  const std::string data_directory = "../../../bamboo-streaming/";

  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  int64_t rows;
  int64_t bytes;
  for (auto _ : state) {
    state.PauseTiming();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::compute::ExecPlan> plan,
                         ExecPlan::Make(&ctx));
    std::shared_ptr<arrow::fs::FileSystem> fs =
        std::make_shared<arrow::fs::LocalFileSystem>();
    auto [left_table_source, left_table_rows, left_table_batches, left_table_bytes] =
        make_arrow_ipc_reader_node(plan, fs, data_directory + left_table);
    std::vector<ExecNode*> inputs = {left_table_source};
    int right_hand_rows = 0;
    int64_t right_hand_bytes = 0;
    for (std::string right_table : right_tables) {
      auto [right_table_source, right_table_rows, right_table_batches,
            right_table_bytes] =
          make_arrow_ipc_reader_node(plan, fs, data_directory + right_table);
      inputs.push_back(right_table_source);
      right_hand_rows += right_table_rows;
      right_hand_bytes += right_table_bytes;
    }

    rows = left_table_rows + right_hand_rows;
    bytes = left_table_bytes + right_hand_bytes;

    ASSERT_OK_AND_ASSIGN(arrow::compute::ExecNode * asof_join_node,
                         MakeExecNode("asofjoin", plan.get(), inputs,
                                      AsofJoinNodeOptions(time_col, key_col, tolerance)));

    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    MakeExecNode("sink", plan.get(), {asof_join_node}, SinkNodeOptions{&sink_gen});
    state.ResumeTiming();
    ASSERT_FINISHES_OK(StartAndCollect(plan.get(), sink_gen));
  }

  state.counters["total_rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * rows), benchmark::Counter::kIsRate);

  state.counters["total_bytes_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * bytes), benchmark::Counter::kIsRate);
}

static void HashJoinOverhead(benchmark::State& state, std::string left_table,
                             std::vector<std::string> right_tables) {
  int64_t tolerance = 0;
  const std::string data_directory = "../../../bamboo-streaming/";

  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  int64_t rows;
  size_t bytes;
  for (auto _ : state) {
    state.PauseTiming();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::compute::ExecPlan> plan,
                         ExecPlan::Make(&ctx));
    std::shared_ptr<arrow::fs::FileSystem> fs =
        std::make_shared<arrow::fs::LocalFileSystem>();
    auto [left_table_source, left_table_rows, left_table_batches, left_table_bytes] =
        make_arrow_ipc_reader_node(plan, fs, data_directory + left_table);

    std::vector<ExecNode*> inputs = {left_table_source};
    int right_hand_rows = 0;
    size_t right_hand_bytes = 0;
    for (std::string right_table : right_tables) {
      auto [right_table_source, right_table_rows, right_table_batches,
            right_table_bytes] =
          make_arrow_ipc_reader_node(plan, fs, data_directory + right_table);
      inputs.push_back(right_table_source);
      right_hand_rows += right_table_rows;
      right_hand_bytes += right_table_bytes;
    }

    rows = left_table_rows + right_hand_rows;
    bytes = left_table_bytes + right_hand_bytes;

    ASSERT_OK_AND_ASSIGN(
        arrow::compute::ExecNode * asof_join_node,
        MakeExecNode("hashjoin", plan.get(), inputs,
                     HashJoinNodeOptions({time_col, key_col}, {time_col, key_col})));

    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    MakeExecNode("sink", plan.get(), {asof_join_node}, SinkNodeOptions{&sink_gen});
    state.ResumeTiming();
    ASSERT_FINISHES_OK(StartAndCollect(plan.get(), sink_gen));
  }

  state.counters["total_rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * rows), benchmark::Counter::kIsRate);

  state.counters["total_bytes_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * bytes), benchmark::Counter::kIsRate);
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

// this generates the set of right hand tables to test on.
void SetArgs(benchmark::internal::Benchmark* bench) { bench->UseRealTime(); }

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
/*
    std::string width_table[] = {"1_cols", "10_cols", "20_cols", "40_cols", "80_cols",
   "100_cols"}; // 0 - 5 std::string num_ids_table[] = {"100_ids", "2000_ids", "5k_ids"};
   // 0 - 2
*/
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 100_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 100_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 100_cols | 100_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 1, 0))
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 2, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 2, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | high_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 2, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 2, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 2, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | high_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 2, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 2, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 2, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | high_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 2, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 100_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 2, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 100_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 2, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | high_freq | 100_cols | 100_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 2, 0))
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 4, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 4, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | high_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 4, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 4, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 4, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | high_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 4, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 4, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 4, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | high_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 4, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 100_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 4, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 100_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 4, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | high_freq | 100_cols | 100_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 4, 0))
    ->Apply(SetArgs);

//

BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 20_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 20_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 20_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 80_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 80_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 80_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | mid_freq | 1_cols | 5k_ids | 1_join | high_freq |
                                        100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 1, 1))
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 2, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 2, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | high_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 2, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 20_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 2, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 20_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 2, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | high_freq | 20_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 2, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 80_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 2, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 80_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 2, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | high_freq | 80_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 2, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 2, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 2, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | mid_freq | 1_cols | 5k_ids | 2_join | high_freq |
                                        100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 2, 1))
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 4, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 4, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | high_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 4, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 20_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 4, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 20_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 4, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | high_freq | 20_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 4, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 80_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 4, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 80_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 4, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | high_freq | 80_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 4, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 4, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 4, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | mid_freq | 1_cols | 5k_ids | 4_join | high_freq |
                                        100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 4, 1))
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 1, 2))
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 2, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 2, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | high_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 2, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 2, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 2, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | high_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 2, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 2, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 2, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | high_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 2, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | low_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 2, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | mid_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 2, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 2_join | high_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 2, 2))
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 4, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 4, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | high_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 4, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 4, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 4, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | high_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 4, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 4, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 4, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | high_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 4, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | low_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 4, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | mid_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 4, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 4_join | high_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 4, 2))
    ->Apply(SetArgs);

// asymmetric joins
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | high_freq | 20_cols | 2000_ids | 1_join | low_freq | 20_cols | 2000_ids |,
                  "high_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("low_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | low_freq | 20_cols | 2000_ids | 1_join | high_freq | 20_cols | 2000_ids |,
                  "low_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("high_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 20_cols | 100_ids | 1_join | mid_freq | 20_cols | 5k_ids |,
                  "mid_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 20_cols | 5k_ids | 1_join | mid_freq | 20_cols | 100_ids |,
                  "mid_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 100_cols | 2000_ids | 1_join | mid_freq | 20_cols | 2000_ids |,
                  "mid_freq_100_cols_2000_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | mid_freq | 20_cols | 2000_ids | 1_join | mid_freq | 100_cols | 2000_ids |,
                  "mid_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead,
                  | low_freq | 20_cols | 2000_ids | 4_join | high_freq | 20_cols | 2000_ids |,
                  "low_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("high_freq", 2, 4, 1))
    ->Apply(SetArgs);
}  // namespace compute
}  // namespace arrow
