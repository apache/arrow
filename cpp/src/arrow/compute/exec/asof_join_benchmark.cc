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

#include <stdio.h>
#include <stdlib.h>
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
static bool createdBenchmarkFiles = false;

// requires export PYTHONPATH=/path/to/bamboo-streaming
// calls generate_benchmark_files to create tables (feather files) varying in frequency,
// width, key density for benchmarks. places generated files in benchmark/data. This
// operation runs once at the beginning of benchmarking.
static void DoSetup() {
  if (!createdBenchmarkFiles) {
    system("mkdir benchmark_data/"); 
    system("python3 ~/summer/bamboo-streaming/bamboo/generate_benchmark_files.py");
    createdBenchmarkFiles = true;
  }
}

static void DoTeardown() { system("rm -rf benchmark_data/"); }

static std::vector<std::string> generateRightHandTables(std::string freq, int width_index,
                                                        int num_tables,
                                                        int num_ids_index) {
  auto const generate_file_name = [](std::string freq, std::string is_wide,
                                     std::string num_ids, std::string num) {
    return freq + "_" + is_wide + "_" + num_ids + num + ".feather";
  };

  std::string width_table[] = {"5_cols", "20_cols", "100_cols", "500_cols", "1000_cols"};   // 0 - 7
  std::string num_ids_table[] = {"100_ids", "500_ids", "1000_ids", "5000_ids", "10000_ids"};  // 0 - 5

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

static void TableJoinOverhead(benchmark::State& state, std::string left_table,
                              std::vector<std::string> right_tables,
                              std::string factory_name, ExecNodeOptions& options) {
  const std::string data_directory = "./benchmark_data/";
  DoSetup();
  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  // std::cout << "beginning test for " << left_table << " and " << right_tables[0] << " "
  // << factory_name << std::endl; std::cout << "starting with " <<
  // ctx.memory_pool()->bytes_allocated() << std::endl;
  int64_t rows;
  int64_t bytes;
  for (auto _ : state) {
    state.PauseTiming();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::compute::ExecPlan> plan,
                         ExecPlan::Make(&ctx));
    std::shared_ptr<arrow::fs::FileSystem> fs =
        std::make_shared<arrow::fs::LocalFileSystem>();
    arrow::compute::ExecNode* left_table_source;
    int64_t left_table_rows;
    int left_table_batches;
    size_t left_table_bytes;
    std::tie(left_table_source, left_table_rows, left_table_batches, left_table_bytes) =
        make_arrow_ipc_reader_node(plan, fs, data_directory + left_table);
    std::vector<ExecNode*> inputs = {left_table_source};
    int right_hand_rows = 0;
    int64_t right_hand_bytes = 0;
    for (std::string right_table : right_tables) {
      arrow::compute::ExecNode* right_table_source;
      int64_t right_table_rows;
      int right_table_batches;
      size_t right_table_bytes;
      std::tie(right_table_source, right_table_rows, right_table_batches, right_table_bytes) =
          make_arrow_ipc_reader_node(plan, fs, data_directory + right_table);
      inputs.push_back(right_table_source);
      right_hand_rows += right_table_rows;
      right_hand_bytes += right_table_bytes;
    }
    rows = left_table_rows + right_hand_rows;
    bytes = left_table_bytes + right_hand_bytes;
    ASSERT_OK_AND_ASSIGN(arrow::compute::ExecNode * asof_join_node,
                         MakeExecNode(factory_name, plan.get(), inputs, options));

    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    MakeExecNode("sink", plan.get(), {asof_join_node}, SinkNodeOptions{&sink_gen});
    state.ResumeTiming();
    // std::cout << "starting and collecting with " <<
    // ctx.memory_pool()->bytes_allocated() << std::endl;
    ASSERT_FINISHES_OK(StartAndCollect(plan.get(), sink_gen));
    // std::cout << "finishing with " << ctx.memory_pool()->bytes_allocated() <<
    // std::endl;
  }
  // std::cout << "reporting with " << ctx.memory_pool()->bytes_allocated() << std::endl;
  state.counters["total_rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * rows), benchmark::Counter::kIsRate);

  state.counters["total_bytes_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * bytes), benchmark::Counter::kIsRate);
}

static void AsOfJoinOverhead(benchmark::State& state, std::string left_table,
                             std::vector<std::string> right_tables) {
  int64_t tolerance = 0;
  AsofJoinNodeOptions options = AsofJoinNodeOptions(time_col, key_col, tolerance);
  TableJoinOverhead(state, left_table, right_tables, "asofjoin", options);
}

static void HashJoinOverhead(benchmark::State& state, std::string left_table,
                             std::vector<std::string> right_tables) {
  HashJoinNodeOptions options =
      HashJoinNodeOptions({time_col, key_col}, {time_col, key_col});
  TableJoinOverhead(state, left_table, right_tables, "hashjoin", options);
}

// this generates the set of right hand tables to test on.
void SetArgs(benchmark::internal::Benchmark* bench) { bench->UseRealTime(); }

BENCHMARK_CAPTURE(HashJoinOverhead, | 5m | 20_cols | 500_ids | 1_join | 5m | 20_cols | 500_ids |,
        "5m_20_cols_500_ids0.feather",
        generateRightHandTables("5m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 500_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 3, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 500_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_500_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 5m | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("5m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 10000_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_10000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 5_cols | 500_ids | 1_join | 30m | 5_cols | 500_ids |,
        "30m_5_cols_500_ids0.feather",
        generateRightHandTables("30m", 0, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 100_ids | 1_join | 30m | 20_cols | 10000_ids |,
        "30m_20_cols_100_ids0.feather",
        generateRightHandTables("30m", 1, 1, 4))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 1m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "1m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 100_cols | 500_ids | 1_join | 30m | 100_cols | 500_ids |,
        "30m_100_cols_500_ids0.feather",
        generateRightHandTables("30m", 2, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 100_cols | 500_ids | 1_join | 30m | 500_cols | 500_ids |,
        "30m_100_cols_500_ids0.feather",
        generateRightHandTables("30m", 3, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 5_cols | 500_ids | 1_join | 30m | 1000_cols | 500_ids |,
        "30m_5_cols_500_ids0.feather",
        generateRightHandTables("30m", 4, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 5000_ids | 1_join | 30m | 20_cols | 1000_ids |,
        "30m_20_cols_5000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 2))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 1000_cols | 500_ids | 1_join | 30m | 1000_cols | 500_ids |,
        "30m_1000_cols_500_ids0.feather",
        generateRightHandTables("30m", 4, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 1d | 20_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "1d_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 5000_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_5000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 1000_cols | 500_ids | 1_join | 30m | 5_cols | 500_ids |,
        "30m_1000_cols_500_ids0.feather",
        generateRightHandTables("30m", 0, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 1d | 20_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "1d_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 1d | 20_cols | 500_ids | 1_join | 5m | 20_cols | 500_ids |,
        "1d_20_cols_500_ids0.feather",
        generateRightHandTables("5m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 5m | 20_cols | 500_ids | 1_join | 1m | 20_cols | 500_ids |,
        "5m_20_cols_500_ids0.feather",
        generateRightHandTables("1m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 1m | 20_cols | 500_ids | 1_join | 1m | 20_cols | 500_ids |,
        "1m_20_cols_500_ids0.feather",
        generateRightHandTables("1m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 100_ids | 1_join | 30m | 20_cols | 1000_ids |,
        "30m_20_cols_100_ids0.feather",
        generateRightHandTables("30m", 1, 1, 2))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 5m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "5m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 5_cols | 500_ids | 1_join | 30m | 100_cols | 500_ids |,
        "30m_5_cols_500_ids0.feather",
        generateRightHandTables("30m", 2, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 5000_ids | 1_join | 30m | 20_cols | 10000_ids |,
        "30m_20_cols_5000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 4))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 100_ids | 1_join | 30m | 20_cols | 1000_ids |,
        "30m_20_cols_100_ids0.feather",
        generateRightHandTables("30m", 1, 1, 2))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 1000_ids | 1_join | 30m | 20_cols | 10000_ids |,
        "30m_20_cols_1000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 4))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 5_cols | 500_ids | 1_join | 30m | 5_cols | 500_ids |,
        "30m_5_cols_500_ids0.feather",
        generateRightHandTables("30m", 0, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 500_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_500_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 1m | 20_cols | 500_ids | 1_join | 5m | 20_cols | 500_ids |,
        "1m_20_cols_500_ids0.feather",
        generateRightHandTables("5m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 100_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 2, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 500_cols | 500_ids | 1_join | 30m | 5_cols | 500_ids |,
        "30m_500_cols_500_ids0.feather",
        generateRightHandTables("30m", 0, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 1000_cols | 500_ids | 1_join | 30m | 1000_cols | 500_ids |,
        "30m_1000_cols_500_ids0.feather",
        generateRightHandTables("30m", 4, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 5000_ids | 1_join | 30m | 20_cols | 5000_ids |,
        "30m_20_cols_5000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 3))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 10000_ids | 1_join | 30m | 20_cols | 100_ids |,
        "30m_20_cols_10000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 0))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 10000_ids | 1_join | 30m | 20_cols | 5000_ids |,
        "30m_20_cols_10000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 3))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 1000_ids | 1_join | 30m | 20_cols | 5000_ids |,
        "30m_20_cols_1000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 3))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 100_ids | 1_join | 30m | 20_cols | 100_ids |,
        "30m_20_cols_100_ids0.feather",
        generateRightHandTables("30m", 1, 1, 0))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 500_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 3, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 100_cols | 500_ids | 1_join | 30m | 100_cols | 500_ids |,
        "30m_100_cols_500_ids0.feather",
        generateRightHandTables("30m", 2, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 1m | 20_cols | 500_ids | 1_join | 1d | 20_cols | 500_ids |,
        "1m_20_cols_500_ids0.feather",
        generateRightHandTables("1d", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 10000_ids | 1_join | 30m | 20_cols | 10000_ids |,
        "30m_20_cols_10000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 4))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 500_cols | 500_ids | 1_join | 30m | 5_cols | 500_ids |,
        "30m_500_cols_500_ids0.feather",
        generateRightHandTables("30m", 0, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 5m | 20_cols | 500_ids | 1_join | 5m | 20_cols | 500_ids |,
        "5m_20_cols_500_ids0.feather",
        generateRightHandTables("5m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 1d | 20_cols | 500_ids | 1_join | 5m | 20_cols | 500_ids |,
        "1d_20_cols_500_ids0.feather",
        generateRightHandTables("5m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 5_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_5_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 5000_ids | 1_join | 30m | 20_cols | 1000_ids |,
        "30m_20_cols_5000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 2))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 5m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "5m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 5_cols | 500_ids | 1_join | 30m | 1000_cols | 500_ids |,
        "30m_5_cols_500_ids0.feather",
        generateRightHandTables("30m", 4, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 10000_ids | 1_join | 30m | 20_cols | 100_ids |,
        "30m_20_cols_10000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 0))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 500_cols | 500_ids | 1_join | 30m | 100_cols | 500_ids |,
        "30m_500_cols_500_ids0.feather",
        generateRightHandTables("30m", 2, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 1d | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("1d", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 5m | 20_cols | 500_ids | 1_join | 1d | 20_cols | 500_ids |,
        "5m_20_cols_500_ids0.feather",
        generateRightHandTables("1d", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 1d | 20_cols | 500_ids | 1_join | 1m | 20_cols | 500_ids |,
        "1d_20_cols_500_ids0.feather",
        generateRightHandTables("1m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 1000_cols | 500_ids | 1_join | 30m | 100_cols | 500_ids |,
        "30m_1000_cols_500_ids0.feather",
        generateRightHandTables("30m", 2, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 1000_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 4, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 1m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "1m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 1d | 20_cols | 500_ids | 1_join | 1d | 20_cols | 500_ids |,
        "1d_20_cols_500_ids0.feather",
        generateRightHandTables("1d", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 10000_ids | 1_join | 30m | 20_cols | 1000_ids |,
        "30m_20_cols_10000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 2))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 1000_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 4, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 100_ids | 1_join | 30m | 20_cols | 5000_ids |,
        "30m_20_cols_100_ids0.feather",
        generateRightHandTables("30m", 1, 1, 3))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 100_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_100_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 1d | 20_cols | 500_ids | 1_join | 1m | 20_cols | 500_ids |,
        "1d_20_cols_500_ids0.feather",
        generateRightHandTables("1m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 10000_ids | 1_join | 30m | 20_cols | 1000_ids |,
        "30m_20_cols_10000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 2))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 1000_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 2))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 100_cols | 500_ids | 1_join | 30m | 500_cols | 500_ids |,
        "30m_100_cols_500_ids0.feather",
        generateRightHandTables("30m", 3, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 1000_ids | 1_join | 30m | 20_cols | 5000_ids |,
        "30m_20_cols_1000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 3))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 100_cols | 500_ids | 1_join | 30m | 1000_cols | 500_ids |,
        "30m_100_cols_500_ids0.feather",
        generateRightHandTables("30m", 4, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 21_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 21, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 41_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 41, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 1d | 20_cols | 500_ids | 1_join | 1d | 20_cols | 500_ids |,
        "1d_20_cols_500_ids0.feather",
        generateRightHandTables("1d", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 100_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 0))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 1000_ids | 1_join | 30m | 20_cols | 100_ids |,
        "30m_20_cols_1000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 0))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 10000_ids | 1_join | 30m | 20_cols | 10000_ids |,
        "30m_20_cols_10000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 4))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 100_ids | 1_join | 30m | 20_cols | 5000_ids |,
        "30m_20_cols_100_ids0.feather",
        generateRightHandTables("30m", 1, 1, 3))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 10000_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 4))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 1m | 20_cols | 500_ids | 1_join | 5m | 20_cols | 500_ids |,
        "1m_20_cols_500_ids0.feather",
        generateRightHandTables("5m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 1000_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_1000_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 1000_ids | 1_join | 30m | 20_cols | 1000_ids |,
        "30m_20_cols_1000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 2))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 500_cols | 500_ids | 1_join | 30m | 500_cols | 500_ids |,
        "30m_500_cols_500_ids0.feather",
        generateRightHandTables("30m", 3, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 100_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 0))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 5000_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 3))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 5000_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_5000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 10000_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_10000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 1d | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("1d", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 100_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_100_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 5000_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 3))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 100_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 2, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 1000_cols | 500_ids | 1_join | 30m | 500_cols | 500_ids |,
        "30m_1000_cols_500_ids0.feather",
        generateRightHandTables("30m", 3, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 5_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 0, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 100_cols | 500_ids | 1_join | 30m | 1000_cols | 500_ids |,
        "30m_100_cols_500_ids0.feather",
        generateRightHandTables("30m", 4, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 5000_ids | 1_join | 30m | 20_cols | 5000_ids |,
        "30m_20_cols_5000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 3))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 100_ids | 1_join | 30m | 20_cols | 10000_ids |,
        "30m_20_cols_100_ids0.feather",
        generateRightHandTables("30m", 1, 1, 4))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 100_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_100_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 5000_ids | 1_join | 30m | 20_cols | 100_ids |,
        "30m_20_cols_5000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 0))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 1m | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("1m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 500_cols | 500_ids | 1_join | 30m | 1000_cols | 500_ids |,
        "30m_500_cols_500_ids0.feather",
        generateRightHandTables("30m", 4, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 1m | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("1m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 5m | 20_cols | 500_ids | 1_join | 1d | 20_cols | 500_ids |,
        "5m_20_cols_500_ids0.feather",
        generateRightHandTables("1d", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 31_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 31, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 1000_cols | 500_ids | 1_join | 30m | 5_cols | 500_ids |,
        "30m_1000_cols_500_ids0.feather",
        generateRightHandTables("30m", 0, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 1000_ids | 1_join | 30m | 20_cols | 10000_ids |,
        "30m_20_cols_1000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 4))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 5_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_5_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 100_ids | 1_join | 30m | 20_cols | 100_ids |,
        "30m_20_cols_100_ids0.feather",
        generateRightHandTables("30m", 1, 1, 0))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 1m | 20_cols | 500_ids | 1_join | 1m | 20_cols | 500_ids |,
        "1m_20_cols_500_ids0.feather",
        generateRightHandTables("1m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 100_cols | 500_ids | 1_join | 30m | 5_cols | 500_ids |,
        "30m_100_cols_500_ids0.feather",
        generateRightHandTables("30m", 0, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 500_cols | 500_ids | 1_join | 30m | 500_cols | 500_ids |,
        "30m_500_cols_500_ids0.feather",
        generateRightHandTables("30m", 3, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 1000_cols | 500_ids | 1_join | 30m | 100_cols | 500_ids |,
        "30m_1000_cols_500_ids0.feather",
        generateRightHandTables("30m", 2, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 1000_cols | 500_ids | 1_join | 30m | 500_cols | 500_ids |,
        "30m_1000_cols_500_ids0.feather",
        generateRightHandTables("30m", 3, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 5_cols | 500_ids | 1_join | 30m | 500_cols | 500_ids |,
        "30m_5_cols_500_ids0.feather",
        generateRightHandTables("30m", 3, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 1000_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_1000_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 1m | 20_cols | 500_ids | 1_join | 1d | 20_cols | 500_ids |,
        "1m_20_cols_500_ids0.feather",
        generateRightHandTables("1d", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 5m | 20_cols | 500_ids | 1_join | 1m | 20_cols | 500_ids |,
        "5m_20_cols_500_ids0.feather",
        generateRightHandTables("1m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 5_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 0, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 500_cols | 500_ids | 1_join | 30m | 100_cols | 500_ids |,
        "30m_500_cols_500_ids0.feather",
        generateRightHandTables("30m", 2, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 1000_ids | 1_join | 30m | 20_cols | 1000_ids |,
        "30m_20_cols_1000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 2))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 1000_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_1000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 1000_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 2))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 10000_ids | 1_join | 30m | 20_cols | 5000_ids |,
        "30m_20_cols_10000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 3))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 500_cols | 500_ids | 1_join | 30m | 1000_cols | 500_ids |,
        "30m_500_cols_500_ids0.feather",
        generateRightHandTables("30m", 4, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 100_cols | 500_ids | 1_join | 30m | 5_cols | 500_ids |,
        "30m_100_cols_500_ids0.feather",
        generateRightHandTables("30m", 0, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 5_cols | 500_ids | 1_join | 30m | 500_cols | 500_ids |,
        "30m_5_cols_500_ids0.feather",
        generateRightHandTables("30m", 3, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 30m | 20_cols | 10000_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 4))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 5000_ids | 1_join | 30m | 20_cols | 100_ids |,
        "30m_20_cols_5000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 0))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 1000_ids | 1_join | 30m | 20_cols | 100_ids |,
        "30m_20_cols_1000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 0))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 5000_ids | 1_join | 30m | 20_cols | 10000_ids |,
        "30m_20_cols_5000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 4))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 11_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 11, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 5_cols | 500_ids | 1_join | 30m | 100_cols | 500_ids |,
        "30m_5_cols_500_ids0.feather",
        generateRightHandTables("30m", 2, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | 30m | 20_cols | 500_ids | 1_join | 5m | 20_cols | 500_ids |,
        "30m_20_cols_500_ids0.feather",
        generateRightHandTables("5m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 100_cols | 500_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_100_cols_500_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | 30m | 20_cols | 1000_ids | 1_join | 30m | 20_cols | 500_ids |,
        "30m_20_cols_1000_ids0.feather",
        generateRightHandTables("30m", 1, 1, 1))->Apply(SetArgs);


}  // namespace compute
}  // namespace arrow
