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

static void DoSetup() {
  // requires export PYTHONPATH=/path/to/bamboo-streaming
  // calls generate_benchmark_files to create tables varying in frequency, width, key density for benchmarks.
  if (!createdBenchmarkFiles) {
    system("mkdir benchmark_data/");
    // system("python3 ~/summer/bamboo-streaming/bamboo/generate_benchmark_files.py");
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

static void TableOverhead(benchmark::State& state, std::string left_table,
                          std::vector<std::string> right_tables, std::string factory_name,
                          ExecNodeOptions& options) {
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
    tie(left_table_source, left_table_rows, left_table_batches, left_table_bytes) =
        make_arrow_ipc_reader_node(plan, fs, data_directory + left_table);
    std::vector<ExecNode*> inputs = {left_table_source};
    int right_hand_rows = 0;
    int64_t right_hand_bytes = 0;
    for (std::string right_table : right_tables) {
      arrow::compute::ExecNode* right_table_source;
      int64_t right_table_rows;
      int right_table_batches;
      size_t right_table_bytes;
      tie(right_table_source, right_table_rows, right_table_batches, right_table_bytes) =
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
  TableOverhead(state, left_table, right_tables, "asofjoin", options);
}

static void HashJoinOverhead(benchmark::State& state, std::string left_table,
                             std::vector<std::string> right_tables) {
  HashJoinNodeOptions options =
      HashJoinNodeOptions({time_col, key_col}, {time_col, key_col});
  TableOverhead(state, left_table, right_tables, "hashjoin", options);
}

// this generates the set of right hand tables to test on.
void SetArgs(benchmark::internal::Benchmark* bench) { bench->UseRealTime(); }

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
BENCHMARK_CAPTURE(AsOfJoinOverhead, | high_freq | 20_cols | 2000_ids | 1_join | low_freq |
                                        20_cols | 2000_ids |
                  , "high_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("low_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | low_freq | 20_cols | 2000_ids | 1_join | high_freq |
                                        20_cols | 2000_ids |
                  , "low_freq_20_cols_2000_ids0.feather",
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
BENCHMARK_CAPTURE(AsOfJoinOverhead, | mid_freq | 100_cols | 2000_ids | 1_join | mid_freq |
                                        20_cols | 2000_ids |
                  , "mid_freq_100_cols_2000_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | mid_freq | 20_cols | 2000_ids | 1_join | mid_freq |
                                        100_cols | 2000_ids |
                  , "mid_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinOverhead, | low_freq | 20_cols | 2000_ids | 4_join | high_freq |
                                        20_cols | 2000_ids |
                  , "low_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("high_freq", 2, 4, 1))
    ->Apply(SetArgs);

// hash joins
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 1_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 20_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 80_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 100_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 100_cols | 100_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 100_cols | 100_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 1, 0))
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 1_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 20_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 20_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 20_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 80_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 80_cols | 2000_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 80_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | mid_freq | 1_cols | 5k_ids | 1_join | high_freq |
                                        100_cols | 2000_ids |
                  , "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 1, 1))
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 0, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 0, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 1_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 0, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 2, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 20_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 2, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 4, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 4, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 80_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 4, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | low_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("low_freq", 5, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | mid_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 1_cols | 5k_ids | 1_join | high_freq | 100_cols | 5k_ids |,
                  "mid_freq_1_cols_5k_ids0.feather",
                  generateRightHandTables("high_freq", 5, 1, 2))
    ->Apply(SetArgs);

// asymmetric joins
BENCHMARK_CAPTURE(HashJoinOverhead, | high_freq | 20_cols | 2000_ids | 1_join | low_freq |
                                        20_cols | 2000_ids |
                  , "high_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("low_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | low_freq | 20_cols | 2000_ids | 1_join | high_freq |
                                        20_cols | 2000_ids |
                  , "low_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("high_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 20_cols | 100_ids | 1_join | mid_freq | 20_cols | 5k_ids |,
                  "mid_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 2))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead,
                  | mid_freq | 20_cols | 5k_ids | 1_join | mid_freq | 20_cols | 100_ids |,
                  "mid_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 0))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | mid_freq | 100_cols | 2000_ids | 1_join | mid_freq |
                                        20_cols | 2000_ids |
                  , "mid_freq_100_cols_2000_ids0.feather",
                  generateRightHandTables("mid_freq", 2, 1, 1))
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(HashJoinOverhead, | mid_freq | 20_cols | 2000_ids | 1_join | mid_freq |
                                        100_cols | 2000_ids |
                  , "mid_freq_20_cols_2000_ids0.feather",
                  generateRightHandTables("mid_freq", 5, 1, 1))
    ->Apply(SetArgs);
}  // namespace compute
}  // namespace arrow
