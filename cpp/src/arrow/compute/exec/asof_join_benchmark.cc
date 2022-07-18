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

#include <string>

#include "benchmark/benchmark.h"

#include "arrow/compute/exec/test_util.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"

namespace arrow {
namespace compute {

static const char* kTimeCol = "time";
static const char* kKeyCol = "id";
const int kDefaultStart = 0;
const int kDefaultEnd = 500;
const int kDefaultMinColumnVal = -10000;
const int kDefaultMaxColumnVal = 10000;

struct TableStats {
  std::shared_ptr<Table> table;
  size_t total_rows;
  size_t total_bytes;
};

static TableStats MakeTable(const TableGenerationProperties& properties) {
  std::shared_ptr<Table> table = MakeRandomTimeSeriesTable(properties);
  size_t row_size = sizeof(double) * (table.get()->schema()->num_fields() - 2) +
                    sizeof(int64_t) + sizeof(int32_t);
  size_t rows = table.get()->num_rows();
  return {table, rows, rows * row_size};
}

static ExecNode* MakeTableSourceNode(std::shared_ptr<arrow::compute::ExecPlan> plan,
                                     std::shared_ptr<Table> table, int batch_size) {
  return *arrow::compute::MakeExecNode(
      "table_source", plan.get(), {},
      arrow::compute::TableSourceNodeOptions(table, batch_size));
}

static void TableJoinOverhead(benchmark::State& state,
                              TableGenerationProperties left_table_properties,
                              int left_table_batch_size,
                              TableGenerationProperties right_table_properties,
                              int right_table_batch_size, int num_right_tables,
                              std::string factory_name, ExecNodeOptions& options) {
  ExecContext ctx(default_memory_pool(), nullptr);

  left_table_properties.column_prefix = "lt";
  left_table_properties.seed = 0;
  TableStats left_table_stats = MakeTable(left_table_properties);

  size_t right_hand_rows = 0;
  size_t right_hand_bytes = 0;
  std::vector<TableStats> right_input_tables;
  right_input_tables.reserve(num_right_tables);

  for (int i = 0; i < num_right_tables; i++) {
    right_table_properties.column_prefix = "rt" + std::to_string(i);
    right_table_properties.seed = i + 1;
    TableStats right_table_stats = MakeTable(right_table_properties);
    right_hand_rows += right_table_stats.total_rows;
    right_hand_bytes += right_table_stats.total_bytes;
    right_input_tables.push_back(right_table_stats);
  }

  for (auto _ : state) {
    state.PauseTiming();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::compute::ExecPlan> plan,
                         ExecPlan::Make(&ctx));
    std::vector<ExecNode*> input_nodes = {
        MakeTableSourceNode(plan, left_table_stats.table, left_table_batch_size)};
    input_nodes.reserve(right_input_tables.size() + 1);
    for (TableStats table_stats : right_input_tables) {
      input_nodes.push_back(
          MakeTableSourceNode(plan, table_stats.table, right_table_batch_size));
    }
    ASSERT_OK_AND_ASSIGN(arrow::compute::ExecNode * join_node,
                         MakeExecNode(factory_name, plan.get(), input_nodes, options));
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    MakeExecNode("sink", plan.get(), {join_node}, SinkNodeOptions{&sink_gen});
    state.ResumeTiming();
    ASSERT_FINISHES_OK(StartAndCollect(plan.get(), sink_gen));
  }

  state.counters["total_rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() *
                          (left_table_stats.total_rows + right_hand_rows)),
      benchmark::Counter::kIsRate);

  state.counters["total_bytes_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() *
                          (left_table_stats.total_bytes + right_hand_bytes)),
      benchmark::Counter::kIsRate);

  state.counters["maximum_peak_memory"] =
      benchmark::Counter(static_cast<double>(ctx.memory_pool()->max_memory()));
}

static void AsOfJoinOverhead(benchmark::State& state) {
  int64_t tolerance = 0;
  AsofJoinNodeOptions options = AsofJoinNodeOptions(kTimeCol, kKeyCol, tolerance);
  TableJoinOverhead(
      state,
      TableGenerationProperties{int(state.range(0)), int(state.range(1)),
                                int(state.range(2)), "", kDefaultMinColumnVal,
                                kDefaultMaxColumnVal, 0, kDefaultStart, kDefaultEnd},
      int(state.range(3)),
      TableGenerationProperties{int(state.range(5)), int(state.range(6)),
                                int(state.range(7)), "", kDefaultMinColumnVal,
                                kDefaultMaxColumnVal, 0, kDefaultStart, kDefaultEnd},
      int(state.range(8)), int(state.range(4)), "asofjoin", options);
}

// this generates the set of right hand tables to test on.
void SetArgs(benchmark::internal::Benchmark* bench) {
  bench
      ->ArgNames({"left_freq", "left_cols", "left_ids", "left_batch_size",
                  "num_right_tables", "right_freq", "right_cols", "right_ids",
                  "right_batch_size"})
      ->UseRealTime();

  int default_freq = 5;
  int default_cols = 20;
  int default_ids = 500;
  int default_num_tables = 1;
  int default_batch_size = 100;

  for (int freq : {1, 5, 10}) {
    bench->Args({freq, default_cols, default_ids, default_batch_size, default_num_tables,
                 freq, default_cols, default_ids, default_batch_size});
  }
  for (int cols : {10, 20, 100}) {
    bench->Args({default_freq, cols, default_ids, default_batch_size, default_num_tables,
                 default_freq, cols, default_ids, default_batch_size});
  }
  for (int ids : {100, 500, 1000}) {
    bench->Args({default_freq, default_cols, ids, default_batch_size, default_num_tables,
                 default_freq, default_cols, ids, default_batch_size});
  }
  for (int num_tables : {1, 10, 50}) {
    bench->Args({default_freq, default_cols, default_ids, default_batch_size, num_tables,
                 default_freq, default_cols, default_ids, default_batch_size});
  }
  for (int batch_size : {1, 500, 1000}) {
    bench->Args({default_freq, default_cols, default_ids, batch_size, default_num_tables,
                 default_freq, default_cols, default_ids, batch_size});
  }
}

BENCHMARK(AsOfJoinOverhead)->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow
