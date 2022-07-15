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

#include <boost/process.hpp>
#include <string>

#include "benchmark/benchmark.h"

#include "arrow/compute/exec/test_util.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"

namespace arrow {
namespace compute {

static const char* time_col = "time";
static const char* key_col = "id";
const int default_start = 0;
const int default_end = 500;

struct TableSourceNodeStats {
  ExecNode* execNode;
  size_t total_rows;
  size_t total_bytes;
};

static TableSourceNodeStats MakeTableSourceNode(
    std::shared_ptr<arrow::compute::ExecPlan>& plan, TableGenerationProperties properties,
    int batch_size) {
  std::shared_ptr<Table> table = MakeRandomTable(properties);
  size_t row_size = sizeof(double) * (table.get()->schema()->num_fields() - 2) +
                    sizeof(int64_t) + sizeof(int32_t);
  size_t rows = table.get()->num_rows();
  return {*arrow::compute::MakeExecNode(
              "table_source",  // registered type
              plan.get(),      // execution plan
              {},              // inputs
              arrow::compute::TableSourceNodeOptions(table, batch_size)),
          rows, row_size * rows};
}

static void TableJoinOverhead(benchmark::State& state,
                              TableGenerationProperties left_table_properties,
                              int left_table_batch_size,
                              TableGenerationProperties right_table_properties,
                              int right_table_batch_size, int num_right_tables,
                              std::string factory_name, ExecNodeOptions& options) {
  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  size_t rows = 0;
  size_t bytes = 0;
  for (auto _ : state) {
    state.PauseTiming();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::compute::ExecPlan> plan,
                         ExecPlan::Make(&ctx));
    left_table_properties.column_prefix = "lt";
    left_table_properties.seed = 0;
    TableSourceNodeStats left_table_stats =
        MakeTableSourceNode(plan, left_table_properties, left_table_batch_size);
    std::vector<ExecNode*> inputs = {left_table_stats.execNode};
    int right_hand_rows = 0;
    size_t right_hand_bytes = 0;
    std::vector<std::shared_ptr<arrow::Schema>> schemas;
    for (int i = 0; i < num_right_tables; i++) {
      std::ostringstream string_stream;
      string_stream << "rt" << i;
      right_table_properties.column_prefix = string_stream.str();
      left_table_properties.seed = i + 1;
      TableSourceNodeStats right_table_stats =
          MakeTableSourceNode(plan, right_table_properties, right_table_batch_size);
      inputs.push_back(right_table_stats.execNode);
      right_hand_rows += right_table_stats.total_rows;
      right_hand_bytes += right_table_stats.total_bytes;
    }
    rows = left_table_stats.total_rows + right_hand_rows;
    bytes = left_table_stats.total_bytes + right_hand_bytes;
    AsofJoinNodeOptions options = AsofJoinNodeOptions(time_col, key_col, 0);
    ASSERT_OK_AND_ASSIGN(arrow::compute::ExecNode * join_node,
                         MakeExecNode("asofjoin", plan.get(), inputs, options));
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    MakeExecNode("sink", plan.get(), {join_node}, SinkNodeOptions{&sink_gen});
    state.ResumeTiming();
    ASSERT_FINISHES_OK(StartAndCollect(plan.get(), sink_gen));
  }

  state.counters["total_rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * rows), benchmark::Counter::kIsRate);

  state.counters["total_bytes_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * bytes), benchmark::Counter::kIsRate);

  state.counters["maximum_peak_memory"] =
      benchmark::Counter(static_cast<double>(ctx.memory_pool()->max_memory()));
}

static void AsOfJoinOverhead(benchmark::State& state) {
  int64_t tolerance = 0;
  AsofJoinNodeOptions options = AsofJoinNodeOptions(time_col, key_col, tolerance);
  TableJoinOverhead(
      state,
      TableGenerationProperties{int(state.range(0)), int(state.range(1)),
                                int(state.range(2)), "", 0, default_start, default_end},
      int(state.range(3)),
      TableGenerationProperties{int(state.range(5)), int(state.range(6)),
                                int(state.range(7)), "", 0, default_start, default_end},
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
