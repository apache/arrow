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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/acero/options.h"
#include "arrow/acero/test_util_internal.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/util/byte_size.h"

namespace arrow {
namespace acero {

static const char* kTimeCol = "time";
static const char* kKeyCol = "id";
const int kDefaultStart = 0;
const int kDefaultEnd = 32000;
const int kDefaultMinColumnVal = -10000;
const int kDefaultMaxColumnVal = 10000;
const int64_t kLongStringKeyBytes = 128;

struct TableStats {
  std::shared_ptr<Table> table;
  size_t rows;
  size_t bytes;
};

static Result<std::shared_ptr<Table>> WithFixedSizeStringKeys(
    std::shared_ptr<Table> table, int num_ids, int64_t key_bytes) {
  if (key_bytes == 0) {
    return table;
  }

  std::vector<std::string> keys;
  keys.reserve(num_ids);
  for (int id = 0; id < num_ids; ++id) {
    std::string suffix = std::to_string(id);
    if (static_cast<int64_t>(suffix.size()) > key_bytes) {
      return Status::Invalid("Key size is too small for id ", id);
    }
    keys.emplace_back(static_cast<size_t>(key_bytes - suffix.size()), 'k');
    keys.back() += suffix;
  }

  StringBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Resize(table->num_rows()));
  ARROW_RETURN_NOT_OK(builder.ReserveData(table->num_rows() * key_bytes));
  for (const auto& chunk : table->GetColumnByName(kKeyCol)->chunks()) {
    const auto& ids = static_cast<const Int32Array&>(*chunk);
    for (int64_t row = 0; row < ids.length(); ++row) {
      builder.UnsafeAppend(keys[ids.Value(row)]);
    }
  }

  std::shared_ptr<StringArray> string_keys;
  ARROW_RETURN_NOT_OK(builder.Finish(&string_keys));
  int key_index = table->schema()->GetFieldIndex(kKeyCol);
  return table->SetColumn(key_index, field(kKeyCol, utf8()),
                          std::make_shared<ChunkedArray>(std::move(string_keys)));
}

static Result<TableStats> MakeTable(const TableGenerationProperties& properties,
                                    int64_t string_key_bytes) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> table,
                        MakeRandomTimeSeriesTable(properties));
  ARROW_ASSIGN_OR_RAISE(
      table,
      WithFixedSizeStringKeys(std::move(table), properties.num_ids, string_key_bytes));
  size_t rows = table->num_rows();
  size_t bytes = static_cast<size_t>(util::TotalBufferSize(*table));
  return Result<TableStats>({std::move(table), rows, bytes});
}

static void TableJoinOverhead(benchmark::State& state,
                              TableGenerationProperties left_table_properties,
                              TableGenerationProperties right_table_properties,
                              int batch_size, int num_right_tables,
                              std::string factory_name,
                              std::shared_ptr<ExecNodeOptions> options, bool use_threads,
                              int64_t string_key_bytes = 0) {
  left_table_properties.column_prefix = "lt";
  left_table_properties.seed = 0;
  ASSERT_OK_AND_ASSIGN(TableStats left_table_stats,
                       MakeTable(left_table_properties, string_key_bytes));

  size_t right_hand_rows = 0;
  size_t right_hand_bytes = 0;
  std::vector<TableStats> right_input_tables;
  right_input_tables.reserve(num_right_tables);

  for (int i = 0; i < num_right_tables; i++) {
    right_table_properties.column_prefix = "rt" + std::to_string(i);
    right_table_properties.seed = i + 1;
    ASSERT_OK_AND_ASSIGN(TableStats right_table_stats,
                         MakeTable(right_table_properties, string_key_bytes));
    right_hand_rows += right_table_stats.rows;
    right_hand_bytes += right_table_stats.bytes;
    right_input_tables.push_back(std::move(right_table_stats));
  }

  for (auto _ : state) {
    state.PauseTiming();
    std::vector<Declaration::Input> input_nodes = {Declaration(
        "table_source",
        arrow::acero::TableSourceNodeOptions(left_table_stats.table, batch_size))};
    input_nodes.reserve(right_input_tables.size() + 1);
    for (TableStats table_stats : right_input_tables) {
      input_nodes.push_back(Declaration(
          "table_source",
          arrow::acero::TableSourceNodeOptions(table_stats.table, batch_size)));
    }
    Declaration join_node{factory_name, {input_nodes}, options};
    state.ResumeTiming();
    ASSERT_OK(DeclarationToStatus(std::move(join_node), use_threads));
  }

  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * (left_table_stats.rows + right_hand_rows)),
      benchmark::Counter::kIsRate);

  state.counters["bytes_per_second"] =
      benchmark::Counter(static_cast<double>(state.iterations() *
                                             (left_table_stats.bytes + right_hand_bytes)),
                         benchmark::Counter::kIsRate);

  state.counters["maximum_peak_memory"] =
      benchmark::Counter(static_cast<double>(default_memory_pool()->max_memory()));
}

AsofJoinNodeOptions GetRepeatedOptions(size_t repeat, FieldRef on_key,
                                       std::vector<FieldRef> by_key, int64_t tolerance) {
  std::vector<AsofJoinNodeOptions::Keys> input_keys(repeat);
  for (size_t i = 0; i < repeat; i++) {
    input_keys[i] = {on_key, by_key};
  }
  return AsofJoinNodeOptions(input_keys, tolerance);
}

static void AsOfJoinOverhead(benchmark::State& state, bool use_threads) {
  int64_t tolerance = 0;
  auto options = std::make_shared<AsofJoinNodeOptions>(
      GetRepeatedOptions(int(state.range(4) + 1), kTimeCol, {kKeyCol}, tolerance));
  TableJoinOverhead(
      state,
      TableGenerationProperties{int(state.range(0)), int(state.range(1)),
                                int(state.range(2)), "", kDefaultMinColumnVal,
                                kDefaultMaxColumnVal, 0, kDefaultStart, kDefaultEnd},
      TableGenerationProperties{int(state.range(5)), int(state.range(6)),
                                int(state.range(7)), "", kDefaultMinColumnVal,
                                kDefaultMaxColumnVal, 0, kDefaultStart, kDefaultEnd},
      int(state.range(3)), int(state.range(4)), "asofjoin", std::move(options),
      use_threads);
}

static void AsOfJoinKeyToleranceDensity(benchmark::State& state, bool use_threads,
                                        int64_t string_key_bytes) {
  constexpr int kColumns = 20;
  constexpr int kIds = 500;
  constexpr int kBatchSize = 4000;
  constexpr int kNumRightTables = 1;

  auto options = std::make_shared<AsofJoinNodeOptions>(
      GetRepeatedOptions(kNumRightTables + 1, kTimeCol, {kKeyCol}, state.range(2)));
  TableJoinOverhead(state,
                    TableGenerationProperties{int(state.range(0)), kColumns, kIds, "",
                                              kDefaultMinColumnVal, kDefaultMaxColumnVal,
                                              0, kDefaultStart, kDefaultEnd},
                    TableGenerationProperties{int(state.range(1)), kColumns, kIds, "",
                                              kDefaultMinColumnVal, kDefaultMaxColumnVal,
                                              0, kDefaultStart, kDefaultEnd},
                    kBatchSize, kNumRightTables, "asofjoin", std::move(options),
                    use_threads, string_key_bytes);
}

// this generates the set of right hand tables to test on.
void SetArgs(benchmark::internal::Benchmark* bench) {
  bench
      ->ArgNames({"left_freq", "left_cols", "left_ids", "batch_size", "num_right_tables",
                  "right_freq", "right_cols", "right_ids"})
      ->UseRealTime();

  int default_freq = 400;
  int default_cols = 20;
  int default_ids = 500;
  int default_num_tables = 1;
  int default_batch_size = 4000;

  for (int freq : {200, 400, 1000}) {
    bench->Args({freq, default_cols, default_ids, default_batch_size, default_num_tables,
                 freq, default_cols, default_ids});
  }
  for (int cols : {10, 20, 100}) {
    bench->Args({default_freq, cols, default_ids, default_batch_size, default_num_tables,
                 default_freq, cols, default_ids});
  }
  for (int ids : {100, 500, 1000}) {
    bench->Args({default_freq, default_cols, ids, default_batch_size, default_num_tables,
                 default_freq, default_cols, ids});
  }
  for (int num_tables : {1, 10, 50}) {
    bench->Args({default_freq, default_cols, default_ids, default_batch_size, num_tables,
                 default_freq, default_cols, default_ids});
  }
  for (int batch_size : {1000, 4000, 32000}) {
    bench->Args({default_freq, default_cols, default_ids, batch_size, default_num_tables,
                 default_freq, default_cols, default_ids});
  }
}

void SetKeyToleranceDensityArgs(benchmark::internal::Benchmark* bench) {
  bench->ArgNames({"left_freq", "right_freq", "tolerance"})->UseRealTime();

  // A smaller frequency means a denser input. Include balanced inputs, dense left with
  // sparse right (repeated right matches), and sparse left with dense right (many right
  // candidates per left row).
  for (const auto& [left_freq, right_freq] :
       {std::pair<int, int>{400, 400}, {200, 1000}, {1000, 200}}) {
    for (int64_t tolerance : {-1000, 1000}) {
      bench->Args({left_freq, right_freq, tolerance});
    }
  }
}

BENCHMARK_CAPTURE(AsOfJoinOverhead, serial_executor, false)->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinKeyToleranceDensity, serial_executor_int32_keys, false, 0)
    ->Apply(SetKeyToleranceDensityArgs);
BENCHMARK_CAPTURE(AsOfJoinKeyToleranceDensity, serial_executor_string128_keys, false,
                  kLongStringKeyBytes)
    ->Apply(SetKeyToleranceDensityArgs);
#ifdef ARROW_ENABLE_THREADING
BENCHMARK_CAPTURE(AsOfJoinOverhead, threaded_executor, true)->Apply(SetArgs);
BENCHMARK_CAPTURE(AsOfJoinKeyToleranceDensity, threaded_executor_int32_keys, true, 0)
    ->Apply(SetKeyToleranceDensityArgs);
BENCHMARK_CAPTURE(AsOfJoinKeyToleranceDensity, threaded_executor_string128_keys, true,
                  kLongStringKeyBytes)
    ->Apply(SetKeyToleranceDensityArgs);
#endif

}  // namespace acero
}  // namespace arrow
