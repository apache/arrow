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

#include "benchmark/benchmark.h"

#include <cstdint>
#include <numeric>

#include "parquet/arrow/writer.h"
#include "parquet/platform.h"
#include "parquet/properties.h"

#include "arrow/array.h"
#include "arrow/io/memory.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace parquet::benchmark {

// This should result in multiple pages for most primitive types
constexpr int64_t kBenchmarkSize = 10 * 1024 * 1024;
constexpr int64_t kRowGroupSize = DEFAULT_MAX_ROW_GROUP_LENGTH;
constexpr double kNullProbability = 0.5;

int64_t GetTotalBytes(const std::shared_ptr<::arrow::ArrayData>& data) {
  if (data == nullptr) {
    return 0;
  }
  int64_t total_bytes =
      std::accumulate(data->buffers.cbegin(), data->buffers.cend(), 0,
                      [](int64_t acc, const auto& buffer) {
                        return acc + (buffer != nullptr ? buffer->size() : 0);
                      });
  total_bytes += std::accumulate(
      data->child_data.cbegin(), data->child_data.cend(), 0,
      [](int64_t acc, const auto& child) { return acc + GetTotalBytes(child); });
  total_bytes += GetTotalBytes(data->dictionary);
  return total_bytes;
}

void WriteColumn(::benchmark::State& state,
                 const std::shared_ptr<::arrow::DataType>& type,
                 SizeStatisticsLevel stats_level) {
  ::arrow::random::RandomArrayGenerator generator(/*seed=*/42);
  std::shared_ptr<::arrow::Array> arr =
      generator.ArrayOf(type, kBenchmarkSize, kNullProbability);
  auto table = ::arrow::Table::Make(
      ::arrow::schema({::arrow::field("column", type, kNullProbability > 0)}), {arr});

  for (auto _ : state) {
    state.PauseTiming();
    auto output = std::static_pointer_cast<::arrow::io::OutputStream>(
        parquet::CreateOutputStream());
    auto properties = WriterProperties::Builder()
                          .enable_statistics()
                          ->enable_write_page_index()
                          ->set_size_statistics_level(stats_level)
                          ->build();
    state.ResumeTiming();
    ARROW_EXPECT_OK(::parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(),
                                                 output, kRowGroupSize, properties));
    state.counters["output_size"] = static_cast<double>(output->Tell().ValueOrDie());
  }

  int64_t bytes_processed = 0;
  for (const auto& column : table->columns()) {
    for (const auto& chunk : column->chunks()) {
      bytes_processed += GetTotalBytes(chunk->data());
    }
  }
  state.SetItemsProcessed(state.iterations() * kBenchmarkSize);
  state.SetBytesProcessed(state.iterations() * bytes_processed);
}

template <SizeStatisticsLevel level, typename ArrowType>
void BM_WritePrimitiveColumn(::benchmark::State& state) {
  WriteColumn(state, std::make_shared<ArrowType>(), level);
}

template <SizeStatisticsLevel level, typename ArrowType>
void BM_WriteListColumn(::benchmark::State& state) {
  WriteColumn(state, ::arrow::list(std::make_shared<ArrowType>()), level);
}

BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::None,
                   ::arrow::Int64Type);
BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::ColumnChunk,
                   ::arrow::Int64Type);
BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::PageAndColumnChunk,
                   ::arrow::Int64Type);

BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::None,
                   ::arrow::StringType);
BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::ColumnChunk,
                   ::arrow::StringType);
BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::PageAndColumnChunk,
                   ::arrow::StringType);

BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::None, ::arrow::Int64Type);
BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::ColumnChunk,
                   ::arrow::Int64Type);
BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::PageAndColumnChunk,
                   ::arrow::Int64Type);

BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::None, ::arrow::StringType);
BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::ColumnChunk,
                   ::arrow::StringType);
BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::PageAndColumnChunk,
                   ::arrow::StringType);

}  // namespace parquet::benchmark

BENCHMARK_MAIN();
