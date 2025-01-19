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
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/properties.h"

#include "arrow/array.h"
#include "arrow/io/buffered.h"
#include "arrow/io/memory.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace parquet::benchmark {

// This should result in multiple pages for most primitive types
constexpr int64_t kBenchmarkSize = 1024 * 1024;
// Use a skewed null probability to reduce levels encoding overhead
constexpr double kNullProbability = 0.95;

int64_t GetTotalBytes(const std::shared_ptr<::arrow::ArrayData>& data) {
  if (data == nullptr) {
    return 0;
  }
  int64_t total_bytes =
      std::accumulate(data->buffers.cbegin(), data->buffers.cend(), int64_t{0},
                      [](int64_t acc, const auto& buffer) {
                        return acc + (buffer != nullptr ? buffer->size() : int64_t{0});
                      });
  total_bytes += std::accumulate(
      data->child_data.cbegin(), data->child_data.cend(), int64_t{0},
      [](int64_t acc, const auto& child) { return acc + GetTotalBytes(child); });
  total_bytes += GetTotalBytes(data->dictionary);
  return total_bytes;
}

int64_t GetTotalBytes(const std::shared_ptr<::arrow::Table>& table) {
  int64_t total_bytes = 0;
  for (const auto& column : table->columns()) {
    for (const auto& chunk : column->chunks()) {
      total_bytes += GetTotalBytes(chunk->data());
    }
  }
  return total_bytes;
}

int64_t GetTotalPageIndexSize(const std::shared_ptr<::parquet::FileMetaData>& metadata) {
  int64_t total_page_index_size = 0;
  for (int i = 0; i < metadata->num_row_groups(); ++i) {
    auto row_group = metadata->RowGroup(i);
    for (int j = 0; j < row_group->num_columns(); ++j) {
      auto column = row_group->ColumnChunk(j);
      total_page_index_size +=
          column->GetColumnIndexLocation().value_or(parquet::IndexLocation{0, 0}).length;
    }
  }
  return total_page_index_size;
}

void WriteColumn(::benchmark::State& state, const std::shared_ptr<::arrow::Table>& table,
                 SizeStatisticsLevel stats_level, bool enable_page_index) {
  // Use the fastest possible encoding and compression settings, to better exhibit
  // the size statistics overhead.
  auto builder = WriterProperties::Builder();
  if (enable_page_index) {
    builder.enable_write_page_index();
  } else {
    builder.disable_write_page_index();
  }
  auto properties = builder.enable_statistics()
                        ->disable_dictionary()
                        ->encoding(Encoding::PLAIN)
                        ->set_size_statistics_level(stats_level)
                        ->build();

  for (auto _ : state) {
    auto output = parquet::CreateOutputStream();
    ARROW_EXPECT_OK(::parquet::arrow::WriteTable(
        *table, ::arrow::default_memory_pool(),
        std::static_pointer_cast<::arrow::io::OutputStream>(output),
        DEFAULT_MAX_ROW_GROUP_LENGTH, properties));

    if (state.counters.find("page_index_size") == state.counters.end()) {
      state.PauseTiming();
      auto metadata = parquet::ReadMetaData(
          std::make_shared<::arrow::io::BufferReader>(output->Finish().ValueOrDie()));
      state.counters["output_size"] = static_cast<double>(output->Tell().ValueOrDie());
      state.counters["page_index_size"] =
          static_cast<double>(GetTotalPageIndexSize(metadata));
      state.ResumeTiming();
    }
  }

  state.SetItemsProcessed(state.iterations() * kBenchmarkSize);
  state.SetBytesProcessed(state.iterations() * GetTotalBytes(table));
}

template <SizeStatisticsLevel level, typename ArrowType, bool enable_page_index>
void BM_WritePrimitiveColumn(::benchmark::State& state) {
  ::arrow::random::RandomArrayGenerator generator(/*seed=*/42);
  auto type = std::make_shared<ArrowType>();
  auto array = generator.ArrayOf(type, kBenchmarkSize, kNullProbability);
  auto table = ::arrow::Table::Make(
      ::arrow::schema({::arrow::field("column", type, kNullProbability > 0)}), {array});
  WriteColumn(state, table, level, enable_page_index);
}

template <SizeStatisticsLevel level, typename ArrowType, bool enable_page_index>
void BM_WriteListColumn(::benchmark::State& state) {
  ::arrow::random::RandomArrayGenerator generator(/*seed=*/42);
  auto element_type = std::make_shared<ArrowType>();
  auto element_array = generator.ArrayOf(element_type, kBenchmarkSize, kNullProbability);
  auto list_type = ::arrow::list(element_type);
  auto list_array = generator.List(*element_array, kBenchmarkSize / 10, kNullProbability);
  auto table = ::arrow::Table::Make(
      ::arrow::schema({::arrow::field("column", list_type, kNullProbability > 0)}),
      {list_array});
  WriteColumn(state, table, level, enable_page_index);
}

BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::None, ::arrow::Int64Type,
                   /*enable_page_index=*/false);
BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::None, ::arrow::Int64Type,
                   /*enable_page_index=*/true);
BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::ColumnChunk,
                   ::arrow::Int64Type, /*enable_page_index=*/true);
BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::PageAndColumnChunk,
                   ::arrow::Int64Type, /*enable_page_index=*/true);

BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::None,
                   ::arrow::StringType, /*enable_page_index=*/false);
BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::None,
                   ::arrow::StringType, /*enable_page_index=*/true);
BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::ColumnChunk,
                   ::arrow::StringType, /*enable_page_index=*/true);
BENCHMARK_TEMPLATE(BM_WritePrimitiveColumn, SizeStatisticsLevel::PageAndColumnChunk,
                   ::arrow::StringType, /*enable_page_index=*/true);

BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::None, ::arrow::Int64Type,
                   /*enable_page_index=*/false);
BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::None, ::arrow::Int64Type,
                   /*enable_page_index=*/true);
BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::ColumnChunk,
                   ::arrow::Int64Type, /*enable_page_index=*/true);
BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::PageAndColumnChunk,
                   ::arrow::Int64Type, /*enable_page_index=*/true);

BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::None, ::arrow::StringType,
                   /*enable_page_index=*/false);
BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::None, ::arrow::StringType,
                   /*enable_page_index=*/true);
BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::ColumnChunk,
                   ::arrow::StringType, /*enable_page_index=*/true);
BENCHMARK_TEMPLATE(BM_WriteListColumn, SizeStatisticsLevel::PageAndColumnChunk,
                   ::arrow::StringType, /*enable_page_index=*/true);

}  // namespace parquet::benchmark
