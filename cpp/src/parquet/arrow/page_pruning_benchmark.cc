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

#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/io/memory.h"
#include "arrow/table.h"
#include "arrow/testing/util.h"
#include "arrow/util/range.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/test_util.h"
#include "parquet/arrow/writer.h"
#include "parquet/platform.h"

using arrow::Result;
using arrow::TimeUnit;
using arrow::internal::Iota;
using arrow::io::BufferReader;

/// Benchmarks primarily aim to explore the impact of the number of pages hit and the
/// number of row sets hit within a single page on scan performance in page pruning.
namespace parquet::benchmark {

const int64_t num_rows = 2000000;

static void InitReader(std::shared_ptr<::arrow::Table> table,
                       std::unique_ptr<arrow::FileReader>* reader) {
  auto sink = CreateOutputStream();

  WriterProperties::Builder builder;
  auto writer_properties = builder.max_row_group_length(num_rows)
                               ->enable_write_page_index()
                               ->disable_dictionary()
                               ->build();
  ASSERT_OK_NO_THROW(arrow::WriteTable(*table, ::arrow::default_memory_pool(), sink,
                                       num_rows, std::move(writer_properties)));
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  auto arrow_reader_properties = ::parquet::default_arrow_reader_properties();
  arrow_reader_properties.set_batch_size(num_rows);
  arrow_reader_properties.set_pre_buffer(false);

  auto reader_properties = default_reader_properties();
  reader_properties.set_buffer_size(1024 * 1024);
  arrow::FileReaderBuilder file_reader_builder;
  ASSERT_OK_NO_THROW(file_reader_builder.Open(
      std::make_shared<BufferReader>(std::move(buffer)), std::move(reader_properties)));
  ASSERT_OK_NO_THROW(
      file_reader_builder.properties(arrow_reader_properties)->Build(reader));
  ASSERT_EQ(1, (*reader)->num_row_groups());
}

template <bool nullable, typename ArrowType>
static void PrepareDataAndReader(std::unique_ptr<arrow::FileReader>* reader) {
  std::shared_ptr<::arrow::Array> values;
  std::shared_ptr<::arrow::Table> table;
  if (nullable) {
    ASSERT_OK(arrow::NullableArray<ArrowType>(num_rows, num_rows / 2, 0, &values));
    table = arrow::MakeSimpleTable(values, true);
  } else {
    ASSERT_OK(arrow::NonNullArray<ArrowType>(num_rows, &values));
    table = arrow::MakeSimpleTable(values, false);
  }

  InitReader(std::move(table), reader);
}

struct PageInfo {
  int num_pages;      // Total page num for all columns
  int min_num_pages;  // The min page num between all columns
  int max_num_pages;  // The max page num between all columns
};

/// \breif Build page read info based on  hit page percent.
///
/// @param reader File reader to get page index
/// @param hit_page_percent How many pages should be hit
/// \return The tuple of PageInfo, the index of column which has the max page num
/// and the indices of hit pages.
static std::tuple<PageInfo, int, std::vector<int>> BuildPageReadInfo(
    const std::unique_ptr<arrow::FileReader>& reader, int hit_page_percent) {
  int num_pages = 0;
  int min_num_pages = std::numeric_limits<int>::max();
  int max_num_pages = std::numeric_limits<int>::min();
  int column_index = 0;

  // Find out the column which has max page num and calculate the total page num
  auto rg_pi_reader = reader->parquet_reader()->GetPageIndexReader()->RowGroup(0);
  for (int i = 0; i < reader->parquet_reader()->metadata()->num_columns(); i++) {
    int col_num_pages =
        static_cast<int>(rg_pi_reader->GetOffsetIndex(i)->page_locations().size());
    if (col_num_pages < min_num_pages) {
      min_num_pages = col_num_pages;
    }
    if (col_num_pages > max_num_pages) {
      column_index = i;
      max_num_pages = col_num_pages;
    }

    num_pages += col_num_pages;
  }

  int hit_page_num = std::ceil(max_num_pages * hit_page_percent / 100.0);

  // Random generate the hit page indices
  std::vector<int> page_indices;
  if (hit_page_percent == 100) {
    page_indices.resize(hit_page_num);
    std::iota(page_indices.begin(), page_indices.end(), 0);
  } else {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> d(0, max_num_pages - 1);

    for (int i = 0; i < hit_page_num; i++) {
      auto page_index = d(gen);
      auto it = std::find(page_indices.begin(), page_indices.end(), page_index);
      while (it != page_indices.end()) {
        page_index = d(gen);
        it = std::find(page_indices.begin(), page_indices.end(), page_index);
      }
      page_indices.push_back(page_index);
    }
    std::sort(page_indices.begin(), page_indices.end());
  }

  return std::make_tuple(PageInfo{num_pages, min_num_pages, max_num_pages}, column_index,
                         std::move(page_indices));
}

static inline int64_t GetLastRowIndex(const std::vector<PageLocation>& page_locations,
                                      size_t page_index) {
  const size_t next_page_index = page_index + 1;
  if (next_page_index >= page_locations.size()) {
    return num_rows - 1;
  } else {
    return page_locations[next_page_index].first_row_index - 1;
  }
}

/// \brief Build read info with given hit page percent.
///
/// \param reader  File reader to get page index
/// \param hit_page_percent How many pages should be hit
/// \param page_hit_rows How many rows should be hit in each page
/// \return  The tuple of PageInfo and the final read row ranges.
static std::tuple<PageInfo, std::optional<std::unordered_map<int, RowRanges>>>
BuildReadInfoWithPageHitPercent(const std::unique_ptr<arrow::FileReader>& reader,
                                int hit_page_percent, int page_hit_rows) {
  auto info = BuildPageReadInfo(reader, hit_page_percent);
  auto page_info = std::get<0>(info);
  auto column_index = std::get<1>(info);
  auto page_indices = std::get<2>(info);
  auto hit_page_num = static_cast<int>(page_indices.size());

  auto rg_pi_reader = reader->parquet_reader()->GetPageIndexReader()->RowGroup(0);
  const auto page_locations =
      rg_pi_reader->GetOffsetIndex(column_index)->page_locations();

  std::vector<RowRanges::Range> ranges;
  bool read_all_rows = page_hit_rows == std::numeric_limits<int>::max();
  if (read_all_rows) {
    for (int i = 0; i < hit_page_num; i++) {
      const int page_ordinal = page_indices[i];
      const int64_t first_row_index = page_locations[page_ordinal].first_row_index;
      ranges.push_back({first_row_index, GetLastRowIndex(page_locations, page_ordinal)});
    }
  } else {
    std::random_device rd1;
    std::mt19937_64 gen1(rd1());

    for (int i = 0; i < hit_page_num; i++) {
      const int page_ordinal = page_indices[i];
      const int64_t first_row_index = page_locations[page_ordinal].first_row_index;
      std::uniform_int_distribution<int64_t> d1(
          first_row_index,
          GetLastRowIndex(page_locations, page_ordinal) - page_hit_rows + 1);
      const int64_t start = d1(gen1);
      ranges.push_back({start, start + page_hit_rows - 1});
    }
  }

  std::unordered_map<int, RowRanges> row_ranges = {{0, RowRanges(std::move(ranges))}};
  return std::make_tuple(page_info, std::make_optional(std::move(row_ranges)));
}

static std::vector<RowRanges::Range> GenerateRanges(int num_ranges, int64_t start_index,
                                                    int64_t end_index) {
  const int64_t total_rows = end_index - start_index + 1;
  int64_t step;
  if (num_ranges * 2 > total_rows) {
    num_ranges = static_cast<int>(total_rows / 2);
    step = 1;
  } else {
    step = total_rows / (num_ranges * 2);
  }
  std::vector<RowRanges::Range> ranges;
  for (int i = 1; i <= num_ranges; i++) {
    auto start = start_index + (2 * i - 1) * step;
    auto end = start_index + 2 * i * step - 1;
    ranges.push_back(RowRanges::Range{start, end});
  }
  return ranges;
}

/// \brief Build read info with given hit page percent and range num.
///
/// \param reader File reader to get page index
/// \param hit_page_percent How many pages should be hit
/// \param num_ranges How many row ranges in each page should be generate
/// \return The tuple of PageInfo and the final read row ranges.
static std::tuple<PageInfo, std::unordered_map<int, RowRanges>>
BuildReadInfoWithPageHitRanges(const std::unique_ptr<arrow::FileReader>& reader,
                               int hit_page_percent, int num_ranges) {
  auto info = BuildPageReadInfo(reader, hit_page_percent);
  auto page_info = std::get<0>(info);
  auto column_index = std::get<1>(info);
  auto page_indices = std::get<2>(info);
  auto hit_page_num = static_cast<int>(page_indices.size());

  auto rg_pi_reader = reader->parquet_reader()->GetPageIndexReader()->RowGroup(0);
  const auto page_locations =
      rg_pi_reader->GetOffsetIndex(column_index)->page_locations();

  std::vector<RowRanges::Range> ranges;
  for (int i = 0; i < hit_page_num; i++) {
    const int page_ordinal = page_indices[i];
    const int64_t first_row_index = page_locations[page_ordinal].first_row_index;
    const int64_t end_index = GetLastRowIndex(page_locations, page_ordinal);
    const auto page_row_ranges = GenerateRanges(num_ranges, first_row_index, end_index);
    ranges.insert(ranges.end(), page_row_ranges.begin(), page_row_ranges.end());
  }

  std::unordered_map<int, RowRanges> row_ranges = {{0, RowRanges(std::move(ranges))}};
  return std::make_tuple(page_info, std::move(row_ranges));
}

static void RegisterCounters(::benchmark::State& state, int num_pages,
                             int64_t rows_read) {
  auto iterations = state.iterations();

  state.counters["TotalPage"] = num_pages;
  state.counters["HitRows"] = rows_read;
}

static void RunBenchmark(
    ::benchmark::State& state, std::unique_ptr<arrow::FileReader> reader,
    const std::vector<int>& column_indices,
    std::tuple<PageInfo, std::optional<std::unordered_map<int, RowRanges>>> info) {
  auto page_info = std::get<0>(info);
  auto row_ranges = std::get<1>(info);
  auto rows_read = row_ranges.has_value()
                       ? row_ranges.value()[0].GetRowCount()
                       : reader->parquet_reader()->metadata()->num_rows();

  for (auto _ : state) {
    std::unique_ptr<::arrow::RecordBatchReader> batch_reader;
    ASSERT_OK(
        reader->GetRecordBatchReader({0}, column_indices, row_ranges, &batch_reader));
    std::shared_ptr<::arrow::RecordBatch> record_batch;
    ASSERT_OK(batch_reader->ReadNext(&record_batch));
    ASSERT_EQ(rows_read, record_batch->num_rows());
    ::benchmark::DoNotOptimize(record_batch);
    ::benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(state.iterations() * rows_read);

  RegisterCounters(state, page_info.num_pages, rows_read);
}

template <bool nullable, typename ArrowType>
static inline void ReadRowGroup(::benchmark::State& state) {
  std::unique_ptr<arrow::FileReader> reader;
  PrepareDataAndReader<nullable, ArrowType>(&reader);

  const auto page_locations = reader->parquet_reader()
                                  ->GetPageIndexReader()
                                  ->RowGroup(0)
                                  ->GetOffsetIndex(0)
                                  ->page_locations();
  auto num_pages = static_cast<int>(page_locations.size());
  auto info = std::make_tuple(PageInfo{num_pages, num_pages, num_pages}, std::nullopt);
  RunBenchmark(state, std::move(reader), {0}, std::move(info));
}

/// \brief Read hit rows in all hit pages for single column using page pruning.
///
/// All hit pages are random generated according to hit page percent, and hit
/// rows are also random generated according to page hit rows param.
template <bool nullable, typename ArrowType>
static void BM_SingleColumn_NumPages_PagePruning(::benchmark::State& state) {
  const int hit_page_percent = static_cast<int>(state.range(0));
  const int page_hit_rows = static_cast<int>(state.range(1));

  std::unique_ptr<arrow::FileReader> reader;
  PrepareDataAndReader<nullable, ArrowType>(&reader);

  auto info = BuildReadInfoWithPageHitPercent(reader, hit_page_percent, page_hit_rows);
  RunBenchmark(state, std::move(reader), {0}, std::move(info));
}

/// \brief Read all rows in all pages for single column using page pruning.
template <bool nullable, typename ArrowType>
static void BM_SingleColumn_NumPages_PagePruningWithHitAll(::benchmark::State& state) {
  const int hit_page_percent = 100;
  const int page_hit_rows = std::numeric_limits<int>::max();

  std::unique_ptr<arrow::FileReader> reader;
  PrepareDataAndReader<nullable, ArrowType>(&reader);

  auto info = BuildReadInfoWithPageHitPercent(reader, hit_page_percent, page_hit_rows);
  RunBenchmark(state, std::move(reader), {0}, std::move(info));
}

/// \brief Read all data in row group for single column without page pruning.
template <bool nullable, typename ArrowType>
static void BM_SingleColumn_NumPages_ReadRowGroup(::benchmark::State& state) {
  ReadRowGroup<nullable, ArrowType>(state);
}

/// \brief Generate row ranges and read hit data for single column with page pruning.
template <bool nullable, typename ArrowType>
static void BM_SingleColumn_NumRanges_PagePruning(::benchmark::State& state) {
  const int hit_page_percent = static_cast<int>(state.range(0));
  const int page_hit_ranges = static_cast<int>(state.range(1));

  std::unique_ptr<arrow::FileReader> reader;
  PrepareDataAndReader<nullable, ArrowType>(&reader);

  auto info = BuildReadInfoWithPageHitRanges(reader, hit_page_percent, page_hit_ranges);
  RunBenchmark(state, std::move(reader), {0}, std::move(info));
}

/// \brief Read all data in row group for single column without page pruning.
template <bool nullable, typename ArrowType>
static void BM_SingleColumn_NumRanges_ReadRowGroup(::benchmark::State& state) {
  ReadRowGroup<nullable, ArrowType>(state);
}

// ----------------------------------------------------------------------
// The following benchmarks aim to assess the influence of the number of hit pages on scan
// performance for different types. In contrast, the comparative test for each type
// involves scanning the entire row group.

const int64_t hit_rows_val = 1;

// ----------------------------------------------------------------------
// Int32
BENCHMARK_TEMPLATE(BM_SingleColumn_NumPages_PagePruning, false, ::arrow::Int32Type)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_rows"})
    ->Args({10, hit_rows_val})
    ->Args({30, hit_rows_val})
    ->Args({50, hit_rows_val})
    ->Args({70, hit_rows_val})
    ->Args({100, hit_rows_val});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_PagePruningWithHitAll, false,
                    ::arrow::Int32Type)
    ->Iterations(50);
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_ReadRowGroup, false, ::arrow::Int32Type)
    ->Iterations(50);

BENCHMARK_TEMPLATE(BM_SingleColumn_NumPages_PagePruning, true, ::arrow::Int32Type)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_rows"})
    ->Args({10, hit_rows_val})
    ->Args({30, hit_rows_val})
    ->Args({50, hit_rows_val})
    ->Args({70, hit_rows_val})
    ->Args({100, hit_rows_val});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_PagePruningWithHitAll, true,
                    ::arrow::Int32Type)
    ->Iterations(50);
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_ReadRowGroup, true, ::arrow::Int32Type)
    ->Iterations(50);

// ----------------------------------------------------------------------
// Int64
BENCHMARK_TEMPLATE(BM_SingleColumn_NumPages_PagePruning, false, ::arrow::Int64Type)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_rows"})
    ->Args({10, hit_rows_val})
    ->Args({30, hit_rows_val})
    ->Args({50, hit_rows_val})
    ->Args({70, hit_rows_val})
    ->Args({100, hit_rows_val});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_PagePruningWithHitAll, false,
                    ::arrow::Int64Type)
    ->Iterations(50);
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_ReadRowGroup, false, ::arrow::Int64Type)
    ->Iterations(50);

BENCHMARK_TEMPLATE(BM_SingleColumn_NumPages_PagePruning, true, ::arrow::Int64Type)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_rows"})
    ->Args({10, hit_rows_val})
    ->Args({30, hit_rows_val})
    ->Args({50, hit_rows_val})
    ->Args({70, hit_rows_val})
    ->Args({100, hit_rows_val});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_PagePruningWithHitAll, true,
                    ::arrow::Int64Type)
    ->Iterations(50);
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_ReadRowGroup, true, ::arrow::Int64Type)
    ->Iterations(50);

// ----------------------------------------------------------------------
// Float
BENCHMARK_TEMPLATE(BM_SingleColumn_NumPages_PagePruning, false, ::arrow::FloatType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_rows"})
    ->Args({10, hit_rows_val})
    ->Args({30, hit_rows_val})
    ->Args({50, hit_rows_val})
    ->Args({70, hit_rows_val})
    ->Args({100, hit_rows_val});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_PagePruningWithHitAll, false,
                    ::arrow::FloatType)
    ->Iterations(50);
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_ReadRowGroup, false, ::arrow::FloatType)
    ->Iterations(50);

BENCHMARK_TEMPLATE(BM_SingleColumn_NumPages_PagePruning, true, ::arrow::FloatType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_rows"})
    ->Args({10, hit_rows_val})
    ->Args({30, hit_rows_val})
    ->Args({50, hit_rows_val})
    ->Args({70, hit_rows_val})
    ->Args({100, hit_rows_val});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_PagePruningWithHitAll, true,
                    ::arrow::FloatType)
    ->Iterations(50);
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_ReadRowGroup, true, ::arrow::FloatType)
    ->Iterations(50);

// ----------------------------------------------------------------------
// Double
BENCHMARK_TEMPLATE(BM_SingleColumn_NumPages_PagePruning, false, ::arrow::DoubleType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_rows"})
    ->Args({10, hit_rows_val})
    ->Args({30, hit_rows_val})
    ->Args({50, hit_rows_val})
    ->Args({70, hit_rows_val})
    ->Args({100, hit_rows_val});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_PagePruningWithHitAll, false,
                    ::arrow::DoubleType)
    ->Iterations(50);
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_ReadRowGroup, false, ::arrow::DoubleType)
    ->Iterations(50);

BENCHMARK_TEMPLATE(BM_SingleColumn_NumPages_PagePruning, true, ::arrow::DoubleType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_rows"})
    ->Args({10, hit_rows_val})
    ->Args({30, hit_rows_val})
    ->Args({50, hit_rows_val})
    ->Args({70, hit_rows_val})
    ->Args({100, hit_rows_val});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_PagePruningWithHitAll, true,
                    ::arrow::DoubleType)
    ->Iterations(50);
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_ReadRowGroup, true, ::arrow::DoubleType)
    ->Iterations(50);

// ----------------------------------------------------------------------
// String
BENCHMARK_TEMPLATE(BM_SingleColumn_NumPages_PagePruning, false, ::arrow::StringType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_rows"})
    ->Args({10, hit_rows_val})
    ->Args({30, hit_rows_val})
    ->Args({50, hit_rows_val})
    ->Args({70, hit_rows_val})
    ->Args({100, hit_rows_val});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_PagePruningWithHitAll, false,
                    ::arrow::StringType)
    ->Iterations(50);
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_ReadRowGroup, false, ::arrow::StringType)
    ->Iterations(50);

BENCHMARK_TEMPLATE(BM_SingleColumn_NumPages_PagePruning, true, ::arrow::StringType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_rows"})
    ->Args({10, hit_rows_val})
    ->Args({30, hit_rows_val})
    ->Args({50, hit_rows_val})
    ->Args({70, hit_rows_val})
    ->Args({100, hit_rows_val});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_PagePruningWithHitAll, true,
                    ::arrow::StringType)
    ->Iterations(50);
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumPages_ReadRowGroup, true, ::arrow::StringType)
    ->Iterations(50);

// ----------------------------------------------------------------------
// The following benchmarks are designed to evaluate the effect of the number of hit row
// ranges within each page on scan performance for different types. Additionally, the
// comparative test for each type involves scanning the entire row group.

const int hit_page_val = 100;

// ----------------------------------------------------------------------
// Int32
BENCHMARK_TEMPLATE(BM_SingleColumn_NumRanges_PagePruning, false, ::arrow::Int32Type)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_ranges"})
    ->Args({hit_page_val, 500})
    ->Args({hit_page_val, 1000})
    ->Args({hit_page_val, 10000})
    ->Args({hit_page_val, 100000})
    ->Args({hit_page_val, 1000000});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumRanges_ReadRowGroup, false, ::arrow::Int32Type)
    ->Iterations(50);

BENCHMARK_TEMPLATE(BM_SingleColumn_NumRanges_PagePruning, true, ::arrow::Int32Type)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_ranges"})
    ->Args({hit_page_val, 500})
    ->Args({hit_page_val, 1000})
    ->Args({hit_page_val, 10000})
    ->Args({hit_page_val, 100000})
    ->Args({hit_page_val, 1000000});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumRanges_ReadRowGroup, true, ::arrow::Int32Type)
    ->Iterations(50);

// ----------------------------------------------------------------------
// Int64
BENCHMARK_TEMPLATE(BM_SingleColumn_NumRanges_PagePruning, false, ::arrow::Int64Type)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_ranges"})
    ->Args({hit_page_val, 500})
    ->Args({hit_page_val, 1000})
    ->Args({hit_page_val, 10000})
    ->Args({hit_page_val, 100000})
    ->Args({hit_page_val, 1000000});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumRanges_ReadRowGroup, false, ::arrow::Int64Type)
    ->Iterations(50);

BENCHMARK_TEMPLATE(BM_SingleColumn_NumRanges_PagePruning, true, ::arrow::Int64Type)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_ranges"})
    ->Args({hit_page_val, 500})
    ->Args({hit_page_val, 1000})
    ->Args({hit_page_val, 10000})
    ->Args({hit_page_val, 100000})
    ->Args({hit_page_val, 1000000});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumRanges_ReadRowGroup, true, ::arrow::Int64Type)
    ->Iterations(50);

// ----------------------------------------------------------------------
// Float
BENCHMARK_TEMPLATE(BM_SingleColumn_NumRanges_PagePruning, false, ::arrow::FloatType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_ranges"})
    ->Args({hit_page_val, 500})
    ->Args({hit_page_val, 1000})
    ->Args({hit_page_val, 10000})
    ->Args({hit_page_val, 100000})
    ->Args({hit_page_val, 1000000});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumRanges_ReadRowGroup, false, ::arrow::FloatType)
    ->Iterations(50);

BENCHMARK_TEMPLATE(BM_SingleColumn_NumRanges_PagePruning, true, ::arrow::FloatType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_ranges"})
    ->Args({hit_page_val, 500})
    ->Args({hit_page_val, 1000})
    ->Args({hit_page_val, 10000})
    ->Args({hit_page_val, 100000})
    ->Args({hit_page_val, 1000000});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumRanges_ReadRowGroup, true, ::arrow::FloatType)
    ->Iterations(50);

// ----------------------------------------------------------------------
// Double
BENCHMARK_TEMPLATE(BM_SingleColumn_NumRanges_PagePruning, false, ::arrow::DoubleType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_ranges"})
    ->Args({hit_page_val, 500})
    ->Args({hit_page_val, 1000})
    ->Args({hit_page_val, 10000})
    ->Args({hit_page_val, 100000})
    ->Args({hit_page_val, 1000000});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumRanges_ReadRowGroup, false, ::arrow::DoubleType)
    ->Iterations(50);

BENCHMARK_TEMPLATE(BM_SingleColumn_NumRanges_PagePruning, true, ::arrow::DoubleType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_ranges"})
    ->Args({hit_page_val, 500})
    ->Args({hit_page_val, 1000})
    ->Args({hit_page_val, 10000})
    ->Args({hit_page_val, 100000})
    ->Args({hit_page_val, 1000000});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumRanges_ReadRowGroup, true, ::arrow::DoubleType)
    ->Iterations(50);

// ----------------------------------------------------------------------
// String
BENCHMARK_TEMPLATE(BM_SingleColumn_NumRanges_PagePruning, false, ::arrow::StringType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_ranges"})
    ->Args({hit_page_val, 500})
    ->Args({hit_page_val, 1000})
    ->Args({hit_page_val, 10000})
    ->Args({hit_page_val, 100000})
    ->Args({hit_page_val, 1000000});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumRanges_ReadRowGroup, false, ::arrow::StringType)
    ->Iterations(50);

BENCHMARK_TEMPLATE(BM_SingleColumn_NumRanges_PagePruning, true, ::arrow::StringType)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_ranges"})
    ->Args({hit_page_val, 500})
    ->Args({hit_page_val, 1000})
    ->Args({hit_page_val, 10000})
    ->Args({hit_page_val, 100000})
    ->Args({hit_page_val, 1000000});
BENCHMARK_TEMPLATE2(BM_SingleColumn_NumRanges_ReadRowGroup, true, ::arrow::StringType)
    ->Iterations(50);

// ----------------------------------------------------------------------
// The following benchmarks are designed to test the impact of the number of hit pages on
// scan performance for different types, while the comparative test scans the entire row
// group for all columns. Different from above benchmarks, following benchmarks execute
// with 25 columns with different types, which is closer to real-world usage scenarios.

static const std::shared_ptr<::arrow::Table> table_with_multiple_columns =
    arrow::MakeSimpleTable(num_rows);

static void BM_MultipleColumns_PagePruning(::benchmark::State& state) {
  const int hit_page_percent = static_cast<int>(state.range(0));
  const int page_hit_rows = static_cast<int>(state.range(1));

  std::unique_ptr<arrow::FileReader> reader;
  InitReader(table_with_multiple_columns, &reader);

  const int num_columns = reader->parquet_reader()->metadata()->num_columns();
  auto column_indices = Iota(num_columns);
  auto info = BuildReadInfoWithPageHitPercent(reader, hit_page_percent, page_hit_rows);
  RunBenchmark(state, std::move(reader), column_indices, info);
  auto page_info = std::get<0>(info);
  state.counters["MinPageNum"] = page_info.min_num_pages;
  state.counters["MaxPageNum"] = page_info.max_num_pages;
}

static void BM_MultipleColumns_ReadRowGroup(::benchmark::State& state) {
  std::unique_ptr<arrow::FileReader> reader;
  InitReader(table_with_multiple_columns, &reader);
  const int num_columns = reader->parquet_reader()->metadata()->num_columns();
  auto column_indices = Iota(num_columns);

  int num_pages = 0, min_page_num = std::numeric_limits<int>::max(),
      max_page_num = std::numeric_limits<int>::min();
  auto rg_pi_reader = reader->parquet_reader()->GetPageIndexReader()->RowGroup(0);
  for (int column_index : column_indices) {
    auto col_num_pages = static_cast<int>(
        rg_pi_reader->GetOffsetIndex(column_index)->page_locations().size());
    num_pages += col_num_pages;
    if (col_num_pages < min_page_num) {
      min_page_num = col_num_pages;
    }
    if (col_num_pages > max_page_num) {
      max_page_num = col_num_pages;
    }
  }
  auto info =
      std::make_tuple(PageInfo{num_pages, min_page_num, max_page_num}, std::nullopt);
  RunBenchmark(state, std::move(reader), column_indices, std::move(info));
  state.counters["MinPageNum"] = min_page_num;
  state.counters["MaxPageNum"] = max_page_num;
}

BENCHMARK(BM_MultipleColumns_PagePruning)
    ->Iterations(50)
    ->ArgNames({"hit_page(%)", "hit_rows"})
    ->Args({5, hit_rows_val})
    ->Args({10, hit_rows_val})
    ->Args({30, hit_rows_val})
    ->Args({50, hit_rows_val})
    ->Args({70, hit_rows_val})
    ->Args({100, hit_rows_val});
BENCHMARK(BM_MultipleColumns_ReadRowGroup)->Iterations(50);

}  // namespace parquet::benchmark
