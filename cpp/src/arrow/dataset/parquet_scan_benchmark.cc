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

#include <tuple>
#include "arrow/testing/gtest_util.h"
#include "benchmark/benchmark.h"

#include "arrow/api.h"
#include "arrow/compute/initialize.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/scanner.h"
#include "arrow/io/memory.h"
#include "parquet/arrow/writer.h"

namespace arrow {
namespace dataset {

using parquet::arrow::WriteTable;

Result<std::shared_ptr<Buffer>> WriteStringColParquetBuffer(int64_t nrows) {
  auto schema = arrow::schema({arrow::field("my_string_col", arrow::utf8())});

  arrow::StringBuilder builder;
  for (int64_t i = 0; i < nrows; i++) {
    ARROW_RETURN_NOT_OK(builder.Append("row_" + std::to_string(i)));
  }
  std::shared_ptr<arrow::Array> arr;
  ARROW_RETURN_NOT_OK(builder.Finish(&arr));
  auto table = arrow::Table::Make(schema, {arr});

  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create());
  ARROW_RETURN_NOT_OK(WriteTable(*table, arrow::default_memory_pool(), sink));
  return sink->Finish();
}

static void ParquetScanToTableCastStrings(benchmark::State& state) {
  // GH-43660: Scan parquet data including a String column using a dataset object with
  // LargeString in schema.
  size_t num_batches = state.range(0);
  size_t batch_size = state.range(1);
  size_t nrows = num_batches * batch_size;
  auto format = std::make_shared<ParquetFileFormat>();

  // Create a buffer with a single String column and wrap with FileFragment
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buffer,
                       WriteStringColParquetBuffer(nrows));
  auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
  FileSource source(buffer_reader, buffer->size());
  ASSERT_OK_AND_ASSIGN(auto fragment, format->MakeFragment(source));
  std::vector<std::shared_ptr<FileFragment>> fragments{fragment};

  // Create a dataset from FileFragment and set schema to LargeString (require casting).
  auto schema = arrow::schema({field("my_string_col", arrow::large_utf8())});
  ASSERT_OK_AND_ASSIGN(auto dataset, FileSystemDataset::Make(
                                         schema, compute::literal(true), format,
                                         /*filesystem=*/nullptr, std::move(fragments)));

  ASSERT_OK_AND_ASSIGN(auto builder, dataset->NewScan());
  ASSERT_OK(builder->BatchSize(batch_size));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder->Finish());

  for (auto _ : state) {
    ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
    benchmark::DoNotOptimize(table);
  }

  state.SetItemsProcessed(state.iterations() * nrows);
}

static void ParquetScanBenchmark_Customize(benchmark::internal::Benchmark* b) {
  std::vector<int64_t> num_batches = {1000, 100, 10};
  std::vector<int64_t> batch_sizes = {1000, 10000, 100000};

  for (size_t i = 0; i < batch_sizes.size(); i++) {
    b->Args({(num_batches[i]), batch_sizes[i]});
  }
  b->ArgNames({"num_batches", "batch_size"});
}

BENCHMARK(ParquetScanToTableCastStrings)->Apply(ParquetScanBenchmark_Customize);

}  // namespace dataset
}  // namespace arrow
