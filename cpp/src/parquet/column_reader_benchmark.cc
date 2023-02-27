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

#include <type_traits>
#include "benchmark/benchmark.h"
#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/types.h"

namespace parquet {

using benchmark::DoNotOptimize;
using parquet::Repetition;
using parquet::internal::RecordReader;
using parquet::test::MakePages;
using schema::NodePtr;

namespace benchmark {

class BenchmarkHelper {
 public:
  BenchmarkHelper(Repetition::type repetition, int num_pages, int levels_per_page) {
    NodePtr type = schema::Int32("b", repetition);

    if (repetition == Repetition::REQUIRED) {
      descr_ = std::make_unique<ColumnDescriptor>(type, 0, 0);
    } else if (repetition == Repetition::OPTIONAL) {
      descr_ = std::make_unique<ColumnDescriptor>(type, 1, 0);
    } else {
      descr_ = std::make_unique<ColumnDescriptor>(type, 1, 1);
    }

    // Vectors filled with random rep/defs and values to make pages.
    std::vector<int32_t> values;
    std::vector<int16_t> def_levels;
    std::vector<int16_t> rep_levels;
    std::vector<uint8_t> data_buffer;
    MakePages<Int32Type>(descr_.get(), num_pages, levels_per_page, def_levels, rep_levels,
                         values, data_buffer, pages_, Encoding::PLAIN);
    for (const auto& page : pages_) {
      total_size_ += page->size();
    }
  }

  Int32Reader* ResetColumnReader() {
    std::unique_ptr<PageReader> pager;
    pager.reset(new test::MockPageReader(pages_));
    column_reader_ = ColumnReader::Make(descr_.get(), std::move(pager));
    return static_cast<Int32Reader*>(column_reader_.get());
  }

  RecordReader* ResetRecordReader(bool read_dense_for_nullable) {
    std::unique_ptr<PageReader> pager;
    pager.reset(new test::MockPageReader(pages_));
    internal::LevelInfo level_info;
    level_info.def_level = descr_->max_definition_level();
    level_info.rep_level = descr_->max_repetition_level();
    record_reader_ = internal::RecordReader::Make(
        descr_.get(), level_info, ::arrow::default_memory_pool(),
        /*read_dictionary=*/false, read_dense_for_nullable);
    record_reader_->SetPageReader(std::move(pager));
    return record_reader_.get();
  }

  int64_t total_size() const { return total_size_; }

 private:
  std::vector<std::shared_ptr<Page>> pages_;
  std::unique_ptr<ColumnDescriptor> descr_;
  // Reader for column reader benchmarks.
  std::shared_ptr<ColumnReader> column_reader_;
  // Reader for record reader benchmarks.
  std::shared_ptr<RecordReader> record_reader_;
  int64_t total_size_ = 0;
};

// Benchmarks Skip for ColumnReader with the following parameters in order:
// - repetition: 0 for REQUIRED, 1 for OPTIONAL, 2 for REPEATED.
// - batch_size: sets how many values to read at each call.
static void ColumnReaderSkipInt32(::benchmark::State& state) {
  const auto repetition = static_cast<Repetition::type>(state.range(0));
  const auto batch_size = static_cast<int64_t>(state.range(1));

  BenchmarkHelper helper(repetition, /*num_pages=*/16, /*levels_per_page=*/80000);

  for (auto _ : state) {
    state.PauseTiming();
    Int32Reader* reader = helper.ResetColumnReader();
    int64_t values_count = -1;
    state.ResumeTiming();
    while (values_count != 0) {
      DoNotOptimize(values_count = reader->Skip(batch_size));
    }
  }

  state.SetBytesProcessed(state.iterations() * helper.total_size());
}

// Benchmarks ReadBatch for ColumnReader with the following parameters in order:
// - repetition: 0 for REQUIRED, 1 for OPTIONAL, 2 for REPEATED.
// - batch_size: sets how many values to read at each call.
static void ColumnReaderReadBatchInt32(::benchmark::State& state) {
  const auto repetition = static_cast<Repetition::type>(state.range(0));
  const auto batch_size = static_cast<int64_t>(state.range(1));

  BenchmarkHelper helper(repetition, /*num_pages=*/16, /*levels_per_page=*/80000);

  // Vectors to read the values into.
  std::vector<int32_t> read_values(batch_size, -1);
  std::vector<int16_t> read_defs(batch_size, -1);
  std::vector<int16_t> read_reps(batch_size, -1);
  for (auto _ : state) {
    state.PauseTiming();
    Int32Reader* reader = helper.ResetColumnReader();
    int64_t values_count = -1;
    state.ResumeTiming();
    while (values_count != 0) {
      int64_t values_read = 0;
      DoNotOptimize(values_count =
                        reader->ReadBatch(batch_size, read_defs.data(), read_reps.data(),
                                          read_values.data(), &values_read));
    }
  }

  state.SetBytesProcessed(state.iterations() * helper.total_size());
}

// Benchmarks ReadRecords for RecordReader with the following parameters in order:
// - repetition: 0 for REQUIRED, 1 for OPTIONAL, 2 for REPEATED.
// - batch_size: sets how many values to read at each call.
// - read_dense_for_nullable: sets reading dense or spaced.
static void RecordReaderReadRecords(::benchmark::State& state) {
  const auto repetition = static_cast<Repetition::type>(state.range(0));
  const auto batch_size = static_cast<int64_t>(state.range(1));
  const bool read_dense_for_nullable = state.range(2);

  BenchmarkHelper helper(repetition, /*num_pages=*/16, /*levels_per_page=*/80000);

  // Vectors to read the values into.
  for (auto _ : state) {
    state.PauseTiming();
    RecordReader* reader = helper.ResetRecordReader(read_dense_for_nullable);
    int64_t records_read = -1;
    state.ResumeTiming();
    while (records_read != 0) {
      DoNotOptimize(records_read = reader->ReadRecords(batch_size));
      reader->Reset();
    }
  }

  state.SetBytesProcessed(state.iterations() * helper.total_size());
}

// Benchmarks SkipRecords for RecordReader with the following parameters in order:
// - repetition: 0 for REQUIRED, 1 for OPTIONAL, 2 for REPEATED.
// - batch_size: sets how many values to read at each call.
static void RecordReaderSkipRecords(::benchmark::State& state) {
  const auto repetition = static_cast<Repetition::type>(state.range(0));
  const auto batch_size = static_cast<int64_t>(state.range(1));

  BenchmarkHelper helper(repetition, /*num_pages=*/16, /*levels_per_page=*/80000);

  // Vectors to read the values into.
  for (auto _ : state) {
    state.PauseTiming();
    // read_dense_for_nullable should not matter for skip.
    RecordReader* reader = helper.ResetRecordReader(/*read_dense_for_nullable=*/false);
    int64_t records_skipped = -1;
    state.ResumeTiming();
    while (records_skipped != 0) {
      DoNotOptimize(records_skipped = reader->SkipRecords(batch_size));
      reader->Reset();
    }
  }

  state.SetBytesProcessed(state.iterations() * helper.total_size());
}

BENCHMARK(ColumnReaderSkipInt32)
    ->ArgNames({"Repetition", "BatchSize"})
    ->Args({0, 1000})
    ->Args({1, 1000})
    ->Args({2, 1000});

BENCHMARK(ColumnReaderReadBatchInt32)
    ->ArgNames({"Repetition", "BatchSize"})
    ->Args({0, 1000})
    ->Args({1, 1000})
    ->Args({2, 1000});

BENCHMARK(RecordReaderSkipRecords)
    ->ArgNames({"Repetition", "BatchSize"})
    ->Args({0, 1000})
    ->Args({1, 1000})
    ->Args({2, 1000});

BENCHMARK(RecordReaderReadRecords)
    ->ArgNames({"Repetition", "BatchSize", "ReadDense"})
    ->Args({0, 1000, true})
    ->Args({0, 1000, false})
    ->Args({1, 1000, true})
    ->Args({1, 1000, false})
    ->Args({2, 1000, true})
    ->Args({2, 1000, false});

}  // namespace benchmark
}  // namespace parquet
