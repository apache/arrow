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
#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/types.h"

namespace parquet {

using benchmark::DoNotOptimize;
using parquet::Repetition;
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
  }

  Int32Reader* ResetReader() {
    std::unique_ptr<PageReader> pager;
    pager.reset(new test::MockPageReader(pages_));
    column_reader_ = ColumnReader::Make(descr_.get(), std::move(pager));
    return static_cast<Int32Reader*>(column_reader_.get());
  }

 private:
  std::vector<std::shared_ptr<Page>> pages_;
  std::unique_ptr<ColumnDescriptor> descr_;
  std::shared_ptr<ColumnReader> column_reader_;
};

// Benchmarks Skip for ColumnReader with the following parameters in order:
// - repetition: 0 for REQUIRED, 1 for OPTIONAL, 2 for REPEATED.
// - batch_size: sets how many values to read at each call.
static void BM_Skip(::benchmark::State& state) {
  const auto repetition = static_cast<Repetition::type>(state.range(0));
  const int batch_size = state.range(1);

  BenchmarkHelper helper(repetition, /*num_pages=*/5, /*levels_per_page=*/100000);

  for (auto _ : state) {
    state.PauseTiming();
    Int32Reader* reader = helper.ResetReader();
    int values_count = -1;
    state.ResumeTiming();
    while (values_count != 0) {
      DoNotOptimize(values_count = reader->Skip(batch_size));
    }
  }
}

// Benchmarks ReadBatch for ColumnReader with the following parameters in order:
// - repetition: 0 for REQUIRED, 1 for OPTIONAL, 2 for REPEATED.
// - batch_size: sets how many values to read at each call.
static void BM_ReadBatch(::benchmark::State& state) {
  const auto repetition = static_cast<Repetition::type>(state.range(0));
  const int batch_size = state.range(1);

  BenchmarkHelper helper(repetition, /*num_pages=*/5, /*levels_per_page=*/100000);

  // Vectors to read the values into.
  std::vector<int32_t> read_values(batch_size, -1);
  std::vector<int16_t> read_defs(batch_size, -1);
  std::vector<int16_t> read_reps(batch_size, -1);
  for (auto _ : state) {
    state.PauseTiming();
    Int32Reader* reader = helper.ResetReader();
    int values_count = -1;
    state.ResumeTiming();
    while (values_count != 0) {
      int64_t values_read = 0;
      DoNotOptimize(values_count =
                        reader->ReadBatch(batch_size, read_defs.data(), read_reps.data(),
                                          read_values.data(), &values_read));
    }
  }
}

BENCHMARK(BM_Skip)
    ->Iterations(10)
    ->ArgNames({"Repetition", "BatchSize"})
    ->Args({0, 100})
    ->Args({0, 1000})
    ->Args({0, 10000})
    ->Args({0, 100000})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({1, 100000})
    ->Args({2, 100})
    ->Args({2, 1000})
    ->Args({2, 10000})
    ->Args({2, 100000});

BENCHMARK(BM_ReadBatch)
    ->Iterations(10)
    ->ArgNames({"Repetition", "BatchSize"})
    ->Args({0, 100})
    ->Args({0, 1000})
    ->Args({0, 10000})
    ->Args({0, 100000})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({1, 100000})
    ->Args({2, 100})
    ->Args({2, 1000})
    ->Args({2, 10000})
    ->Args({2, 100000});

}  // namespace benchmark
}  // namespace parquet
