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

#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/io/memory.h"
#include "arrow/json/options.h"
#include "arrow/json/writer.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace json {

using internal::checked_cast;

namespace {

constexpr int kSeed = 12345;
// length range of json test strings
constexpr int kStrLenMin = 5;
constexpr int kStrLenMax = 50;
// rows/cols of json test dataset
constexpr int kJsonRows = 2000;
constexpr int kJsonCols = 10;

std::shared_ptr<RecordBatch> MakeIntTestBatch(int rows, int cols, int64_t null_percent) {
  random::RandomArrayGenerator rg(kSeed);

  FieldVector fields(cols);
  ArrayVector arrays(cols);
  for (int i = 0; i < cols; ++i) {
    fields[i] = field("i" + std::to_string(i), int64());
    arrays[i] = rg.Int64(rows, -87654321, 123456789, null_percent / 100.);
  }
  return RecordBatch::Make(schema(fields), rows, arrays);
}

std::shared_ptr<RecordBatch> MakeStrTestBatch(int rows, int cols, int64_t null_percent) {
  random::RandomArrayGenerator rg(kSeed + 1);

  // string length varies from kStrLenMin to kStrLenMax for different columns
  auto lengths =
      std::dynamic_pointer_cast<Int32Array>(rg.Int32(cols, kStrLenMin, kStrLenMax));
  FieldVector fields(cols);
  ArrayVector arrays(cols);
  for (int i = 0; i < cols; ++i) {
    fields[i] = field('s' + std::to_string(i), utf8());
    // string length varies by 20% for different rows in same column
    arrays[i] = rg.String(rows, lengths->Value(i), lengths->Value(i) * 6 / 5,
                          null_percent / 100.);
  }

  return RecordBatch::Make(schema(fields), rows, arrays);
}

void BenchmarkWriteJSON(benchmark::State& state, const WriteOptions& options,
                        const RecordBatch& batch) {
  int64_t total_size = 0;

  for (auto _ : state) {
    auto out = io::BufferOutputStream::Create().ValueOrDie();
    ABORT_NOT_OK(WriteJSON(batch, options, out.get()));
    auto buffer = out->Finish().ValueOrDie();
    total_size += buffer->size();
  }

  // byte size of the generated json dataset
  state.SetBytesProcessed(total_size);
  state.SetItemsProcessed(state.iterations() * batch.num_columns() * batch.num_rows());
  state.counters["null_percent"] = static_cast<double>(state.range(0));
}

// Exercises integers with emit_null = true
void WriteJSONNumericEmitNull(benchmark::State& state) {  // NOLINT non-const reference
  auto batch = MakeIntTestBatch(kJsonRows, kJsonCols, state.range(0));
  auto options = WriteOptions::Defaults();
  options.emit_null = true;
  BenchmarkWriteJSON(state, options, *batch);
}

// Exercises integers with emit_null = false
void WriteJSONNumericSkipNull(benchmark::State& state) {  // NOLINT non-const reference
  auto batch = MakeIntTestBatch(kJsonRows, kJsonCols, state.range(0));
  auto options = WriteOptions::Defaults();
  options.emit_null = false;
  BenchmarkWriteJSON(state, options, *batch);
}

// Exercises strings with emit_null = true
void WriteJSONStringEmitNull(benchmark::State& state) {  // NOLINT non-const reference
  auto batch = MakeStrTestBatch(kJsonRows, kJsonCols, state.range(0));
  auto options = WriteOptions::Defaults();
  options.emit_null = true;
  BenchmarkWriteJSON(state, options, *batch);
}

// Exercises with emit_null = false
void WriteJSONStringSkipNull(benchmark::State& state) {  // NOLINT non-const reference
  auto batch = MakeStrTestBatch(kJsonRows, kJsonCols, state.range(0));
  auto options = WriteOptions::Defaults();
  options.emit_null = false;
  BenchmarkWriteJSON(state, options, *batch);
}

void NullPercents(benchmark::internal::Benchmark* bench) {
  std::vector<int> null_percents = {0, 1, 10, 50};
  for (int null_percent : null_percents) {
    bench->Args({null_percent});
  }
}

}  // namespace

BENCHMARK(WriteJSONNumericEmitNull)->Apply(NullPercents);
BENCHMARK(WriteJSONNumericSkipNull)->Apply(NullPercents);
BENCHMARK(WriteJSONStringEmitNull)->Apply(NullPercents);
BENCHMARK(WriteJSONStringSkipNull)->Apply(NullPercents);

}  // namespace json
}  // namespace arrow
