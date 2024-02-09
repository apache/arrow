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

#include "arrow/type.h"
#include "benchmark/benchmark.h"

#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/csv/options.h"
#include "arrow/csv/writer.h"
#include "arrow/io/memory.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace csv {

namespace {

constexpr int kSeed = 12345;
// length range of csv test strings
constexpr int kStrLenMin = 5;
constexpr int kStrLenMax = 50;
// rows/cols of csv test dataset
constexpr int kCsvRows = 2000;
constexpr int kCsvCols = 10;

std::shared_ptr<RecordBatch> MakeIntTestBatch(int rows, int cols, int64_t null_percent) {
  random::RandomArrayGenerator rg(kSeed);

  FieldVector fields(cols);
  ArrayVector arrays(cols);
  for (int i = 0; i < cols; ++i) {
    fields[i] = field('i' + std::to_string(i), int32());
    arrays[i] = rg.Int32(rows, -87654321, 123456789, null_percent / 100.);
  }
  return RecordBatch::Make(schema(fields), rows, arrays);
}

std::shared_ptr<RecordBatch> MakeStrTestBatch(int rows, int cols, bool quote,
                                              int64_t null_percent) {
  random::RandomArrayGenerator rg(kSeed + 1);

  // append quote to array elements (return a new array)
  auto append_quote = [](const Array& array) -> std::shared_ptr<Array> {
    typename TypeTraits<StringType>::BuilderType builder;
    const auto& string_array = dynamic_cast<const StringArray&>(array);
    for (int64_t i = 0; i < string_array.length(); ++i) {
      if (string_array.IsValid(i)) {
        ABORT_NOT_OK(builder.Append(string_array.GetString(i) + '"'));
      } else {
        ABORT_NOT_OK(builder.AppendNull());
      }
    }
    std::shared_ptr<Array> result;
    ABORT_NOT_OK(builder.Finish(&result));
    return result;
  };

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
    if (quote) {
      arrays[i] = append_quote(*arrays[i]);
    }
  }

  return RecordBatch::Make(schema(fields), rows, arrays);
}

void BenchmarkWriteCsv(benchmark::State& state, const WriteOptions& options,
                       const RecordBatch& batch) {
  int64_t total_size = 0;

  for (auto _ : state) {
    auto out = io::BufferOutputStream::Create().ValueOrDie();
    ABORT_NOT_OK(WriteCSV(batch, options, out.get()));
    auto buffer = out->Finish().ValueOrDie();
    total_size += buffer->size();
  }

  // byte size of the generated csv dataset
  state.SetBytesProcessed(total_size);
  state.SetItemsProcessed(state.iterations() * batch.num_columns() * batch.num_rows());
  state.counters["null_percent"] = static_cast<double>(state.range(0));
}

// Exercises UnQuotedColumnPopulator with integer
void WriteCsvNumeric(benchmark::State& state) {
  auto batch = MakeIntTestBatch(kCsvRows, kCsvCols, state.range(0));
  BenchmarkWriteCsv(state, WriteOptions::Defaults(), *batch);
}

// Exercise QuotedColumnPopulator with string (without quote)
void WriteCsvStringNoQuote(benchmark::State& state) {
  auto batch = MakeStrTestBatch(kCsvRows, kCsvCols, /*quote=*/false, state.range(0));
  BenchmarkWriteCsv(state, WriteOptions::Defaults(), *batch);
}

// Exercise QuotedColumnPopulator with string (with quote)
void WriteCsvStringWithQuote(benchmark::State& state) {
  auto batch = MakeStrTestBatch(kCsvRows, kCsvCols, /*quote=*/true, state.range(0));
  BenchmarkWriteCsv(state, WriteOptions::Defaults(), *batch);
}

// Exercise UnQuotedColumnPopulator with string
// - caller guarantees there will be no quote in the inputs
void WriteCsvStringRejectQuote(benchmark::State& state) {
  auto batch = MakeStrTestBatch(kCsvRows, kCsvCols, /*quote=*/false, state.range(0));
  auto options = WriteOptions::Defaults();
  options.quoting_style = QuotingStyle::None;
  BenchmarkWriteCsv(state, options, *batch);
}

// Exercise QuotedColumnPopulator with integer
// - check quote even for numeric type (is it useful?)
void WriteCsvNumericCheckQuote(benchmark::State& state) {
  auto batch = MakeIntTestBatch(kCsvRows, kCsvCols, state.range(0));
  auto options = WriteOptions::Defaults();
  options.quoting_style = QuotingStyle::AllValid;
  BenchmarkWriteCsv(state, options, *batch);
}

void NullPercents(benchmark::internal::Benchmark* bench) {
  std::vector<int> null_percents = {0, 1, 10, 50};
  for (int null_percent : null_percents) {
    bench->Args({null_percent});
  }
}

}  // namespace

BENCHMARK(WriteCsvNumeric)->Apply(NullPercents);
BENCHMARK(WriteCsvStringNoQuote)->Apply(NullPercents);
BENCHMARK(WriteCsvStringWithQuote)->Apply(NullPercents);
BENCHMARK(WriteCsvStringRejectQuote)->Apply(NullPercents);
BENCHMARK(WriteCsvNumericCheckQuote)->Apply(NullPercents);

}  // namespace csv
}  // namespace arrow
