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

#include <sstream>
#include <string>

#include "arrow/buffer.h"
#include "arrow/csv/converter.h"
#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/csv/reader.h"
#include "arrow/csv/test_common.h"
#include "arrow/io/memory.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/parsing.h"

namespace arrow {
namespace csv {

static std::shared_ptr<Buffer> GenerateTimestampsCSV(
    const std::vector<std::string>& dates, int32_t cols, int32_t rows) {
  std::stringstream ss;
  for (int32_t row = 0; row < rows; ++row) {
    for (int32_t col = 0; col < cols; ++col) {
      ss << dates[rand() % dates.size()];
    }
    ss << "\n";
  }
  return Buffer::FromString(ss.str());
}

static Result<std::shared_ptr<Table>> ReadCSV(const Buffer& data,
                                              const csv::ConvertOptions& convert_opt) {
  ARROW_ASSIGN_OR_RAISE(
      auto table_reader,
      csv::TableReader::Make(
          default_memory_pool(), std::make_shared<io::BufferReader>(data),
          csv::ReadOptions::Defaults(), csv::ParseOptions::Defaults(), convert_opt));
  return table_reader->Read();
}

const std::vector<std::string> kExampleDates = {
    "1917-10-17,", "2018-09-13 22,", "1941-06-22 04:00,", "1945-05-09 09:45:38,"};
constexpr int32_t kNumRows = 10000000;
constexpr int32_t kNumCols = 10;

static void ConvertTimestampVirtualISO8601(benchmark::State& state) {
  auto data = GenerateTimestampsCSV(kExampleDates, kNumCols, kNumRows);
  auto convert_options = csv::ConvertOptions::Defaults();
  convert_options.timestamp_converters.push_back(TimestampParser::MakeISO8601());
  for (auto _ : state) {
    auto result = ReadCSV(*data, convert_options);
    benchmark::DoNotOptimize(result);
  }
  state.SetItemsProcessed(state.iterations());
}

static void ConvertTimestampInlineISO8601(benchmark::State& state) {
  auto data = GenerateTimestampsCSV(kExampleDates, kNumCols, kNumRows);
  auto convert_options = csv::ConvertOptions::Defaults();
  for (auto _ : state) {
    auto result = ReadCSV(*data, convert_options);
    benchmark::DoNotOptimize(result);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(ConvertTimestampInlineISO8601);
BENCHMARK(ConvertTimestampVirtualISO8601);

static std::shared_ptr<BlockParser> BuildInt64Data(int32_t num_rows) {
  const std::vector<std::string> base_rows = {"123\n", "4\n",   "-317005557\n",
                                              "\n",    "N/A\n", "0\n"};
  std::vector<std::string> rows;
  for (int32_t i = 0; i < num_rows; ++i) {
    rows.push_back(base_rows[i % base_rows.size()]);
  }

  std::shared_ptr<BlockParser> result;
  MakeCSVParser(rows, &result);
  return result;
}

static std::shared_ptr<BlockParser> BuildFloatData(int32_t num_rows) {
  const std::vector<std::string> base_rows = {"0\n", "123.456\n", "-3170.55766\n", "\n",
                                              "N/A\n"};
  std::vector<std::string> rows;
  for (int32_t i = 0; i < num_rows; ++i) {
    rows.push_back(base_rows[i % base_rows.size()]);
  }

  std::shared_ptr<BlockParser> result;
  MakeCSVParser(rows, &result);
  return result;
}

static std::shared_ptr<BlockParser> BuildDecimal128Data(int32_t num_rows) {
  const std::vector<std::string> base_rows = {"0\n", "123.456\n", "-3170.55766\n",
                                              "\n",  "N/A\n",     "1233456789.123456789"};
  std::vector<std::string> rows;
  for (int32_t i = 0; i < num_rows; ++i) {
    rows.push_back(base_rows[i % base_rows.size()]);
  }

  std::shared_ptr<BlockParser> result;
  MakeCSVParser(rows, &result);
  return result;
}

static std::shared_ptr<BlockParser> BuildStringData(int32_t num_rows) {
  return BuildDecimal128Data(num_rows);
}

static void BenchmarkConversion(benchmark::State& state,  // NOLINT non-const reference
                                BlockParser& parser,
                                const std::shared_ptr<DataType>& type,
                                ConvertOptions options) {
  std::shared_ptr<Converter> converter = *Converter::Make(type, options);

  while (state.KeepRunning()) {
    auto converted = *converter->Convert(parser, 0 /* col_index */);
    if (converted->length() != parser.num_rows()) {
      std::cerr << "Conversion incomplete\n";
      std::abort();
    }
  }

  state.SetItemsProcessed(state.iterations() * parser.num_rows());
}

constexpr size_t num_rows = 10000;

static void Int64Conversion(benchmark::State& state) {  // NOLINT non-const reference
  auto parser = BuildInt64Data(num_rows);
  auto options = ConvertOptions::Defaults();

  BenchmarkConversion(state, *parser, int64(), options);
}

static void FloatConversion(benchmark::State& state) {  // NOLINT non-const reference
  auto parser = BuildFloatData(num_rows);
  auto options = ConvertOptions::Defaults();

  BenchmarkConversion(state, *parser, float64(), options);
}

static void Decimal128Conversion(benchmark::State& state) {  // NOLINT non-const reference
  auto parser = BuildDecimal128Data(num_rows);
  auto options = ConvertOptions::Defaults();

  BenchmarkConversion(state, *parser, decimal(24, 9), options);
}

static void StringConversion(benchmark::State& state) {  // NOLINT non-const reference
  auto parser = BuildStringData(num_rows);
  auto options = ConvertOptions::Defaults();

  BenchmarkConversion(state, *parser, utf8(), options);
}

BENCHMARK(Int64Conversion);
BENCHMARK(FloatConversion);
BENCHMARK(Decimal128Conversion);
BENCHMARK(StringConversion);

}  // namespace csv
}  // namespace arrow
