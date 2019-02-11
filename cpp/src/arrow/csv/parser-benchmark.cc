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

#include "arrow/csv/chunker.h"
#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace csv {

static std::string BuildQuotedData(int32_t num_rows = 10000) {
  std::string one_row = "abc,\"d,f\",12.34,\n";
  std::stringstream ss;
  for (int32_t i = 0; i < num_rows; ++i) {
    ss << one_row;
  }
  return ss.str();
}

static std::string BuildEscapedData(int32_t num_rows = 10000) {
  std::string one_row = "abc,d\\,f,12.34,\n";
  std::stringstream ss;
  for (int32_t i = 0; i < num_rows; ++i) {
    ss << one_row;
  }
  return ss.str();
}

static void BenchmarkCSVChunking(benchmark::State& state,  // NOLINT non-const reference
                                 const std::string& csv, ParseOptions options) {
  Chunker chunker(options);

  while (state.KeepRunning()) {
    uint32_t chunk_size;
    ABORT_NOT_OK(
        chunker.Process(csv.data(), static_cast<uint32_t>(csv.size()), &chunk_size));
    if (chunk_size != csv.size()) {
      std::cerr << "Parsing incomplete\n";
      std::abort();
    }
  }
  state.SetBytesProcessed(state.iterations() * csv.size());
}

static void BM_ChunkCSVQuotedBlock(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto csv = BuildQuotedData(num_rows);
  auto options = ParseOptions::Defaults();
  options.quoting = true;
  options.escaping = false;
  options.newlines_in_values = true;

  BenchmarkCSVChunking(state, csv, options);
}

static void BM_ChunkCSVEscapedBlock(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto csv = BuildEscapedData(num_rows);
  auto options = ParseOptions::Defaults();
  options.quoting = false;
  options.escaping = true;
  options.newlines_in_values = true;

  BenchmarkCSVChunking(state, csv, options);
}

static void BM_ChunkCSVNoNewlinesBlock(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto csv = BuildEscapedData(num_rows);
  auto options = ParseOptions::Defaults();
  options.quoting = true;
  options.escaping = false;
  options.newlines_in_values = false;

  BenchmarkCSVChunking(state, csv, options);
}

static void BenchmarkCSVParsing(benchmark::State& state,  // NOLINT non-const reference
                                const std::string& csv, int32_t num_rows,
                                ParseOptions options) {
  BlockParser parser(options, -1, num_rows + 1);

  while (state.KeepRunning()) {
    uint32_t parsed_size;
    ABORT_NOT_OK(
        parser.Parse(csv.data(), static_cast<uint32_t>(csv.size()), &parsed_size));
    if (parsed_size != csv.size() || parser.num_rows() != num_rows) {
      std::cerr << "Parsing incomplete\n";
      std::abort();
    }
    // Include performance of visiting the parsed values, as that might
    // vary depending on the parser's internal data structures.
    bool dummy_quoted = false;
    uint32_t dummy_size = 0;
    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) {
      dummy_size += size;
      dummy_quoted ^= quoted;
      return Status::OK();
    };
    for (int32_t col = 0; col < parser.num_cols(); ++col) {
      ABORT_NOT_OK(parser.VisitColumn(col, visit));
      benchmark::DoNotOptimize(dummy_size);
      benchmark::DoNotOptimize(dummy_quoted);
    }
  }
  state.SetBytesProcessed(state.iterations() * csv.size());
}

static void BM_ParseCSVQuotedBlock(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto csv = BuildQuotedData(num_rows);
  auto options = ParseOptions::Defaults();
  options.quoting = true;
  options.escaping = false;

  BenchmarkCSVParsing(state, csv, num_rows, options);
}

static void BM_ParseCSVEscapedBlock(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto csv = BuildEscapedData(num_rows);
  auto options = ParseOptions::Defaults();
  options.quoting = false;
  options.escaping = true;

  BenchmarkCSVParsing(state, csv, num_rows, options);
}

BENCHMARK(BM_ChunkCSVQuotedBlock)->Repetitions(3)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_ChunkCSVEscapedBlock)->Repetitions(3)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_ChunkCSVNoNewlinesBlock)->Repetitions(3)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_ParseCSVQuotedBlock)->Repetitions(3)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_ParseCSVEscapedBlock)->Repetitions(3)->Unit(benchmark::kMicrosecond);

}  // namespace csv
}  // namespace arrow
