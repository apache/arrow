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

// Linter stipulates:
// >> For a static/global string constant, use a C style string instead
const char* one_row = "abc,\"d,f\",12.34,\n";
const char* one_row_escaped = "abc,d\\,f,12.34,\n";

const auto num_rows = static_cast<int32_t>((1024 * 64) / strlen(one_row));

static std::string BuildCSVData(const std::string& row, int32_t repeat) {
  std::stringstream ss;
  for (int32_t i = 0; i < repeat; ++i) {
    ss << row;
  }
  return ss.str();
}

static void BenchmarkCSVChunking(benchmark::State& state,  // NOLINT non-const reference
                                 const std::string& csv, ParseOptions options) {
  Chunker chunker(options);
  const uint32_t csv_size = static_cast<uint32_t>(csv.size());

  while (state.KeepRunning()) {
    uint32_t chunk_size = 0;
    ABORT_NOT_OK(chunker.Process(csv.data(), csv_size, &chunk_size));
    benchmark::DoNotOptimize(chunk_size);
  }

  state.SetBytesProcessed(state.iterations() * csv_size);
}

static void ChunkCSVQuotedBlock(benchmark::State& state) {  // NOLINT non-const reference
  auto csv = BuildCSVData(one_row, num_rows);
  auto options = ParseOptions::Defaults();
  options.quoting = true;
  options.escaping = false;
  options.newlines_in_values = true;

  BenchmarkCSVChunking(state, csv, options);
}

static void ChunkCSVEscapedBlock(benchmark::State& state) {  // NOLINT non-const reference
  auto csv = BuildCSVData(one_row_escaped, num_rows);
  auto options = ParseOptions::Defaults();
  options.quoting = false;
  options.escaping = true;
  options.newlines_in_values = true;

  BenchmarkCSVChunking(state, csv, options);
}

static void ChunkCSVNoNewlinesBlock(
    benchmark::State& state) {  // NOLINT non-const reference
  auto csv = BuildCSVData(one_row_escaped, num_rows);
  auto options = ParseOptions::Defaults();
  options.quoting = true;
  options.escaping = false;
  options.newlines_in_values = false;

  BenchmarkCSVChunking(state, csv, options);
  // Provides better regression stability with timings rather than bogus
  // bandwidth.
  state.SetBytesProcessed(0);
}

static void BenchmarkCSVParsing(benchmark::State& state,  // NOLINT non-const reference
                                const std::string& csv, int32_t rows,
                                ParseOptions options) {
  BlockParser parser(options, -1, rows + 1);
  const uint32_t csv_size = static_cast<uint32_t>(csv.size());

  while (state.KeepRunning()) {
    uint32_t parsed_size = 0;
    ABORT_NOT_OK(parser.Parse(csv.data(), csv_size, &parsed_size));

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

  state.SetBytesProcessed(state.iterations() * csv_size);
}

static void ParseCSVQuotedBlock(benchmark::State& state) {  // NOLINT non-const reference
  auto csv = BuildCSVData(one_row, num_rows);
  auto options = ParseOptions::Defaults();
  options.quoting = true;
  options.escaping = false;

  BenchmarkCSVParsing(state, csv, num_rows, options);
}

static void ParseCSVEscapedBlock(benchmark::State& state) {  // NOLINT non-const reference
  auto csv = BuildCSVData(one_row_escaped, num_rows);
  auto options = ParseOptions::Defaults();
  options.quoting = false;
  options.escaping = true;

  BenchmarkCSVParsing(state, csv, num_rows, options);
}

BENCHMARK(ChunkCSVQuotedBlock);
BENCHMARK(ChunkCSVEscapedBlock);
BENCHMARK(ChunkCSVNoNewlinesBlock);
BENCHMARK(ParseCSVQuotedBlock);
BENCHMARK(ParseCSVEscapedBlock);

}  // namespace csv
}  // namespace arrow
