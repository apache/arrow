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

#include <string>

#include "arrow/json/chunker.h"
#include "arrow/json/options.h"
#include "arrow/json/parser.h"
#include "arrow/json/reader.h"
#include "arrow/json/test-common.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace json {

std::shared_ptr<Schema> TestSchema() {
  return schema({field("int", int32()), field("str", utf8())});
}

constexpr int seed = 0x432432;

std::string TestJsonData(int num_rows, bool pretty = false) {
  std::default_random_engine engine(seed);
  std::string json;
  for (int i = 0; i < num_rows; ++i) {
    StringBuffer sb;
    Writer writer(sb);
    ABORT_NOT_OK(Generate(TestSchema(), engine, &writer));
    json += pretty ? PrettyPrint(sb.GetString()) : sb.GetString();
    json += "\n";
  }

  return json;
}

static void BenchmarkJSONChunking(benchmark::State& state,
                                  const std::shared_ptr<Buffer>& json,
                                  ParseOptions options) {  // NOLINT non-const reference
  auto chunker = Chunker::Make(options);

  for (auto _ : state) {
    std::shared_ptr<Buffer> chunked, partial;
    ABORT_NOT_OK(chunker->Process(json, &chunked, &partial));
  }

  state.SetBytesProcessed(state.iterations() * json->size());
}

static void ChunkJSONPrettyPrinted(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;

  auto options = ParseOptions::Defaults();
  options.newlines_in_values = true;
  options.explicit_schema = TestSchema();

  auto json = TestJsonData(num_rows, /* pretty */ true);
  BenchmarkJSONChunking(state, std::make_shared<Buffer>(json), options);
}

static void ChunkJSONLineDelimited(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;

  auto options = ParseOptions::Defaults();
  options.newlines_in_values = false;
  options.explicit_schema = TestSchema();

  auto json = TestJsonData(num_rows);
  BenchmarkJSONChunking(state, std::make_shared<Buffer>(json), options);
  state.SetBytesProcessed(0);
}

static void BenchmarkJSONParsing(benchmark::State& state,  // NOLINT non-const reference
                                 const std::shared_ptr<Buffer>& json, int32_t num_rows,
                                 ParseOptions options) {
  for (auto _ : state) {
    std::unique_ptr<BlockParser> parser;
    ABORT_NOT_OK(BlockParser::Make(options, &parser));
    ABORT_NOT_OK(parser->Parse(json));

    std::shared_ptr<Array> parsed;
    ABORT_NOT_OK(parser->Finish(&parsed));
  }
  state.SetBytesProcessed(state.iterations() * json->size());
}

static void ParseJSONBlockWithSchema(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  options.explicit_schema = TestSchema();

  auto json = TestJsonData(num_rows);
  BenchmarkJSONParsing(state, std::make_shared<Buffer>(json), num_rows, options);
}

static void BenchmarkJSONReading(benchmark::State& state,  // NOLINT non-const reference
                                 const std::string& json, int32_t num_rows,
                                 ReadOptions read_options, ParseOptions parse_options) {
  for (auto _ : state) {
    std::shared_ptr<io::InputStream> input;
    ABORT_NOT_OK(MakeStream(json, &input));

    std::shared_ptr<TableReader> reader;
    ASSERT_OK(TableReader::Make(default_memory_pool(), input, read_options, parse_options,
                                &reader));

    std::shared_ptr<Table> table;
    ABORT_NOT_OK(reader->Read(&table));
  }

  state.SetBytesProcessed(state.iterations() * json.size());
}

static void BenchmarkReadJSONBlockWithSchema(
    benchmark::State& state, bool use_threads) {  // NOLINT non-const reference
  const int32_t num_rows = 500000;
  auto read_options = ReadOptions::Defaults();
  read_options.use_threads = use_threads;

  auto parse_options = ParseOptions::Defaults();
  parse_options.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  parse_options.explicit_schema = TestSchema();

  auto json = TestJsonData(num_rows);
  BenchmarkJSONReading(state, json, num_rows, read_options, parse_options);
}

static void ReadJSONBlockWithSchemaSingleThread(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkReadJSONBlockWithSchema(state, false);
}

static void ReadJSONBlockWithSchemaMultiThread(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkReadJSONBlockWithSchema(state, true);
}

BENCHMARK(ChunkJSONPrettyPrinted);
BENCHMARK(ChunkJSONLineDelimited);
BENCHMARK(ParseJSONBlockWithSchema);

BENCHMARK(ReadJSONBlockWithSchemaSingleThread);
BENCHMARK(ReadJSONBlockWithSchemaMultiThread)->UseRealTime();

}  // namespace json
}  // namespace arrow
