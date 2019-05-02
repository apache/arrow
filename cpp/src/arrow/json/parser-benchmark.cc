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

static void BenchmarkJSONChunking(benchmark::State& state,  // NOLINT non-const reference
                                  const std::shared_ptr<Buffer>& json,
                                  ParseOptions options) {
  auto chunker = Chunker::Make(options);
  for (auto _ : state) {
    std::shared_ptr<Buffer> chunked, partial;
    ABORT_NOT_OK(chunker->Process(json, &chunked, &partial));
  }
  state.SetBytesProcessed(state.iterations() * json->size());
}

static void BM_ChunkJSONPrettyPrinted(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto options = ParseOptions::Defaults();
  options.newlines_in_values = true;
  options.explicit_schema = schema({field("int", int32()), field("str", utf8())});
  std::default_random_engine engine;
  std::string json;
  for (int i = 0; i < num_rows; ++i) {
    StringBuffer sb;
    Writer writer(sb);
    ABORT_NOT_OK(Generate(options.explicit_schema, engine, &writer));
    json += PrettyPrint(sb.GetString());
    json += "\n";
  }
  BenchmarkJSONChunking(state, std::make_shared<Buffer>(json), options);
}

BENCHMARK(BM_ChunkJSONPrettyPrinted)->MinTime(1.0)->Unit(benchmark::kMicrosecond);

static void BM_ChunkJSONLineDelimited(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto options = ParseOptions::Defaults();
  options.newlines_in_values = false;
  options.explicit_schema = schema({field("int", int32()), field("str", utf8())});
  std::default_random_engine engine;
  std::string json;
  for (int i = 0; i < num_rows; ++i) {
    StringBuffer sb;
    Writer writer(sb);
    ABORT_NOT_OK(Generate(options.explicit_schema, engine, &writer));
    json += sb.GetString();
    json += "\n";
  }
  BenchmarkJSONChunking(state, std::make_shared<Buffer>(json), options);
}

BENCHMARK(BM_ChunkJSONLineDelimited)->MinTime(1.0)->Unit(benchmark::kMicrosecond);

static void BenchmarkJSONParsing(benchmark::State& state,  // NOLINT non-const reference
                                 const std::shared_ptr<Buffer>& json, int32_t num_rows,
                                 ParseOptions options) {
  for (auto _ : state) {
    std::unique_ptr<BlockParser> parser;
    ABORT_NOT_OK(BlockParser::Make(options, &parser));
    ABORT_NOT_OK(parser->Parse(json));
    if (parser->num_rows() != num_rows) {
      std::cerr << "Parsing incomplete\n";
      std::abort();
    }
    std::shared_ptr<Array> parsed;
    ABORT_NOT_OK(parser->Finish(&parsed));
  }
  state.SetBytesProcessed(state.iterations() * json->size());
}

static void BM_ParseJSONBlockWithSchema(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  options.explicit_schema = schema({field("int", int32()), field("str", utf8())});
  std::default_random_engine engine;
  std::string json;
  for (int i = 0; i < num_rows; ++i) {
    StringBuffer sb;
    Writer writer(sb);
    ABORT_NOT_OK(Generate(options.explicit_schema, engine, &writer));
    json += sb.GetString();
    json += "\n";
  }
  BenchmarkJSONParsing(state, std::make_shared<Buffer>(json), num_rows, options);
}

BENCHMARK(BM_ParseJSONBlockWithSchema)->MinTime(1.0)->Unit(benchmark::kMicrosecond);

std::shared_ptr<Table> tables[2];

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

    if (table->num_rows() != num_rows) {
      std::cerr << "Parsing incomplete\n";
      std::abort();
    }

    tables[read_options.use_threads] = table;
  }
  state.SetBytesProcessed(state.iterations() * json.size());

  if (tables[false] && tables[true]) {
    AssertTablesEqual(*tables[false], *tables[true]);
  }
}

static void BM_ReadJSONBlockWithSchema(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 50000;
  auto read_options = ReadOptions::Defaults();
  read_options.use_threads = state.range(0);

  auto parse_options = ParseOptions::Defaults();
  parse_options.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  parse_options.explicit_schema = schema({field("int", int32()), field("str", utf8())});

  std::default_random_engine engine;
  std::string json;
  for (int i = 0; i < num_rows; ++i) {
    StringBuffer sb;
    Writer writer(sb);
    ABORT_NOT_OK(Generate(parse_options.explicit_schema, engine, &writer));
    json += sb.GetString();
    json += "\n";
  }
  BenchmarkJSONReading(state, json, num_rows, read_options, parse_options);
}

BENCHMARK(BM_ReadJSONBlockWithSchema)
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond)
    ->Arg(true)
    ->Arg(false)
    ->UseRealTime();

}  // namespace json
}  // namespace arrow
