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

#include <unordered_set>

#include "arrow/json/chunker.h"
#include "arrow/json/options.h"
#include "arrow/json/parser.h"
#include "arrow/json/reader.h"
#include "arrow/json/test_common.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace json {

constexpr int kSeed = 0x432432;

template <typename Input>
std::string GenerateTestData(const Input& input, int num_rows,
                             const GenerateOptions& options, bool pretty = false) {
  std::default_random_engine engine(kSeed);
  std::string json;
  for (int i = 0; i < num_rows; ++i) {
    StringBuffer sb;
    Writer writer(sb);
    ABORT_NOT_OK(Generate(input, engine, &writer, options));
    json += pretty ? PrettyPrint(sb.GetString()) : sb.GetString();
    json += "\n";
  }
  return json;
}

template <typename Input>
std::string GenerateTestData(const Input& input, int num_rows, bool pretty = false) {
  return GenerateTestData(input, num_rows, GenerateOptions::Defaults(), pretty);
}

FieldVector GenerateTestFields(int num_fields, int mean_name_length) {
  const std::shared_ptr<DataType> types[] = {boolean(), int64(), float64(), utf8()};

  std::default_random_engine engine(kSeed);

  std::poisson_distribution<int> length_dist(mean_name_length);
  std::uniform_int_distribution<uint16_t> char_dist(32, 126);
  std::uniform_int_distribution<size_t> type_dist(0, std::size(types) - 1);

  std::unordered_set<std::string> names;
  names.reserve(num_fields);

  while (static_cast<int>(names.size()) < num_fields) {
    auto length = length_dist(engine);
    if (!length) continue;
    std::string name(length, '\0');
    for (auto& ch : name) ch = static_cast<char>(char_dist(engine));
    names.emplace(std::move(name));
  }

  FieldVector fields;
  fields.reserve(num_fields);
  for (const auto& name : names) {
    fields.push_back(field(name, types[type_dist(engine)]));
  }

  return fields;
}

FieldVector TestFields() { return {field("int", int32()), field("str", utf8())}; }

static void BenchmarkJSONChunking(benchmark::State& state,  // NOLINT non-const reference
                                  const std::shared_ptr<Buffer>& json,
                                  ParseOptions options) {
  auto chunker = MakeChunker(options);

  for (auto _ : state) {
    std::shared_ptr<Buffer> chunked, partial;
    ABORT_NOT_OK(chunker->Process(json, &chunked, &partial));
  }

  state.SetBytesProcessed(state.iterations() * json->size());
  state.counters["json_size"] = static_cast<double>(json->size());
}

static void ChunkJSONPrettyPrinted(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;

  auto options = ParseOptions::Defaults();
  options.newlines_in_values = true;
  options.explicit_schema = schema(TestFields());

  auto json = GenerateTestData(options.explicit_schema, num_rows, /*pretty=*/true);
  BenchmarkJSONChunking(state, std::make_shared<Buffer>(json), options);
}

static void ChunkJSONLineDelimited(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;

  auto options = ParseOptions::Defaults();
  options.newlines_in_values = false;
  options.explicit_schema = schema(TestFields());

  auto json = GenerateTestData(options.explicit_schema, num_rows);
  BenchmarkJSONChunking(state, std::make_shared<Buffer>(json), options);
  state.SetBytesProcessed(0);
}

static void BenchmarkJSONParsing(benchmark::State& state,  // NOLINT non-const reference
                                 const std::shared_ptr<Buffer>& json,
                                 ParseOptions options) {
  for (auto _ : state) {
    std::unique_ptr<BlockParser> parser;
    ABORT_NOT_OK(BlockParser::Make(options, &parser));
    ABORT_NOT_OK(parser->Parse(json));

    std::shared_ptr<Array> parsed;
    ABORT_NOT_OK(parser->Finish(&parsed));
  }
  state.SetBytesProcessed(state.iterations() * json->size());
  state.counters["json_size"] = static_cast<double>(json->size());
}

static void ParseJSONBlockWithSchema(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  options.explicit_schema = schema(TestFields());

  auto json = GenerateTestData(options.explicit_schema, num_rows);
  BenchmarkJSONParsing(state, std::make_shared<Buffer>(json), options);
}

static void BenchmarkJSONReading(benchmark::State& state,  // NOLINT non-const reference
                                 const std::string& json, ReadOptions read_options,
                                 ParseOptions parse_options) {
  for (auto _ : state) {
    std::shared_ptr<io::InputStream> input;
    ABORT_NOT_OK(MakeStream(json, &input));

    ASSERT_OK_AND_ASSIGN(auto reader, TableReader::Make(default_memory_pool(), input,
                                                        read_options, parse_options));

    std::shared_ptr<Table> table = *reader->Read();
  }

  state.SetBytesProcessed(state.iterations() * json.size());
  state.counters["json_size"] = static_cast<double>(json.size());
}

static void BenchmarkReadJSONBlockWithSchema(
    benchmark::State& state,  // NOLINT non-const reference
    bool use_threads) {
  const int32_t num_rows = 500000;
  auto read_options = ReadOptions::Defaults();
  read_options.use_threads = use_threads;

  auto parse_options = ParseOptions::Defaults();
  parse_options.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  parse_options.explicit_schema = schema(TestFields());

  auto json = GenerateTestData(parse_options.explicit_schema, num_rows);
  BenchmarkJSONReading(state, json, read_options, parse_options);
}

static void ReadJSONBlockWithSchemaSingleThread(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkReadJSONBlockWithSchema(state, false);
}

static void ReadJSONBlockWithSchemaMultiThread(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkReadJSONBlockWithSchema(state, true);
}

static void ParseJSONFields(benchmark::State& state) {  // NOLINT non-const reference
  const bool ordered = !!state.range(0);
  const bool with_schema = !!state.range(1);
  const double sparsity = state.range(2) / 100.0;
  const auto num_fields = static_cast<int>(state.range(3));

  // This would generate approximately 400 kB of JSON data
  int32_t num_rows = static_cast<int32_t>(2e4 / (1.0 - sparsity) / num_fields);
  // ... however, we want enough rows to make setup/finish overhead negligible
  num_rows = std::max<int32_t>(num_rows, 200);
  // ... and also we want to avoid an "Exceeded maximum rows" error.
  num_rows = std::min<int32_t>(num_rows, kMaxParserNumRows);
  // In the end, we will empirically generate between 400 kB and 4 MB of JSON data.

  auto fields = GenerateTestFields(num_fields, 10);

  auto parse_options = ParseOptions::Defaults();
  if (with_schema) {
    parse_options.explicit_schema = schema(fields);
    parse_options.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  }

  auto gen_options = GenerateOptions::Defaults();
  gen_options.field_probability = 1.0 - sparsity;
  gen_options.randomize_field_order = !ordered;

  auto json = GenerateTestData(fields, num_rows, gen_options);
  BenchmarkJSONParsing(state, std::make_shared<Buffer>(json), parse_options);
}

BENCHMARK(ChunkJSONPrettyPrinted);
BENCHMARK(ChunkJSONLineDelimited);
BENCHMARK(ParseJSONBlockWithSchema);

BENCHMARK(ReadJSONBlockWithSchemaSingleThread);
BENCHMARK(ReadJSONBlockWithSchemaMultiThread)->UseRealTime();

BENCHMARK(ParseJSONFields)
    // NOTE: "sparsity" is the percentage of missing fields
    ->ArgNames({"ordered", "schema", "sparsity", "num_fields"})
    ->ArgsProduct({{1, 0}, {1, 0}, {0, 10, 90}, {10, 100, 1000}});

}  // namespace json
}  // namespace arrow
