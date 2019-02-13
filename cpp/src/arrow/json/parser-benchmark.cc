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

#include <iostream>
#include <string>

#include "arrow/json/options.h"
#include "arrow/json/parser.h"
#include "arrow/json/test-common.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace json {

static void BenchmarkJSONParsing(benchmark::State& state,  // NOLINT non-const reference
                                 const std::string& json, int32_t num_rows,
                                 ParseOptions options) {
  for (auto _ : state) {
    std::shared_ptr<Buffer> src;
    ABORT_NOT_OK(MakeBuffer(json, &src));
    BlockParser parser(options, src);
    ABORT_NOT_OK(parser.Parse(src));
    if (parser.num_rows() != num_rows) {
      std::cerr << "Parsing incomplete\n";
      std::abort();
    }
    std::shared_ptr<Array> parsed;
    ABORT_NOT_OK(parser.Finish(&parsed));
  }
  state.SetBytesProcessed(state.iterations() * json.size());
}

static void BM_ParseJSONBlockWithSchema(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  options.explicit_schema = schema({field("int", int32()), field("str", utf8())});
  std::mt19937_64 engine;
  std::string json;
  for (int i = 0; i != num_rows; ++i) {
    StringBuffer sb;
    Writer writer(sb);
    ABORT_NOT_OK(Generate(options.explicit_schema, engine, &writer));
    json += sb.GetString();
    json += "\n";
  }
  BenchmarkJSONParsing(state, json, num_rows, options);
}

BENCHMARK(BM_ParseJSONBlockWithSchema)->MinTime(1.0)->Unit(benchmark::kMicrosecond);

}  // namespace json
}  // namespace arrow
