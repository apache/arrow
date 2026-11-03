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

// Generates the seed corpus for the arrow-json-fuzz target. Each seed is the
// target's 8-byte option header followed by an NDJSON payload; the header
// matrix covers the ParseOptions/ReadOptions combinations the fuzzer is
// expected to explore.

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {

constexpr uint8_t kFuzzMarker = 0xFA;

struct Seed {
  std::string name;
  std::vector<uint8_t> header;  // exactly 8 bytes
  std::string payload;
};

void Emit(std::vector<Seed>* seeds, std::string name, std::vector<uint8_t> header,
          const std::string& payload) {
  header.resize(8, 0);
  seeds->push_back(Seed{std::move(name), std::move(header), payload});
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::fprintf(stderr, "Usage: %s <output-dir>\n", argv[0]);
    return 1;
  }
  std::filesystem::create_directories(argv[1]);

  std::vector<Seed> seeds;

  // Header combos:
  //  A: InferType, no schema, 1024, no threads
  //  B: Ignore, no schema, 1024, threads
  //  C: Error, no schema, 4096, no threads
  //  D: newlines_in_values, full explicit schema, 1024
  //  E: InferType, mismatched "x" schema, 65536
  //  F: Error, single uint64 "a" schema, 1024
  const std::vector<std::vector<uint8_t>> headers = {
      {kFuzzMarker, 0, 0, 0}, {kFuzzMarker, 1, 0, 1}, {kFuzzMarker, 2, 0, 2},
      {kFuzzMarker, 4, 1, 0}, {kFuzzMarker, 0, 3, 5}, {kFuzzMarker, 5, 4, 3},
  };
  const char* kHeaderNames[] = {"a", "b", "c", "d", "e", "f"};

  const std::vector<std::pair<std::string, std::string>> samples = {
      {"basic", "{\"a\": 1, \"b\": 2.5, \"c\": \"x\"}\n"
                "{\"a\": null, \"b\": -1e3, \"c\": \"longer\"}\n"},
      {"bools_nested", "{\"a\": true, \"o\": {\"k\": 1}, \"arr\": [1, 2, null]}\n"
                       "{\"a\": false, \"o\": {}, \"arr\": []}\n"},
      {"unexpected_field", "{\"a\": 1, \"zzz\": [1, 2]}\n{\"a\": 2, \"zzz\": null}\n"},
      {"pretty", "{\n\"a\" : 1,\n\"b\" : 2.5\n}\n{\n\"a\" : 3,\n\"b\" : 4.5\n}\n"},
      {"malformed", "{\"a\": 1,\n{\"a\": ,}\n[1, 2\n"},
      {"misc", "{\"c\": \"\\u00e9\", \"a\": 1234567890123456789, \"a\": 1}\n"},
      {"type_mismatch", "{\"a\": \"not an int\"}\n{\"a\": 7}\n"},
      {"blank_lines", "\n\n{\"a\": 1}\n\n\n{\"a\": 2}\n\n"},
      {"empty", ""},
      {"whitespace", "   \n\t\n"},
  };

  for (const auto& sample : samples) {
    for (size_t i = 0; i < headers.size(); ++i) {
      Emit(&seeds, sample.first + "_" + kHeaderNames[i], headers[i], sample.second);
    }
  }

  for (const auto& seed : seeds) {
    std::string path = std::string(argv[1]) + "/" + seed.name;
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(seed.header.data()),
              static_cast<std::streamsize>(seed.header.size()));
    out.write(seed.payload.data(), static_cast<std::streamsize>(seed.payload.size()));
    if (!out) {
      std::fprintf(stderr, "Failed to write %s\n", path.c_str());
      return 1;
    }
  }
  std::printf("Wrote %zu seeds to %s\n", seeds.size(), argv[1]);
  return 0;
}