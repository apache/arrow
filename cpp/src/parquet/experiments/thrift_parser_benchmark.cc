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

// Standalone Unix experiment driver, intentionally outside the default build.
// It creates synthetic metadata or reads only a local file's plaintext footer.
// Output contains aggregate sizes, counts and timings, never file paths, schema
// names, statistics values, key-value metadata or serialized footer contents.
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "parquet/exception.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"

namespace {
using Clock = std::chrono::steady_clock;

std::shared_ptr<const arrow::Buffer> SyntheticFooter(int columns, int row_groups) {
  parquet::schema::NodeVector fields;
  fields.reserve(columns);
  for (int i = 0; i < columns; ++i) {
    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "column_" + std::to_string(i), parquet::Repetition::OPTIONAL,
        parquet::LogicalType::String(), parquet::Type::BYTE_ARRAY));
  }
  parquet::SchemaDescriptor schema;
  schema.Init(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
  auto properties = parquet::WriterProperties::Builder().build();
  auto builder = parquet::FileMetaDataBuilder::Make(&schema, properties);
  parquet::EncodedStatistics statistics;
  statistics.set_min("alpha").set_max("omega").set_null_count(0).set_is_signed(false);
  const std::map<parquet::Encoding::type, int32_t> dictionary_encodings{
      {parquet::Encoding::PLAIN, 1}};
  const std::map<parquet::Encoding::type, int32_t> data_encodings{
      {parquet::Encoding::RLE_DICTIONARY, 1}};
  for (int r = 0; r < row_groups; ++r) {
    auto row_group = builder->AppendRowGroup();
    for (int c = 0; c < columns; ++c) {
      auto chunk = row_group->NextColumnChunk();
      chunk->SetStatistics(statistics);
      const int64_t offset = 4 + (static_cast<int64_t>(r) * columns + c) * 128;
      chunk->Finish(1, offset, 0, offset + 32, 128, 128, true, false,
                    dictionary_encodings, data_encodings);
    }
    row_group->set_num_rows(1);
    row_group->Finish(static_cast<int64_t>(columns) * 128);
  }
  auto metadata = builder->Finish();
  return arrow::Buffer::FromString(metadata->SerializeToString());
}

std::shared_ptr<const arrow::Buffer> ReadFooter(const std::string& path) {
  std::ifstream input(path, std::ios::binary | std::ios::ate);
  const auto file_size = static_cast<int64_t>(input.tellg());
  if (!input || file_size < 12) throw std::runtime_error("Cannot read input footer");
  std::array<uint8_t, 8> trailer{};
  input.seekg(file_size - 8);
  input.read(reinterpret_cast<char*>(trailer.data()), trailer.size());
  if (!input ||
      std::string(reinterpret_cast<const char*>(trailer.data() + 4), 4) != "PAR1") {
    throw std::runtime_error("Expected a plaintext Parquet footer");
  }
  const uint32_t length = static_cast<uint32_t>(trailer[0]) |
                          (static_cast<uint32_t>(trailer[1]) << 8) |
                          (static_cast<uint32_t>(trailer[2]) << 16) |
                          (static_cast<uint32_t>(trailer[3]) << 24);
  if (length > file_size - 12) throw std::runtime_error("Invalid footer length");
  std::string bytes(length, '\0');
  input.seekg(file_size - 8 - length);
  input.read(bytes.data(), length);
  if (!input) throw std::runtime_error("Incomplete footer read");
  std::cout << "file_bytes=" << file_size << '\n';
  return arrow::Buffer::FromString(std::move(bytes));
}

enum Mode {
  Generated,
  Direct,
  Mask,
  SkipStatistics,
  FirstColumnStatistics,
  Summary,
  Count
};
const std::array<const char*, Count> kNames{"generated",           "direct",
                                            "encoding_mask",       "skip_statistics",
                                            "column_0_statistics", "summary_only"};

struct Sample {
  double open_ms;
  double destroy_ms;
};

template <typename Metadata>
Sample Measure(const std::shared_ptr<Metadata>& metadata, Clock::time_point start,
               int expected_columns, int expected_row_groups, Clock::time_point* opened) {
  *opened = Clock::now();
  if (metadata->num_columns() != expected_columns ||
      metadata->num_row_groups() != expected_row_groups) {
    throw std::runtime_error("Metadata counts differ between parser modes");
  }
  return {std::chrono::duration<double, std::milli>(*opened - start).count(), 0};
}

Sample Run(Mode mode, const std::shared_ptr<const arrow::Buffer>& footer,
           int expected_columns, int expected_row_groups) {
  if (mode == Generated) {
    setenv("PARQUET_USE_GENERATED_THRIFT", "1", 1);
  } else {
    unsetenv("PARQUET_USE_GENERATED_THRIFT");
  }
  parquet::ReaderProperties properties;
  if (mode == Mask) properties.set_thrift_encoding_stats_as_mask(true);
  if (mode == SkipStatistics) {
    properties.set_thrift_skip_column_statistics(true);
    properties.set_thrift_skip_encoding_statistics(true);
    properties.set_thrift_skip_size_statistics(true);
  }
  if (mode == FirstColumnStatistics) properties.set_thrift_statistics_columns({0});
  const auto start = Clock::now();
  Clock::time_point opened;
  Sample sample;
  if (mode == Summary) {
    auto metadata = parquet::FastFileMetaData::Make(footer, footer->size(), properties);
    sample = Measure(metadata, start, expected_columns, expected_row_groups, &opened);
    metadata.reset();
  } else {
    auto metadata = parquet::FileMetaData::Make(footer, footer->size(), properties);
    sample = Measure(metadata, start, expected_columns, expected_row_groups, &opened);
    metadata.reset();
  }
  sample.destroy_ms =
      std::chrono::duration<double, std::milli>(Clock::now() - opened).count();
  return sample;
}

double Median(std::vector<double> values) {
  std::sort(values.begin(), values.end());
  const auto middle = values.size() / 2;
  return values.size() % 2 ? values[middle] : (values[middle - 1] + values[middle]) / 2;
}
}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc < 4) {
      std::cerr
          << "Usage: thrift_parser_benchmark --synthetic COLUMNS ROW_GROUPS [ROUNDS]\n"
             "   or: thrift_parser_benchmark --file LOCAL_FILE ROUNDS\n";
      return 2;
    }
    unsetenv("PARQUET_USE_GENERATED_THRIFT");
    unsetenv("PARQUET_SKIP_COLUMN_STATS");
    unsetenv("PARQUET_SKIP_ENCODING_STATS");
    unsetenv("PARQUET_SKIP_SIZE_STATS");
    const std::string kind(argv[1]);
    int rounds = 5;
    std::shared_ptr<const arrow::Buffer> footer;
    if (kind == "--synthetic") {
      const int columns = std::stoi(argv[2]);
      const int row_groups = std::stoi(argv[3]);
      if (columns < 1 || row_groups < 1) throw std::runtime_error("Invalid dimensions");
      rounds = argc > 4 ? std::stoi(argv[4]) : 5;
      footer = SyntheticFooter(columns, row_groups);
    } else if (kind == "--file") {
      rounds = std::stoi(argv[3]);
      footer = ReadFooter(argv[2]);
    } else {
      throw std::runtime_error("Unknown experiment input mode");
    }
    if (rounds < 1) throw std::runtime_error("Invalid round count");
    auto summary = parquet::FastFileMetaData::Make(footer, footer->size());
    const int columns = summary->num_columns();
    const int row_groups = summary->num_row_groups();
    std::cout << "footer_bytes=" << footer->size() << ",columns=" << columns
              << ",row_groups=" << row_groups << ",rounds=" << rounds << '\n';
    std::array<std::vector<double>, Count> open_times, destroy_times;
    for (int mode = 0; mode < Count; ++mode) {
      Run(static_cast<Mode>(mode), footer, columns, row_groups);  // Untimed warm-up.
    }
    for (int round = 0; round < rounds; ++round) {
      for (int i = 0; i < Count; ++i) {
        const int mode = (i + round) % Count;  // Rotate to reduce order bias.
        const auto sample = Run(static_cast<Mode>(mode), footer, columns, row_groups);
        open_times[mode].push_back(sample.open_ms);
        destroy_times[mode].push_back(sample.destroy_ms);
      }
    }
    std::cout << "mode,median_open_ms,median_destroy_ms\n"
              << std::fixed << std::setprecision(3);
    for (int mode = 0; mode < Count; ++mode) {
      std::cout << kNames[mode] << ',' << Median(open_times[mode]) << ','
                << Median(destroy_times[mode]) << '\n';
    }
    unsetenv("PARQUET_USE_GENERATED_THRIFT");
  } catch (...) {
    // Do not echo library errors: they may contain paths or file metadata.
    std::cerr << "Experiment failed; inspect the local input and build configuration.\n";
    return 1;
  }
}
