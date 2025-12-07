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

#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <unistd.h>
#include <unordered_set>
#include <vector>

#include <benchmark/benchmark.h>

#include "arrow/buffer.h"
#include "arrow/util/alp/AlpWrapper.h"
#include "arrow/util/compression.h"
#include "parquet/encoding.h"
#include "parquet/schema.h"
#include "parquet/types.h"

// This file benchmarks multiple encoding schemes for floating point values in
// Parquet. Structure mirrors Snowflake's FloatComprBenchmark.cpp
//
// It evaluates:
// 1) Compression Ratio
// 2) Encoding Speed
// 3) Decoding Speed
//
// Encoding schemes:
// 1) ALP encoding
// 2) ByteStreamSplit encoding
// 3) ZSTD compression
//
// On synthetic datasets:
// 1) Constant Value
// 2) Increasing values
// 3) Small Range decimal
// 4) Range decimal
// 5) Large Range decimal
// 6) Random values
//
// And real-world datasets:
// 1) floatingpoint_spotify1.csv (9 columns)
// 2) floatingpoint_spotify2.csv (9 columns)
// 3) floatingpoint_citytemperature.csv (1 column)
// 4) floatingpoint_poi.csv (2 columns)
// 5) floatingpoint_birdmigration.csv (1 column)
// 6) floatingpoint_commongovernment.csv (3 columns)
// 7) floatingpoint_arade.csv (4 columns)
// 8) floatingpoint_num_brain.csv (1 column)
// 9) floatingpoint_num_comet.csv (1 column)
// 10) floatingpoint_num_control.csv (1 column)
// 11) floatingpoint_num_plasma.csv (1 column)
// 12) floatingpoint_obs_error.csv (1 column)
// 13) floatingpoint_obs_info.csv (1 column)
// 14) floatingpoint_obs_spitzer.csv (1 column)
// 15) floatingpoint_obs_temp.csv (1 column)
// 16) floatingpoint_msg_bt.csv (1 column)
// 17) floatingpoint_msg_lu.csv (1 column)
// 18) floatingpoint_msg_sp.csv (1 column)
// 19) floatingpoint_msg_sppm.csv (1 column)
// 20) floatingpoint_msg_sweep3d.csv (1 column)

namespace parquet {

using schema::PrimitiveNode;

// Helper function matching Snowflake's pow10
constexpr uint64_t Pow10(uint64_t exp) {
  uint64_t result = 1;
  for (uint64_t i = 0; i < exp; ++i) {
    result *= 10;
  }
  return result;
}

// Encoding type enum (matching Snowflake's ComprEngine pattern)
enum class EncodingType {
  kALP,
  kByteStreamSplit,
  kZSTD,
};

// Helper to create column descriptor for float/double
template <typename DType>
std::shared_ptr<ColumnDescriptor> MakeColumnDescriptor() {
  auto node = PrimitiveNode::Make("column", Repetition::REQUIRED, DType::type_num);
  return std::make_shared<ColumnDescriptor>(node, false, false);
}

// ============================================================================
// Benchmark data base class
// ============================================================================

/// \brief Helper class to set up encoding benchmark data.
///
/// Matches Snowflake's RealComprBenchmarkData<T> structure with encoding parameter.
template <typename T>
struct RealComprBenchmarkData {
  std::vector<T> input_uncompressed;
  std::shared_ptr<Buffer> encoded_data;
  std::vector<T> output_uncompressed;
  uint64_t encoded_size = 0;
  Encoding::type current_encoding;
  std::unique_ptr<::arrow::util::Codec> codec;  // For ZSTD

  virtual ~RealComprBenchmarkData() = default;

  void PrepareBenchmarkData(uint64_t element_count, EncodingType encoding_type) {
    FillUncompressedInput(element_count);

    using DType =
        typename std::conditional<std::is_same<T, float>::value, FloatType,
                                  DoubleType>::type;
    auto descr = MakeColumnDescriptor<DType>();

    // Select encoding based on type
    switch (encoding_type) {
      case EncodingType::kALP:
        current_encoding = Encoding::ALP;
        break;
      case EncodingType::kByteStreamSplit:
        current_encoding = Encoding::BYTE_STREAM_SPLIT;
        codec = ::arrow::util::Codec::Create(::arrow::Compression::ZSTD).ValueOrDie();
        break;
      case EncodingType::kZSTD:
        // ZSTD uses PLAIN encoding + compression
        current_encoding = Encoding::PLAIN;
        codec = ::arrow::util::Codec::Create(::arrow::Compression::ZSTD).ValueOrDie();
        break;
    }

    // Do initial encoding to size buffers
    if (encoding_type == EncodingType::kALP) {
      auto encoder = MakeTypedEncoder<DType>(Encoding::ALP, false, descr.get());
      encoder->Put(input_uncompressed.data(),
                   static_cast<int>(input_uncompressed.size()));
      encoded_data = encoder->FlushValues();
      encoded_size = encoded_data->size();
    } else if (encoding_type == EncodingType::kZSTD) {
      // For ZSTD: Plain encode then compress
      auto encoder = MakeTypedEncoder<DType>(Encoding::PLAIN, false, descr.get());
      encoder->Put(input_uncompressed.data(),
                   static_cast<int>(input_uncompressed.size()));
      auto plain_data = encoder->FlushValues();

      // Compress with ZSTD - use AllocateBuffer to properly manage memory
      int64_t max_compressed_len =
          codec->MaxCompressedLen(plain_data->size(), plain_data->data());
      auto compressed_buffer =
          ::arrow::AllocateResizableBuffer(max_compressed_len).ValueOrDie();
      int64_t actual_size =
          codec
              ->Compress(plain_data->size(), plain_data->data(), max_compressed_len,
                         compressed_buffer->mutable_data())
              .ValueOrDie();
      // Resize to actual compressed size and move to shared_ptr
      (void)compressed_buffer->Resize(actual_size);  // Resize can't fail for shrinking
      encoded_data = std::shared_ptr<Buffer>(std::move(compressed_buffer));
      encoded_size = actual_size;
    } else {
      // For ByteStreamSplit: Direct encoding
      auto encoder = MakeTypedEncoder<DType>(current_encoding, false, descr.get());
      encoder->Put(input_uncompressed.data(),
                   static_cast<int>(input_uncompressed.size()));
      auto byte_stream_split_data = encoder->FlushValues();
      // Compress with ZSTD - use AllocateBuffer to properly manage memory
      int64_t max_compressed_len = codec->MaxCompressedLen(
          byte_stream_split_data->size(), byte_stream_split_data->data());
      auto compressed_buffer =
          ::arrow::AllocateResizableBuffer(max_compressed_len).ValueOrDie();
      int64_t actual_size =
          codec
              ->Compress(byte_stream_split_data->size(), byte_stream_split_data->data(),
                         max_compressed_len, compressed_buffer->mutable_data())
              .ValueOrDie();
      // Resize to actual compressed size and move to shared_ptr
      (void)compressed_buffer->Resize(actual_size);  // Resize can't fail for shrinking
      encoded_data = std::shared_ptr<Buffer>(std::move(compressed_buffer));
      encoded_size = actual_size;
    }

    // Prepare output buffer
    output_uncompressed.resize(input_uncompressed.size());
  }

  virtual void FillUncompressedInput(uint64_t element_count) = 0;
};

// ============================================================================
// Synthetic Data Generators
// ============================================================================

template <typename T>
struct ConstantValues : public RealComprBenchmarkData<T> {
  void FillUncompressedInput(uint64_t element_count) override {
    const T value = static_cast<T>(1.1);
    this->input_uncompressed = std::vector<T>(element_count, value);
  }
};

template <typename T>
struct IncreasingValues : public RealComprBenchmarkData<T> {
  void FillUncompressedInput(uint64_t element_count) override {
    this->input_uncompressed.resize(element_count);
    T current_value = 0.0;
    for (uint64_t i = 0; i < element_count; i++) {
      this->input_uncompressed[i] = current_value;
      current_value += 1.0;
    }
  }
};

template <typename T>
struct DecimalSmallRange : public RealComprBenchmarkData<T> {
  void FillUncompressedInput(uint64_t element_count) override {
    this->input_uncompressed.resize(element_count);
    const uint64_t min_val = 100;
    const uint64_t max_val = 1000;
    const uint64_t decimal_places = 2;
    const uint64_t mult = Pow10(decimal_places);

    std::uniform_int_distribution<uint64_t> unif(min_val * mult, max_val * mult);
    std::default_random_engine re;
    for (uint64_t i = 0; i < element_count; i++) {
      this->input_uncompressed[i] = unif(re) * 1.0 / mult;
    }
  }
};

template <typename T>
struct DecimalRange : public RealComprBenchmarkData<T> {
  void FillUncompressedInput(uint64_t element_count) override {
    this->input_uncompressed.resize(element_count);
    const uint64_t min_val = 1000;
    const uint64_t max_val = 100000;
    const uint64_t decimal_places = 6;
    const uint64_t mult = Pow10(decimal_places);

    std::uniform_int_distribution<uint64_t> unif(min_val * mult, max_val * mult);
    std::default_random_engine re;
    for (uint64_t i = 0; i < element_count; i++) {
      this->input_uncompressed[i] = unif(re) * 1.0 / mult;
    }
  }
};

template <typename T>
struct DecimalLargeRange : public RealComprBenchmarkData<T> {
  void FillUncompressedInput(uint64_t element_count) override {
    this->input_uncompressed.resize(element_count);
    const uint64_t min_val = 1000;
    const uint64_t max_val = 1000000;
    const uint64_t decimal_places = 6;
    const uint64_t mult = Pow10(decimal_places);

    std::uniform_int_distribution<uint64_t> unif(min_val * mult, max_val * mult);
    std::default_random_engine re;
    for (uint64_t i = 0; i < element_count; i++) {
      this->input_uncompressed[i] = unif(re) * 1.0 / mult;
    }
  }
};

template <typename T>
struct RandomValues : public RealComprBenchmarkData<T> {
  void FillUncompressedInput(uint64_t element_count) override {
    this->input_uncompressed.resize(element_count);
    std::uniform_real_distribution<T> unif(std::numeric_limits<T>::min(),
                                           std::numeric_limits<T>::max());
    std::default_random_engine re;
    for (uint64_t i = 0; i < element_count; i++) {
      this->input_uncompressed[i] = unif(re);
    }
  }
};

// ============================================================================
// CSV Loading Infrastructure (for real-world datasets)
// ============================================================================

// Extract tarball once and return the data directory path
std::string GetDataDirectory() {
  static std::string data_dir;
  static bool initialized = false;

  if (!initialized) {
    // Find the tarball location relative to this source file
    std::string tarball_path = std::string(__FILE__);
    tarball_path = tarball_path.substr(0, tarball_path.find_last_of("/\\"));
    tarball_path = tarball_path.substr(0, tarball_path.find_last_of("/\\"));
    tarball_path += "/arrow/util/alp/data/floatingpoint_data.tar.gz";

    // Use a fixed extraction directory that can be reused across runs
    data_dir = "/tmp/parquet_alp_benchmark_data";

    // Check if tarball exists
    std::ifstream tarball_check(tarball_path);
    if (!tarball_check.good()) {
      // Fall back to original directory if tarball not found
      data_dir = std::string(__FILE__);
      data_dir = data_dir.substr(0, data_dir.find_last_of("/\\"));
      data_dir = data_dir.substr(0, data_dir.find_last_of("/\\"));
      data_dir += "/arrow/util/alp/data";
      initialized = true;
      return data_dir;
    }

    // Check if extraction directory already exists and has files
    std::ifstream check_file(data_dir + "/floatingpoint_spotify1.csv");
    if (check_file.good()) {
      // Directory already exists with data, reuse it
      initialized = true;
      return data_dir;
    }

    // Create extraction directory and extract tarball
    std::string mkdir_cmd = "mkdir -p " + data_dir;
    std::string extract_cmd = "tar -xzf " + tarball_path + " -C " + data_dir;

    if (system(mkdir_cmd.c_str()) == 0 && system(extract_cmd.c_str()) == 0) {
      initialized = true;
    } else {
      // Extraction failed, fall back to original directory
      data_dir = std::string(__FILE__);
      data_dir = data_dir.substr(0, data_dir.find_last_of("/\\"));
      data_dir = data_dir.substr(0, data_dir.find_last_of("/\\"));
      data_dir += "/arrow/util/alp/data";
      initialized = true;
    }
  }

  return data_dir;
}

std::vector<std::string> SplitCsvRow(const std::string& line, char delimiter = ',') {
  std::vector<std::string> columns;
  std::istringstream stream(line);
  std::string cell;

  while (std::getline(stream, cell, delimiter)) {
    columns.push_back(cell);
  }
  return columns;
}

std::vector<double> LoadSpotifyColumn(const std::string& column_name,
                                      const std::string& filename) {
  std::vector<double> values;

  static const std::unordered_set<std::string> kValidFloatColumns = {
      "danceability", "energy",     "loudness",        "speechiness", "acousticness",
      "instrumentalness", "liveness", "valence",         "tempo"};

  if (kValidFloatColumns.find(column_name) == kValidFloatColumns.end()) {
    std::cerr << "Column '" << column_name << "' is not a supported double column"
              << std::endl;
    return values;
  }

  std::string file_path = GetDataDirectory() + "/" + filename;

  std::ifstream file(file_path);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << file_path << std::endl;
    return values;
  }

  std::string file_content((std::istreambuf_iterator<char>(file)),
                           std::istreambuf_iterator<char>());
  file.close();

  std::istringstream ss(file_content);
  std::string line;
  size_t column_index = SIZE_MAX;

  if (std::getline(ss, line)) {
    std::istringstream header_stream(line);
    std::string header;
    size_t index = 0;

    while (std::getline(header_stream, header, ',')) {
      header.erase(0, header.find_first_not_of(" \t\r\n"));
      header.erase(header.find_last_not_of(" \t\r\n") + 1);

      if (header == column_name) {
        column_index = index;
        break;
      }
      index++;
    }
  }

  if (column_index == SIZE_MAX) {
    std::cerr << "Column '" << column_name << "' not found in header" << std::endl;
    return values;
  }

  while (std::getline(ss, line)) {
    std::vector<std::string> columns = SplitCsvRow(line);
    if (column_index < columns.size()) {
      try {
        double value = std::stod(columns[column_index]);
        values.push_back(value);
      } catch (const std::exception& e) {
        // Skip invalid values silently
      }
    }
  }

  return values;
}

// ============================================================================
// Real-World Dataset Classes
// ============================================================================

template <typename T>
struct SpotifyData : public RealComprBenchmarkData<T> {
  std::string column_name;

  explicit SpotifyData(const std::string& column) : column_name(column) {}

  void FillUncompressedInput(uint64_t /*element_count*/) override {
    std::vector<double> spotify_values =
        LoadSpotifyColumn(column_name, "floatingpoint_spotify1.csv");

    this->input_uncompressed.resize(spotify_values.size());
    for (size_t i = 0; i < spotify_values.size(); ++i) {
      this->input_uncompressed[i] = static_cast<T>(spotify_values[i]);
    }
  }
};

template <typename T>
struct SpotifyData2 : public RealComprBenchmarkData<T> {
  std::string column_name;

  explicit SpotifyData2(const std::string& column) : column_name(column) {}

  void FillUncompressedInput(uint64_t /*element_count*/) override {
    std::vector<double> spotify_values =
        LoadSpotifyColumn(column_name, "floatingpoint_spotify2.csv");

    this->input_uncompressed.resize(spotify_values.size());
    for (size_t i = 0; i < spotify_values.size(); ++i) {
      this->input_uncompressed[i] = static_cast<T>(spotify_values[i]);
    }
  }
};

// Load AvgTemperature column from City Temperature CSV data
std::vector<double> LoadCityTemperatureColumn() {
  std::vector<double> values;

  std::string file_path = GetDataDirectory() + "/floatingpoint_citytemperature.csv";

  std::ifstream file(file_path);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << file_path << std::endl;
    return values;
  }

  std::string line;
  // Skip header line
  if (std::getline(file, line)) {
    // Process data lines - each line is a single temperature value
    while (std::getline(file, line)) {
      try {
        double value = std::stod(line);
        values.push_back(value);
      } catch (const std::exception& e) {
        // Skip invalid values
        continue;
      }
    }
  }
  file.close();

  return values;
}

// Load any double-point column from POI CSV data
std::vector<double> LoadPoiColumn(const std::string& column_name) {
  std::vector<double> values;

  static const std::unordered_set<std::string> kValidFloatColumns = {"latitude_radian",
                                                                     "longitude_radian"};

  if (kValidFloatColumns.find(column_name) == kValidFloatColumns.end()) {
    std::cerr << "Column '" << column_name << "' is not a supported double column"
              << std::endl;
    return values;
  }

  std::string file_path = GetDataDirectory() + "/floatingpoint_poi.csv";

  std::ifstream file(file_path);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << file_path << std::endl;
    return values;
  }

  std::string line;
  // Read header line to find column index
  if (!std::getline(file, line)) {
    std::cerr << "Failed to read header from POI CSV" << std::endl;
    return values;
  }

  std::vector<std::string> headers = SplitCsvRow(line);
  int column_index = -1;
  for (size_t i = 0; i < headers.size(); ++i) {
    std::string trimmed_header = headers[i];
    trimmed_header.erase(0, trimmed_header.find_first_not_of(" \t\r\n"));
    trimmed_header.erase(trimmed_header.find_last_not_of(" \t\r\n") + 1);

    if (trimmed_header == column_name) {
      column_index = static_cast<int>(i);
      break;
    }
  }

  if (column_index == -1) {
    std::cerr << "Column '" << column_name << "' not found in POI CSV header"
              << std::endl;
    return values;
  }

  // Process data lines
  while (std::getline(file, line)) {
    std::vector<std::string> columns = SplitCsvRow(line);
    if (columns.size() > static_cast<size_t>(column_index)) {
      try {
        double value = std::stod(columns[column_index]);
        values.push_back(value);
      } catch (const std::exception& e) {
        continue;
      }
    }
  }
  file.close();

  return values;
}

// Load Bird Migration data
std::vector<double> LoadBirdMigrationData() {
  std::vector<double> values;

  std::string file_path = GetDataDirectory() + "/floatingpoint_birdmigration.csv";

  std::ifstream file(file_path);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << file_path << std::endl;
    return values;
  }

  std::string line;
  // Skip header line
  if (!std::getline(file, line)) {
    std::cerr << "Failed to read header from bird-migration CSV" << std::endl;
    return values;
  }

  while (std::getline(file, line)) {
    try {
      double value = std::stod(line);
      values.push_back(value);
    } catch (const std::exception& e) {
      continue;
    }
  }
  file.close();

  return values;
}

// Load Common Government column
std::vector<double> LoadCommonGovernmentColumn(const std::string& column_name) {
  std::vector<double> values;

  static const std::unordered_set<std::string> kValidFloatColumns = {"amount1", "amount2",
                                                                     "amount3"};

  if (kValidFloatColumns.find(column_name) == kValidFloatColumns.end()) {
    std::cerr << "Column '" << column_name << "' is not a supported double column"
              << std::endl;
    return values;
  }

  size_t column_index = SIZE_MAX;
  if (column_name == "amount1")
    column_index = 0;
  else if (column_name == "amount2")
    column_index = 1;
  else if (column_name == "amount3")
    column_index = 2;

  std::string file_path = GetDataDirectory() + "/floatingpoint_commongovernment.csv";

  std::ifstream file(file_path);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << file_path << std::endl;
    return values;
  }

  std::string line;
  while (std::getline(file, line)) {
    std::vector<std::string> columns = SplitCsvRow(line, '|');
    if (column_index < columns.size()) {
      try {
        double value = std::stod(columns[column_index]);
        values.push_back(value);
      } catch (const std::exception& e) {
        // Skip invalid values
      }
    }
  }
  file.close();

  return values;
}

// Load Arade column
std::vector<double> LoadAradeColumn(const std::string& column_name) {
  std::vector<double> values;

  static const std::unordered_set<std::string> kValidFloatColumns = {"value1", "value2",
                                                                     "value3", "value4"};

  if (kValidFloatColumns.find(column_name) == kValidFloatColumns.end()) {
    std::cerr << "Column '" << column_name << "' is not a supported double column"
              << std::endl;
    return values;
  }

  size_t column_index = SIZE_MAX;
  if (column_name == "value1")
    column_index = 0;
  else if (column_name == "value2")
    column_index = 1;
  else if (column_name == "value3")
    column_index = 2;
  else if (column_name == "value4")
    column_index = 3;

  std::string file_path = GetDataDirectory() + "/floatingpoint_arade.csv";

  std::ifstream file(file_path);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << file_path << std::endl;
    return values;
  }

  std::string line;
  while (std::getline(file, line)) {
    std::vector<std::string> columns = SplitCsvRow(line, '|');
    if (column_index < columns.size()) {
      try {
        double value = std::stod(columns[column_index]);
        values.push_back(value);
      } catch (const std::exception& e) {
        // Skip invalid values
      }
    }
  }
  file.close();

  return values;
}

// Generic loader for single-column FPC-format CSV files (with header)
std::vector<double> LoadSingleColumnFpcData(const std::string& dataset_name) {
  std::vector<double> values;

  std::string file_path = GetDataDirectory() + "/floatingpoint_" + dataset_name + ".csv";

  std::ifstream file(file_path);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << file_path << std::endl;
    return values;
  }

  std::string line;
  // Skip header line
  if (!std::getline(file, line)) {
    std::cerr << "Failed to read header from " << dataset_name << " CSV" << std::endl;
    return values;
  }

  while (std::getline(file, line)) {
    try {
      double value = std::stod(line);
      values.push_back(value);
    } catch (const std::exception& e) {
      continue;
    }
  }
  file.close();

  return values;
}

// Individual loaders for FPC datasets
std::vector<double> LoadNumBrainData() { return LoadSingleColumnFpcData("num_brain"); }
std::vector<double> LoadNumCometData() { return LoadSingleColumnFpcData("num_comet"); }
std::vector<double> LoadNumControlData() {
  return LoadSingleColumnFpcData("num_control");
}
std::vector<double> LoadNumPlasmaData() { return LoadSingleColumnFpcData("num_plasma"); }
std::vector<double> LoadObsErrorData() { return LoadSingleColumnFpcData("obs_error"); }
std::vector<double> LoadObsInfoData() { return LoadSingleColumnFpcData("obs_info"); }
std::vector<double> LoadObsSpitzerData() {
  return LoadSingleColumnFpcData("obs_spitzer");
}
std::vector<double> LoadObsTempData() { return LoadSingleColumnFpcData("obs_temp"); }
std::vector<double> LoadMsgBtData() { return LoadSingleColumnFpcData("msg_bt"); }
std::vector<double> LoadMsgLuData() { return LoadSingleColumnFpcData("msg_lu"); }
std::vector<double> LoadMsgSpData() { return LoadSingleColumnFpcData("msg_sp"); }
std::vector<double> LoadMsgSppmData() { return LoadSingleColumnFpcData("msg_sppm"); }
std::vector<double> LoadMsgSweep3dData() {
  return LoadSingleColumnFpcData("msg_sweep3d");
}

// Data classes for all additional datasets
template <typename T>
struct CityTemperatureData : public RealComprBenchmarkData<T> {
  CityTemperatureData() = default;

  void FillUncompressedInput(uint64_t /*element_count*/) override {
    std::vector<double> values = LoadCityTemperatureColumn();
    this->input_uncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->input_uncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

template <typename T>
struct PoiData : public RealComprBenchmarkData<T> {
  std::string column_name;

  explicit PoiData(const std::string& column) : column_name(column) {}

  void FillUncompressedInput(uint64_t /*element_count*/) override {
    std::vector<double> values = LoadPoiColumn(column_name);
    this->input_uncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->input_uncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

template <typename T>
struct BirdMigrationData : public RealComprBenchmarkData<T> {
  explicit BirdMigrationData() {}

  void FillUncompressedInput(uint64_t /*element_count*/) override {
    std::vector<double> values = LoadBirdMigrationData();
    this->input_uncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->input_uncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

template <typename T>
struct CommonGovernmentData : public RealComprBenchmarkData<T> {
  std::string column_name;

  explicit CommonGovernmentData(const std::string& column) : column_name(column) {}

  void FillUncompressedInput(uint64_t /*element_count*/) override {
    std::vector<double> values = LoadCommonGovernmentColumn(column_name);
    this->input_uncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->input_uncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

template <typename T>
struct AradeData : public RealComprBenchmarkData<T> {
  std::string column_name;

  explicit AradeData(const std::string& column) : column_name(column) {}

  void FillUncompressedInput(uint64_t /*element_count*/) override {
    std::vector<double> values = LoadAradeColumn(column_name);
    this->input_uncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->input_uncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

// Generic template for FPC single-column datasets
template <typename T, std::vector<double> (*LoaderFunc)()>
struct FpcDataset : public RealComprBenchmarkData<T> {
  explicit FpcDataset() {}

  void FillUncompressedInput(uint64_t /*element_count*/) override {
    std::vector<double> values = LoaderFunc();
    this->input_uncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->input_uncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

// Type aliases for each FPC dataset
template <typename T>
using NumBrainData = FpcDataset<T, LoadNumBrainData>;
template <typename T>
using NumCometData = FpcDataset<T, LoadNumCometData>;
template <typename T>
using NumControlData = FpcDataset<T, LoadNumControlData>;
template <typename T>
using NumPlasmaData = FpcDataset<T, LoadNumPlasmaData>;
template <typename T>
using ObsErrorData = FpcDataset<T, LoadObsErrorData>;
template <typename T>
using ObsInfoData = FpcDataset<T, LoadObsInfoData>;
template <typename T>
using ObsSpitzerData = FpcDataset<T, LoadObsSpitzerData>;
template <typename T>
using ObsTempData = FpcDataset<T, LoadObsTempData>;
template <typename T>
using MsgBtData = FpcDataset<T, LoadMsgBtData>;
template <typename T>
using MsgLuData = FpcDataset<T, LoadMsgLuData>;
template <typename T>
using MsgSpData = FpcDataset<T, LoadMsgSpData>;
template <typename T>
using MsgSppmData = FpcDataset<T, LoadMsgSppmData>;
template <typename T>
using MsgSweep3dData = FpcDataset<T, LoadMsgSweep3dData>;

// ============================================================================
// Benchmark Fixture (matching Snowflake's DoubleBenchmark structure)
// ============================================================================

template <typename T>
class DoubleBenchmark : public benchmark::Fixture {
 public:
  static constexpr uint64_t kElementCount = 50000;  // Matches Snowflake exactly

  void Setup(std::unique_ptr<RealComprBenchmarkData<T>> bd, uint64_t element_count,
             EncodingType encoding_type) {
    encoding_type_ = encoding_type;
    bd_ = std::move(bd);
    bd_->PrepareBenchmarkData(element_count, encoding_type);
  }

  void VerifyDataCompress() {
    Decompress();
    if (memcmp(bd_->input_uncompressed.data(), bd_->output_uncompressed.data(),
               bd_->input_uncompressed.size() * sizeof(T)) != 0) {
      std::cerr << "verificationFailed" << std::endl;
    }
  }

  void VerifyDataDecompress() {
    if (memcmp(bd_->input_uncompressed.data(), bd_->output_uncompressed.data(),
               bd_->input_uncompressed.size() * sizeof(T)) != 0) {
      std::cerr << "verificationFailed" << std::endl;
    }
  }

  void Compress() {
    using DType =
        typename std::conditional<std::is_same<T, float>::value, FloatType,
                                  DoubleType>::type;
    auto descr = MakeColumnDescriptor<DType>();

    if (encoding_type_ == EncodingType::kALP) {
      auto encoder = MakeTypedEncoder<DType>(Encoding::ALP, false, descr.get());
      encoder->Put(bd_->input_uncompressed.data(),
                   static_cast<int>(bd_->input_uncompressed.size()));
      bd_->encoded_data = encoder->FlushValues();
      bd_->encoded_size = bd_->encoded_data->size();
    } else if (encoding_type_ == EncodingType::kZSTD) {
      // For ZSTD: Plain encode then compress
      auto encoder = MakeTypedEncoder<DType>(Encoding::PLAIN, false, descr.get());
      encoder->Put(bd_->input_uncompressed.data(),
                   static_cast<int>(bd_->input_uncompressed.size()));
      auto plain_data = encoder->FlushValues();

      // Compress with ZSTD - use AllocateBuffer to properly manage memory
      int64_t max_compressed_len =
          bd_->codec->MaxCompressedLen(plain_data->size(), plain_data->data());
      auto compressed_buffer =
          ::arrow::AllocateResizableBuffer(max_compressed_len).ValueOrDie();
      int64_t actual_size =
          bd_->codec
              ->Compress(plain_data->size(), plain_data->data(), max_compressed_len,
                         compressed_buffer->mutable_data())
              .ValueOrDie();
      // Resize to actual compressed size and move to shared_ptr
      (void)compressed_buffer->Resize(actual_size);  // Resize can't fail for shrinking
      bd_->encoded_data = std::shared_ptr<Buffer>(std::move(compressed_buffer));
      bd_->encoded_size = actual_size;
    } else {
      // For ByteStreamSplit: Direct encoding
      auto encoder = MakeTypedEncoder<DType>(bd_->current_encoding, false, descr.get());
      encoder->Put(bd_->input_uncompressed.data(),
                   static_cast<int>(bd_->input_uncompressed.size()));
      auto byte_stream_split_data = encoder->FlushValues();
      // Compress with ZSTD - use AllocateBuffer to properly manage memory
      int64_t max_compressed_len = bd_->codec->MaxCompressedLen(
          byte_stream_split_data->size(), byte_stream_split_data->data());
      auto compressed_buffer =
          ::arrow::AllocateResizableBuffer(max_compressed_len).ValueOrDie();
      int64_t actual_size =
          bd_->codec
              ->Compress(byte_stream_split_data->size(), byte_stream_split_data->data(),
                         max_compressed_len, compressed_buffer->mutable_data())
              .ValueOrDie();
      // Resize to actual compressed size and move to shared_ptr
      (void)compressed_buffer->Resize(actual_size);  // Resize can't fail for shrinking
      bd_->encoded_data = std::shared_ptr<Buffer>(std::move(compressed_buffer));
      bd_->encoded_size = actual_size;
    }
  }

  void Decompress() {
    using DType =
        typename std::conditional<std::is_same<T, float>::value, FloatType,
                                  DoubleType>::type;
    auto descr = MakeColumnDescriptor<DType>();

    if (encoding_type_ == EncodingType::kALP) {
      // For ALP: Use Parquet decoder
      auto decoder = MakeTypedDecoder<DType>(Encoding::ALP, descr.get());
      decoder->SetData(static_cast<int>(bd_->input_uncompressed.size()),
                       bd_->encoded_data->data(),
                       static_cast<int>(bd_->encoded_data->size()));
      decoder->Decode(bd_->output_uncompressed.data(),
                      static_cast<int>(bd_->output_uncompressed.size()));
    } else if (encoding_type_ == EncodingType::kZSTD) {
      // For ZSTD: Decompress then plain decode
      int64_t decompressed_len = bd_->input_uncompressed.size() * sizeof(T);
      std::vector<uint8_t> decompressed(decompressed_len);
      int64_t actual_size =
          bd_->codec
              ->Decompress(bd_->encoded_data->size(), bd_->encoded_data->data(),
                           decompressed_len, decompressed.data())
              .ValueOrDie();

      // Plain decode
      auto decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, descr.get());
      decoder->SetData(static_cast<int>(bd_->input_uncompressed.size()),
                       decompressed.data(), static_cast<int>(actual_size));
      decoder->Decode(bd_->output_uncompressed.data(),
                      static_cast<int>(bd_->output_uncompressed.size()));
    } else {
      int64_t decompressed_len = bd_->input_uncompressed.size() * sizeof(T);
      std::vector<uint8_t> decompressed(decompressed_len);
      int64_t actual_size =
          bd_->codec
              ->Decompress(bd_->encoded_data->size(), bd_->encoded_data->data(),
                           decompressed_len, decompressed.data())
              .ValueOrDie();

      // For ByteStreamSplit: Direct decoding
      auto decoder = MakeTypedDecoder<DType>(bd_->current_encoding, descr.get());
      decoder->SetData(static_cast<int>(bd_->input_uncompressed.size()),
                       decompressed.data(), static_cast<int>(actual_size));
      decoder->Decode(bd_->output_uncompressed.data(),
                      static_cast<int>(bd_->output_uncompressed.size()));
    }
  }

  void BenchmarkCompress(benchmark::State& state,
                         std::unique_ptr<RealComprBenchmarkData<T>> bd,
                         EncodingType encoding_type) {
    Setup(std::move(bd), kElementCount, encoding_type);

    uint64_t iteration_count = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for (auto _ : state) {
      Compress();
      iteration_count++;
    }
    auto end = std::chrono::high_resolution_clock::now();
    const uint64_t overall_time_us =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    state.counters["MB/s"] =
        static_cast<double>(bd_->input_uncompressed.size() * sizeof(T) *
                            iteration_count) /
        (overall_time_us);

    VerifyDataCompress();
    state.counters["Compression Ratio Percent"] =
        0.64 *
        (100 * bd_->encoded_size / (1.0 * bd_->input_uncompressed.size() * sizeof(T)));
  }

  void BenchmarkDecompress(benchmark::State& state,
                           std::unique_ptr<RealComprBenchmarkData<T>> bd,
                           EncodingType encoding_type) {
    Setup(std::move(bd), kElementCount, encoding_type);

    uint64_t iteration_count = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for (auto _ : state) {
      Decompress();
      iteration_count++;
    }
    auto end = std::chrono::high_resolution_clock::now();
    const uint64_t overall_time_us =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    state.counters["MB/s"] =
        static_cast<double>(bd_->input_uncompressed.size() * sizeof(T) *
                            iteration_count) /
        (overall_time_us);

    VerifyDataDecompress();
  }

  std::unique_ptr<RealComprBenchmarkData<T>> bd_;
  EncodingType encoding_type_;
};

// ============================================================================
// Column Lists (matching Snowflake's pattern)
// ============================================================================

#define COLUMN_LIST                      \
  X(Valence, "valence")                  \
  X(Acousticness, "acousticness")        \
  X(Danceability, "danceability")        \
  X(Energy, "energy")                    \
  X(Instrumentalness, "instrumentalness")\
  X(Liveness, "liveness")                \
  X(Loudness, "loudness")                \
  X(Tempo, "tempo")                      \
  X(Speechiness, "speechiness")

// For new dataset (Spotify2), we need lowercase identifiers
#define COLUMN_LIST_NEW    \
  X(valence)               \
  X(acousticness)          \
  X(danceability)          \
  X(energy)                \
  X(instrumentalness)      \
  X(liveness)              \
  X(loudness)              \
  X(tempo)                 \
  X(speechiness)

// POI dataset columns
#define POI_COLUMN_LIST                    \
  X(LatitudeRadian, "latitude_radian")     \
  X(LongitudeRadian, "longitude_radian")

// Common Government dataset columns
#define COMMON_GOVERNMENT_COLUMN_LIST \
  X(Amount1, "amount1")               \
  X(Amount2, "amount2")               \
  X(Amount3, "amount3")

// Arade dataset columns
#define ARADE_COLUMN_LIST   \
  X(Value1, "value1")       \
  X(Value2, "value2")       \
  X(Value3, "value3")       \
  X(Value4, "value4")

// Algorithm list for all benchmarks (matching Snowflake's pattern)
#define ALGORITHM_LIST                  \
  X(ALP, kALP)                          \
  X(BYTESTREAMSPLIT, kByteStreamSplit)  \
  X(ZSTD, kZSTD)

// ============================================================================
// Benchmark Generation Macros (matching Snowflake's pattern)
// ============================================================================

// Synthetic data benchmark macros
#define BENCHMARK_SYNTHETIC_COMPRESS(ALGO, NAME, CLASS, ENGINE)                      \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##NAME##Float, double)         \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<CLASS<double>>()),                                      \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, NAME, CLASS, ENGINE)                    \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##NAME##Float, double)       \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<CLASS<double>>()),                                      \
        EncodingType::ENGINE);                                                       \
  }

// Original Spotify dataset (Dataset 1) benchmark macros
#define BENCHMARK_ORIGINAL_DATASET_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)  \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##Spotify##COLUMN_CAP##Float,  \
                       double)                                                       \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<SpotifyData<double>>(COLUMN_LOWER)),                    \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_ORIGINAL_DATASET_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER,        \
                                              ENGINE)                                \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark,                                              \
                       ALGO##decompress##Spotify##COLUMN_CAP##Float, double)         \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<SpotifyData<double>>(COLUMN_LOWER)),                    \
        EncodingType::ENGINE);                                                       \
  }

// New Spotify dataset (Dataset 2) benchmark macros
#define BENCHMARK_NEW_DATASET_COMPRESS(ALGO, COLUMN, ENGINE)                         \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##Spotify##COLUMN##2Float,     \
                       double)                                                       \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<SpotifyData2<double>>(#COLUMN)),                        \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_NEW_DATASET_DECOMPRESS(ALGO, COLUMN, ENGINE)                       \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##Spotify##COLUMN##2Float,   \
                       double)                                                       \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<SpotifyData2<double>>(#COLUMN)),                        \
        EncodingType::ENGINE);                                                       \
  }

// City Temperature dataset benchmark macros
#define BENCHMARK_CITY_TEMP_COMPRESS(ALGO, ENGINE)                                   \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##CityTemperatureFloat,        \
                       double)                                                       \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<CityTemperatureData<double>>()),                        \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_CITY_TEMP_DECOMPRESS(ALGO, ENGINE)                                 \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##CityTemperatureFloat,      \
                       double)                                                       \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<CityTemperatureData<double>>()),                        \
        EncodingType::ENGINE);                                                       \
  }

// POI dataset benchmark macros
#define BENCHMARK_POI_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)               \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##Poi##COLUMN_CAP##Float,      \
                       double)                                                       \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<PoiData<double>>(COLUMN_LOWER)),                        \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_POI_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)             \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##Poi##COLUMN_CAP##Float,    \
                       double)                                                       \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<PoiData<double>>(COLUMN_LOWER)),                        \
        EncodingType::ENGINE);                                                       \
  }

// Bird Migration dataset benchmark macros
#define BENCHMARK_BIRD_MIGRATION_COMPRESS(ALGO, ENGINE)                              \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##BirdMigrationFloat, double)  \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<BirdMigrationData<double>>()),                          \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_BIRD_MIGRATION_DECOMPRESS(ALGO, ENGINE)                            \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##BirdMigrationFloat,        \
                       double)                                                       \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<BirdMigrationData<double>>()),                          \
        EncodingType::ENGINE);                                                       \
  }

// Common Government dataset benchmark macros
#define BENCHMARK_COMMON_GOVERNMENT_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark,                                              \
                       ALGO##compress##CommonGovernment##COLUMN_CAP##Float, double)  \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<CommonGovernmentData<double>>(COLUMN_LOWER)),           \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_COMMON_GOVERNMENT_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER,       \
                                               ENGINE)                               \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark,                                              \
                       ALGO##decompress##CommonGovernment##COLUMN_CAP##Float,        \
                       double)                                                       \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<CommonGovernmentData<double>>(COLUMN_LOWER)),           \
        EncodingType::ENGINE);                                                       \
  }

// Arade dataset benchmark macros
#define BENCHMARK_ARADE_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)             \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##Arade##COLUMN_CAP##Float,    \
                       double)                                                       \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<AradeData<double>>(COLUMN_LOWER)),                      \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_ARADE_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)           \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##Arade##COLUMN_CAP##Float,  \
                       double)                                                       \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<AradeData<double>>(COLUMN_LOWER)),                      \
        EncodingType::ENGINE);                                                       \
  }

// FPC dataset benchmark macros (generic for single-column datasets)
#define BENCHMARK_NUM_BRAIN_COMPRESS(ALGO, ENGINE)                                   \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##NumBrainFloat, double)       \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<NumBrainData<double>>()),                               \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_NUM_BRAIN_DECOMPRESS(ALGO, ENGINE)                                 \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##NumBrainFloat, double)     \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<NumBrainData<double>>()),                               \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_NUM_COMET_COMPRESS(ALGO, ENGINE)                                   \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##NumCometFloat, double)       \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<NumCometData<double>>()),                               \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_NUM_COMET_DECOMPRESS(ALGO, ENGINE)                                 \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##NumCometFloat, double)     \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<NumCometData<double>>()),                               \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_NUM_CONTROL_COMPRESS(ALGO, ENGINE)                                 \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##NumControlFloat, double)     \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<NumControlData<double>>()),                             \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_NUM_CONTROL_DECOMPRESS(ALGO, ENGINE)                               \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##NumControlFloat, double)   \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<NumControlData<double>>()),                             \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_NUM_PLASMA_COMPRESS(ALGO, ENGINE)                                  \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##NumPlasmaFloat, double)      \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<NumPlasmaData<double>>()),                              \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_NUM_PLASMA_DECOMPRESS(ALGO, ENGINE)                                \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##NumPlasmaFloat, double)    \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<NumPlasmaData<double>>()),                              \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_OBS_ERROR_COMPRESS(ALGO, ENGINE)                                   \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##ObsErrorFloat, double)       \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<ObsErrorData<double>>()),                               \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_OBS_ERROR_DECOMPRESS(ALGO, ENGINE)                                 \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##ObsErrorFloat, double)     \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<ObsErrorData<double>>()),                               \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_OBS_INFO_COMPRESS(ALGO, ENGINE)                                    \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##ObsInfoFloat, double)        \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<ObsInfoData<double>>()),                                \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_OBS_INFO_DECOMPRESS(ALGO, ENGINE)                                  \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##ObsInfoFloat, double)      \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<ObsInfoData<double>>()),                                \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_OBS_SPITZER_COMPRESS(ALGO, ENGINE)                                 \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##ObsSpitzerFloat, double)     \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<ObsSpitzerData<double>>()),                             \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_OBS_SPITZER_DECOMPRESS(ALGO, ENGINE)                               \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##ObsSpitzerFloat, double)   \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<ObsSpitzerData<double>>()),                             \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_OBS_TEMP_COMPRESS(ALGO, ENGINE)                                    \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##ObsTempFloat, double)        \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<ObsTempData<double>>()),                                \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_OBS_TEMP_DECOMPRESS(ALGO, ENGINE)                                  \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##ObsTempFloat, double)      \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<ObsTempData<double>>()),                                \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_MSG_BT_COMPRESS(ALGO, ENGINE)                                      \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##MsgBtFloat, double)          \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<MsgBtData<double>>()),                                  \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_MSG_BT_DECOMPRESS(ALGO, ENGINE)                                    \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##MsgBtFloat, double)        \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<MsgBtData<double>>()),                                  \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_MSG_LU_COMPRESS(ALGO, ENGINE)                                      \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##MsgLuFloat, double)          \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<MsgLuData<double>>()),                                  \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_MSG_LU_DECOMPRESS(ALGO, ENGINE)                                    \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##MsgLuFloat, double)        \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<MsgLuData<double>>()),                                  \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_MSG_SP_COMPRESS(ALGO, ENGINE)                                      \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##MsgSpFloat, double)          \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<MsgSpData<double>>()),                                  \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_MSG_SP_DECOMPRESS(ALGO, ENGINE)                                    \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##MsgSpFloat, double)        \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<MsgSpData<double>>()),                                  \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_MSG_SPPM_COMPRESS(ALGO, ENGINE)                                    \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##MsgSppmFloat, double)        \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<MsgSppmData<double>>()),                                \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_MSG_SPPM_DECOMPRESS(ALGO, ENGINE)                                  \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##MsgSppmFloat, double)      \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<MsgSppmData<double>>()),                                \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_MSG_SWEEP3D_COMPRESS(ALGO, ENGINE)                                 \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##MsgSweep3dFloat, double)     \
  (benchmark::State & state) {                                                       \
    BenchmarkCompress(                                                               \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<MsgSweep3dData<double>>()),                             \
        EncodingType::ENGINE);                                                       \
  }

#define BENCHMARK_MSG_SWEEP3D_DECOMPRESS(ALGO, ENGINE)                               \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##MsgSweep3dFloat, double)   \
  (benchmark::State & state) {                                                       \
    BenchmarkDecompress(                                                             \
        state,                                                                       \
        std::unique_ptr<RealComprBenchmarkData<double>>(                             \
            std::make_unique<MsgSweep3dData<double>>()),                             \
        EncodingType::ENGINE);                                                       \
  }

// ============================================================================
// Benchmark Registrations - Synthetic Data (All Algorithms)
// COMMENTED OUT - Using only real-world Spotify data
// ============================================================================

#if 0
#define GENERATE_SYNTHETIC_BENCHMARKS(ALGO, ENGINE)                \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, Constant, ConstantValues, ENGINE)     \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, Constant, ConstantValues, ENGINE)   \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, Increasing, IncreasingValues, ENGINE) \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, Increasing, IncreasingValues, ENGINE)     \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, SmallRange, DecimalSmallRange, ENGINE)      \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, SmallRange, DecimalSmallRange, ENGINE)    \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, Range, DecimalRange, ENGINE)                \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, Range, DecimalRange, ENGINE)              \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, LargeRange, DecimalLargeRange, ENGINE)      \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, LargeRange, DecimalLargeRange, ENGINE)    \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, Random, RandomValues, ENGINE)               \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, Random, RandomValues, ENGINE)

#define X(ALGO, ENGINE) GENERATE_SYNTHETIC_BENCHMARKS(ALGO, ENGINE)
ALGORITHM_LIST
#undef X
#endif

// ============================================================================
// Benchmark Registrations - Spotify Dataset 1 (All Algorithms x 9 columns)
// ============================================================================

#define GENERATE_SPOTIFY_BENCHMARKS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_ORIGINAL_DATASET_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)     \
  BENCHMARK_ORIGINAL_DATASET_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)

#define GENERATE_ALGORITHM_FOR_SPOTIFY(ALGO, ENGINE)                         \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Valence, "valence", ENGINE)              \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Acousticness, "acousticness", ENGINE)    \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Danceability, "danceability", ENGINE)    \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Energy, "energy", ENGINE)                \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Instrumentalness, "instrumentalness", ENGINE) \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Liveness, "liveness", ENGINE)            \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Loudness, "loudness", ENGINE)            \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Tempo, "tempo", ENGINE)                  \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Speechiness, "speechiness", ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_SPOTIFY(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ============================================================================
// Benchmark Registrations - Spotify Dataset 2 (All Algorithms x 9 columns)
// ============================================================================

#define GENERATE_SPOTIFY2_BENCHMARKS(ALGO, COLUMN, ENGINE) \
  BENCHMARK_NEW_DATASET_COMPRESS(ALGO, COLUMN, ENGINE)     \
  BENCHMARK_NEW_DATASET_DECOMPRESS(ALGO, COLUMN, ENGINE)

#define GENERATE_ALGORITHM_FOR_SPOTIFY2(ALGO, ENGINE)      \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, valence, ENGINE)      \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, acousticness, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, danceability, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, energy, ENGINE)       \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, instrumentalness, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, liveness, ENGINE)     \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, loudness, ENGINE)     \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, tempo, ENGINE)        \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, speechiness, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_SPOTIFY2(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ============================================================================
// Benchmark Registrations - City Temperature Dataset (1 column x 3 algorithms)
// ============================================================================

#define GENERATE_ALGORITHM_FOR_CITY_TEMP(ALGO, ENGINE) \
  BENCHMARK_CITY_TEMP_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_CITY_TEMP_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_CITY_TEMP(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ============================================================================
// Benchmark Registrations - POI Dataset (2 columns x 3 algorithms)
// ============================================================================

#define GENERATE_ALGORITHM_FOR_POI(COLUMN_CAP, COLUMN_LOWER, ALGO, ENGINE) \
  BENCHMARK_POI_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)           \
  BENCHMARK_POI_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)

#define GENERATE_ALGORITHMS_FOR_POI_COLUMN(COLUMN_CAP, COLUMN_LOWER)           \
  GENERATE_ALGORITHM_FOR_POI(COLUMN_CAP, COLUMN_LOWER, ALP, kALP)              \
  GENERATE_ALGORITHM_FOR_POI(COLUMN_CAP, COLUMN_LOWER, BYTESTREAMSPLIT,        \
                             kByteStreamSplit)                                 \
  GENERATE_ALGORITHM_FOR_POI(COLUMN_CAP, COLUMN_LOWER, ZSTD, kZSTD)

#define X(COLUMN_CAP, COLUMN_LOWER) \
  GENERATE_ALGORITHMS_FOR_POI_COLUMN(COLUMN_CAP, COLUMN_LOWER)
POI_COLUMN_LIST
#undef X

// ============================================================================
// Benchmark Registrations - Bird Migration Dataset (1 column x 3 algorithms)
// ============================================================================

#define GENERATE_ALGORITHM_FOR_BIRD_MIGRATION(ALGO, ENGINE) \
  BENCHMARK_BIRD_MIGRATION_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_BIRD_MIGRATION_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_BIRD_MIGRATION(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ============================================================================
// Benchmark Registrations - Common Government Dataset (3 columns x 3 algorithms)
// ============================================================================

#define GENERATE_ALGORITHM_FOR_COMMON_GOVERNMENT(COLUMN_CAP, COLUMN_LOWER, ALGO, \
                                                 ENGINE)                         \
  BENCHMARK_COMMON_GOVERNMENT_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)   \
  BENCHMARK_COMMON_GOVERNMENT_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)

#define GENERATE_ALGORITHMS_FOR_COMMON_GOVERNMENT_COLUMN(COLUMN_CAP, COLUMN_LOWER) \
  GENERATE_ALGORITHM_FOR_COMMON_GOVERNMENT(COLUMN_CAP, COLUMN_LOWER, ALP, kALP)    \
  GENERATE_ALGORITHM_FOR_COMMON_GOVERNMENT(COLUMN_CAP, COLUMN_LOWER,               \
                                           BYTESTREAMSPLIT, kByteStreamSplit)      \
  GENERATE_ALGORITHM_FOR_COMMON_GOVERNMENT(COLUMN_CAP, COLUMN_LOWER, ZSTD, kZSTD)

#define X(COLUMN_CAP, COLUMN_LOWER) \
  GENERATE_ALGORITHMS_FOR_COMMON_GOVERNMENT_COLUMN(COLUMN_CAP, COLUMN_LOWER)
COMMON_GOVERNMENT_COLUMN_LIST
#undef X

// ============================================================================
// Benchmark Registrations - Arade Dataset (4 columns x 3 algorithms)
// ============================================================================

#define GENERATE_ALGORITHM_FOR_ARADE(COLUMN_CAP, COLUMN_LOWER, ALGO, ENGINE) \
  BENCHMARK_ARADE_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)           \
  BENCHMARK_ARADE_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)

#define GENERATE_ALGORITHMS_FOR_ARADE_COLUMN(COLUMN_CAP, COLUMN_LOWER)          \
  GENERATE_ALGORITHM_FOR_ARADE(COLUMN_CAP, COLUMN_LOWER, ALP, kALP)             \
  GENERATE_ALGORITHM_FOR_ARADE(COLUMN_CAP, COLUMN_LOWER, BYTESTREAMSPLIT,       \
                               kByteStreamSplit)                                \
  GENERATE_ALGORITHM_FOR_ARADE(COLUMN_CAP, COLUMN_LOWER, ZSTD, kZSTD)

#define X(COLUMN_CAP, COLUMN_LOWER) \
  GENERATE_ALGORITHMS_FOR_ARADE_COLUMN(COLUMN_CAP, COLUMN_LOWER)
ARADE_COLUMN_LIST
#undef X

// ============================================================================
// Benchmark Registrations - FPC Datasets (13 single-column datasets x 3 each)
// ============================================================================

// NumBrain dataset
#define GENERATE_ALGORITHM_FOR_NUM_BRAIN(ALGO, ENGINE) \
  BENCHMARK_NUM_BRAIN_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_NUM_BRAIN_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_NUM_BRAIN(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// NumComet dataset
#define GENERATE_ALGORITHM_FOR_NUM_COMET(ALGO, ENGINE) \
  BENCHMARK_NUM_COMET_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_NUM_COMET_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_NUM_COMET(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// NumControl dataset
#define GENERATE_ALGORITHM_FOR_NUM_CONTROL(ALGO, ENGINE) \
  BENCHMARK_NUM_CONTROL_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_NUM_CONTROL_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_NUM_CONTROL(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// NumPlasma dataset
#define GENERATE_ALGORITHM_FOR_NUM_PLASMA(ALGO, ENGINE) \
  BENCHMARK_NUM_PLASMA_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_NUM_PLASMA_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_NUM_PLASMA(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ObsError dataset
#define GENERATE_ALGORITHM_FOR_OBS_ERROR(ALGO, ENGINE) \
  BENCHMARK_OBS_ERROR_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_OBS_ERROR_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_OBS_ERROR(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ObsInfo dataset
#define GENERATE_ALGORITHM_FOR_OBS_INFO(ALGO, ENGINE) \
  BENCHMARK_OBS_INFO_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_OBS_INFO_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_OBS_INFO(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ObsSpitzer dataset
#define GENERATE_ALGORITHM_FOR_OBS_SPITZER(ALGO, ENGINE) \
  BENCHMARK_OBS_SPITZER_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_OBS_SPITZER_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_OBS_SPITZER(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ObsTemp dataset
#define GENERATE_ALGORITHM_FOR_OBS_TEMP(ALGO, ENGINE) \
  BENCHMARK_OBS_TEMP_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_OBS_TEMP_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_OBS_TEMP(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// MsgBt dataset
#define GENERATE_ALGORITHM_FOR_MSG_BT(ALGO, ENGINE) \
  BENCHMARK_MSG_BT_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_MSG_BT_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_MSG_BT(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// MsgLu dataset
#define GENERATE_ALGORITHM_FOR_MSG_LU(ALGO, ENGINE) \
  BENCHMARK_MSG_LU_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_MSG_LU_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_MSG_LU(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// MsgSp dataset
#define GENERATE_ALGORITHM_FOR_MSG_SP(ALGO, ENGINE) \
  BENCHMARK_MSG_SP_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_MSG_SP_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_MSG_SP(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// MsgSppm dataset
#define GENERATE_ALGORITHM_FOR_MSG_SPPM(ALGO, ENGINE) \
  BENCHMARK_MSG_SPPM_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_MSG_SPPM_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_MSG_SPPM(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// MsgSweep3d dataset
#define GENERATE_ALGORITHM_FOR_MSG_SWEEP3D(ALGO, ENGINE) \
  BENCHMARK_MSG_SWEEP3D_COMPRESS(ALGO, ENGINE)           \
  BENCHMARK_MSG_SWEEP3D_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_MSG_SWEEP3D(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

}  // namespace parquet

BENCHMARK_MAIN();
