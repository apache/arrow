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

#include "parquet/encoding.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "arrow/buffer.h"
#include "arrow/util/compression.h"
#include "arrow/util/alp/AlpWrapper.h"

/*
 * This file benchmarks multiple encoding schemes for floating point values in Parquet.
 * Structure mirrors Snowflake's FloatComprBenchmark.cpp
 *
 * It evaluates:
 * 1) Compression Ratio
 * 2) Encoding Speed
 * 3) Decoding Speed
 *
 * Encoding schemes:
 * 1) ALP encoding
 * 2) ByteStreamSplit encoding
 * 3) ZSTD compression
 *
 * On synthetic datasets:
 * 1) Constant Value
 * 2) Increasing values
 * 3) Small Range decimal
 * 4) Range decimal
 * 5) Large Range decimal
 * 6) Random values
 *
 * And real-world datasets:
 * 1) floatingpoint_spotify1.csv (9 columns)
 * 2) floatingpoint_spotify2.csv (9 columns)
 * 3) floatingpoint_citytemperature.csv (1 column)
 * 4) floatingpoint_poi.csv (2 columns)
 * 5) floatingpoint_birdmigration.csv (1 column)
 * 6) floatingpoint_commongovernment.csv (3 columns)
 * 7) floatingpoint_arade.csv (4 columns)
 * 8) floatingpoint_num_brain.csv (1 column)
 * 9) floatingpoint_num_comet.csv (1 column)
 * 10) floatingpoint_num_control.csv (1 column)
 * 11) floatingpoint_num_plasma.csv (1 column)
 * 12) floatingpoint_obs_error.csv (1 column)
 * 13) floatingpoint_obs_info.csv (1 column)
 * 14) floatingpoint_obs_spitzer.csv (1 column)
 * 15) floatingpoint_obs_temp.csv (1 column)
 * 16) floatingpoint_msg_bt.csv (1 column)
 * 17) floatingpoint_msg_lu.csv (1 column)
 * 18) floatingpoint_msg_sp.csv (1 column)
 * 19) floatingpoint_msg_sppm.csv (1 column)
 * 20) floatingpoint_msg_sweep3d.csv (1 column)
 */

namespace parquet {

using schema::PrimitiveNode;

// Type alias to match Snowflake's naming
using ub8 = uint64_t;

// Helper function matching Snowflake's pow10
constexpr ub8 pow10(ub8 exp) {
  ub8 result = 1;
  for (ub8 i = 0; i < exp; ++i) {
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

/**
 * Helper class to set up encoding benchmark data.
 * Matches Snowflake's RealComprBenchmarkData<T> structure with encoding parameter.
 */
template <typename T>
struct RealComprBenchmarkData {
  std::vector<T> inputUncompressed;
  std::shared_ptr<Buffer> encodedData;
  std::vector<T> outputUncompressed;
  ub8 encodedSize = 0;
  Encoding::type currentEncoding;
  std::unique_ptr<::arrow::util::Codec> codec;  // For ZSTD

  virtual ~RealComprBenchmarkData<T>() = default;

  void prepareBenchmarkData(ub8 elementCount, EncodingType encodingType) {
    fillUncompressedInput(elementCount);

    using DType = typename std::conditional<std::is_same<T, float>::value,
                                           FloatType, DoubleType>::type;
    auto descr = MakeColumnDescriptor<DType>();

    // Select encoding based on type
    switch (encodingType) {
      case EncodingType::kALP:
        currentEncoding = Encoding::ALP;
        break;
      case EncodingType::kByteStreamSplit:
        currentEncoding = Encoding::BYTE_STREAM_SPLIT;
        codec = ::arrow::util::Codec::Create(::arrow::Compression::ZSTD).ValueOrDie();
        break;
      case EncodingType::kZSTD:
        // ZSTD uses PLAIN encoding + compression
        currentEncoding = Encoding::PLAIN;
        codec = ::arrow::util::Codec::Create(::arrow::Compression::ZSTD).ValueOrDie();
        break;
    }

    // Do initial encoding to size buffers
    if (encodingType == EncodingType::kALP) {
      auto encoder = MakeTypedEncoder<DType>(Encoding::ALP, false, descr.get());
      encoder->Put(inputUncompressed.data(),
                   static_cast<int>(inputUncompressed.size()));
      encodedData = encoder->FlushValues();
      encodedSize = encodedData->size();
    } else if (encodingType == EncodingType::kZSTD) {
      // For ZSTD: Plain encode then compress
      auto encoder = MakeTypedEncoder<DType>(Encoding::PLAIN, false, descr.get());
      encoder->Put(inputUncompressed.data(),
                   static_cast<int>(inputUncompressed.size()));
      auto plainData = encoder->FlushValues();

      // Compress with ZSTD - use AllocateBuffer to properly manage memory
      int64_t max_compressed_len = codec->MaxCompressedLen(plainData->size(),
                                                           plainData->data());
      auto compressed_buffer = ::arrow::AllocateResizableBuffer(max_compressed_len).ValueOrDie();
      int64_t actual_size = codec->Compress(plainData->size(), plainData->data(),
                                            max_compressed_len,
                                            compressed_buffer->mutable_data())
                                  .ValueOrDie();
      // Resize to actual compressed size and move to shared_ptr
      (void)compressed_buffer->Resize(actual_size);  // Resize can't fail for shrinking
      encodedData = std::shared_ptr<Buffer>(std::move(compressed_buffer));
      encodedSize = actual_size;
    } else {
      // For ByteStreamSplit: Direct encoding
      auto encoder = MakeTypedEncoder<DType>(currentEncoding, false, descr.get());
      encoder->Put(inputUncompressed.data(),
                   static_cast<int>(inputUncompressed.size()));
      auto byteStreamSplitData = encoder->FlushValues();
      // Compress with ZSTD - use AllocateBuffer to properly manage memory
      int64_t max_compressed_len = codec->MaxCompressedLen(byteStreamSplitData->size(),
                                                           byteStreamSplitData->data());
      auto compressed_buffer = ::arrow::AllocateResizableBuffer(max_compressed_len).ValueOrDie();
      int64_t actual_size = codec->Compress(byteStreamSplitData->size(), byteStreamSplitData->data(),
                                            max_compressed_len,
                                            compressed_buffer->mutable_data())
                                  .ValueOrDie();
      // Resize to actual compressed size and move to shared_ptr
      (void)compressed_buffer->Resize(actual_size);  // Resize can't fail for shrinking
      encodedData = std::shared_ptr<Buffer>(std::move(compressed_buffer));
      encodedSize = actual_size;
    }

    // Prepare output buffer
    outputUncompressed.resize(inputUncompressed.size());
  }

  virtual void fillUncompressedInput(ub8 elementCount) = 0;
};

// ============================================================================
// Synthetic Data Generators
// ============================================================================

template <typename T>
struct ConstantValues : public RealComprBenchmarkData<T> {
  void fillUncompressedInput(ub8 elementCount) override {
    const T value = static_cast<T>(1.1);
    this->inputUncompressed = std::vector<T>(elementCount, value);
  }
};

template <typename T>
struct IncreasingValues : public RealComprBenchmarkData<T> {
  void fillUncompressedInput(ub8 elementCount) override {
    this->inputUncompressed.resize(elementCount);
    T currentValue = 0.0;
    for (ub8 i = 0; i < elementCount; i++) {
      this->inputUncompressed[i] = currentValue;
      currentValue += 1.0;
    }
  }
};

template <typename T>
struct DecimalSmallRange : public RealComprBenchmarkData<T> {
  void fillUncompressedInput(ub8 elementCount) override {
    this->inputUncompressed.resize(elementCount);
    const ub8 minVal = 100;
    const ub8 maxVal = 1000;
    const ub8 decimalPlaces = 2;
    const ub8 mult = pow10(decimalPlaces);

    std::uniform_int_distribution<ub8> unif(minVal * mult, maxVal * mult);
    std::default_random_engine re;
    for (ub8 i = 0; i < elementCount; i++) {
      this->inputUncompressed[i] = unif(re) * 1.0 / mult;
    }
  }
};

template <typename T>
struct DecimalRange : public RealComprBenchmarkData<T> {
  void fillUncompressedInput(ub8 elementCount) override {
    this->inputUncompressed.resize(elementCount);
    const ub8 minVal = 1000;
    const ub8 maxVal = 100000;
    const ub8 decimalPlaces = 6;
    const ub8 mult = pow10(decimalPlaces);

    std::uniform_int_distribution<ub8> unif(minVal * mult, maxVal * mult);
    std::default_random_engine re;
    for (ub8 i = 0; i < elementCount; i++) {
      this->inputUncompressed[i] = unif(re) * 1.0 / mult;
    }
  }
};

template <typename T>
struct DecimalLargeRange : public RealComprBenchmarkData<T> {
  void fillUncompressedInput(ub8 elementCount) override {
    this->inputUncompressed.resize(elementCount);
    const ub8 minVal = 1000;
    const ub8 maxVal = 1000000;
    const ub8 decimalPlaces = 6;
    const ub8 mult = pow10(decimalPlaces);

    std::uniform_int_distribution<ub8> unif(minVal * mult, maxVal * mult);
    std::default_random_engine re;
    for (ub8 i = 0; i < elementCount; i++) {
      this->inputUncompressed[i] = unif(re) * 1.0 / mult;
    }
  }
};

template <typename T>
struct RandomValues : public RealComprBenchmarkData<T> {
  void fillUncompressedInput(ub8 elementCount) override {
    this->inputUncompressed.resize(elementCount);
    std::uniform_real_distribution<T> unif(std::numeric_limits<T>::min(),
                                           std::numeric_limits<T>::max());
    std::default_random_engine re;
    for (ub8 i = 0; i < elementCount; i++) {
      this->inputUncompressed[i] = unif(re);
    }
  }
};

// ============================================================================
// CSV Loading Infrastructure (for real-world datasets)
// ============================================================================

// Extract tarball once and return the data directory path
std::string getDataDirectory() {
  static std::string dataDir;
  static bool initialized = false;

  if (!initialized) {
    // Find the tarball location relative to this source file
    std::string tarballPath = std::string(__FILE__);
    tarballPath = tarballPath.substr(0, tarballPath.find_last_of("/\\"));
    tarballPath = tarballPath.substr(0, tarballPath.find_last_of("/\\"));
    tarballPath += "/arrow/util/alp/data/floatingpoint_data.tar.gz";

    // Use a fixed extraction directory that can be reused across runs
    dataDir = "/tmp/parquet_alp_benchmark_data";

    // Check if tarball exists
    std::ifstream tarballCheck(tarballPath);
    if (!tarballCheck.good()) {
      // Fall back to original directory if tarball not found
      dataDir = std::string(__FILE__);
      dataDir = dataDir.substr(0, dataDir.find_last_of("/\\"));
      dataDir = dataDir.substr(0, dataDir.find_last_of("/\\"));
      dataDir += "/arrow/util/alp/data";
      initialized = true;
      return dataDir;
    }

    // Check if extraction directory already exists and has files
    std::ifstream checkFile(dataDir + "/floatingpoint_spotify1.csv");
    if (checkFile.good()) {
      // Directory already exists with data, reuse it
      initialized = true;
      return dataDir;
    }

    // Create extraction directory and extract tarball
    std::string mkdirCmd = "mkdir -p " + dataDir;
    std::string extractCmd = "tar -xzf " + tarballPath + " -C " + dataDir;

    if (system(mkdirCmd.c_str()) == 0 && system(extractCmd.c_str()) == 0) {
      initialized = true;
    } else {
      // Extraction failed, fall back to original directory
      dataDir = std::string(__FILE__);
      dataDir = dataDir.substr(0, dataDir.find_last_of("/\\"));
      dataDir = dataDir.substr(0, dataDir.find_last_of("/\\"));
      dataDir += "/arrow/util/alp/data";
      initialized = true;
    }
  }

  return dataDir;
}

std::vector<std::string> splitCSVRow(const std::string& line, char delimiter = ',') {
  std::vector<std::string> columns;
  std::istringstream stream(line);
  std::string cell;

  while (std::getline(stream, cell, delimiter)) {
    columns.push_back(cell);
  }
  return columns;
}

std::vector<double> loadSpotifyColumn(const std::string& columnName,
                                      const std::string& filename) {
  std::vector<double> values;

  static const std::unordered_set<std::string> VALID_FLOAT_COLUMNS = {
    "danceability", "energy", "loudness", "speechiness", "acousticness",
    "instrumentalness", "liveness", "valence", "tempo"
  };

  if (VALID_FLOAT_COLUMNS.find(columnName) == VALID_FLOAT_COLUMNS.end()) {
    std::cerr << "Column '" << columnName << "' is not a supported double column" << std::endl;
    return values;
  }

  std::string filePath = getDataDirectory() + "/" + filename;

  std::ifstream file(filePath);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filePath << std::endl;
    return values;
  }

  std::string fileContent((std::istreambuf_iterator<char>(file)),
                          std::istreambuf_iterator<char>());
  file.close();

  std::istringstream ss(fileContent);
  std::string line;
  size_t columnIndex = SIZE_MAX;

  if (std::getline(ss, line)) {
    std::istringstream headerStream(line);
    std::string header;
    size_t index = 0;

    while (std::getline(headerStream, header, ',')) {
      header.erase(0, header.find_first_not_of(" \t\r\n"));
      header.erase(header.find_last_not_of(" \t\r\n") + 1);

      if (header == columnName) {
        columnIndex = index;
        break;
      }
      index++;
    }
  }

  if (columnIndex == SIZE_MAX) {
    std::cerr << "Column '" << columnName << "' not found in header" << std::endl;
    return values;
  }

  while (std::getline(ss, line)) {
    std::vector<std::string> columns = splitCSVRow(line);
    if (columnIndex < columns.size()) {
      try {
        double value = std::stod(columns[columnIndex]);
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
  std::string columnName;

  explicit SpotifyData(const std::string& column) : columnName(column) {}

  void fillUncompressedInput(ub8 /*elementCount*/) override {
    std::vector<double> spotifyValues = loadSpotifyColumn(columnName, "floatingpoint_spotify1.csv");

    this->inputUncompressed.resize(spotifyValues.size());
    for (size_t i = 0; i < spotifyValues.size(); ++i) {
      this->inputUncompressed[i] = static_cast<T>(spotifyValues[i]);
    }
  }
};

template <typename T>
struct SpotifyData2 : public RealComprBenchmarkData<T> {
  std::string columnName;

  explicit SpotifyData2(const std::string& column) : columnName(column) {}

  void fillUncompressedInput(ub8 /*elementCount*/) override {
    std::vector<double> spotifyValues = loadSpotifyColumn(columnName, "floatingpoint_spotify2.csv");

    this->inputUncompressed.resize(spotifyValues.size());
    for (size_t i = 0; i < spotifyValues.size(); ++i) {
      this->inputUncompressed[i] = static_cast<T>(spotifyValues[i]);
    }
  }
};

// Load AvgTemperature column from City Temperature CSV data
std::vector<double> loadCityTemperatureColumn() {
  std::vector<double> values;

  std::string filePath = getDataDirectory() + "/floatingpoint_citytemperature.csv";

  std::ifstream file(filePath);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filePath << std::endl;
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
std::vector<double> loadPoiColumn(const std::string& columnName) {
  std::vector<double> values;

  static const std::unordered_set<std::string> VALID_FLOAT_COLUMNS = {
    "latitude_radian", "longitude_radian"
  };

  if (VALID_FLOAT_COLUMNS.find(columnName) == VALID_FLOAT_COLUMNS.end()) {
    std::cerr << "Column '" << columnName << "' is not a supported double column" << std::endl;
    return values;
  }

  std::string filePath = getDataDirectory() + "/floatingpoint_poi.csv";

  std::ifstream file(filePath);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filePath << std::endl;
    return values;
  }

  std::string line;
  // Read header line to find column index
  if (!std::getline(file, line)) {
    std::cerr << "Failed to read header from POI CSV" << std::endl;
    return values;
  }

  std::vector<std::string> headers = splitCSVRow(line);
  int columnIndex = -1;
  for (size_t i = 0; i < headers.size(); ++i) {
    std::string trimmedHeader = headers[i];
    trimmedHeader.erase(0, trimmedHeader.find_first_not_of(" \t\r\n"));
    trimmedHeader.erase(trimmedHeader.find_last_not_of(" \t\r\n") + 1);

    if (trimmedHeader == columnName) {
      columnIndex = static_cast<int>(i);
      break;
    }
  }

  if (columnIndex == -1) {
    std::cerr << "Column '" << columnName << "' not found in POI CSV header" << std::endl;
    return values;
  }

  // Process data lines
  while (std::getline(file, line)) {
    std::vector<std::string> columns = splitCSVRow(line);
    if (columns.size() > static_cast<size_t>(columnIndex)) {
      try {
        double value = std::stod(columns[columnIndex]);
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
std::vector<double> loadBirdMigrationData() {
  std::vector<double> values;

  std::string filePath = getDataDirectory() + "/floatingpoint_birdmigration.csv";

  std::ifstream file(filePath);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filePath << std::endl;
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
std::vector<double> loadCommonGovernmentColumn(const std::string& columnName) {
  std::vector<double> values;

  static const std::unordered_set<std::string> VALID_FLOAT_COLUMNS = {
    "amount1", "amount2", "amount3"
  };

  if (VALID_FLOAT_COLUMNS.find(columnName) == VALID_FLOAT_COLUMNS.end()) {
    std::cerr << "Column '" << columnName << "' is not a supported double column" << std::endl;
    return values;
  }

  size_t columnIndex = SIZE_MAX;
  if (columnName == "amount1") columnIndex = 0;
  else if (columnName == "amount2") columnIndex = 1;
  else if (columnName == "amount3") columnIndex = 2;

  std::string filePath = getDataDirectory() + "/floatingpoint_commongovernment.csv";

  std::ifstream file(filePath);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filePath << std::endl;
    return values;
  }

  std::string line;
  while (std::getline(file, line)) {
    std::vector<std::string> columns = splitCSVRow(line, '|');
    if (columnIndex < columns.size()) {
      try {
        double value = std::stod(columns[columnIndex]);
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
std::vector<double> loadAradeColumn(const std::string& columnName) {
  std::vector<double> values;

  static const std::unordered_set<std::string> VALID_FLOAT_COLUMNS = {
    "value1", "value2", "value3", "value4"
  };

  if (VALID_FLOAT_COLUMNS.find(columnName) == VALID_FLOAT_COLUMNS.end()) {
    std::cerr << "Column '" << columnName << "' is not a supported double column" << std::endl;
    return values;
  }

  size_t columnIndex = SIZE_MAX;
  if (columnName == "value1") columnIndex = 0;
  else if (columnName == "value2") columnIndex = 1;
  else if (columnName == "value3") columnIndex = 2;
  else if (columnName == "value4") columnIndex = 3;

  std::string filePath = getDataDirectory() + "/floatingpoint_arade.csv";

  std::ifstream file(filePath);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filePath << std::endl;
    return values;
  }

  std::string line;
  while (std::getline(file, line)) {
    std::vector<std::string> columns = splitCSVRow(line, '|');
    if (columnIndex < columns.size()) {
      try {
        double value = std::stod(columns[columnIndex]);
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
std::vector<double> loadSingleColumnFpcData(const std::string& datasetName) {
  std::vector<double> values;

  std::string filePath = getDataDirectory() + "/floatingpoint_" + datasetName + ".csv";

  std::ifstream file(filePath);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filePath << std::endl;
    return values;
  }

  std::string line;
  // Skip header line
  if (!std::getline(file, line)) {
    std::cerr << "Failed to read header from " << datasetName << " CSV" << std::endl;
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
std::vector<double> loadNumBrainData() { return loadSingleColumnFpcData("num_brain"); }
std::vector<double> loadNumCometData() { return loadSingleColumnFpcData("num_comet"); }
std::vector<double> loadNumControlData() { return loadSingleColumnFpcData("num_control"); }
std::vector<double> loadNumPlasmaData() { return loadSingleColumnFpcData("num_plasma"); }
std::vector<double> loadObsErrorData() { return loadSingleColumnFpcData("obs_error"); }
std::vector<double> loadObsInfoData() { return loadSingleColumnFpcData("obs_info"); }
std::vector<double> loadObsSpitzerData() { return loadSingleColumnFpcData("obs_spitzer"); }
std::vector<double> loadObsTempData() { return loadSingleColumnFpcData("obs_temp"); }
std::vector<double> loadMsgBtData() { return loadSingleColumnFpcData("msg_bt"); }
std::vector<double> loadMsgLuData() { return loadSingleColumnFpcData("msg_lu"); }
std::vector<double> loadMsgSpData() { return loadSingleColumnFpcData("msg_sp"); }
std::vector<double> loadMsgSppmData() { return loadSingleColumnFpcData("msg_sppm"); }
std::vector<double> loadMsgSweep3dData() { return loadSingleColumnFpcData("msg_sweep3d"); }

// Data classes for all additional datasets
template <typename T>
struct CityTemperatureData : public RealComprBenchmarkData<T> {
  CityTemperatureData() = default;

  void fillUncompressedInput(ub8 /*elementCount*/) override {
    std::vector<double> values = loadCityTemperatureColumn();
    this->inputUncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->inputUncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

template <typename T>
struct PoiData : public RealComprBenchmarkData<T> {
  std::string columnName;

  explicit PoiData(const std::string& column) : columnName(column) {}

  void fillUncompressedInput(ub8 /*elementCount*/) override {
    std::vector<double> values = loadPoiColumn(columnName);
    this->inputUncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->inputUncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

template <typename T>
struct BirdMigrationData : public RealComprBenchmarkData<T> {
  explicit BirdMigrationData() {}

  void fillUncompressedInput(ub8 /*elementCount*/) override {
    std::vector<double> values = loadBirdMigrationData();
    this->inputUncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->inputUncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

template <typename T>
struct CommonGovernmentData : public RealComprBenchmarkData<T> {
  std::string columnName;

  explicit CommonGovernmentData(const std::string& column) : columnName(column) {}

  void fillUncompressedInput(ub8 /*elementCount*/) override {
    std::vector<double> values = loadCommonGovernmentColumn(columnName);
    this->inputUncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->inputUncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

template <typename T>
struct AradeData : public RealComprBenchmarkData<T> {
  std::string columnName;

  explicit AradeData(const std::string& column) : columnName(column) {}

  void fillUncompressedInput(ub8 /*elementCount*/) override {
    std::vector<double> values = loadAradeColumn(columnName);
    this->inputUncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->inputUncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

// Generic template for FPC single-column datasets
template <typename T, std::vector<double> (*LoaderFunc)()>
struct FpcDataset : public RealComprBenchmarkData<T> {
  explicit FpcDataset() {}

  void fillUncompressedInput(ub8 /*elementCount*/) override {
    std::vector<double> values = LoaderFunc();
    this->inputUncompressed.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      this->inputUncompressed[i] = static_cast<T>(values[i]);
    }
  }
};

// Type aliases for each FPC dataset
template <typename T> using NumBrainData = FpcDataset<T, loadNumBrainData>;
template <typename T> using NumCometData = FpcDataset<T, loadNumCometData>;
template <typename T> using NumControlData = FpcDataset<T, loadNumControlData>;
template <typename T> using NumPlasmaData = FpcDataset<T, loadNumPlasmaData>;
template <typename T> using ObsErrorData = FpcDataset<T, loadObsErrorData>;
template <typename T> using ObsInfoData = FpcDataset<T, loadObsInfoData>;
template <typename T> using ObsSpitzerData = FpcDataset<T, loadObsSpitzerData>;
template <typename T> using ObsTempData = FpcDataset<T, loadObsTempData>;
template <typename T> using MsgBtData = FpcDataset<T, loadMsgBtData>;
template <typename T> using MsgLuData = FpcDataset<T, loadMsgLuData>;
template <typename T> using MsgSpData = FpcDataset<T, loadMsgSpData>;
template <typename T> using MsgSppmData = FpcDataset<T, loadMsgSppmData>;
template <typename T> using MsgSweep3dData = FpcDataset<T, loadMsgSweep3dData>;

// ============================================================================
// Benchmark Fixture (matching Snowflake's DoubleBenchmark structure)
// ============================================================================

template <typename T>
class DoubleBenchmark : public benchmark::Fixture {
 public:
  static constexpr ub8 kElementCount = 50000;  // Matches Snowflake exactly

  void setup(std::unique_ptr<RealComprBenchmarkData<T>> bd, ub8 elementCount,
             EncodingType encodingType) {
    m_encodingType = encodingType;
    m_bd = std::move(bd);
    m_bd->prepareBenchmarkData(elementCount, encodingType);
  }

  void verifyDataCompress() {
    decompress();
    if (memcmp(m_bd->inputUncompressed.data(), m_bd->outputUncompressed.data(),
               m_bd->inputUncompressed.size() * sizeof(T)) != 0) {
      std::cerr << "verificationFailed" << std::endl;
    }
  }

  void verifyDataDecompress() {
    if (memcmp(m_bd->inputUncompressed.data(), m_bd->outputUncompressed.data(),
               m_bd->inputUncompressed.size() * sizeof(T)) != 0) {
      std::cerr << "verificationFailed" << std::endl;
    }
  }

  void compress() {
    using DType = typename std::conditional<std::is_same<T, float>::value,
                                           FloatType, DoubleType>::type;
    auto descr = MakeColumnDescriptor<DType>();

    if (m_encodingType == EncodingType::kALP) {
      auto encoder = MakeTypedEncoder<DType>(Encoding::ALP, false, descr.get());
      encoder->Put(m_bd->inputUncompressed.data(),
                   static_cast<int>(m_bd->inputUncompressed.size()));
      m_bd->encodedData = encoder->FlushValues();
      m_bd->encodedSize = m_bd->encodedData->size();
    } else if (m_encodingType == EncodingType::kZSTD) {
      // For ZSTD: Plain encode then compress
      auto encoder = MakeTypedEncoder<DType>(Encoding::PLAIN, false, descr.get());
      encoder->Put(m_bd->inputUncompressed.data(),
                   static_cast<int>(m_bd->inputUncompressed.size()));
      auto plainData = encoder->FlushValues();

      // Compress with ZSTD - use AllocateBuffer to properly manage memory
      int64_t max_compressed_len = m_bd->codec->MaxCompressedLen(plainData->size(),
                                                                 plainData->data());
      auto compressed_buffer = ::arrow::AllocateResizableBuffer(max_compressed_len).ValueOrDie();
      int64_t actual_size = m_bd->codec->Compress(plainData->size(), plainData->data(),
                                                  max_compressed_len,
                                                  compressed_buffer->mutable_data())
                                      .ValueOrDie();
      // Resize to actual compressed size and move to shared_ptr
      (void)compressed_buffer->Resize(actual_size);  // Resize can't fail for shrinking
      m_bd->encodedData = std::shared_ptr<Buffer>(std::move(compressed_buffer));
      m_bd->encodedSize = actual_size;
    } else {
      // For ByteStreamSplit: Direct encoding
      auto encoder = MakeTypedEncoder<DType>(m_bd->currentEncoding, false, descr.get());
      encoder->Put(m_bd->inputUncompressed.data(),
                   static_cast<int>(m_bd->inputUncompressed.size()));
      auto byteStreamSplitData = encoder->FlushValues();
      // Compress with ZSTD - use AllocateBuffer to properly manage memory
      int64_t max_compressed_len = m_bd->codec->MaxCompressedLen(byteStreamSplitData->size(),
                                                           byteStreamSplitData->data());
      auto compressed_buffer = ::arrow::AllocateResizableBuffer(max_compressed_len).ValueOrDie();
      int64_t actual_size = m_bd->codec->Compress(byteStreamSplitData->size(), byteStreamSplitData->data(),
                                            max_compressed_len,
                                            compressed_buffer->mutable_data())
                                  .ValueOrDie();
      // Resize to actual compressed size and move to shared_ptr
      (void)compressed_buffer->Resize(actual_size);  // Resize can't fail for shrinking
      m_bd->encodedData = std::shared_ptr<Buffer>(std::move(compressed_buffer));
      m_bd->encodedSize = actual_size;
    }
  }

  void decompress() {
    using DType = typename std::conditional<std::is_same<T, float>::value,
                                           FloatType, DoubleType>::type;
    auto descr = MakeColumnDescriptor<DType>();

    if (m_encodingType == EncodingType::kALP) {
      // For ALP: Use Parquet decoder
      auto decoder = MakeTypedDecoder<DType>(Encoding::ALP, descr.get());
      decoder->SetData(static_cast<int>(m_bd->inputUncompressed.size()),
                       m_bd->encodedData->data(),
                       static_cast<int>(m_bd->encodedData->size()));
      decoder->Decode(m_bd->outputUncompressed.data(),
                      static_cast<int>(m_bd->outputUncompressed.size()));
    } else if (m_encodingType == EncodingType::kZSTD) {
      // For ZSTD: Decompress then plain decode
      int64_t decompressed_len = m_bd->inputUncompressed.size() * sizeof(T);
      std::vector<uint8_t> decompressed(decompressed_len);
      int64_t actual_size = m_bd->codec->Decompress(m_bd->encodedData->size(),
                                                    m_bd->encodedData->data(),
                                                    decompressed_len,
                                                    decompressed.data())
                                       .ValueOrDie();

      // Plain decode
      auto decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, descr.get());
      decoder->SetData(static_cast<int>(m_bd->inputUncompressed.size()),
                      decompressed.data(),
                      static_cast<int>(actual_size));
      decoder->Decode(m_bd->outputUncompressed.data(),
                      static_cast<int>(m_bd->outputUncompressed.size()));
    } else {

      int64_t decompressed_len = m_bd->inputUncompressed.size() * sizeof(T);
      std::vector<uint8_t> decompressed(decompressed_len);
      int64_t actual_size = m_bd->codec->Decompress(m_bd->encodedData->size(),
                                                    m_bd->encodedData->data(),
                                                    decompressed_len,
                                                    decompressed.data())
                                       .ValueOrDie();

      // For ByteStreamSplit: Direct decoding
      auto decoder = MakeTypedDecoder<DType>(m_bd->currentEncoding, descr.get());
      decoder->SetData(static_cast<int>(m_bd->inputUncompressed.size()),
                       decompressed.data(),
                       static_cast<int>(actual_size));
      decoder->Decode(m_bd->outputUncompressed.data(),
                      static_cast<int>(m_bd->outputUncompressed.size()));
    }
  }

  void benchmarkCompress(benchmark::State& state,
                        std::unique_ptr<RealComprBenchmarkData<T>> bd,
                        EncodingType encodingType) {
    setup(std::move(bd), kElementCount, encodingType);

    ub8 iterationCount = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for (auto _ : state) {
      compress();
      iterationCount++;
    }
    auto end = std::chrono::high_resolution_clock::now();
    const ub8 overallTimeUs =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    state.counters["MB/s"] =
        static_cast<double>(m_bd->inputUncompressed.size() * sizeof(T) * iterationCount) / (overallTimeUs);

    verifyDataCompress();
    state.counters["Compression Ratio Percent"] =
        0.64 * (100 * m_bd->encodedSize / (1.0 * m_bd->inputUncompressed.size() * sizeof(T)));
  }

  void benchmarkDecompress(benchmark::State& state,
                          std::unique_ptr<RealComprBenchmarkData<T>> bd,
                          EncodingType encodingType) {
    setup(std::move(bd), kElementCount, encodingType);

    ub8 iterationCount = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for (auto _ : state) {
      decompress();
      iterationCount++;
    }
    auto end = std::chrono::high_resolution_clock::now();
    const ub8 overallTimeUs =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    state.counters["MB/s"] =
        static_cast<double>(m_bd->inputUncompressed.size() * sizeof(T) * iterationCount) / (overallTimeUs);

    verifyDataDecompress();
  }

  std::unique_ptr<RealComprBenchmarkData<T>> m_bd;
  EncodingType m_encodingType;
};

// ============================================================================
// Column Lists (matching Snowflake's pattern)
// ============================================================================

#define COLUMN_LIST \
  X(Valence, "valence") \
  X(Acousticness, "acousticness") \
  X(Danceability, "danceability") \
  X(Energy, "energy") \
  X(Instrumentalness, "instrumentalness") \
  X(Liveness, "liveness") \
  X(Loudness, "loudness") \
  X(Tempo, "tempo") \
  X(Speechiness, "speechiness")

// For new dataset (Spotify2), we need lowercase identifiers
#define COLUMN_LIST_NEW \
  X(valence) \
  X(acousticness) \
  X(danceability) \
  X(energy) \
  X(instrumentalness) \
  X(liveness) \
  X(loudness) \
  X(tempo) \
  X(speechiness)

// POI dataset columns
#define POI_COLUMN_LIST \
  X(LatitudeRadian, "latitude_radian") \
  X(LongitudeRadian, "longitude_radian")

// Common Government dataset columns
#define COMMON_GOVERNMENT_COLUMN_LIST \
  X(Amount1, "amount1") \
  X(Amount2, "amount2") \
  X(Amount3, "amount3")

// Arade dataset columns
#define ARADE_COLUMN_LIST \
  X(Value1, "value1") \
  X(Value2, "value2") \
  X(Value3, "value3") \
  X(Value4, "value4")

// Algorithm list for all benchmarks (matching Snowflake's pattern)
#define ALGORITHM_LIST \
  X(ALP, kALP) \
  X(BYTESTREAMSPLIT, kByteStreamSplit) \
  X(ZSTD, kZSTD)

// ============================================================================
// Benchmark Generation Macros (matching Snowflake's pattern)
// ============================================================================

// Synthetic data benchmark macros
#define BENCHMARK_SYNTHETIC_COMPRESS(ALGO, NAME, CLASS, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##NAME##Float, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<CLASS<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, NAME, CLASS, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##NAME##Float, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<CLASS<double>>()), EncodingType::ENGINE); \
  }

// Original Spotify dataset (Dataset 1) benchmark macros
#define BENCHMARK_ORIGINAL_DATASET_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##Spotify##COLUMN_CAP##Float, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<SpotifyData<double>>(COLUMN_LOWER)), EncodingType::ENGINE); \
  }

#define BENCHMARK_ORIGINAL_DATASET_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##Spotify##COLUMN_CAP##Float, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<SpotifyData<double>>(COLUMN_LOWER)), EncodingType::ENGINE); \
  }

// New Spotify dataset (Dataset 2) benchmark macros
#define BENCHMARK_NEW_DATASET_COMPRESS(ALGO, COLUMN, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##Spotify##COLUMN##2Float, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<SpotifyData2<double>>(#COLUMN)), EncodingType::ENGINE); \
  }

#define BENCHMARK_NEW_DATASET_DECOMPRESS(ALGO, COLUMN, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##Spotify##COLUMN##2Float, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<SpotifyData2<double>>(#COLUMN)), EncodingType::ENGINE); \
  }

// City Temperature dataset benchmark macros
#define BENCHMARK_CITY_TEMP_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##CityTemperatureFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<CityTemperatureData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_CITY_TEMP_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##CityTemperatureFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<CityTemperatureData<double>>()), EncodingType::ENGINE); \
  }

// POI dataset benchmark macros
#define BENCHMARK_POI_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##Poi##COLUMN_CAP##Float, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<PoiData<double>>(COLUMN_LOWER)), EncodingType::ENGINE); \
  }

#define BENCHMARK_POI_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##Poi##COLUMN_CAP##Float, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<PoiData<double>>(COLUMN_LOWER)), EncodingType::ENGINE); \
  }

// Bird Migration dataset benchmark macros
#define BENCHMARK_BIRD_MIGRATION_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##BirdMigrationFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<BirdMigrationData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_BIRD_MIGRATION_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##BirdMigrationFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<BirdMigrationData<double>>()), EncodingType::ENGINE); \
  }

// Common Government dataset benchmark macros
#define BENCHMARK_COMMON_GOVERNMENT_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##CommonGovernment##COLUMN_CAP##Float, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<CommonGovernmentData<double>>(COLUMN_LOWER)), EncodingType::ENGINE); \
  }

#define BENCHMARK_COMMON_GOVERNMENT_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##CommonGovernment##COLUMN_CAP##Float, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<CommonGovernmentData<double>>(COLUMN_LOWER)), EncodingType::ENGINE); \
  }

// Arade dataset benchmark macros
#define BENCHMARK_ARADE_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##Arade##COLUMN_CAP##Float, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<AradeData<double>>(COLUMN_LOWER)), EncodingType::ENGINE); \
  }

#define BENCHMARK_ARADE_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##Arade##COLUMN_CAP##Float, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<AradeData<double>>(COLUMN_LOWER)), EncodingType::ENGINE); \
  }

// FPC dataset benchmark macros (generic for single-column datasets)
#define BENCHMARK_NUM_BRAIN_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##NumBrainFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<NumBrainData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_NUM_BRAIN_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##NumBrainFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<NumBrainData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_NUM_COMET_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##NumCometFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<NumCometData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_NUM_COMET_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##NumCometFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<NumCometData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_NUM_CONTROL_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##NumControlFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<NumControlData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_NUM_CONTROL_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##NumControlFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<NumControlData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_NUM_PLASMA_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##NumPlasmaFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<NumPlasmaData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_NUM_PLASMA_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##NumPlasmaFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<NumPlasmaData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_OBS_ERROR_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##ObsErrorFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<ObsErrorData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_OBS_ERROR_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##ObsErrorFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<ObsErrorData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_OBS_INFO_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##ObsInfoFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<ObsInfoData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_OBS_INFO_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##ObsInfoFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<ObsInfoData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_OBS_SPITZER_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##ObsSpitzerFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<ObsSpitzerData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_OBS_SPITZER_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##ObsSpitzerFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<ObsSpitzerData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_OBS_TEMP_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##ObsTempFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<ObsTempData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_OBS_TEMP_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##ObsTempFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<ObsTempData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_MSG_BT_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##MsgBtFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<MsgBtData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_MSG_BT_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##MsgBtFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<MsgBtData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_MSG_LU_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##MsgLuFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<MsgLuData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_MSG_LU_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##MsgLuFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<MsgLuData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_MSG_SP_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##MsgSpFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<MsgSpData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_MSG_SP_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##MsgSpFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<MsgSpData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_MSG_SPPM_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##MsgSppmFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<MsgSppmData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_MSG_SPPM_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##MsgSppmFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<MsgSppmData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_MSG_SWEEP3D_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##compress##MsgSweep3dFloat, double)(benchmark::State& state) { \
    benchmarkCompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<MsgSweep3dData<double>>()), EncodingType::ENGINE); \
  }

#define BENCHMARK_MSG_SWEEP3D_DECOMPRESS(ALGO, ENGINE) \
  BENCHMARK_TEMPLATE_F(DoubleBenchmark, ALGO##decompress##MsgSweep3dFloat, double)(benchmark::State& state) { \
    benchmarkDecompress(state, std::unique_ptr<RealComprBenchmarkData<double>>(std::make_unique<MsgSweep3dData<double>>()), EncodingType::ENGINE); \
  }

// ============================================================================
// Benchmark Registrations - Synthetic Data (All Algorithms)
// COMMENTED OUT - Using only real-world Spotify data
// ============================================================================

#if 0
#define GENERATE_SYNTHETIC_BENCHMARKS(ALGO, ENGINE) \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, Constant, ConstantValues, ENGINE) \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, Constant, ConstantValues, ENGINE) \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, Increasing, IncreasingValues, ENGINE) \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, Increasing, IncreasingValues, ENGINE) \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, SmallRange, DecimalSmallRange, ENGINE) \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, SmallRange, DecimalSmallRange, ENGINE) \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, Range, DecimalRange, ENGINE) \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, Range, DecimalRange, ENGINE) \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, LargeRange, DecimalLargeRange, ENGINE) \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, LargeRange, DecimalLargeRange, ENGINE) \
  BENCHMARK_SYNTHETIC_COMPRESS(ALGO, Random, RandomValues, ENGINE) \
  BENCHMARK_SYNTHETIC_DECOMPRESS(ALGO, Random, RandomValues, ENGINE)

#define X(ALGO, ENGINE) GENERATE_SYNTHETIC_BENCHMARKS(ALGO, ENGINE)
ALGORITHM_LIST
#undef X
#endif

// ============================================================================
// Benchmark Registrations - Spotify Dataset 1 (All Algorithms × 9 columns)
// ============================================================================

#define GENERATE_SPOTIFY_BENCHMARKS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_ORIGINAL_DATASET_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_ORIGINAL_DATASET_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)

#define GENERATE_ALGORITHM_FOR_SPOTIFY(ALGO, ENGINE) \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Valence, "valence", ENGINE) \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Acousticness, "acousticness", ENGINE) \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Danceability, "danceability", ENGINE) \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Energy, "energy", ENGINE) \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Instrumentalness, "instrumentalness", ENGINE) \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Liveness, "liveness", ENGINE) \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Loudness, "loudness", ENGINE) \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Tempo, "tempo", ENGINE) \
  GENERATE_SPOTIFY_BENCHMARKS(ALGO, Speechiness, "speechiness", ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_SPOTIFY(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ============================================================================
// Benchmark Registrations - Spotify Dataset 2 (All Algorithms × 9 columns)
// ============================================================================

#define GENERATE_SPOTIFY2_BENCHMARKS(ALGO, COLUMN, ENGINE) \
  BENCHMARK_NEW_DATASET_COMPRESS(ALGO, COLUMN, ENGINE) \
  BENCHMARK_NEW_DATASET_DECOMPRESS(ALGO, COLUMN, ENGINE)

#define GENERATE_ALGORITHM_FOR_SPOTIFY2(ALGO, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, valence, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, acousticness, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, danceability, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, energy, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, instrumentalness, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, liveness, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, loudness, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, tempo, ENGINE) \
  GENERATE_SPOTIFY2_BENCHMARKS(ALGO, speechiness, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_SPOTIFY2(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ============================================================================
// Benchmark Registrations - City Temperature Dataset (1 column × 3 algorithms)
// ============================================================================

#define GENERATE_ALGORITHM_FOR_CITY_TEMP(ALGO, ENGINE) \
  BENCHMARK_CITY_TEMP_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_CITY_TEMP_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_CITY_TEMP(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ============================================================================
// Benchmark Registrations - POI Dataset (2 columns × 3 algorithms)
// ============================================================================

#define GENERATE_ALGORITHM_FOR_POI(COLUMN_CAP, COLUMN_LOWER, ALGO, ENGINE) \
  BENCHMARK_POI_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_POI_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)

#define GENERATE_ALGORITHMS_FOR_POI_COLUMN(COLUMN_CAP, COLUMN_LOWER) \
  GENERATE_ALGORITHM_FOR_POI(COLUMN_CAP, COLUMN_LOWER, ALP, kALP) \
  GENERATE_ALGORITHM_FOR_POI(COLUMN_CAP, COLUMN_LOWER, BYTESTREAMSPLIT, kByteStreamSplit) \
  GENERATE_ALGORITHM_FOR_POI(COLUMN_CAP, COLUMN_LOWER, ZSTD, kZSTD)

#define X(COLUMN_CAP, COLUMN_LOWER) GENERATE_ALGORITHMS_FOR_POI_COLUMN(COLUMN_CAP, COLUMN_LOWER)
POI_COLUMN_LIST
#undef X

// ============================================================================
// Benchmark Registrations - Bird Migration Dataset (1 column × 3 algorithms)
// ============================================================================

#define GENERATE_ALGORITHM_FOR_BIRD_MIGRATION(ALGO, ENGINE) \
  BENCHMARK_BIRD_MIGRATION_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_BIRD_MIGRATION_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_BIRD_MIGRATION(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ============================================================================
// Benchmark Registrations - Common Government Dataset (3 columns × 3 algorithms)
// ============================================================================

#define GENERATE_ALGORITHM_FOR_COMMON_GOVERNMENT(COLUMN_CAP, COLUMN_LOWER, ALGO, ENGINE) \
  BENCHMARK_COMMON_GOVERNMENT_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_COMMON_GOVERNMENT_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)

#define GENERATE_ALGORITHMS_FOR_COMMON_GOVERNMENT_COLUMN(COLUMN_CAP, COLUMN_LOWER) \
  GENERATE_ALGORITHM_FOR_COMMON_GOVERNMENT(COLUMN_CAP, COLUMN_LOWER, ALP, kALP) \
  GENERATE_ALGORITHM_FOR_COMMON_GOVERNMENT(COLUMN_CAP, COLUMN_LOWER, BYTESTREAMSPLIT, kByteStreamSplit) \
  GENERATE_ALGORITHM_FOR_COMMON_GOVERNMENT(COLUMN_CAP, COLUMN_LOWER, ZSTD, kZSTD)

#define X(COLUMN_CAP, COLUMN_LOWER) GENERATE_ALGORITHMS_FOR_COMMON_GOVERNMENT_COLUMN(COLUMN_CAP, COLUMN_LOWER)
COMMON_GOVERNMENT_COLUMN_LIST
#undef X

// ============================================================================
// Benchmark Registrations - Arade Dataset (4 columns × 3 algorithms)
// ============================================================================

#define GENERATE_ALGORITHM_FOR_ARADE(COLUMN_CAP, COLUMN_LOWER, ALGO, ENGINE) \
  BENCHMARK_ARADE_COMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE) \
  BENCHMARK_ARADE_DECOMPRESS(ALGO, COLUMN_CAP, COLUMN_LOWER, ENGINE)

#define GENERATE_ALGORITHMS_FOR_ARADE_COLUMN(COLUMN_CAP, COLUMN_LOWER) \
  GENERATE_ALGORITHM_FOR_ARADE(COLUMN_CAP, COLUMN_LOWER, ALP, kALP) \
  GENERATE_ALGORITHM_FOR_ARADE(COLUMN_CAP, COLUMN_LOWER, BYTESTREAMSPLIT, kByteStreamSplit) \
  GENERATE_ALGORITHM_FOR_ARADE(COLUMN_CAP, COLUMN_LOWER, ZSTD, kZSTD)

#define X(COLUMN_CAP, COLUMN_LOWER) GENERATE_ALGORITHMS_FOR_ARADE_COLUMN(COLUMN_CAP, COLUMN_LOWER)
ARADE_COLUMN_LIST
#undef X

// ============================================================================
// Benchmark Registrations - FPC Datasets (13 single-column datasets × 3 algorithms each)
// ============================================================================

// NumBrain dataset
#define GENERATE_ALGORITHM_FOR_NUM_BRAIN(ALGO, ENGINE) \
  BENCHMARK_NUM_BRAIN_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_NUM_BRAIN_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_NUM_BRAIN(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// NumComet dataset
#define GENERATE_ALGORITHM_FOR_NUM_COMET(ALGO, ENGINE) \
  BENCHMARK_NUM_COMET_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_NUM_COMET_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_NUM_COMET(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// NumControl dataset
#define GENERATE_ALGORITHM_FOR_NUM_CONTROL(ALGO, ENGINE) \
  BENCHMARK_NUM_CONTROL_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_NUM_CONTROL_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_NUM_CONTROL(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// NumPlasma dataset
#define GENERATE_ALGORITHM_FOR_NUM_PLASMA(ALGO, ENGINE) \
  BENCHMARK_NUM_PLASMA_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_NUM_PLASMA_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_NUM_PLASMA(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ObsError dataset
#define GENERATE_ALGORITHM_FOR_OBS_ERROR(ALGO, ENGINE) \
  BENCHMARK_OBS_ERROR_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_OBS_ERROR_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_OBS_ERROR(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ObsInfo dataset
#define GENERATE_ALGORITHM_FOR_OBS_INFO(ALGO, ENGINE) \
  BENCHMARK_OBS_INFO_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_OBS_INFO_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_OBS_INFO(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ObsSpitzer dataset
#define GENERATE_ALGORITHM_FOR_OBS_SPITZER(ALGO, ENGINE) \
  BENCHMARK_OBS_SPITZER_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_OBS_SPITZER_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_OBS_SPITZER(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// ObsTemp dataset
#define GENERATE_ALGORITHM_FOR_OBS_TEMP(ALGO, ENGINE) \
  BENCHMARK_OBS_TEMP_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_OBS_TEMP_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_OBS_TEMP(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// MsgBt dataset
#define GENERATE_ALGORITHM_FOR_MSG_BT(ALGO, ENGINE) \
  BENCHMARK_MSG_BT_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_MSG_BT_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_MSG_BT(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// MsgLu dataset
#define GENERATE_ALGORITHM_FOR_MSG_LU(ALGO, ENGINE) \
  BENCHMARK_MSG_LU_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_MSG_LU_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_MSG_LU(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// MsgSp dataset
#define GENERATE_ALGORITHM_FOR_MSG_SP(ALGO, ENGINE) \
  BENCHMARK_MSG_SP_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_MSG_SP_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_MSG_SP(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// MsgSppm dataset
#define GENERATE_ALGORITHM_FOR_MSG_SPPM(ALGO, ENGINE) \
  BENCHMARK_MSG_SPPM_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_MSG_SPPM_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_MSG_SPPM(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

// MsgSweep3d dataset
#define GENERATE_ALGORITHM_FOR_MSG_SWEEP3D(ALGO, ENGINE) \
  BENCHMARK_MSG_SWEEP3D_COMPRESS(ALGO, ENGINE) \
  BENCHMARK_MSG_SWEEP3D_DECOMPRESS(ALGO, ENGINE)

#define X(ALGO, ENGINE) GENERATE_ALGORITHM_FOR_MSG_SWEEP3D(ALGO, ENGINE)
ALGORITHM_LIST
#undef X

}  // namespace parquet

BENCHMARK_MAIN();
