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

// Generates ALP-encoded parquet test files from CSV floating-point datasets.
//
// Usage:
//   generate_alp_parquet <csv_dir> <output_dir>
//
// Reads floatingpoint_spotify1.csv and floatingpoint_arade.csv from <csv_dir>,
// writes ALP-encoded .parquet and _expect.csv files to <output_dir>.

#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/io/file.h"
#include "parquet/arrow/writer.h"
#include "parquet/properties.h"

// Parse a CSV/delimited file into column vectors of doubles.
// Returns column names (empty strings if no header).
static std::vector<std::string> ParseCsv(const std::string& path, char delimiter,
                                         bool has_header, int num_columns,
                                         std::vector<std::vector<double>>* columns) {
  columns->resize(num_columns);
  std::vector<std::string> col_names(num_columns);

  std::ifstream file(path);
  if (!file.is_open()) {
    std::cerr << "Failed to open: " << path << std::endl;
    return col_names;
  }

  std::string line;
  if (has_header && std::getline(file, line)) {
    std::istringstream ss(line);
    std::string token;
    for (int i = 0; i < num_columns && std::getline(ss, token, delimiter); i++) {
      col_names[i] = token;
    }
  }

  while (std::getline(file, line)) {
    if (line.empty()) continue;
    std::istringstream ss(line);
    std::string token;
    for (int i = 0; i < num_columns && std::getline(ss, token, delimiter); i++) {
      (*columns)[i].push_back(std::stod(token));
    }
  }
  return col_names;
}

// Write an ALP-encoded parquet file from column data.
static arrow::Status WriteAlpParquet(
    const std::string& output_path,
    const std::vector<std::string>& col_names,
    const std::vector<std::vector<double>>& columns) {
  // Build Arrow schema
  arrow::FieldVector fields;
  for (const auto& name : col_names) {
    fields.push_back(arrow::field(name, arrow::float64()));
  }
  auto schema = arrow::schema(fields);

  // Build Arrow arrays
  arrow::ArrayVector arrays;
  for (const auto& col : columns) {
    arrow::DoubleBuilder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(col));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    arrays.push_back(array);
  }

  auto table = arrow::Table::Make(schema, arrays);

  // Writer properties: ALP encoding, no dictionary, uncompressed
  auto props = parquet::WriterProperties::Builder()
                   .encoding(parquet::Encoding::ALP)
                   ->disable_dictionary()
                   ->compression(parquet::Compression::UNCOMPRESSED)
                   ->build();

  // Write
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(output_path));
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile,
      static_cast<int64_t>(columns[0].size()), props));

  std::cout << "Wrote " << output_path << " (" << columns[0].size() << " rows, "
            << columns.size() << " columns)" << std::endl;
  return arrow::Status::OK();
}

// Write an ALP-encoded float32 parquet file from column data (double->float cast).
static arrow::Status WriteAlpParquetFloat(
    const std::string& output_path,
    const std::vector<std::string>& col_names,
    const std::vector<std::vector<double>>& columns) {
  // Build Arrow schema with float32 fields
  arrow::FieldVector fields;
  for (const auto& name : col_names) {
    fields.push_back(arrow::field(name, arrow::float32()));
  }
  auto schema = arrow::schema(fields);

  // Build Arrow arrays — cast doubles to float
  arrow::ArrayVector arrays;
  for (const auto& col : columns) {
    arrow::FloatBuilder builder;
    for (double val : col) {
      ARROW_RETURN_NOT_OK(builder.Append(static_cast<float>(val)));
    }
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    arrays.push_back(array);
  }

  auto table = arrow::Table::Make(schema, arrays);

  // Writer properties: ALP encoding, no dictionary, uncompressed
  auto props = parquet::WriterProperties::Builder()
                   .encoding(parquet::Encoding::ALP)
                   ->disable_dictionary()
                   ->compression(parquet::Compression::UNCOMPRESSED)
                   ->build();

  // Write
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(output_path));
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile,
      static_cast<int64_t>(columns[0].size()), props));

  std::cout << "Wrote " << output_path << " (" << columns[0].size() << " rows, "
            << columns.size() << " columns, float32)" << std::endl;
  return arrow::Status::OK();
}

// Write expected CSV for float32 with sufficient precision for bit-exact float round-trip.
static void WriteExpectCsvFloat(const std::string& output_path,
                                const std::vector<std::string>& col_names,
                                const std::vector<std::vector<double>>& columns) {
  std::ofstream file(output_path);

  // Header
  for (size_t i = 0; i < col_names.size(); i++) {
    if (i > 0) file << ",";
    file << col_names[i];
  }
  file << "\n";

  // Data rows — cast to float, then print with 9 significant digits for exact float round-trip
  size_t num_rows = columns[0].size();
  for (size_t r = 0; r < num_rows; r++) {
    for (size_t c = 0; c < columns.size(); c++) {
      if (c > 0) file << ",";
      float val = static_cast<float>(columns[c][r]);
      file << std::setprecision(9) << val;
    }
    file << "\n";
  }

  std::cout << "Wrote " << output_path << std::endl;
}

// Write expected CSV with sufficient precision for bit-exact double round-trip.
static void WriteExpectCsv(const std::string& output_path,
                           const std::vector<std::string>& col_names,
                           const std::vector<std::vector<double>>& columns) {
  std::ofstream file(output_path);

  // Header
  for (size_t i = 0; i < col_names.size(); i++) {
    if (i > 0) file << ",";
    file << col_names[i];
  }
  file << "\n";

  // Data rows with full precision (17 significant digits for exact double round-trip)
  size_t num_rows = columns[0].size();
  for (size_t r = 0; r < num_rows; r++) {
    for (size_t c = 0; c < columns.size(); c++) {
      if (c > 0) file << ",";
      double val = columns[c][r];
      // Use hex float for exact representation
      uint64_t bits;
      std::memcpy(&bits, &val, sizeof(bits));
      file << std::setprecision(17) << val;
    }
    file << "\n";
  }

  std::cout << "Wrote " << output_path << std::endl;
}

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <csv_dir> <output_dir>" << std::endl;
    std::cerr << "  csv_dir:    directory containing floatingpoint_*.csv files" << std::endl;
    std::cerr << "  output_dir: directory for output .parquet and _expect.csv files"
              << std::endl;
    return 1;
  }

  std::string csv_dir = argv[1];
  std::string output_dir = argv[2];

  // === Dataset 1: Spotify1 (comma-separated, header, 9 double columns) ===
  {
    std::vector<std::vector<double>> columns;
    auto names = ParseCsv(csv_dir + "/floatingpoint_spotify1.csv", ',',
                          /*has_header=*/true, 9, &columns);

    auto status =
        WriteAlpParquet(output_dir + "/alp_spotify1.parquet", names, columns);
    if (!status.ok()) {
      std::cerr << "Error writing spotify1 parquet: " << status.ToString() << std::endl;
      return 1;
    }
    WriteExpectCsv(output_dir + "/alp_spotify1_expect.csv", names, columns);

    // Float32 version
    auto float_status =
        WriteAlpParquetFloat(output_dir + "/alp_float_spotify1.parquet", names, columns);
    if (!float_status.ok()) {
      std::cerr << "Error writing float spotify1 parquet: " << float_status.ToString()
                << std::endl;
      return 1;
    }
    WriteExpectCsvFloat(output_dir + "/alp_float_spotify1_expect.csv", names, columns);
  }

  // === Dataset 2: Arade (pipe-separated, no header, 4 double columns) ===
  {
    std::vector<std::string> names = {"value1", "value2", "value3", "value4"};
    std::vector<std::vector<double>> columns;
    ParseCsv(csv_dir + "/floatingpoint_arade.csv", '|',
             /*has_header=*/false, 4, &columns);

    auto status =
        WriteAlpParquet(output_dir + "/alp_arade.parquet", names, columns);
    if (!status.ok()) {
      std::cerr << "Error writing arade parquet: " << status.ToString() << std::endl;
      return 1;
    }
    WriteExpectCsv(output_dir + "/alp_arade_expect.csv", names, columns);

    // Float32 version
    auto float_status =
        WriteAlpParquetFloat(output_dir + "/alp_float_arade.parquet", names, columns);
    if (!float_status.ok()) {
      std::cerr << "Error writing float arade parquet: " << float_status.ToString()
                << std::endl;
      return 1;
    }
    WriteExpectCsvFloat(output_dir + "/alp_float_arade_expect.csv", names, columns);
  }

  std::cout << "Done." << std::endl;
  return 0;
}
