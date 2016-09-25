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

#include <ctime>
#include <iostream>
#include <list>
#include <memory>

#include "parquet/api/reader.h"

int main(int argc, char** argv) {
  if (argc > 4 || argc < 1) {
    std::cerr << "Usage: parquet-scan [--batch-size=] [--columns=...] <file>"
              << std::endl;
    return -1;
  }

  std::string filename;

  // Read command-line options
  int batch_size = 256;
  const std::string COLUMNS_PREFIX = "--columns=";
  const std::string BATCH_SIZE_PREFIX = "--batch-size=";
  std::vector<int> columns;
  int num_columns = 0;

  char *param, *value;
  for (int i = 1; i < argc; i++) {
    if ((param = std::strstr(argv[i], COLUMNS_PREFIX.c_str()))) {
      value = std::strtok(param + COLUMNS_PREFIX.length(), ",");
      while (value) {
        columns.push_back(std::atoi(value));
        value = std::strtok(nullptr, ",");
        num_columns++;
      }
    } else if ((param = std::strstr(argv[i], BATCH_SIZE_PREFIX.c_str()))) {
      value = std::strtok(param + BATCH_SIZE_PREFIX.length(), " ");
      if (value) { batch_size = std::atoi(value); }
    } else {
      filename = argv[i];
    }
  }

  std::vector<int16_t> rep_levels(batch_size);
  std::vector<int16_t> def_levels(batch_size);
  try {
    double total_time;
    std::clock_t start_time = std::clock();
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::OpenFile(filename);
    // columns are not specified explicitly. Add all columns
    if (num_columns == 0) {
      num_columns = reader->metadata()->num_columns();
      columns.resize(num_columns);
      for (int i = 0; i < num_columns; i++) {
        columns[i] = i;
      }
    }

    int64_t total_rows[num_columns];

    for (int r = 0; r < reader->metadata()->num_row_groups(); ++r) {
      auto group_reader = reader->RowGroup(r);
      int col = 0;
      for (auto i : columns) {
        total_rows[col] = 0;
        std::shared_ptr<parquet::ColumnReader> col_reader = group_reader->Column(i);
        size_t value_byte_size = GetTypeByteSize(col_reader->descr()->physical_type());
        std::vector<uint8_t> values(batch_size * value_byte_size);

        int64_t values_read = 0;
        while (col_reader->HasNext()) {
          total_rows[col] += ScanAllValues(batch_size, def_levels.data(),
              rep_levels.data(), values.data(), &values_read, col_reader.get());
        }
        col++;
      }
    }

    total_time = (std::clock() - start_time) / static_cast<double>(CLOCKS_PER_SEC);
    for (int ct = 1; ct < num_columns; ++ct) {
      if (total_rows[0] != total_rows[ct]) {
        std::cerr << "Parquet error: Total rows among columns do not match" << std::endl;
      }
    }
    std::cout << total_rows[0] << " rows scanned in " << total_time << " seconds."
              << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Parquet error: " << e.what() << std::endl;
    return -1;
  }

  return 0;
}
