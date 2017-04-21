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

#include <iostream>
#include <list>
#include <memory>

#include "parquet/api/reader.h"

int main(int argc, char** argv) {
  if (argc > 5 || argc < 2) {
    std::cerr << "Usage: parquet_reader [--only-metadata] [--no-memory-map] "
                 "[--columns=...] <file>"
              << std::endl;
    return -1;
  }

  std::string filename;
  bool print_values = true;
  bool memory_map = true;

  // Read command-line options
  const std::string COLUMNS_PREFIX = "--columns=";
  std::list<int> columns;

  char *param, *value;
  for (int i = 1; i < argc; i++) {
    if ((param = std::strstr(argv[i], "--only-metadata"))) {
      print_values = false;
    } else if ((param = std::strstr(argv[i], "--no-memory-map"))) {
      memory_map = false;
    } else if ((param = std::strstr(argv[i], COLUMNS_PREFIX.c_str()))) {
      value = std::strtok(param + COLUMNS_PREFIX.length(), ",");
      while (value) {
        columns.push_back(std::atoi(value));
        value = std::strtok(nullptr, ",");
      }
    } else {
      filename = argv[i];
    }
  }

  try {
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::OpenFile(filename, memory_map);
    parquet::ParquetFilePrinter printer(reader.get());
    printer.DebugPrint(std::cout, columns, print_values);
  } catch (const std::exception& e) {
    std::cerr << "Parquet error: " << e.what() << std::endl;
    return -1;
  }

  return 0;
}
