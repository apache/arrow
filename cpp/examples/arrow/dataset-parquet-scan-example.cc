// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdlib>
#include <iostream>

#include <arrow/api.h>

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/filter.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/localfs.h>

using arrow::field;
using arrow::int16;
using arrow::Schema;
using arrow::Table;

namespace fs = arrow::fs;

namespace ds = arrow::dataset;

using ds::string_literals::operator"" _;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

struct Configuration {
  // Increase the ds::DataSet by repeating `repeat` times the ds::DataSource.
  size_t repeat = 1;

  // Indicates if the Scanner::ToTable should consume in parallel.
  bool use_threads = true;

  // Indicates to the Scan operator which columns are requested. This
  // optimization avoid deserializing unneeded columns.
  std::vector<std::string> projected_columns = {"pickup_at", "dropoff_at",
                                                "total_amount"};

  // Indicates the filter by which rows will be filtered. This optimization can
  // make use of partition information and/or file metadata if possible.
  std::shared_ptr<ds::Expression> filter = ("total_amount"_ > 1000.0f).Copy();
} conf;

std::shared_ptr<ds::Dataset> GetDatasetFromPath(fs::FileSystem* fs,
                                                std::shared_ptr<ds::FileFormat> format,
                                                std::string path) {
  // Find all files under `path`
  fs::Selector s;
  s.base_dir = path;
  s.recursive = true;

  std::shared_ptr<ds::DataSourceDiscovery> discovery;
  ABORT_ON_FAILURE(ds::FileSystemDataSourceDiscovery::Make(fs, s, format, &discovery));

  std::shared_ptr<Schema> inspect_schema;
  ABORT_ON_FAILURE(discovery->Inspect(&inspect_schema));

  std::shared_ptr<ds::DataSource> source;
  ABORT_ON_FAILURE(discovery->Finish(&source));

  return std::make_shared<ds::Dataset>(ds::DataSourceVector{conf.repeat, source},
                                       inspect_schema);
}

std::shared_ptr<ds::Scanner> GetScannerFromDataset(std::shared_ptr<ds::Dataset> dataset,
                                                   std::vector<std::string> columns,
                                                   std::shared_ptr<ds::Expression> filter,
                                                   bool use_threads) {
  std::unique_ptr<ds::ScannerBuilder> scanner_builder;
  ABORT_ON_FAILURE(dataset->NewScan(&scanner_builder));

  if (!columns.empty()) {
    ABORT_ON_FAILURE(scanner_builder->Project(columns));
  }

  if (filter != nullptr) {
    ABORT_ON_FAILURE(scanner_builder->Filter(filter));
  }

  ABORT_ON_FAILURE(scanner_builder->UseThreads(use_threads));

  std::unique_ptr<ds::Scanner> scanner;
  ABORT_ON_FAILURE(scanner_builder->Finish(&scanner));

  return scanner;
}

std::shared_ptr<Table> GetTableFromScanner(std::shared_ptr<ds::Scanner> scanner) {
  std::shared_ptr<Table> table;
  ABORT_ON_FAILURE(scanner->ToTable(&table));
  return table;
}

int main(int argc, char** argv) {
  auto fs = std::make_shared<fs::LocalFileSystem>();
  auto format = std::make_shared<ds::ParquetFileFormat>();

  if (argc != 2) {
    // Fake success for CI purposes.
    return EXIT_SUCCESS;
  }

  auto dataset = GetDatasetFromPath(fs.get(), format, argv[1]);

  auto scanner = GetScannerFromDataset(dataset, conf.projected_columns, conf.filter,
                                       conf.use_threads);

  auto table = GetTableFromScanner(scanner);
  std::cout << "Table size: " << table->num_rows() << "\n";

  return EXIT_SUCCESS;
}
