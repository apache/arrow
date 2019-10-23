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
#include <arrow/util/iterator.h>
#include <arrow/util/task_group.h>
#include <arrow/util/thread_pool.h>

using arrow::field;
using arrow::int16;
using arrow::Schema;
using arrow::Table;

using arrow::internal::TaskGroup;
using arrow::internal::ThreadPool;

using arrow::fs::FileSystem;
using arrow::fs::LocalFileSystem;
using arrow::fs::Selector;

using arrow::dataset::Dataset;
using arrow::dataset::DataSource;
using arrow::dataset::DataSourceDiscovery;
using arrow::dataset::DataSourceVector;
using arrow::dataset::Expression;
using arrow::dataset::FieldExpression;
using arrow::dataset::FileFormat;
using arrow::dataset::FileSystemDataSourceDiscovery;
using arrow::dataset::ParquetFileFormat;
using arrow::dataset::PartitionScheme;
using arrow::dataset::Scanner;
using arrow::dataset::ScannerBuilder;
using arrow::dataset::ScanTask;
using arrow::dataset::string_literals::operator"" _;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

std::shared_ptr<Dataset> GetDatasetFromPath(FileSystem* fs,
                                            std::shared_ptr<FileFormat> format,
                                            std::string path) {
  // Find all files under `path`
  Selector s;
  s.base_dir = path;
  s.recursive = true;

  std::shared_ptr<DataSourceDiscovery> discovery;
  ABORT_ON_FAILURE(FileSystemDataSourceDiscovery::Make(fs, s, format, &discovery));

  std::shared_ptr<Schema> inspect_schema;
  ABORT_ON_FAILURE(discovery->Inspect(&inspect_schema));

  std::shared_ptr<DataSource> source;
  ABORT_ON_FAILURE(discovery->Finish(&source));

  return std::make_shared<Dataset>(DataSourceVector{source}, inspect_schema);
}

std::shared_ptr<Scanner> GetScannerFromDataset(std::shared_ptr<Dataset> dataset,
                                               std::vector<std::string> columns,
                                               std::shared_ptr<Expression> filter) {
  std::unique_ptr<ScannerBuilder> scanner_builder;
  ABORT_ON_FAILURE(dataset->NewScan(&scanner_builder));

  if (!columns.empty()) {
    ABORT_ON_FAILURE(scanner_builder->Project(columns));
  }

  if (filter != nullptr) {
    ABORT_ON_FAILURE(scanner_builder->Filter(filter));
  }

  std::unique_ptr<Scanner> scanner;
  ABORT_ON_FAILURE(scanner_builder->Finish(&scanner));

  return scanner;
}

std::shared_ptr<Table> GetTableFromScanner(std::shared_ptr<Scanner> scanner) {
  std::shared_ptr<Table> table;
  ABORT_ON_FAILURE(scanner->ToTable(&table));
  return table;
}

int main(int argc, char** argv) {
  auto fs = std::make_shared<LocalFileSystem>();
  auto format = std::make_shared<ParquetFileFormat>();

  if (argc != 2) {
    // Fake success for CI purposes.
    return EXIT_SUCCESS;
  }

  auto dataset = GetDatasetFromPath(fs.get(), format, argv[1]);

  // Limit the number of returned columns
  std::vector<std::string> columns{"pickup_at", "dropoff_at", "total_amount"};
  // Filter the rows on a predicate
  auto filter = ("total_amount"_ > 1000.0f).Copy();
  auto scanner = GetScannerFromDataset(dataset, columns, filter);

  auto table = GetTableFromScanner(scanner);
  std::cout << "Table size: " << table->num_rows() << "\n";

  return EXIT_SUCCESS;
}
