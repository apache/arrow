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

#include <arrow/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include "arrow/compute/expression.h"

#include <cstdlib>
#include <iostream>

/**
 * \brief Run Example
 *
 * Make sure there is a parquet dataset with the column_names
 * mentioned in the Congiguration struct.
 *
 * Example run:
 * ./dataset_parquet_scan_example file:///<some-path>/data.parquet
 *
 */

using arrow::field;
using arrow::int16;
using arrow::Schema;
using arrow::Table;

namespace fs = arrow::fs;

namespace ds = arrow::dataset;

namespace cp = arrow::compute;

struct Configuration {
  // Increase the ds::DataSet by repeating `repeat` times the ds::Dataset.
  size_t repeat = 1;

  // Indicates if the Scanner::ToTable should consume in parallel.
  bool use_threads = true;

  // Indicates to the Scan operator which columns are requested. This
  // optimization avoid deserializing unneeded columns.
  std::vector<std::string> projected_columns = {"pickup_at", "dropoff_at",
                                                "total_amount"};

  // Indicates the filter by which rows will be filtered. This optimization can
  // make use of partition information and/or file metadata if possible.
  cp::Expression filter = cp::greater(cp::field_ref("total_amount"), cp::literal(100.0f));

  ds::InspectOptions inspect_options{};
  ds::FinishOptions finish_options{};
} conf;

arrow::Result<std::shared_ptr<fs::FileSystem>> GetFileSystemFromUri(
    const std::string& uri, std::string* path) {
  return fs::FileSystemFromUri(uri, path);
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromDirectory(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string dir) {
  // Find all files under `path`
  fs::FileSelector s;
  s.base_dir = dir;
  s.recursive = true;

  ds::FileSystemFactoryOptions options;
  // The factory will try to build a child dataset.
  ARROW_ASSIGN_OR_RAISE(auto factory,
                        ds::FileSystemDatasetFactory::Make(fs, s, format, options));

  // Try to infer a common schema for all files.
  ARROW_ASSIGN_OR_RAISE(auto schema, factory->Inspect(conf.inspect_options));
  // Caller can optionally decide another schema as long as it is compatible
  // with the previous one, e.g. `factory->Finish(compatible_schema)`.
  ARROW_ASSIGN_OR_RAISE(auto child, factory->Finish(conf.finish_options));

  ds::DatasetVector children{conf.repeat, child};
  auto dataset = ds::UnionDataset::Make(std::move(schema), std::move(children));

  return dataset;
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetParquetDatasetFromMetadata(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string metadata_path) {
  ds::ParquetFactoryOptions options;
  ARROW_ASSIGN_OR_RAISE(
      auto factory, ds::ParquetDatasetFactory::Make(metadata_path, fs, format, options));
  return factory->Finish();
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromFile(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string file) {
  ds::FileSystemFactoryOptions options;
  // The factory will try to build a child dataset.
  ARROW_ASSIGN_OR_RAISE(auto factory,
                        ds::FileSystemDatasetFactory::Make(fs, {file}, format, options));

  // Try to infer a common schema for all files.
  ARROW_ASSIGN_OR_RAISE(auto schema, factory->Inspect(conf.inspect_options));
  // Caller can optionally decide another schema as long as it is compatible
  // with the previous one, e.g. `factory->Finish(compatible_schema)`.
  ARROW_ASSIGN_OR_RAISE(auto child, factory->Finish(conf.finish_options));

  ds::DatasetVector children;
  children.resize(conf.repeat, child);
  return ds::UnionDataset::Make(std::move(schema), std::move(children));
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromPath(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string path) {
  ARROW_ASSIGN_OR_RAISE(auto info, fs->GetFileInfo(path));
  if (info.IsDirectory()) {
    return GetDatasetFromDirectory(fs, format, path);
  }

  auto dirname_basename = arrow::fs::internal::GetAbstractPathParent(path);
  auto basename = dirname_basename.second;

  if (basename == "_metadata") {
    return GetParquetDatasetFromMetadata(fs, format, path);
  }

  return GetDatasetFromFile(fs, format, path);
}

arrow::Result<std::shared_ptr<ds::Scanner>> GetScannerFromDataset(
    std::shared_ptr<ds::Dataset> dataset, std::vector<std::string> columns,
    cp::Expression filter, bool use_threads) {
  ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());

  if (!columns.empty()) {
    ARROW_RETURN_NOT_OK(scanner_builder->Project(columns));
  }

  ARROW_RETURN_NOT_OK(scanner_builder->Filter(filter));

  ARROW_RETURN_NOT_OK(scanner_builder->UseThreads(use_threads));

  return scanner_builder->Finish();
}

arrow::Result<std::shared_ptr<Table>> GetTableFromScanner(
    std::shared_ptr<ds::Scanner> scanner) {
  return scanner->ToTable();
}

arrow::Status RunDatasetParquetScan(char** argv) {
  std::string path;
  auto format = std::make_shared<ds::ParquetFileFormat>();
  ARROW_ASSIGN_OR_RAISE(auto fs, GetFileSystemFromUri(argv[1], &path));
  ARROW_ASSIGN_OR_RAISE(auto dataset, GetDatasetFromPath(fs, format, path));

  ARROW_ASSIGN_OR_RAISE(
      auto scanner, GetScannerFromDataset(dataset, conf.projected_columns, conf.filter,
                                          conf.use_threads));

  ARROW_ASSIGN_OR_RAISE(auto table, GetTableFromScanner(scanner));
  std::cout << "Table size: " << table->num_rows() << "\n";
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    // Fake success for CI purposes.
    return EXIT_SUCCESS;
  }

  auto status = RunDatasetParquetScan(argv);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
