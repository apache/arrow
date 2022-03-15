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
#include <arrow/compute/exec/expression.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include <skyhook/client/file_skyhook.h>

#include <cstdlib>
#include <iostream>

using arrow::field;
using arrow::int16;
using arrow::Schema;
using arrow::Table;

namespace fs = arrow::fs;

namespace ds = arrow::dataset;

namespace cp = arrow::compute;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

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
  cp::Expression filter =
      cp::greater(cp::field_ref("total_amount"), cp::literal(1000.0f));

  ds::InspectOptions inspect_options{};
  ds::FinishOptions finish_options{};
} conf;

arrow::Result<std::shared_ptr<fs::FileSystem>> GetFileSystemFromUri(const std::string& uri,
                                                     std::string* path) {
  return fs::FileSystemFromUri(uri, path);
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromDirectory(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::FileFormat> format,
    std::string dir) {
  // Find all files under `path`
  fs::FileSelector s;
  s.base_dir = dir;
  s.recursive = true;

  ds::FileSystemFactoryOptions options;
  // The factory will try to build a child dataset.
  ARROW_ASSIGN_OR_RASIE(auto factory, ds::FileSystemDatasetFactory::Make(fs, s, format, options));

  // Try to infer a common schema for all files.
  ARROW_ASSIGN_OR_RASIE(auto schema, factory->Inspect(conf.inspect_options));
  // Caller can optionally decide another schema as long as it is compatible
  // with the previous one, e.g. `factory->Finish(compatible_schema)`.
  ARROW_ASSIGN_OR_RASIE(auto child, factory->Finish(conf.finish_options));

  ds::DatasetVector children{conf.repeat, child};
  ARROW_ASSIGN_OR_RASIE(auto dataset, ds::UnionDataset::Make(std::move(schema), std::move(children)));

  return dataset;
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromFile(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::FileFormat> format,
    std::string file) {
  ds::FileSystemFactoryOptions options;
  // The factory will try to build a child dataset.
  ARROW_ASSIGN_OR_RASIE(auto factory,
      ds::FileSystemDatasetFactory::Make(fs, {file}, format, options));

  // Try to infer a common schema for all files.
  ARROW_ASSIGN_OR_RASIE(auto schema, factory->Inspect(conf.inspect_options));
  // Caller can optionally decide another schema as long as it is compatible
  // with the previous one, e.g. `factory->Finish(compatible_schema)`.
  ARROW_ASSIGN_OR_RASIE(auto child, factory->Finish(conf.finish_options));

  ds::DatasetVector children;
  children.resize(conf.repeat, child);
  ARROW_ASSIGN_OR_RASIE(auto dataset, ds::UnionDataset::Make(std::move(schema), std::move(children)));

  return dataset;
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromPath(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::FileFormat> format,
    std::string path) {
  auto info = fs->GetFileInfo(path);
  if (info.IsDirectory()) {
    return GetDatasetFromDirectory(fs, format, path);
  }
  return GetDatasetFromFile(fs, format, path);
}

arrow::Result<std::shared_ptr<ds::Scanner>> GetScannerFromDataset(std::shared_ptr<ds::Dataset> dataset,
                                                   std::vector<std::string> columns,
                                                   cp::Expression filter,
                                                   bool use_threads) {
  auto scanner_builder = dataset->NewScan();

  if (!columns.empty()) {
    ABORT_ON_FAILURE(scanner_builder->Project(columns));
  }

  ABORT_ON_FAILURE(scanner_builder->Filter(filter));

  ABORT_ON_FAILURE(scanner_builder->UseThreads(use_threads));

  return scanner_builder->Finish();
}

arrow::Result<std::shared_ptr<Table>> GetTableFromScanner(std::shared_ptr<ds::Scanner> scanner) {
  return scanner->ToTable();
}

arrow::Result<std::shared_ptr<skyhook::SkyhookFileFormat>> InstantiateSkyhookFormat() {
  std::string ceph_config_path = "/etc/ceph/ceph.conf";
  std::string ceph_data_pool = "cephfs_data";
  std::string ceph_user_name = "client.admin";
  std::string ceph_cluster_name = "ceph";
  std::string ceph_cls_name = "arrow";
  std::shared_ptr<skyhook::RadosConnCtx> rados_ctx =
      std::make_shared<skyhook::RadosConnCtx>(ceph_config_path, ceph_data_pool,
                                              ceph_user_name, ceph_cluster_name,
                                              ceph_cls_name);
  auto format = skyhook::SkyhookFileFormat::Make(rados_ctx, "parquet");
  return format;
}

arrow::Status Main(char **argv) {
  ARROW_ASSIGN_OR_RAISE(auto format, InstantiateSkyhookFormat());
  std::string path;

  ARROW_ASSIGN_OR_RAISE(auto fs, GetFileSystemFromUri(argv[1], &path));
  ARROW_ASSIGN_OR_RAISE(auto dataset, GetDatasetFromPath(fs, format, path));
  ARROW_ASSIGN_OR_RAISE(auto scanner, GetScannerFromDataset(
      dataset, conf.projected_columns, conf.filter, conf.use_threads)
  );
  ARROW_ASSIGN_OR_RAISE(auto table, GetTableFromScanner(scanner));
  std::cout << "Table size: " << table->num_rows() << "\n";
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    // Fake success for CI purposes.
    return EXIT_SUCCESS;
  }

  arrow::Status s  = Main(argv);
  if (!s.ok()) {
    std::cerr << "Error: " << s.ToString() << "\n";
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
