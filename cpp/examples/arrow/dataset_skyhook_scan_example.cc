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
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include <skyhook/client/file_skyhook.h>
#include "arrow/compute/expression.h"

#include <cstdlib>
#include <iostream>

using arrow::field;
using arrow::int16;
using arrow::Schema;
using arrow::Table;

namespace fs = arrow::fs;

namespace ds = arrow::dataset;

namespace cp = arrow::compute;

struct Configuration {
  // Indicates if the Scanner::ToTable should consume in parallel.
  bool use_threads = true;

  // Indicates to the Scan operator which columns are requested. This
  // optimization avoid deserializing unneeded columns.
  std::vector<std::string> projected_columns = {"total_amount"};

  // Indicates the filter by which rows will be filtered. This optimization can
  // make use of partition information and/or file metadata if possible.
  cp::Expression filter = cp::greater(cp::field_ref("payment_type"), cp::literal(1));

  ds::InspectOptions inspect_options{};
  ds::FinishOptions finish_options{};
} kConf;

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromDirectory(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::FileFormat> format,
    std::string dir) {
  // Find all files under `path`
  fs::FileSelector s;
  s.base_dir = dir;
  s.recursive = true;

  // Set partitioning strategy
  ds::FileSystemFactoryOptions options;
  options.partitioning = std::make_shared<ds::HivePartitioning>(
      arrow::schema({arrow::field("payment_type", arrow::int32()),
                     arrow::field("VendorID", arrow::int32())}));

  // The factory will try to build a dataset.
  ARROW_ASSIGN_OR_RAISE(auto factory,
                        ds::FileSystemDatasetFactory::Make(fs, s, format, options));

  // Try to infer a common schema for all files.
  ARROW_ASSIGN_OR_RAISE(auto schema, factory->Inspect(kConf.inspect_options));
  // Caller can optionally decide another schema as long as it is compatible
  // with the previous one, e.g. `factory->Finish(compatible_schema)`.
  ARROW_ASSIGN_OR_RAISE(auto dataset, factory->Finish(kConf.finish_options));

  return dataset;
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromFile(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::FileFormat> format,
    std::string file) {
  ds::FileSystemFactoryOptions options;
  // The factory will try to build a dataset.
  ARROW_ASSIGN_OR_RAISE(auto factory,
                        ds::FileSystemDatasetFactory::Make(fs, {file}, format, options));

  // Try to infer a common schema for all files.
  ARROW_ASSIGN_OR_RAISE(auto schema, factory->Inspect(kConf.inspect_options));
  // Caller can optionally decide another schema as long as it is compatible
  // with the previous one, e.g. `factory->Finish(compatible_schema)`.
  ARROW_ASSIGN_OR_RAISE(auto dataset, factory->Finish(kConf.finish_options));

  return dataset;
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDatasetFromPath(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::FileFormat> format,
    std::string path) {
  ARROW_ASSIGN_OR_RAISE(auto info, fs->GetFileInfo(path));
  if (info.IsDirectory()) {
    return GetDatasetFromDirectory(fs, format, path);
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

arrow::Result<std::shared_ptr<skyhook::SkyhookFileFormat>> InstantiateSkyhookFormat() {
  // Path to the Ceph configuration file. It contains cluster wide configuration
  // and most importantly the connection information to the Ceph cluster.
  std::string ceph_config_path = "/etc/ceph/ceph.conf";

  // Ceph data pool containing the objects to be scanned.
  // The default data pool is "cephfs_data".
  std::string ceph_data_pool = "cephfs_data";

  // The user accessing the Ceph cluster. The default username is "client.admin".
  std::string ceph_user_name = "client.admin";

  // Cluster name is an unique identifier for a Ceph cluster. It is especially
  // required when you run multiple Ceph clusters on a multi-site architecture
  // where the cluster name identifies the Ceph cluster for the
  // current session. The default cluster name is "ceph".
  std::string ceph_cluster_name = "ceph";

  // CLS name is used to identify the shared library that needs to be loaded
  // in the Ceph OSDs when invoking an object class method. For Skyhook, the
  // library name is "libcls_skyhook.so", and the object class name is "skyhook".
  std::string ceph_cls_name = "skyhook";
  std::shared_ptr<skyhook::RadosConnCtx> rados_ctx =
      std::make_shared<skyhook::RadosConnCtx>(ceph_config_path, ceph_data_pool,
                                              ceph_user_name, ceph_cluster_name,
                                              ceph_cls_name);
  ARROW_ASSIGN_OR_RAISE(auto format,
                        skyhook::SkyhookFileFormat::Make(rados_ctx, "parquet"));
  return format;
}

arrow::Status Main(std::string dataset_root) {
  ARROW_ASSIGN_OR_RAISE(auto format, InstantiateSkyhookFormat());
  std::string path;

  ARROW_ASSIGN_OR_RAISE(auto fs, fs::FileSystemFromUri(dataset_root, &path));
  ARROW_ASSIGN_OR_RAISE(auto dataset, GetDatasetFromPath(fs, format, path));
  ARROW_ASSIGN_OR_RAISE(
      auto scanner, GetScannerFromDataset(dataset, kConf.projected_columns, kConf.filter,
                                          kConf.use_threads));
  ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());
  std::cout << "Table size: " << table->num_rows() << "\n";
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    // Fake success for CI purposes.
    return EXIT_SUCCESS;
  }
  auto status = Main(argv[1]);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
