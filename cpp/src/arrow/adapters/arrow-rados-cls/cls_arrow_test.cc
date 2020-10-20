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
#define _FILE_OFFSET_BITS 64

#include <iostream>
#include <random>

#include <rados/objclass.h>
#include <rados/librados.hpp>

#include "arrow/api.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/expression.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_rados_parquet.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/util/iterator.h"

#include "gtest/gtest.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

std::shared_ptr<arrow::dataset::RadosParquetFileFormat> GetFormat() {
  std::string ceph_config_path = "/etc/ceph/ceph.conf";
  std::string data_pool = "cephfs_data";
  std::string user_name = "client.admin";
  std::string cluster_name = "ceph";
  return std::make_shared<arrow::dataset::RadosParquetFileFormat>(
      ceph_config_path, data_pool, user_name, cluster_name);
}

std::shared_ptr<arrow::dataset::Dataset> GetDatasetFromDirectory(
    std::shared_ptr<arrow::fs::FileSystem> fs,
    std::shared_ptr<arrow::dataset::RadosParquetFileFormat> format, std::string dir) {
  arrow::fs::FileSelector s;
  s.base_dir = dir;
  s.recursive = true;

  arrow::dataset::FileSystemFactoryOptions options;
  options.partitioning = std::make_shared<arrow::dataset::HivePartitioning>(
      arrow::schema({arrow::field("payment_type", arrow::int32()),
                     arrow::field("VendorID", arrow::int32())}));
  auto factory =
      arrow::dataset::FileSystemDatasetFactory::Make(fs, s, format, options).ValueOrDie();

  arrow::dataset::InspectOptions inspect_options;
  arrow::dataset::FinishOptions finish_options;
  auto schema = factory->Inspect(inspect_options).ValueOrDie();
  auto child = factory->Finish(finish_options).ValueOrDie();

  arrow::dataset::DatasetVector children{1, child};
  auto dataset =
      arrow::dataset::UnionDataset::Make(std::move(schema), std::move(children));

  return dataset.ValueOrDie();
}

std::shared_ptr<arrow::fs::FileSystem> GetFileSystemFromUri(const std::string& uri,
                                                            std::string* path) {
  return arrow::fs::FileSystemFromUri(uri, path).ValueOrDie();
}

std::shared_ptr<arrow::dataset::Dataset> GetDatasetFromPath(
    std::shared_ptr<arrow::fs::FileSystem> fs,
    std::shared_ptr<arrow::dataset::RadosParquetFileFormat> format, std::string path) {
  auto info = fs->GetFileInfo(path).ValueOrDie();
  return GetDatasetFromDirectory(fs, format, path);
}

std::shared_ptr<arrow::dataset::Scanner> GetScannerFromDataset(
    std::shared_ptr<arrow::dataset::Dataset> dataset, std::vector<std::string> columns,
    arrow::dataset::Expression filter, bool use_threads) {
  auto scanner_builder = dataset->NewScan().ValueOrDie();

  if (!columns.empty()) {
    scanner_builder->Project(columns);
  }

  scanner_builder->Filter(filter);
  scanner_builder->UseThreads(use_threads);
  return scanner_builder->Finish().ValueOrDie();
}

TEST(TestClsSDK, SimpleQuery) {
  auto format = GetFormat();

  std::string path;
  auto fs = GetFileSystemFromUri("file:///mnt/cephfs/nyc", &path);
  auto dataset = GetDatasetFromPath(fs, format, path);

  std::vector<std::string> columns = {"fare_amount", "total_amount"};
  auto scanner =
      GetScannerFromDataset(dataset, columns, arrow::dataset::literal(true), false);

  auto table = scanner->ToTable().ValueOrDie();
  std::cout << "Table size: " << table->num_rows() << "\n";
  std::cout << "Table: " << table->ToString() << "\n";
}

TEST(TestClsSDK, QueryOnPartitionKey) {
  auto format = GetFormat();

  std::string path;
  auto fs = GetFileSystemFromUri("file:///mnt/cephfs/nyc", &path);
  auto dataset = GetDatasetFromPath(fs, format, path);

  std::vector<std::string> columns = {"fare_amount", "VendorID", "payment_type"};
  auto filter = arrow::dataset::greater(arrow::dataset::field_ref("payment_type"),
                                        arrow::dataset::literal(2));

  auto scanner = GetScannerFromDataset(dataset, columns, filter, false);

  auto table = scanner->ToTable().ValueOrDie();
  std::cout << "Table size: " << table->num_rows() << "\n";
  std::cout << "Table: " << table->ToString() << "\n";
}

TEST(TestClsSDK, QueryOnlyOnPartitionKey) {
  auto format = GetFormat();
  std::string path;
  auto fs = GetFileSystemFromUri("file:///mnt/cephfs/nyc", &path);
  auto dataset = GetDatasetFromPath(fs, format, path);

  std::vector<std::string> columns = {"total_amount", "VendorID", "payment_type"};
  auto filter = arrow::dataset::and_(
      arrow::dataset::greater(arrow::dataset::field_ref("payment_type"),
                              arrow::dataset::literal(2)),
      arrow::dataset::greater(arrow::dataset::field_ref("VendorID"),
                              arrow::dataset::literal(1)));

  auto scanner = GetScannerFromDataset(dataset, columns, filter, true);

  auto table = scanner->ToTable().ValueOrDie();
  std::cout << "Table size: " << table->num_rows() << "\n";
  std::cout << "Table: " << table->ToString() << "\n";
}
