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
#include "skyhook/client/file_skyhook.h"

#include "arrow/compute/expression.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_base.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "gtest/gtest.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

std::shared_ptr<skyhook::SkyhookFileFormat> GetSkyhookFormat() {
  // The constants below should match the parameters with
  // which the Ceph cluster is configured in integration_skyhook.sh.
  // Currently, all the default values have been used.
  std::string ceph_config_path = "/etc/ceph/ceph.conf";
  std::string ceph_data_pool = "cephfs_data";
  std::string ceph_user_name = "client.admin";
  std::string ceph_cluster_name = "ceph";
  std::string ceph_cls_name = "skyhook";
  std::shared_ptr<skyhook::RadosConnCtx> rados_ctx =
      std::make_shared<skyhook::RadosConnCtx>(ceph_config_path, ceph_data_pool,
                                              ceph_user_name, ceph_cluster_name,
                                              ceph_cls_name);
  EXPECT_OK_AND_ASSIGN(auto format,
                       skyhook::SkyhookFileFormat::Make(rados_ctx, "parquet"));
  return format;
}

std::shared_ptr<arrow::dataset::ParquetFileFormat> GetParquetFormat() {
  return std::make_shared<arrow::dataset::ParquetFileFormat>();
}

std::shared_ptr<arrow::dataset::Dataset> GetDatasetFromDirectory(
    std::shared_ptr<arrow::fs::FileSystem> fs,
    std::shared_ptr<arrow::dataset::FileFormat> format, std::string dir) {
  arrow::fs::FileSelector s;
  s.base_dir = std::move(dir);
  s.recursive = true;

  arrow::dataset::FileSystemFactoryOptions options;
  options.partitioning = std::make_shared<arrow::dataset::HivePartitioning>(
      arrow::schema({arrow::field("payment_type", arrow::int32()),
                     arrow::field("VendorID", arrow::int32())}));
  EXPECT_OK_AND_ASSIGN(auto factory, arrow::dataset::FileSystemDatasetFactory::Make(
                                         std::move(fs), s, std::move(format), options));

  arrow::dataset::InspectOptions inspect_options;
  arrow::dataset::FinishOptions finish_options;
  EXPECT_OK_AND_ASSIGN(auto schema, factory->Inspect(inspect_options));
  EXPECT_OK_AND_ASSIGN(auto dataset, factory->Finish(finish_options));
  return dataset;
}

std::shared_ptr<arrow::fs::FileSystem> GetFileSystemFromUri(const std::string& uri,
                                                            std::string* path) {
  EXPECT_OK_AND_ASSIGN(auto fs, arrow::fs::FileSystemFromUri(uri, path));
  return fs;
}

std::shared_ptr<arrow::dataset::Dataset> GetDatasetFromPath(
    std::shared_ptr<arrow::fs::FileSystem> fs,
    std::shared_ptr<arrow::dataset::FileFormat> format, std::string path) {
  EXPECT_OK_AND_ASSIGN(auto info, fs->GetFileInfo(path));
  return GetDatasetFromDirectory(std::move(fs), std::move(format), std::move(path));
}

std::shared_ptr<arrow::dataset::Scanner> GetScannerFromDataset(
    const std::shared_ptr<arrow::dataset::Dataset>& dataset,
    std::vector<std::string> columns, arrow::compute::Expression filter,
    bool use_threads) {
  EXPECT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());

  if (!columns.empty()) {
    ARROW_EXPECT_OK(scanner_builder->Project(std::move(columns)));
  }

  ARROW_EXPECT_OK(scanner_builder->Filter(std::move(filter)));
  ARROW_EXPECT_OK(scanner_builder->UseThreads(use_threads));
  EXPECT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());
  return scanner;
}

TEST(TestSkyhookCLS, SelectEntireDataset) {
  std::string path;
  auto fs = GetFileSystemFromUri("file:///mnt/cephfs/nyc", &path);
  std::vector<std::string> columns;

  auto parquet_format = GetParquetFormat();
  auto dataset = GetDatasetFromPath(fs, parquet_format, path);
  auto scanner =
      GetScannerFromDataset(dataset, columns, arrow::compute::literal(true), true);
  EXPECT_OK_AND_ASSIGN(auto table_parquet, scanner->ToTable());

  auto skyhook_format = GetSkyhookFormat();
  dataset = GetDatasetFromPath(fs, skyhook_format, path);
  scanner = GetScannerFromDataset(dataset, columns, arrow::compute::literal(true), true);
  EXPECT_OK_AND_ASSIGN(auto table_skyhook_parquet, scanner->ToTable());

  ASSERT_EQ(table_parquet->Equals(*table_skyhook_parquet), 1);
  ASSERT_EQ(table_parquet->num_rows(), table_skyhook_parquet->num_rows());
}

TEST(TestSkyhookCLS, SelectFewRows) {
  std::string path;
  auto fs = GetFileSystemFromUri("file:///mnt/cephfs/nyc", &path);
  std::vector<std::string> columns;
  auto filter = arrow::compute::greater(arrow::compute::field_ref("payment_type"),
                                        arrow::compute::literal(2));
  auto parquet_format = GetParquetFormat();
  auto dataset = GetDatasetFromPath(fs, parquet_format, path);
  auto scanner = GetScannerFromDataset(dataset, columns, filter, true);
  EXPECT_OK_AND_ASSIGN(auto table_parquet, scanner->ToTable());

  auto skyhook_format = GetSkyhookFormat();
  dataset = GetDatasetFromPath(fs, skyhook_format, path);
  scanner = GetScannerFromDataset(dataset, columns, filter, true);
  EXPECT_OK_AND_ASSIGN(auto table_skyhook_parquet, scanner->ToTable());

  ASSERT_EQ(table_parquet->Equals(*table_skyhook_parquet), 1);
  ASSERT_EQ(table_parquet->num_rows(), table_skyhook_parquet->num_rows());
}

TEST(TestSkyhookCLS, SelectFewColumns) {
  std::string path;
  auto fs = GetFileSystemFromUri("file:///mnt/cephfs/nyc", &path);
  std::vector<std::string> columns = {"fare_amount", "total_amount"};

  auto parquet_format = GetParquetFormat();
  auto dataset = GetDatasetFromPath(fs, parquet_format, path);
  auto scanner =
      GetScannerFromDataset(dataset, columns, arrow::compute::literal(true), true);
  EXPECT_OK_AND_ASSIGN(auto table_parquet, scanner->ToTable());

  auto skyhook_format = GetSkyhookFormat();
  dataset = GetDatasetFromPath(fs, skyhook_format, path);
  scanner = GetScannerFromDataset(dataset, columns, arrow::compute::literal(true), true);
  EXPECT_OK_AND_ASSIGN(auto table_skyhook_parquet, scanner->ToTable());

  ASSERT_EQ(table_parquet->Equals(*table_skyhook_parquet), 1);
  ASSERT_EQ(table_parquet->num_rows(), table_skyhook_parquet->num_rows());
}

TEST(TestSkyhookCLS, SelectRowsAndColumnsOnPartitionKey) {
  std::string path;
  auto fs = GetFileSystemFromUri("file:///mnt/cephfs/nyc", &path);
  std::vector<std::string> columns = {"fare_amount", "VendorID", "payment_type"};
  auto filter = arrow::compute::greater(arrow::compute::field_ref("payment_type"),
                                        arrow::compute::literal(2));

  auto parquet_format = GetParquetFormat();
  auto dataset = GetDatasetFromPath(fs, parquet_format, path);
  auto scanner = GetScannerFromDataset(dataset, columns, filter, true);
  EXPECT_OK_AND_ASSIGN(auto table_parquet, scanner->ToTable());

  auto skyhook_format = GetSkyhookFormat();
  dataset = GetDatasetFromPath(fs, skyhook_format, path);
  scanner = GetScannerFromDataset(dataset, columns, filter, true);
  EXPECT_OK_AND_ASSIGN(auto table_skyhook_parquet, scanner->ToTable());

  ASSERT_EQ(table_parquet->Equals(*table_skyhook_parquet), 1);
  ASSERT_EQ(table_parquet->num_rows(), table_skyhook_parquet->num_rows());
}

TEST(TestSkyhookCLS, SelectRowsAndColumnsOnlyOnPartitionKey) {
  std::string path;
  auto fs = GetFileSystemFromUri("file:///mnt/cephfs/nyc", &path);
  std::vector<std::string> columns = {"total_amount", "VendorID", "payment_type"};
  auto filter = arrow::compute::and_(
      arrow::compute::greater(arrow::compute::field_ref("payment_type"),
                              arrow::compute::literal(2)),
      arrow::compute::greater(arrow::compute::field_ref("VendorID"),
                              arrow::compute::literal(1)));

  auto parquet_format = GetParquetFormat();
  auto dataset = GetDatasetFromPath(fs, parquet_format, path);
  auto scanner = GetScannerFromDataset(dataset, columns, filter, true);
  EXPECT_OK_AND_ASSIGN(auto table_parquet, scanner->ToTable());

  auto skyhook_format = GetSkyhookFormat();
  dataset = GetDatasetFromPath(fs, skyhook_format, path);
  scanner = GetScannerFromDataset(dataset, columns, filter, true);
  EXPECT_OK_AND_ASSIGN(auto table_skyhook_parquet, scanner->ToTable());

  ASSERT_EQ(table_parquet->Equals(*table_skyhook_parquet), 1);
  ASSERT_EQ(table_parquet->num_rows(), table_skyhook_parquet->num_rows());
}
