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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)

// // [[arrow::export]]
// std::shared_ptr<arrow::dataset::ScanTaskIterator> dataset___DataFragment__Scan(const
// std::shared_ptr<arrow::dataset::DataFragment>& fragment, const
// std::shared_ptr<arrow::dataset::ScanContext>& scan_context) {
//   std::shared_ptr<arrow::dataset::ScanTaskIterator> iterator;
//   fragment->Scan(scan_context, iterator.get());
//   return iterator;
// }

// [[arrow::export]]
bool dataset___DataFragment__splittable(
    const std::shared_ptr<arrow::dataset::DataFragment>& fragment) {
  return fragment->splittable();
}

// [[arrow::export]]
std::shared_ptr<arrow::dataset::ScanOptions> dataset___DataFragment__scan_options(
    const std::shared_ptr<arrow::dataset::DataFragment>& fragment) {
  return fragment->scan_options();
}

// [[arrow::export]]
std::shared_ptr<arrow::dataset::SimpleDataFragment> dataset___SimpleDataFragment__create(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
  return std::make_shared<arrow::dataset::SimpleDataFragment>(batches);
}

// [[arrow::export]]
std::shared_ptr<arrow::dataset::DataSourceDiscovery> dataset___FSDSDiscovery__Make(
    const std::shared_ptr<arrow::fs::FileSystem>& fs,
    const std::shared_ptr<arrow::fs::Selector>& selector) {
  std::shared_ptr<arrow::dataset::DataSourceDiscovery> discovery;
  // TODO: add format as an argument, don't hard-code Parquet
  std::shared_ptr<arrow::dataset::ParquetFileFormat> format;

  STOP_IF_NOT_OK(arrow::dataset::FileSystemDataSourceDiscovery::Make(fs.get(), *selector,
                                                                     format, &discovery));
  return discovery;
}

// [[arrow::export]]
std::shared_ptr<arrow::dataset::DataSource> dataset___DSDiscovery__Finish(
    const std::shared_ptr<arrow::dataset::DataSourceDiscovery>& discovery) {
  std::shared_ptr<arrow::dataset::DataSource> out;

  STOP_IF_NOT_OK(discovery->Finish(&out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___DSDiscovery__Inspect(
    const std::shared_ptr<arrow::dataset::DataSourceDiscovery>& discovery) {
  std::shared_ptr<arrow::Schema> out;

  STOP_IF_NOT_OK(discovery->Inspect(&out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::dataset::Dataset> dataset___Dataset__create(
    const std::vector<std::shared_ptr<arrow::dataset::DataSource>>& sources,
    const std::shared_ptr<arrow::Schema>& schm) {
  return std::make_shared<arrow::dataset::Dataset>(sources, schm);
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___Dataset__schema(
    const std::unique_ptr<arrow::dataset::Dataset>& ds) {
  return ds->schema();
}

// [[arrow::export]]
std::unique_ptr<arrow::dataset::ScannerBuilder> dataset___Dataset__NewScan(
    const std::shared_ptr<arrow::dataset::Dataset>& ds) {
  std::unique_ptr<arrow::dataset::ScannerBuilder> out;
  STOP_IF_NOT_OK(ds->NewScan(&out));
  return out;
}

// [[arrow::export]]
void dataset___ScannerBuilder__Project(
    const std::unique_ptr<arrow::dataset::ScannerBuilder>& sb,
    const std::vector<std::string>& cols) {
  STOP_IF_NOT_OK(sb->Project(cols));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___ScannerBuilder__schema(
    const std::unique_ptr<arrow::dataset::ScannerBuilder>& sb) {
  return sb->schema();
}

// [[arrow::export]]
std::unique_ptr<arrow::dataset::Scanner> dataset___ScannerBuilder__Finish(
    const std::unique_ptr<arrow::dataset::ScannerBuilder>& sb) {
  std::unique_ptr<arrow::dataset::Scanner> out;
  STOP_IF_NOT_OK(sb->Finish(&out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> dataset___Scanner__ToTable(
    const std::unique_ptr<arrow::dataset::Scanner>& scn) {
  std::shared_ptr<arrow::Table> out;
  STOP_IF_NOT_OK(scn->ToTable(&out));
  return out;
}

#endif
