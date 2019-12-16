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

// [[arrow::export]]
ds::DataSourceDiscoveryPtr dataset___FSDSDiscovery__Make(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector) {
  // TODO(npr): add format as an argument, don't hard-code Parquet
  auto format = std::make_shared<ds::ParquetFileFormat>();
  // TODO(fsaintjacques): Make options configurable
  auto options = ds::FileSystemDiscoveryOptions{};

  return VALUE_OR_STOP(
      ds::FileSystemDataSourceDiscovery::Make(fs, *selector, format, options));
}

// [[arrow::export]]
ds::DataSourcePtr dataset___DSDiscovery__Finish(
    const ds::DataSourceDiscoveryPtr& discovery) {
  return VALUE_OR_STOP(discovery->Finish());
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___DSDiscovery__Inspect(
    const ds::DataSourceDiscoveryPtr& discovery) {
  return VALUE_OR_STOP(discovery->Inspect());
}

// [[arrow::export]]
void dataset___DSDiscovery__SetPartitionScheme(
    const ds::DataSourceDiscoveryPtr& discovery, const ds::PartitionSchemePtr& part) {
  STOP_IF_NOT_OK(discovery->SetPartitionScheme(part));
}

// [[arrow::export]]
ds::PartitionSchemePtr dataset___SchemaPartitionScheme(
    const std::shared_ptr<arrow::Schema>& schm) {
  return std::make_shared<ds::SchemaPartitionScheme>(schm);
}

// [[arrow::export]]
ds::PartitionSchemePtr dataset___HivePartitionScheme(
    const std::shared_ptr<arrow::Schema>& schm) {
  return std::make_shared<ds::HivePartitionScheme>(schm);
}

// [[arrow::export]]
ds::DatasetPtr dataset___Dataset__create(const ds::DataSourceVector& sources,
                                         const std::shared_ptr<arrow::Schema>& schm) {
  return VALUE_OR_STOP(ds::Dataset::Make(sources, schm));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___Dataset__schema(const ds::DatasetPtr& ds) {
  return ds->schema();
}

// [[arrow::export]]
ds::ScannerBuilderPtr dataset___Dataset__NewScan(const ds::DatasetPtr& ds) {
  return VALUE_OR_STOP(ds->NewScan());
}

// [[arrow::export]]
void dataset___ScannerBuilder__Project(const ds::ScannerBuilderPtr& sb,
                                       const std::vector<std::string>& cols) {
  STOP_IF_NOT_OK(sb->Project(cols));
}

// [[arrow::export]]
void dataset___ScannerBuilder__Filter(const ds::ScannerBuilderPtr& sb,
                                      const ds::ExpressionPtr& expr) {
  // Expressions converted from R's expressions are typed with R's native type,
  // i.e. double, int64_t and bool.
  auto cast_filter = VALUE_OR_STOP(InsertImplicitCasts(*expr, *sb->schema()));
  STOP_IF_NOT_OK(sb->Filter(cast_filter));
}

// [[arrow::export]]
void dataset___ScannerBuilder__UseThreads(const ds::ScannerBuilderPtr& sb, bool threads) {
  STOP_IF_NOT_OK(sb->UseThreads(threads));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___ScannerBuilder__schema(
    const ds::ScannerBuilderPtr& sb) {
  return sb->schema();
}

// [[arrow::export]]
ds::ScannerPtr dataset___ScannerBuilder__Finish(const ds::ScannerBuilderPtr& sb) {
  return VALUE_OR_STOP(sb->Finish());
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> dataset___Scanner__ToTable(const ds::ScannerPtr& scanner) {
  return VALUE_OR_STOP(scanner->ToTable());
}

#endif
