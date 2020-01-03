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
std::shared_ptr<ds::DataSourceDiscovery> dataset___FSDSDiscovery__Make2(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::PartitionScheme>& partition_scheme) {
  // TODO(npr): add format as an argument, don't hard-code Parquet
  auto format = std::make_shared<ds::ParquetFileFormat>();

  // TODO(fsaintjacques): Make options configurable
  auto options = ds::FileSystemDiscoveryOptions{};
  if (partition_scheme != nullptr) {
    options.partition_scheme = partition_scheme;
  }

  return VALUE_OR_STOP(
      ds::FileSystemDataSourceDiscovery::Make(fs, *selector, format, options));
}

// [[arrow::export]]
std::shared_ptr<ds::DataSourceDiscovery> dataset___FSDSDiscovery__Make1(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector) {
  return dataset___FSDSDiscovery__Make2(fs, selector, nullptr);
}

// [[arrow::export]]
std::shared_ptr<ds::DataSource> dataset___DSDiscovery__Finish1(
    const std::shared_ptr<ds::DataSourceDiscovery>& discovery) {
  return VALUE_OR_STOP(discovery->Finish());
}

// [[arrow::export]]
std::shared_ptr<ds::DataSource> dataset___DSDiscovery__Finish2(
    const std::shared_ptr<ds::DataSourceDiscovery>& discovery,
    const std::shared_ptr<arrow::Schema>& schema) {
  return VALUE_OR_STOP(discovery->Finish(schema));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___DSDiscovery__Inspect(
    const std::shared_ptr<ds::DataSourceDiscovery>& discovery) {
  return VALUE_OR_STOP(discovery->Inspect());
}

// [[arrow::export]]
std::shared_ptr<ds::PartitionScheme> dataset___SchemaPartitionScheme(
    const std::shared_ptr<arrow::Schema>& schm) {
  return std::make_shared<ds::SchemaPartitionScheme>(schm);
}

// [[arrow::export]]
std::shared_ptr<ds::PartitionScheme> dataset___HivePartitionScheme(
    const std::shared_ptr<arrow::Schema>& schm) {
  return std::make_shared<ds::HivePartitionScheme>(schm);
}

// [[arrow::export]]
std::shared_ptr<ds::Dataset> dataset___Dataset__create(
    const ds::DataSourceVector& sources, const std::shared_ptr<arrow::Schema>& schm) {
  return VALUE_OR_STOP(ds::Dataset::Make(sources, schm));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___Dataset__schema(
    const std::shared_ptr<ds::Dataset>& ds) {
  return ds->schema();
}

// [[arrow::export]]
std::shared_ptr<ds::ScannerBuilder> dataset___Dataset__NewScan(
    const std::shared_ptr<ds::Dataset>& ds) {
  return VALUE_OR_STOP(ds->NewScan());
}

// [[arrow::export]]
void dataset___ScannerBuilder__Project(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                       const std::vector<std::string>& cols) {
  STOP_IF_NOT_OK(sb->Project(cols));
}

// [[arrow::export]]
void dataset___ScannerBuilder__Filter(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                      const std::shared_ptr<ds::Expression>& expr) {
  // Expressions converted from R's expressions are typed with R's native type,
  // i.e. double, int64_t and bool.
  auto cast_filter = VALUE_OR_STOP(InsertImplicitCasts(*expr, *sb->schema()));
  STOP_IF_NOT_OK(sb->Filter(cast_filter));
}

// [[arrow::export]]
void dataset___ScannerBuilder__UseThreads(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                          bool threads) {
  STOP_IF_NOT_OK(sb->UseThreads(threads));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___ScannerBuilder__schema(
    const std::shared_ptr<ds::ScannerBuilder>& sb) {
  return sb->schema();
}

// [[arrow::export]]
std::shared_ptr<ds::Scanner> dataset___ScannerBuilder__Finish(
    const std::shared_ptr<ds::ScannerBuilder>& sb) {
  return VALUE_OR_STOP(sb->Finish());
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> dataset___Scanner__ToTable(
    const std::shared_ptr<ds::Scanner>& scanner) {
  return VALUE_OR_STOP(scanner->ToTable());
}

#endif
