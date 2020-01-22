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
std::shared_ptr<ds::SourceFactory> dataset___FSSFactory__Make2(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format,
    const std::shared_ptr<ds::Partitioning>& partitioning) {
  // TODO(fsaintjacques): Make options configurable
  auto options = ds::FileSystemFactoryOptions{};
  if (partitioning != nullptr) {
    options.partitioning = partitioning;
  }

  return VALUE_OR_STOP(ds::FileSystemSourceFactory::Make(fs, *selector, format, options));
}

// [[arrow::export]]
std::shared_ptr<ds::SourceFactory> dataset___FSSFactory__Make1(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format) {
  return dataset___FSSFactory__Make2(fs, selector, format, nullptr);
}

// [[arrow::export]]
std::shared_ptr<ds::SourceFactory> dataset___FSSFactory__Make3(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format,
    const std::shared_ptr<ds::PartitioningFactory>& factory) {
  // TODO(fsaintjacques): Make options configurable
  auto options = ds::FileSystemFactoryOptions{};
  if (factory != nullptr) {
    options.partitioning = factory;
  }

  return VALUE_OR_STOP(ds::FileSystemSourceFactory::Make(fs, *selector, format, options));
}

// [[arrow::export]]
std::string dataset___FileFormat__type_name(
    const std::shared_ptr<ds::FileFormat>& format) {
  return format->type_name();
}

// [[arrow::export]]
std::shared_ptr<ds::ParquetFileFormat> dataset___ParquetFileFormat__Make() {
  return std::make_shared<ds::ParquetFileFormat>();
}

// [[arrow::export]]
std::shared_ptr<ds::IpcFileFormat> dataset___IpcFileFormat__Make() {
  return std::make_shared<ds::IpcFileFormat>();
}

// [[arrow::export]]
std::shared_ptr<ds::Source> dataset___SFactory__Finish1(
    const std::shared_ptr<ds::SourceFactory>& factory) {
  return VALUE_OR_STOP(factory->Finish());
}

// [[arrow::export]]
std::shared_ptr<ds::Source> dataset___SFactory__Finish2(
    const std::shared_ptr<ds::SourceFactory>& factory,
    const std::shared_ptr<arrow::Schema>& schema) {
  return VALUE_OR_STOP(factory->Finish(schema));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___SFactory__Inspect(
    const std::shared_ptr<ds::SourceFactory>& factory) {
  return VALUE_OR_STOP(factory->Inspect());
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___Source__schema(
    const std::shared_ptr<ds::Source>& source) {
  return source->schema();
}

// [[arrow::export]]
std::string dataset___Source__type_name(const std::shared_ptr<ds::Source>& source) {
  return source->type_name();
}

// [[arrow::export]]
std::shared_ptr<ds::FileFormat> dataset___FSSource__format(
    const std::shared_ptr<ds::FileSystemSource>& source) {
  return source->format();
}

// [[arrow::export]]
std::vector<std::string> dataset___FSSource__files(
    const std::shared_ptr<ds::FileSystemSource>& source) {
  return source->files();
}

// DatasetFactory

// [[arrow::export]]
std::shared_ptr<ds::DatasetFactory> dataset___DFactory__Make(
    const std::vector<std::shared_ptr<ds::SourceFactory>>& sources) {
  return VALUE_OR_STOP(ds::DatasetFactory::Make(sources));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___DFactory__Inspect(
    const std::shared_ptr<ds::DatasetFactory>& factory) {
  return VALUE_OR_STOP(factory->Inspect());
}

// [[arrow::export]]
std::shared_ptr<ds::Dataset> dataset___DFactory__Finish1(
    const std::shared_ptr<ds::DatasetFactory>& factory) {
  return VALUE_OR_STOP(factory->Finish());
}

// [[arrow::export]]
std::shared_ptr<ds::Dataset> dataset___DFactory__Finish2(
    const std::shared_ptr<ds::DatasetFactory>& factory,
    const std::shared_ptr<arrow::Schema>& schema) {
  return VALUE_OR_STOP(factory->Finish(schema));
}

// [[arrow::export]]
std::shared_ptr<ds::Partitioning> dataset___DirectoryPartitioning(
    const std::shared_ptr<arrow::Schema>& schm) {
  return std::make_shared<ds::DirectoryPartitioning>(schm);
}

// [[arrow::export]]
std::shared_ptr<ds::PartitioningFactory> dataset___DirectoryPartitioning__MakeFactory(
    const std::vector<std::string>& field_names) {
  return ds::DirectoryPartitioning::MakeFactory(field_names);
}

// [[arrow::export]]
std::shared_ptr<ds::Partitioning> dataset___HivePartitioning(
    const std::shared_ptr<arrow::Schema>& schm) {
  return std::make_shared<ds::HivePartitioning>(schm);
}

// [[arrow::export]]
std::shared_ptr<ds::PartitioningFactory> dataset___HivePartitioning__MakeFactory() {
  return ds::HivePartitioning::MakeFactory();
}

// [[arrow::export]]
std::shared_ptr<ds::Dataset> dataset___Dataset__create(
    const ds::SourceVector& sources, const std::shared_ptr<arrow::Schema>& schm) {
  return VALUE_OR_STOP(ds::Dataset::Make(sources, schm));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___Dataset__schema(
    const std::shared_ptr<ds::Dataset>& ds) {
  return ds->schema();
}

// [[arrow::export]]
std::vector<std::shared_ptr<ds::Source>> dataset___Dataset__sources(
    const std::shared_ptr<ds::Dataset>& ds) {
  return ds->sources();
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
