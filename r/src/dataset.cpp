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

#include <arrow/dataset/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/ipc/writer.h>
#include <arrow/table.h>
#include <arrow/util/iterator.h>

namespace ds = ::arrow::dataset;
namespace fs = ::arrow::fs;

namespace cpp11 {

R6 r6_Dataset(const std::shared_ptr<arrow::dataset::Dataset>& dataset) {
  auto type_name = dataset->type_name();

  if (type_name == "union") {
    return cpp11::r6(dataset, "UnionDataset");
  } else if (type_name == "filesystem") {
    return cpp11::r6(dataset, "FileSystemDataset");
  } else {
    return cpp11::r6(dataset, "Dataset");
  }
}

R6 r6_FileSystem(const std::shared_ptr<fs::FileSystem>& file_system) {
  auto type_name = file_system->type_name();

  if (type_name == "local") {
    return cpp11::r6(file_system, "LocalFileSystem");
  } else if (type_name == "s3") {
    return cpp11::r6(file_system, "S3FileSystem");
  } else if (type_name == "subtree") {
    return cpp11::r6(file_system, "SubTreeFileSystem");
  } else {
    return cpp11::r6(file_system, "FileSystem");
  }
}

R6 r6_FileFormat(const std::shared_ptr<ds::FileFormat>& file_format) {
  auto type_name = file_format->type_name();
  if (type_name == "parquet") {
    return cpp11::r6(file_format, "ParquetFileFormat");
  } else if (type_name == "ipc") {
    return cpp11::r6(file_format, "IpcFileFormat");
  } else if (type_name == "csv") {
    return cpp11::r6(file_format, "CsvFileFormat");
  } else {
    return cpp11::r6(file_format, "FileFormat");
  }
}

}  // namespace cpp11

// Dataset, UnionDataset, FileSystemDataset

// [[arrow::export]]
R6 dataset___Dataset__NewScan(const std::shared_ptr<ds::Dataset>& ds) {

  auto context = std::make_shared<ds::ScanContext>();
  context->pool = gc_memory_pool();
  auto out = ValueOrStop(ds->NewScan(std::move(context)));
  return cpp11::r6(std::shared_ptr<ds::ScannerBuilder>(std::move(out)), "ScannerBuilder");
}

// [[arrow::export]]
R6 dataset___Dataset__schema(const std::shared_ptr<ds::Dataset>& dataset) {
  return cpp11::r6(dataset->schema(), "Schema");
}

// [[arrow::export]]
std::string dataset___Dataset__type_name(const std::shared_ptr<ds::Dataset>& dataset) {
  return dataset->type_name();
}

// [[arrow::export]]
R6 dataset___Dataset__ReplaceSchema(const std::shared_ptr<ds::Dataset>& dataset,
                                    const std::shared_ptr<arrow::Schema>& schm) {
  return cpp11::r6_Dataset(ValueOrStop(dataset->ReplaceSchema(schm)));
}

// [[arrow::export]]
R6 dataset___UnionDataset__create(const ds::DatasetVector& datasets,
                                  const std::shared_ptr<arrow::Schema>& schm) {
  return cpp11::r6(ValueOrStop(ds::UnionDataset::Make(schm, datasets)), "UnionDataset");
}

// [[arrow::export]]
R6 dataset___InMemoryDataset__create(const std::shared_ptr<arrow::Table>& table) {
  return cpp11::r6(std::make_shared<ds::InMemoryDataset>(table), "InMemoryDataset");
}

// [[arrow::export]]
ds::DatasetVector dataset___UnionDataset__children(
    const std::shared_ptr<ds::UnionDataset>& ds) {
  return ds->children();
}

// [[arrow::export]]
R6 dataset___FileSystemDataset__format(
    const std::shared_ptr<ds::FileSystemDataset>& dataset) {
  return cpp11::r6_FileFormat(dataset->format());
}

// [[arrow::export]]
R6 dataset___FileSystemDataset__filesystem(
    const std::shared_ptr<ds::FileSystemDataset>& dataset) {
  return cpp11::r6_FileSystem(dataset->filesystem());
}

// [[arrow::export]]
std::vector<std::string> dataset___FileSystemDataset__files(
    const std::shared_ptr<ds::FileSystemDataset>& dataset) {
  return dataset->files();
}

// DatasetFactory, UnionDatasetFactory, FileSystemDatasetFactory

// [[arrow::export]]
R6 dataset___DatasetFactory__Finish1(const std::shared_ptr<ds::DatasetFactory>& factory,
                                     bool unify_schemas) {
  ds::FinishOptions opts;
  if (unify_schemas) {
    opts.inspect_options.fragments = ds::InspectOptions::kInspectAllFragments;
  }
  return cpp11::r6_Dataset(ValueOrStop(factory->Finish(opts)));
}

// [[arrow::export]]
R6 dataset___DatasetFactory__Finish2(const std::shared_ptr<ds::DatasetFactory>& factory,
                                     const std::shared_ptr<arrow::Schema>& schema) {
  return cpp11::r6_Dataset(ValueOrStop(factory->Finish(schema)));
}

// [[arrow::export]]
R6 dataset___DatasetFactory__Inspect(const std::shared_ptr<ds::DatasetFactory>& factory,
                                     bool unify_schemas) {
  ds::InspectOptions opts;
  if (unify_schemas) {
    opts.fragments = ds::InspectOptions::kInspectAllFragments;
  }
  return cpp11::r6(ValueOrStop(factory->Inspect(opts)), "Schema");
}

// [[arrow::export]]
R6 dataset___UnionDatasetFactory__Make(
    const std::vector<std::shared_ptr<ds::DatasetFactory>>& children) {
  return cpp11::r6(ValueOrStop(ds::UnionDatasetFactory::Make(children)),
                   "DatasetFactory");
}

// [[arrow::export]]
R6 dataset___FileSystemDatasetFactory__Make2(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format,
    const std::shared_ptr<ds::Partitioning>& partitioning) {
  // TODO(fsaintjacques): Make options configurable
  auto options = ds::FileSystemFactoryOptions{};
  if (partitioning != nullptr) {
    options.partitioning = partitioning;
  }

  auto factory =
      ValueOrStop(ds::FileSystemDatasetFactory::Make(fs, *selector, format, options));
  return cpp11::r6(factory, "FileSystemDatasetFactory");
}

// [[arrow::export]]
R6 dataset___FileSystemDatasetFactory__Make1(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format) {
  return dataset___FileSystemDatasetFactory__Make2(fs, selector, format, nullptr);
}

// [[arrow::export]]
R6 dataset___FileSystemDatasetFactory__Make3(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format,
    const std::shared_ptr<ds::PartitioningFactory>& factory) {
  // TODO(fsaintjacques): Make options configurable
  auto options = ds::FileSystemFactoryOptions{};
  if (factory != nullptr) {
    options.partitioning = factory;
  }

  auto ptr =
      ValueOrStop(ds::FileSystemDatasetFactory::Make(fs, *selector, format, options));
  return cpp11::r6(ptr, "FileSystemDatasetFactory");
}

// FileFormat, ParquetFileFormat, IpcFileFormat

// [[arrow::export]]
std::string dataset___FileFormat__type_name(
    const std::shared_ptr<ds::FileFormat>& format) {
  return format->type_name();
}

// [[arrow::export]]
R6 dataset___FileFormat__DefaultWriteOptions(
    const std::shared_ptr<ds::FileFormat>& fmt) {
  return cpp11::r6(fmt->DefaultWriteOptions(), "FileWriteOptions");
}

// [[arrow::export]]
R6 dataset___ParquetFileFormat__MakeRead(bool use_buffered_stream, int64_t buffer_size,
                                         cpp11::strings dict_columns) {
  auto fmt = std::make_shared<ds::ParquetFileFormat>();

  fmt->reader_options.use_buffered_stream = use_buffered_stream;
  fmt->reader_options.buffer_size = buffer_size;

  auto dict_columns_vector = cpp11::as_cpp<std::vector<std::string>>(dict_columns);
  auto& d = fmt->reader_options.dict_columns;
  std::move(dict_columns_vector.begin(), dict_columns_vector.end(),
            std::inserter(d, d.end()));

  return cpp11::r6(fmt, "ParquetFileFormat");
}

// [[arrow::export]]
std::string dataset___FileWriteOptions__type_name(
    const std::shared_ptr<ds::FileWriteOptions>& options) {
  return options->type_name();
}

// [[arrow::export]]
void dataset___ParquetFileWriteOptions__update(
    const std::shared_ptr<ds::ParquetFileWriteOptions>& options,
    const std::shared_ptr<parquet::WriterProperties>& writer_props,
    const std::shared_ptr<parquet::ArrowWriterProperties>& arrow_writer_props) {
  options->writer_properties = writer_props;
  options->arrow_writer_properties = arrow_writer_props;
}

// [[arrow::export]]
void dataset___IpcFileWriteOptions__update2(
    const std::shared_ptr<ds::IpcFileWriteOptions>& ipc_options, bool use_legacy_format,
    const std::shared_ptr<arrow::util::Codec>& codec,
    arrow::ipc::MetadataVersion metadata_version) {
  ipc_options->options->write_legacy_ipc_format = use_legacy_format;
  ipc_options->options->codec = codec;
  ipc_options->options->metadata_version = metadata_version;
}

// [[arrow::export]]
void dataset___IpcFileWriteOptions__update1(
    const std::shared_ptr<ds::IpcFileWriteOptions>& ipc_options, bool use_legacy_format,
    arrow::ipc::MetadataVersion metadata_version) {
  ipc_options->options->write_legacy_ipc_format = use_legacy_format;
  ipc_options->options->metadata_version = metadata_version;
}

// [[arrow::export]]
R6 dataset___IpcFileFormat__Make() {
  return cpp11::r6(std::make_shared<ds::IpcFileFormat>(), "IpcFileFormat");
}

// [[arrow::export]]
R6 dataset___CsvFileFormat__Make(
    const std::shared_ptr<arrow::csv::ParseOptions>& parse_options) {
  auto format = std::make_shared<ds::CsvFileFormat>();
  format->parse_options = *parse_options;
  return cpp11::r6(format, "CsvFileFormat");
}

// DirectoryPartitioning, HivePartitioning

// [[arrow::export]]
R6 dataset___DirectoryPartitioning(const std::shared_ptr<arrow::Schema>& schm) {
  return cpp11::r6(std::make_shared<ds::DirectoryPartitioning>(schm),
                   "DirectoryPartitioning");
}

// [[arrow::export]]
R6 dataset___DirectoryPartitioning__MakeFactory(
    const std::vector<std::string>& field_names) {
  return cpp11::r6(ds::DirectoryPartitioning::MakeFactory(field_names),
                   "DirectoryPartitioningFactory");
}

// [[arrow::export]]
R6 dataset___HivePartitioning(const std::shared_ptr<arrow::Schema>& schm) {
  return cpp11::r6(std::make_shared<ds::HivePartitioning>(schm), "HivePartitioning");
}

// [[arrow::export]]
R6 dataset___HivePartitioning__MakeFactory() {
  return cpp11::r6(ds::HivePartitioning::MakeFactory(), "HivePartitioningFactory");
}

// ScannerBuilder, Scanner

// [[arrow::export]]
void dataset___ScannerBuilder__Project(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                       const std::vector<std::string>& cols) {
  StopIfNotOk(sb->Project(cols));
}

// [[arrow::export]]
void dataset___ScannerBuilder__Filter(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                      const std::shared_ptr<ds::Expression>& expr) {
  // Expressions converted from R's expressions are typed with R's native type,
  // i.e. double, int64_t and bool.
  auto cast_filter = ValueOrStop(InsertImplicitCasts(*expr, *sb->schema()));
  StopIfNotOk(sb->Filter(cast_filter));
}

// [[arrow::export]]
void dataset___ScannerBuilder__UseThreads(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                          bool threads) {
  StopIfNotOk(sb->UseThreads(threads));
}

// [[arrow::export]]
void dataset___ScannerBuilder__BatchSize(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                         int64_t batch_size) {
  StopIfNotOk(sb->BatchSize(batch_size));
}

// [[arrow::export]]
R6 dataset___ScannerBuilder__schema(const std::shared_ptr<ds::ScannerBuilder>& sb) {
  return cpp11::r6(sb->schema(), "Schema");
}

// [[arrow::export]]
R6 dataset___ScannerBuilder__Finish(const std::shared_ptr<ds::ScannerBuilder>& sb) {
  auto out = ValueOrStop(sb->Finish());

  return cpp11::r6(std::shared_ptr<ds::Scanner>(std::move(out)), "Scanner");
}

// [[arrow::export]]
R6 dataset___Scanner__ToTable(const std::shared_ptr<ds::Scanner>& scanner) {
  return cpp11::r6(ValueOrStop(scanner->ToTable()), "Table");
}

// [[arrow::export]]
R6 dataset___Scanner__head(const std::shared_ptr<ds::Scanner>& scanner, int n) {
  // TODO: make this a full Slice with offset > 0
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  std::shared_ptr<arrow::RecordBatch> current_batch;

  for (auto st : ValueOrStop(scanner->Scan())) {
    for (auto b : ValueOrStop(ValueOrStop(st)->Execute())) {
      current_batch = ValueOrStop(b);
      batches.push_back(current_batch->Slice(0, n));
      n -= current_batch->num_rows();
      if (n < 0) break;
    }
    if (n < 0) break;
  }
  auto out = ValueOrStop(arrow::Table::FromRecordBatches(std::move(batches)));
  return cpp11::r6(out, "Table");
}

// [[arrow::export]]
std::vector<std::shared_ptr<ds::ScanTask>> dataset___Scanner__Scan(
    const std::shared_ptr<ds::Scanner>& scanner) {
  auto it = ValueOrStop(scanner->Scan());
  std::vector<std::shared_ptr<ds::ScanTask>> out;
  std::shared_ptr<ds::ScanTask> scan_task;
  // TODO(npr): can this iteration be parallelized?
  for (auto st : it) {
    scan_task = ValueOrStop(st);
    out.push_back(scan_task);
  }
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> dataset___Scanner__schema(
    const std::shared_ptr<ds::Scanner>& sc) {
  return sc->schema();
}

// [[arrow::export]]
std::vector<std::shared_ptr<arrow::RecordBatch>> dataset___ScanTask__get_batches(
    const std::shared_ptr<ds::ScanTask>& scan_task) {
  arrow::RecordBatchIterator rbi;
  rbi = ValueOrStop(scan_task->Execute());
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  std::shared_ptr<arrow::RecordBatch> batch;
  for (auto b : rbi) {
    batch = ValueOrStop(b);
    out.push_back(batch);
  }
  return out;
}

// [[arrow::export]]
void dataset___Dataset__Write(
    const std::shared_ptr<ds::FileWriteOptions>& file_write_options,
    const std::shared_ptr<fs::FileSystem>& filesystem, std::string base_dir,
    const std::shared_ptr<ds::Partitioning>& partitioning, std::string basename_template,
    const std::shared_ptr<ds::Scanner>& scanner) {
  ds::FileSystemDatasetWriteOptions opts;
  opts.file_write_options = file_write_options;
  opts.filesystem = filesystem;
  opts.base_dir = base_dir;
  opts.partitioning = partitioning;
  opts.basename_template = basename_template;
  StopIfNotOk(ds::FileSystemDataset::Write(opts, scanner));
}

#endif
