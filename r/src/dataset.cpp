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

#if defined(ARROW_R_WITH_DATASET)

#include <arrow/array.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/ipc/writer.h>
#include <arrow/table.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/iterator.h>
#include <parquet/properties.h>

namespace ds = ::arrow::dataset;
namespace fs = ::arrow::fs;
namespace compute = ::arrow::compute;

namespace cpp11 {

const char* r6_class_name<ds::Dataset>::get(const std::shared_ptr<ds::Dataset>& dataset) {
  auto type_name = dataset->type_name();

  if (type_name == "union") {
    return "UnionDataset";
  } else if (type_name == "filesystem") {
    return "FileSystemDataset";
  } else if (type_name == "in-memory") {
    return "InMemoryDataset";
  } else {
    return "Dataset";
  }
}

const char* r6_class_name<ds::FileFormat>::get(
    const std::shared_ptr<ds::FileFormat>& file_format) {
  auto type_name = file_format->type_name();
  if (type_name == "parquet") {
    return "ParquetFileFormat";
  } else if (type_name == "ipc") {
    return "IpcFileFormat";
  } else if (type_name == "csv") {
    return "CsvFileFormat";
  } else {
    return "FileFormat";
  }
}

}  // namespace cpp11

// Dataset, UnionDataset, FileSystemDataset

// [[dataset::export]]
std::shared_ptr<ds::ScannerBuilder> dataset___Dataset__NewScan(
    const std::shared_ptr<ds::Dataset>& ds) {
  auto builder = ValueOrStop(ds->NewScan());
  StopIfNotOk(builder->Pool(gc_memory_pool()));
  return builder;
}

// [[dataset::export]]
std::shared_ptr<arrow::Schema> dataset___Dataset__schema(
    const std::shared_ptr<ds::Dataset>& dataset) {
  return dataset->schema();
}

// [[dataset::export]]
std::string dataset___Dataset__type_name(const std::shared_ptr<ds::Dataset>& dataset) {
  return dataset->type_name();
}

// [[dataset::export]]
std::shared_ptr<ds::Dataset> dataset___Dataset__ReplaceSchema(
    const std::shared_ptr<ds::Dataset>& dataset,
    const std::shared_ptr<arrow::Schema>& schm) {
  return ValueOrStop(dataset->ReplaceSchema(schm));
}

// [[dataset::export]]
std::shared_ptr<ds::Dataset> dataset___UnionDataset__create(
    const ds::DatasetVector& datasets, const std::shared_ptr<arrow::Schema>& schm) {
  return ValueOrStop(ds::UnionDataset::Make(schm, datasets));
}

// [[dataset::export]]
std::shared_ptr<ds::Dataset> dataset___InMemoryDataset__create(
    const std::shared_ptr<arrow::Table>& table) {
  return std::make_shared<ds::InMemoryDataset>(table);
}

// [[dataset::export]]
cpp11::list dataset___UnionDataset__children(
    const std::shared_ptr<ds::UnionDataset>& ds) {
  return arrow::r::to_r_list(ds->children());
}

// [[dataset::export]]
std::shared_ptr<ds::FileFormat> dataset___FileSystemDataset__format(
    const std::shared_ptr<ds::FileSystemDataset>& dataset) {
  return dataset->format();
}

// [[dataset::export]]
std::shared_ptr<fs::FileSystem> dataset___FileSystemDataset__filesystem(
    const std::shared_ptr<ds::FileSystemDataset>& dataset) {
  return dataset->filesystem();
}

// [[dataset::export]]
std::vector<std::string> dataset___FileSystemDataset__files(
    const std::shared_ptr<ds::FileSystemDataset>& dataset) {
  return dataset->files();
}

// DatasetFactory, UnionDatasetFactory, FileSystemDatasetFactory

// [[dataset::export]]
std::shared_ptr<ds::Dataset> dataset___DatasetFactory__Finish1(
    const std::shared_ptr<ds::DatasetFactory>& factory, bool unify_schemas) {
  ds::FinishOptions opts;
  if (unify_schemas) {
    opts.inspect_options.fragments = ds::InspectOptions::kInspectAllFragments;
  }
  return ValueOrStop(factory->Finish(opts));
}

// [[dataset::export]]
std::shared_ptr<ds::Dataset> dataset___DatasetFactory__Finish2(
    const std::shared_ptr<ds::DatasetFactory>& factory,
    const std::shared_ptr<arrow::Schema>& schema) {
  return ValueOrStop(factory->Finish(schema));
}

// [[dataset::export]]
std::shared_ptr<arrow::Schema> dataset___DatasetFactory__Inspect(
    const std::shared_ptr<ds::DatasetFactory>& factory, bool unify_schemas) {
  ds::InspectOptions opts;
  if (unify_schemas) {
    opts.fragments = ds::InspectOptions::kInspectAllFragments;
  }
  return ValueOrStop(factory->Inspect(opts));
}

// [[dataset::export]]
std::shared_ptr<ds::DatasetFactory> dataset___UnionDatasetFactory__Make(
    const std::vector<std::shared_ptr<ds::DatasetFactory>>& children) {
  return ValueOrStop(ds::UnionDatasetFactory::Make(children));
}

// [[dataset::export]]
std::shared_ptr<ds::FileSystemDatasetFactory> dataset___FileSystemDatasetFactory__Make0(
    const std::shared_ptr<fs::FileSystem>& fs, const std::vector<std::string>& paths,
    const std::shared_ptr<ds::FileFormat>& format) {
  // TODO(fsaintjacques): Make options configurable
  auto options = ds::FileSystemFactoryOptions{};

  return arrow::internal::checked_pointer_cast<ds::FileSystemDatasetFactory>(
      ValueOrStop(ds::FileSystemDatasetFactory::Make(fs, paths, format, options)));
}

// [[dataset::export]]
std::shared_ptr<ds::FileSystemDatasetFactory> dataset___FileSystemDatasetFactory__Make2(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format,
    const std::shared_ptr<ds::Partitioning>& partitioning) {
  // TODO(fsaintjacques): Make options configurable
  auto options = ds::FileSystemFactoryOptions{};
  if (partitioning != nullptr) {
    options.partitioning = partitioning;
  }

  return arrow::internal::checked_pointer_cast<ds::FileSystemDatasetFactory>(
      ValueOrStop(ds::FileSystemDatasetFactory::Make(fs, *selector, format, options)));
}

// [[dataset::export]]
std::shared_ptr<ds::FileSystemDatasetFactory> dataset___FileSystemDatasetFactory__Make1(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format) {
  return dataset___FileSystemDatasetFactory__Make2(fs, selector, format, nullptr);
}

// [[dataset::export]]
std::shared_ptr<ds::FileSystemDatasetFactory> dataset___FileSystemDatasetFactory__Make3(
    const std::shared_ptr<fs::FileSystem>& fs,
    const std::shared_ptr<fs::FileSelector>& selector,
    const std::shared_ptr<ds::FileFormat>& format,
    const std::shared_ptr<ds::PartitioningFactory>& factory) {
  // TODO(fsaintjacques): Make options configurable
  auto options = ds::FileSystemFactoryOptions{};
  if (factory != nullptr) {
    options.partitioning = factory;
  }

  return arrow::internal::checked_pointer_cast<ds::FileSystemDatasetFactory>(
      ValueOrStop(ds::FileSystemDatasetFactory::Make(fs, *selector, format, options)));
}

// FileFormat, ParquetFileFormat, IpcFileFormat

// [[dataset::export]]
std::string dataset___FileFormat__type_name(
    const std::shared_ptr<ds::FileFormat>& format) {
  return format->type_name();
}

// [[dataset::export]]
std::shared_ptr<ds::FileWriteOptions> dataset___FileFormat__DefaultWriteOptions(
    const std::shared_ptr<ds::FileFormat>& fmt) {
  return fmt->DefaultWriteOptions();
}

// [[dataset::export]]
std::shared_ptr<ds::ParquetFileFormat> dataset___ParquetFileFormat__Make(
    const std::shared_ptr<ds::ParquetFragmentScanOptions>& options,
    cpp11::strings dict_columns) {
  auto fmt = std::make_shared<ds::ParquetFileFormat>();
  fmt->default_fragment_scan_options = std::move(options);

  auto dict_columns_vector = cpp11::as_cpp<std::vector<std::string>>(dict_columns);
  auto& d = fmt->reader_options.dict_columns;
  std::move(dict_columns_vector.begin(), dict_columns_vector.end(),
            std::inserter(d, d.end()));

  return fmt;
}

// [[dataset::export]]
std::string dataset___FileWriteOptions__type_name(
    const std::shared_ptr<ds::FileWriteOptions>& options) {
  return options->type_name();
}

#if defined(ARROW_R_WITH_PARQUET)
// [[dataset::export]]
void dataset___ParquetFileWriteOptions__update(
    const std::shared_ptr<ds::ParquetFileWriteOptions>& options,
    const std::shared_ptr<parquet::WriterProperties>& writer_props,
    const std::shared_ptr<parquet::ArrowWriterProperties>& arrow_writer_props) {
  options->writer_properties = writer_props;
  options->arrow_writer_properties = arrow_writer_props;
}
#endif

// [[dataset::export]]
void dataset___IpcFileWriteOptions__update2(
    const std::shared_ptr<ds::IpcFileWriteOptions>& ipc_options, bool use_legacy_format,
    const std::shared_ptr<arrow::util::Codec>& codec,
    arrow::ipc::MetadataVersion metadata_version) {
  ipc_options->options->write_legacy_ipc_format = use_legacy_format;
  ipc_options->options->codec = codec;
  ipc_options->options->metadata_version = metadata_version;
}

// [[dataset::export]]
void dataset___IpcFileWriteOptions__update1(
    const std::shared_ptr<ds::IpcFileWriteOptions>& ipc_options, bool use_legacy_format,
    arrow::ipc::MetadataVersion metadata_version) {
  ipc_options->options->write_legacy_ipc_format = use_legacy_format;
  ipc_options->options->metadata_version = metadata_version;
}

// [[dataset::export]]
void dataset___CsvFileWriteOptions__update(
    const std::shared_ptr<ds::CsvFileWriteOptions>& csv_options,
    const std::shared_ptr<arrow::csv::WriteOptions>& write_options) {
  *csv_options->write_options = *write_options;
}

// [[dataset::export]]
std::shared_ptr<ds::IpcFileFormat> dataset___IpcFileFormat__Make() {
  return std::make_shared<ds::IpcFileFormat>();
}

// [[dataset::export]]
std::shared_ptr<ds::CsvFileFormat> dataset___CsvFileFormat__Make(
    const std::shared_ptr<arrow::csv::ParseOptions>& parse_options,
    const std::shared_ptr<arrow::csv::ConvertOptions>& convert_options,
    const std::shared_ptr<arrow::csv::ReadOptions>& read_options) {
  auto format = std::make_shared<ds::CsvFileFormat>();
  format->parse_options = *parse_options;
  auto scan_options = std::make_shared<ds::CsvFragmentScanOptions>();
  if (convert_options) scan_options->convert_options = *convert_options;
  if (read_options) scan_options->read_options = *read_options;
  format->default_fragment_scan_options = std::move(scan_options);
  return format;
}

// FragmentScanOptions, CsvFragmentScanOptions, ParquetFragmentScanOptions

// [[dataset::export]]
std::string dataset___FragmentScanOptions__type_name(
    const std::shared_ptr<ds::FragmentScanOptions>& fragment_scan_options) {
  return fragment_scan_options->type_name();
}

// [[dataset::export]]
std::shared_ptr<ds::CsvFragmentScanOptions> dataset___CsvFragmentScanOptions__Make(
    const std::shared_ptr<arrow::csv::ConvertOptions>& convert_options,
    const std::shared_ptr<arrow::csv::ReadOptions>& read_options) {
  auto options = std::make_shared<ds::CsvFragmentScanOptions>();
  options->convert_options = *convert_options;
  options->read_options = *read_options;
  return options;
}

// [[dataset::export]]
std::shared_ptr<ds::ParquetFragmentScanOptions>
dataset___ParquetFragmentScanOptions__Make(bool use_buffered_stream, int64_t buffer_size,
                                           bool pre_buffer) {
  auto options = std::make_shared<ds::ParquetFragmentScanOptions>();
  if (use_buffered_stream) {
    options->reader_properties->enable_buffered_stream();
  } else {
    options->reader_properties->disable_buffered_stream();
  }
  options->reader_properties->set_buffer_size(buffer_size);
  options->arrow_reader_properties->set_pre_buffer(pre_buffer);
  return options;
}

// DirectoryPartitioning, HivePartitioning

ds::SegmentEncoding GetSegmentEncoding(const std::string& segment_encoding) {
  if (segment_encoding == "none") {
    return ds::SegmentEncoding::None;
  } else if (segment_encoding == "uri") {
    return ds::SegmentEncoding::Uri;
  }
  cpp11::stop("invalid segment encoding: " + segment_encoding);
  return ds::SegmentEncoding::None;
}

// [[dataset::export]]
std::shared_ptr<ds::DirectoryPartitioning> dataset___DirectoryPartitioning(
    const std::shared_ptr<arrow::Schema>& schm, const std::string& segment_encoding) {
  ds::KeyValuePartitioningOptions options;
  options.segment_encoding = GetSegmentEncoding(segment_encoding);
  std::vector<std::shared_ptr<arrow::Array>> dictionaries;
  return std::make_shared<ds::DirectoryPartitioning>(schm, dictionaries, options);
}

// [[dataset::export]]
std::shared_ptr<ds::PartitioningFactory> dataset___DirectoryPartitioning__MakeFactory(
    const std::vector<std::string>& field_names, const std::string& segment_encoding) {
  ds::PartitioningFactoryOptions options;
  options.segment_encoding = GetSegmentEncoding(segment_encoding);
  return ds::DirectoryPartitioning::MakeFactory(field_names, options);
}

// [[dataset::export]]
std::shared_ptr<ds::HivePartitioning> dataset___HivePartitioning(
    const std::shared_ptr<arrow::Schema>& schm, const std::string& null_fallback,
    const std::string& segment_encoding) {
  ds::HivePartitioningOptions options;
  options.null_fallback = null_fallback;
  options.segment_encoding = GetSegmentEncoding(segment_encoding);
  std::vector<std::shared_ptr<arrow::Array>> dictionaries;
  return std::make_shared<ds::HivePartitioning>(schm, dictionaries, options);
}

// [[dataset::export]]
std::shared_ptr<ds::PartitioningFactory> dataset___HivePartitioning__MakeFactory(
    const std::string& null_fallback, const std::string& segment_encoding) {
  ds::HivePartitioningFactoryOptions options;
  options.null_fallback = null_fallback;
  options.segment_encoding = GetSegmentEncoding(segment_encoding);
  return ds::HivePartitioning::MakeFactory(options);
}

// ScannerBuilder, Scanner

// [[dataset::export]]
void dataset___ScannerBuilder__ProjectNames(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                            const std::vector<std::string>& cols) {
  StopIfNotOk(sb->Project(cols));
}

// [[dataset::export]]
void dataset___ScannerBuilder__ProjectExprs(
    const std::shared_ptr<ds::ScannerBuilder>& sb,
    const std::vector<std::shared_ptr<compute::Expression>>& exprs,
    const std::vector<std::string>& names) {
  // We have shared_ptrs of expressions but need the Expressions
  std::vector<compute::Expression> expressions;
  for (auto expr : exprs) {
    expressions.push_back(*expr);
  }
  StopIfNotOk(sb->Project(expressions, names));
}

// [[dataset::export]]
void dataset___ScannerBuilder__Filter(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                      const std::shared_ptr<compute::Expression>& expr) {
  StopIfNotOk(sb->Filter(*expr));
}

// [[dataset::export]]
void dataset___ScannerBuilder__UseThreads(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                          bool threads) {
  StopIfNotOk(sb->UseThreads(threads));
}

// [[dataset::export]]
void dataset___ScannerBuilder__UseAsync(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                        bool use_async) {
  StopIfNotOk(sb->UseAsync(use_async));
}

// [[dataset::export]]
void dataset___ScannerBuilder__BatchSize(const std::shared_ptr<ds::ScannerBuilder>& sb,
                                         int64_t batch_size) {
  StopIfNotOk(sb->BatchSize(batch_size));
}

// [[dataset::export]]
void dataset___ScannerBuilder__FragmentScanOptions(
    const std::shared_ptr<ds::ScannerBuilder>& sb,
    const std::shared_ptr<ds::FragmentScanOptions>& options) {
  StopIfNotOk(sb->FragmentScanOptions(options));
}

// [[dataset::export]]
std::shared_ptr<arrow::Schema> dataset___ScannerBuilder__schema(
    const std::shared_ptr<ds::ScannerBuilder>& sb) {
  return sb->schema();
}

// [[dataset::export]]
std::shared_ptr<ds::Scanner> dataset___ScannerBuilder__Finish(
    const std::shared_ptr<ds::ScannerBuilder>& sb) {
  return ValueOrStop(sb->Finish());
}

// [[dataset::export]]
std::shared_ptr<ds::ScannerBuilder> dataset___ScannerBuilder__FromRecordBatchReader(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  return (ds::ScannerBuilder::FromRecordBatchReader(reader));
}

// [[dataset::export]]
std::shared_ptr<arrow::Table> dataset___Scanner__ToTable(
    const std::shared_ptr<ds::Scanner>& scanner) {
  return ValueOrStop(scanner->ToTable());
}

// [[dataset::export]]
cpp11::list dataset___Scanner__ScanBatches(const std::shared_ptr<ds::Scanner>& scanner) {
  auto it = ValueOrStop(scanner->ScanBatches());
  arrow::RecordBatchVector batches;
  StopIfNotOk(it.Visit([&](ds::TaggedRecordBatch tagged_batch) {
    batches.push_back(std::move(tagged_batch.record_batch));
    return arrow::Status::OK();
  }));
  return arrow::r::to_r_list(batches);
}

// [[dataset::export]]
std::shared_ptr<arrow::RecordBatchReader> dataset___Scanner__ToRecordBatchReader(
    const std::shared_ptr<ds::Scanner>& scanner) {
  return ValueOrStop(scanner->ToRecordBatchReader());
}

// [[dataset::export]]
std::shared_ptr<arrow::Table> dataset___Scanner__head(
    const std::shared_ptr<ds::Scanner>& scanner, int n) {
  // TODO: make this a full Slice with offset > 0
  return ValueOrStop(scanner->Head(n));
}

// [[dataset::export]]
std::shared_ptr<arrow::Schema> dataset___Scanner__schema(
    const std::shared_ptr<ds::Scanner>& sc) {
  return sc->options()->projected_schema;
}

// [[dataset::export]]
cpp11::list dataset___ScanTask__get_batches(
    const std::shared_ptr<ds::ScanTask>& scan_task) {
  arrow::RecordBatchIterator rbi;
  rbi = ValueOrStop(scan_task->Execute());
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  std::shared_ptr<arrow::RecordBatch> batch;
  for (auto b : rbi) {
    batch = ValueOrStop(b);
    out.push_back(batch);
  }
  return arrow::r::to_r_list(out);
}

// [[dataset::export]]
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

// [[dataset::export]]
std::shared_ptr<arrow::Table> dataset___Scanner__TakeRows(
    const std::shared_ptr<ds::Scanner>& scanner,
    const std::shared_ptr<arrow::Array>& indices) {
  return ValueOrStop(scanner->TakeRows(*indices));
}

// [[dataset::export]]
int64_t dataset___Scanner__CountRows(const std::shared_ptr<ds::Scanner>& scanner) {
  return ValueOrStop(scanner->CountRows());
}

#endif
