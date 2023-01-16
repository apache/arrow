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

#include "arrow/dataset/file_ipc.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/scanner.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace dataset {

static inline ipc::IpcReadOptions default_read_options() {
  auto options = ipc::IpcReadOptions::Defaults();
  options.use_threads = false;
  return options;
}

static inline Result<std::shared_ptr<ipc::RecordBatchFileReader>> OpenReader(
    const FileSource& source,
    const ipc::IpcReadOptions& options = default_read_options()) {
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());

  std::shared_ptr<ipc::RecordBatchFileReader> reader;

  auto status =
      ipc::RecordBatchFileReader::Open(std::move(input), options).Value(&reader);
  if (!status.ok()) {
    return status.WithMessage("Could not open IPC input source '", source.path(),
                              "': ", status.message());
  }
  return reader;
}

static inline Future<std::shared_ptr<ipc::RecordBatchFileReader>> OpenReaderAsync(
    const FileSource& source,
    const ipc::IpcReadOptions& options = default_read_options()) {
#ifdef ARROW_WITH_OPENTELEMETRY
  auto tracer = arrow::internal::tracing::GetTracer();
  auto span = tracer->StartSpan("arrow::dataset::IpcFileFormat::OpenReaderAsync");
#endif
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
  auto path = source.path();
  return ipc::RecordBatchFileReader::OpenAsync(std::move(input), options)
      .Then(
          [=](const std::shared_ptr<ipc::RecordBatchFileReader>& reader)
              -> Result<std::shared_ptr<ipc::RecordBatchFileReader>> {
#ifdef ARROW_WITH_OPENTELEMETRY
            span->SetStatus(opentelemetry::trace::StatusCode::kOk);
            span->End();
#endif
            return reader;
          },
          [=](const Status& status)
              -> Result<std::shared_ptr<ipc::RecordBatchFileReader>> {
#ifdef ARROW_WITH_OPENTELEMETRY
            arrow::internal::tracing::MarkSpan(status, span.get());
            span->End();
#endif
            return status.WithMessage("Could not open IPC input source '", path,
                                      "': ", status.message());
          });
}

static inline Result<std::vector<int>> GetIncludedFields(
    const Schema& schema, const std::vector<FieldRef>& materialized_fields) {
  std::vector<int> included_fields;

  for (const auto& ref : materialized_fields) {
    ARROW_ASSIGN_OR_RAISE(auto match, ref.FindOneOrNone(schema));
    if (match.indices().empty()) continue;

    included_fields.push_back(match.indices()[0]);
  }

  return included_fields;
}

static inline Result<ipc::IpcReadOptions> GetReadOptions(
    const Schema& schema, const FileFormat& format, const ScanOptions& scan_options) {
  ARROW_ASSIGN_OR_RAISE(
      auto ipc_scan_options,
      GetFragmentScanOptions<IpcFragmentScanOptions>(
          kIpcTypeName, &scan_options, format.default_fragment_scan_options));
  auto options =
      ipc_scan_options->options ? *ipc_scan_options->options : default_read_options();
  options.memory_pool = scan_options.pool;
  if (!options.included_fields.empty()) {
    // Cannot set them here
    ARROW_LOG(WARNING) << "IpcFragmentScanOptions.options->included_fields was set "
                          "but will be ignored; included_fields are derived from "
                          "fields referenced by the scan";
  }
  ARROW_ASSIGN_OR_RAISE(options.included_fields,
                        GetIncludedFields(schema, scan_options.MaterializedFields()));
  return options;
}

IpcFileFormat::IpcFileFormat() : FileFormat(std::make_shared<IpcFragmentScanOptions>()) {}

Result<bool> IpcFileFormat::IsSupported(const FileSource& source) const {
  RETURN_NOT_OK(source.Open().status());
  return OpenReader(source).ok();
}

Result<std::shared_ptr<Schema>> IpcFileFormat::Inspect(const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source));
  return reader->schema();
}

Result<RecordBatchGenerator> IpcFileFormat::ScanBatchesAsync(
    const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<FileFragment>& file) const {
  auto self = shared_from_this();
  auto source = file->source();
  auto open_reader = OpenReaderAsync(source);
  auto reopen_reader = [self, options,
                        source](std::shared_ptr<ipc::RecordBatchFileReader> reader)
      -> Future<std::shared_ptr<ipc::RecordBatchFileReader>> {
    ARROW_ASSIGN_OR_RAISE(auto options,
                          GetReadOptions(*reader->schema(), *self, *options));
    return OpenReader(source, options);
  };
  auto readahead_level = options->batch_readahead;
  auto default_fragment_scan_options = this->default_fragment_scan_options;
  auto open_generator = [=](const std::shared_ptr<ipc::RecordBatchFileReader>& reader)
      -> Result<RecordBatchGenerator> {
    ARROW_ASSIGN_OR_RAISE(
        auto ipc_scan_options,
        GetFragmentScanOptions<IpcFragmentScanOptions>(kIpcTypeName, options.get(),
                                                       default_fragment_scan_options));

    RecordBatchGenerator generator;
    if (ipc_scan_options->cache_options) {
      // Transferring helps performance when coalescing
      ARROW_ASSIGN_OR_RAISE(generator, reader->GetRecordBatchGenerator(
                                           /*coalesce=*/true, options->io_context,
                                           *ipc_scan_options->cache_options,
                                           ::arrow::internal::GetCpuThreadPool()));
    } else {
      ARROW_ASSIGN_OR_RAISE(generator, reader->GetRecordBatchGenerator(
                                           /*coalesce=*/false, options->io_context));
    }
    WRAP_ASYNC_GENERATOR_WITH_CHILD_SPAN(
        generator, "arrow::dataset::IpcFileFormat::ScanBatchesAsync::Next");
    if (readahead_level == 0) {
      return MakeChunkedBatchGenerator(std::move(generator), options->batch_size);
    }
    auto batch_generator = MakeReadaheadGenerator(std::move(generator), readahead_level);
    return MakeChunkedBatchGenerator(std::move(batch_generator), options->batch_size);
  };
  return MakeFromFuture(open_reader.Then(reopen_reader).Then(open_generator));
}

Future<std::optional<int64_t>> IpcFileFormat::CountRows(
    const std::shared_ptr<FileFragment>& file, compute::Expression predicate,
    const std::shared_ptr<ScanOptions>& options) {
  if (ExpressionHasFieldRefs(predicate)) {
    return Future<std::optional<int64_t>>::MakeFinished(std::nullopt);
  }
  auto self = checked_pointer_cast<IpcFileFormat>(shared_from_this());
  return DeferNotOk(options->io_context.executor()->Submit(
      [self, file]() -> Result<std::optional<int64_t>> {
        ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(file->source()));
        return reader->CountRows();
      }));
}

//
// IpcFileWriter, IpcFileWriteOptions
//

std::shared_ptr<FileWriteOptions> IpcFileFormat::DefaultWriteOptions() {
  std::shared_ptr<IpcFileWriteOptions> ipc_options(
      new IpcFileWriteOptions(shared_from_this()));

  ipc_options->options =
      std::make_shared<ipc::IpcWriteOptions>(ipc::IpcWriteOptions::Defaults());
  return ipc_options;
}

Result<std::shared_ptr<FileWriter>> IpcFileFormat::MakeWriter(
    std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileWriteOptions> options,
    fs::FileLocator destination_locator) const {
  if (!Equals(*options->format())) {
    return Status::TypeError("Mismatching format/write options.");
  }

  auto ipc_options = checked_pointer_cast<IpcFileWriteOptions>(options);

  ARROW_ASSIGN_OR_RAISE(auto writer,
                        ipc::MakeFileWriter(destination, schema, *ipc_options->options,
                                            ipc_options->metadata));

  return std::shared_ptr<FileWriter>(
      new IpcFileWriter(std::move(destination), std::move(writer), std::move(schema),
                        std::move(ipc_options), std::move(destination_locator)));
}

IpcFileWriter::IpcFileWriter(std::shared_ptr<io::OutputStream> destination,
                             std::shared_ptr<ipc::RecordBatchWriter> writer,
                             std::shared_ptr<Schema> schema,
                             std::shared_ptr<IpcFileWriteOptions> options,
                             fs::FileLocator destination_locator)
    : FileWriter(std::move(schema), std::move(options), std::move(destination),
                 std::move(destination_locator)),
      batch_writer_(std::move(writer)) {}

Status IpcFileWriter::Write(const std::shared_ptr<RecordBatch>& batch) {
  return batch_writer_->WriteRecordBatch(*batch);
}

Future<> IpcFileWriter::FinishInternal() {
  return DeferNotOk(destination_locator_.filesystem->io_context().executor()->Submit(
      [this]() { return batch_writer_->Close(); }));
}

}  // namespace dataset
}  // namespace arrow
