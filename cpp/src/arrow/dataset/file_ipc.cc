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
#include "arrow/util/iterator.h"

namespace arrow {
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

static inline Result<std::vector<int>> GetIncludedFields(
    const Schema& schema, const std::vector<std::string>& materialized_fields) {
  std::vector<int> included_fields;

  for (FieldRef ref : materialized_fields) {
    ARROW_ASSIGN_OR_RAISE(auto match, ref.FindOneOrNone(schema));
    if (match.indices().empty()) continue;

    included_fields.push_back(match.indices()[0]);
  }

  return included_fields;
}

/// \brief A ScanTask backed by an Ipc file.
class IpcScanTask : public ScanTask {
 public:
  IpcScanTask(FileSource source, std::shared_ptr<ScanOptions> options,
              std::shared_ptr<ScanContext> context)
      : ScanTask(std::move(options), std::move(context)), source_(std::move(source)) {}

  Result<RecordBatchIterator> Execute() override {
    struct Impl {
      static Result<RecordBatchIterator> Make(
          const FileSource& source, std::vector<std::string> materialized_fields,
          MemoryPool* pool) {
        ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source));

        auto options = default_read_options();
        options.memory_pool = pool;
        ARROW_ASSIGN_OR_RAISE(options.included_fields,
                              GetIncludedFields(*reader->schema(), materialized_fields));

        ARROW_ASSIGN_OR_RAISE(reader, OpenReader(source, options));
        return RecordBatchIterator(Impl{std::move(reader), 0});
      }

      Result<std::shared_ptr<RecordBatch>> Next() {
        if (i_ == reader_->num_record_batches()) {
          return nullptr;
        }

        return reader_->ReadRecordBatch(i_++);
      }

      std::shared_ptr<ipc::RecordBatchFileReader> reader_;
      int i_;
    };

    return Impl::Make(source_, options_->MaterializedFields(), context_->pool);
  }

 private:
  FileSource source_;
};

class IpcScanTaskIterator {
 public:
  static Result<ScanTaskIterator> Make(std::shared_ptr<ScanOptions> options,
                                       std::shared_ptr<ScanContext> context,
                                       FileSource source) {
    return ScanTaskIterator(
        IpcScanTaskIterator(std::move(options), std::move(context), std::move(source)));
  }

  Result<std::shared_ptr<ScanTask>> Next() {
    if (once_) {
      // Iteration is done.
      return nullptr;
    }

    once_ = true;
    return std::shared_ptr<ScanTask>(new IpcScanTask(source_, options_, context_));
  }

 private:
  IpcScanTaskIterator(std::shared_ptr<ScanOptions> options,
                      std::shared_ptr<ScanContext> context, FileSource source)
      : options_(std::move(options)),
        context_(std::move(context)),
        source_(std::move(source)) {}

  bool once_ = false;
  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> context_;
  FileSource source_;
};

Result<bool> IpcFileFormat::IsSupported(const FileSource& source) const {
  RETURN_NOT_OK(source.Open().status());
  return OpenReader(source).ok();
}

Result<std::shared_ptr<Schema>> IpcFileFormat::Inspect(const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source));
  return reader->schema();
}

Result<ScanTaskIterator> IpcFileFormat::ScanFile(std::shared_ptr<ScanOptions> options,
                                                 std::shared_ptr<ScanContext> context,
                                                 FileFragment* fragment) const {
  return IpcScanTaskIterator::Make(std::move(options), std::move(context),
                                   fragment->source());
}

class IpcWriteTask : public WriteTask {
 public:
  IpcWriteTask(WritableFileSource destination, std::shared_ptr<FileFormat> format,
               std::shared_ptr<Fragment> fragment,
               std::shared_ptr<ScanOptions> scan_options,
               std::shared_ptr<ScanContext> scan_context)
      : WriteTask(std::move(destination), std::move(format)),
        fragment_(std::move(fragment)),
        scan_options_(std::move(scan_options)),
        scan_context_(std::move(scan_context)) {}

  Status Execute() override {
    RETURN_NOT_OK(CreateDestinationParentDir());

    auto schema = scan_options_->schema();

    ARROW_ASSIGN_OR_RAISE(auto out_stream, destination_.Open());
    ARROW_ASSIGN_OR_RAISE(auto writer, ipc::NewFileWriter(out_stream.get(), schema));
    ARROW_ASSIGN_OR_RAISE(auto scan_task_it,
                          fragment_->Scan(scan_options_, scan_context_));

    for (auto maybe_scan_task : scan_task_it) {
      ARROW_ASSIGN_OR_RAISE(auto scan_task, maybe_scan_task);

      ARROW_ASSIGN_OR_RAISE(auto batch_it, scan_task->Execute());

      for (auto maybe_batch : batch_it) {
        ARROW_ASSIGN_OR_RAISE(auto batch, std::move(maybe_batch));
        RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
      }
    }

    return writer->Close();
  }

 private:
  std::shared_ptr<Fragment> fragment_;
  std::shared_ptr<ScanOptions> scan_options_;
  std::shared_ptr<ScanContext> scan_context_;
};

Result<std::shared_ptr<WriteTask>> IpcFileFormat::WriteFragment(
    WritableFileSource destination, std::shared_ptr<Fragment> fragment,
    std::shared_ptr<ScanOptions> scan_options,
    std::shared_ptr<ScanContext> scan_context) {
  return std::make_shared<IpcWriteTask>(std::move(destination), shared_from_this(),
                                        std::move(fragment), std::move(scan_options),
                                        std::move(scan_context));
}

}  // namespace dataset
}  // namespace arrow
