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

Result<std::shared_ptr<ipc::RecordBatchFileReader>> OpenReader(
    const FileSource& source, std::shared_ptr<io::RandomAccessFile> input = nullptr) {
  if (input == nullptr) {
    ARROW_ASSIGN_OR_RAISE(input, source.Open());
  }

  std::shared_ptr<ipc::RecordBatchFileReader> reader;
  auto options = ipc::IpcReadOptions::Defaults();
  options.use_threads = false;

  auto status =
      ipc::RecordBatchFileReader::Open(std::move(input), options).Value(&reader);
  if (!status.ok()) {
    return status.WithMessage("Could not open IPC input source '", source.path(),
                              "': ", status.message());
  }
  return reader;
}

/// \brief A ScanTask backed by an Ipc file.
class IpcScanTask : public ScanTask {
 public:
  IpcScanTask(FileSource source, std::shared_ptr<ScanOptions> options,
              std::shared_ptr<ScanContext> context)
      : ScanTask(std::move(options), std::move(context)), source_(std::move(source)) {}

  Result<RecordBatchIterator> Execute() override {
    struct Impl {
      static Result<Impl> Make(const FileSource& source,
                               const std::vector<std::string>& materialized_fields,
                               MemoryPool* pool) {
        ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source));
        auto materialized_schema =
            SchemaFromColumnNames(reader->schema(), materialized_fields);
        return Impl{std::move(reader),
                    RecordBatchProjector(std::move(materialized_schema)), pool, 0};
      }

      Result<std::shared_ptr<RecordBatch>> Next() {
        if (i_ == reader_->num_record_batches()) {
          return nullptr;
        }

        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> batch,
                              reader_->ReadRecordBatch(i_++));
        return projector_.Project(*batch, pool_);
      }

      std::shared_ptr<ipc::RecordBatchFileReader> reader_;
      RecordBatchProjector projector_;
      MemoryPool* pool_;
      int i_;
    };

    // get names of fields explicitly projected or referenced by filter
    auto fields = options_->MaterializedFields();
    std::sort(fields.begin(), fields.end());
    auto unique_end = std::unique(fields.begin(), fields.end());
    fields.erase(unique_end, fields.end());

    ARROW_ASSIGN_OR_RAISE(auto batch_it, Impl::Make(source_, fields, context_->pool));

    return RecordBatchIterator(std::move(batch_it));
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
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
  return OpenReader(source, input).ok();
}

Result<std::shared_ptr<Schema>> IpcFileFormat::Inspect(const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source));
  return reader->schema();
}

Result<ScanTaskIterator> IpcFileFormat::ScanFile(
    const FileSource& source, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<ScanContext> context) const {
  return IpcScanTaskIterator::Make(options, context, source);
}

Result<std::shared_ptr<WriteTask>> IpcFileFormat::WriteFragment(
    FileSource destination, std::shared_ptr<Fragment> fragment,
    std::shared_ptr<ScanContext> scan_context) {
  struct Task : WriteTask {
    Task(FileSource destination, std::shared_ptr<FileFormat> format,
         std::shared_ptr<Fragment> fragment, std::shared_ptr<ScanContext> scan_context)
        : WriteTask(std::move(destination), std::move(format)),
          fragment_(std::move(fragment)),
          scan_context_(std::move(scan_context)) {}

    Status Execute() override {
      RETURN_NOT_OK(CreateDestinationParentDir());

      ARROW_ASSIGN_OR_RAISE(auto out_stream, destination_.OpenWritable());
      ARROW_ASSIGN_OR_RAISE(auto writer,
                            ipc::NewFileWriter(out_stream.get(), fragment_->schema()));
      ARROW_ASSIGN_OR_RAISE(auto scan_task_it, fragment_->Scan(scan_context_));

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

    std::shared_ptr<Fragment> fragment_;
    std::shared_ptr<ScanContext> scan_context_;
  };

  return std::make_shared<Task>(std::move(destination), shared_from_this(),
                                std::move(fragment), std::move(scan_context));
}

}  // namespace dataset
}  // namespace arrow
