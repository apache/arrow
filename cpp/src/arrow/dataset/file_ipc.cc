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

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/ipc/reader.h"
#include "arrow/table.h"
#include "arrow/util/iterator.h"
#include "arrow/util/range.h"

namespace arrow {
namespace dataset {

Result<std::shared_ptr<ipc::RecordBatchFileReader>> OpenReader(
    const FileSource& source, std::shared_ptr<io::RandomAccessFile> input = nullptr) {
  if (input == nullptr) {
    ARROW_ASSIGN_OR_RAISE(input, source.Open());
  }

  std::shared_ptr<ipc::RecordBatchFileReader> reader;
  auto status = ipc::RecordBatchFileReader::Open(std::move(input), &reader);
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
    struct {
      Result<std::shared_ptr<RecordBatch>> Next() {
        if (i_ == reader_->num_record_batches()) {
          return nullptr;
        }

        std::shared_ptr<RecordBatch> batch;
        RETURN_NOT_OK(reader_->ReadRecordBatch(i_++, &batch));
        return batch;
      }

      std::shared_ptr<ipc::RecordBatchFileReader> reader_;
      int i_ = 0;
    } batch_it;

    ARROW_ASSIGN_OR_RAISE(batch_it.reader_, OpenReader(source_));

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

Result<std::shared_ptr<Fragment>> IpcFileFormat::MakeFragment(
    const FileSource& source, std::shared_ptr<ScanOptions> options) {
  return std::make_shared<IpcFragment>(source, options);
}

}  // namespace dataset
}  // namespace arrow
