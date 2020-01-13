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

Status WrapErrorWithSource(Status status, const FileSource& source) {
  if (status.ok()) {
    return Status::OK();
  }

  return status.WithMessage("Could not open IPC input source '", source.path(),
                            "': ", status.message());
}

/// \brief A ScanTask backed by an Ipc file.
class IpcScanTask : public ScanTask {
 public:
  IpcScanTask(FileSource source, std::shared_ptr<ScanOptions> options,
              std::shared_ptr<ScanContext> context)
      : ScanTask(std::move(options), std::move(context)), source_(std::move(source)) {}

  Result<RecordBatchIterator> Execute() override {
    ARROW_ASSIGN_OR_RAISE(auto input, source_.Open());
    std::shared_ptr<RecordBatchReader> reader;
    RETURN_NOT_OK(
        WrapErrorWithSource(ipc::RecordBatchStreamReader::Open(input, &reader), source_));
    return MakeFunctionIterator([reader] { return reader->Next(); });
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
  ipc::DictionaryMemo dictionary_memo;
  std::shared_ptr<Schema> schema;
  return ipc::ReadSchema(input.get(), &dictionary_memo, &schema).ok();
}

Result<std::shared_ptr<Schema>> IpcFileFormat::Inspect(const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
  ipc::DictionaryMemo dictionary_memo;
  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(WrapErrorWithSource(
      ipc::ReadSchema(input.get(), &dictionary_memo, &schema), source));
  return schema;
}

Result<ScanTaskIterator> IpcFileFormat::ScanFile(
    const FileSource& source, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<ScanContext> context) const {
  return IpcScanTaskIterator::Make(options, context, source);
}

Result<std::shared_ptr<DataFragment>> IpcFileFormat::MakeFragment(
    const FileSource& source, std::shared_ptr<ScanOptions> options) {
  return std::make_shared<IpcFragment>(source, options);
}

}  // namespace dataset
}  // namespace arrow
