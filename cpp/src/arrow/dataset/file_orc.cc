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

#include "arrow/dataset/file_orc.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/scanner.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace dataset {

static inline Result<std::unique_ptr<arrow::adapters::orc::ORCFileReader>> OpenReader(
    const FileSource& source
    // const ipc::IpcReadOptions& options = default_read_options()
) {
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());

  std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader;

  auto status = arrow::adapters::orc::ORCFileReader::Open(std::move(input),
                                                          default_memory_pool(), &reader);
  if (!status.ok()) {
    return status.WithMessage("Could not open ORC input source '", source.path(),
                              "': ", status.message());
  }
  return reader;
}

/// \brief A ScanTask backed by an ORC file.
class OrcScanTask : public ScanTask {
 public:
  OrcScanTask(std::shared_ptr<FileFragment> fragment,
              std::shared_ptr<ScanOptions> options)
      : ScanTask(std::move(options), fragment), source_(fragment->source()) {}

  Result<RecordBatchIterator> Execute() override {
    ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source_));
    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    // TODO pass through ScanOptions batch size
    reader->NextStripeReader(1000, &batch_reader);

    auto batch_it = MakeIteratorFromReader(batch_reader);
    return batch_it;
  }

 private:
  FileSource source_;
};

Result<bool> OrcFileFormat::IsSupported(const FileSource& source) const {
  RETURN_NOT_OK(source.Open().status());
  return OpenReader(source).ok();
}

Result<std::shared_ptr<Schema>> OrcFileFormat::Inspect(const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source));
  std::shared_ptr<Schema> schema;
  auto status = reader->ReadSchema(&schema);
  return schema;
}

Result<ScanTaskIterator> OrcFileFormat::ScanFile(
    const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<FileFragment>& fragment) const {
  auto task = std::make_shared<OrcScanTask>(fragment, options);

  return MakeVectorIterator<std::shared_ptr<ScanTask>>({std::move(task)});
}

// //
// // IpcFileWriter, IpcFileWriteOptions
// //

std::shared_ptr<FileWriteOptions> OrcFileFormat::DefaultWriteOptions() {
  // TODO
  return NULLPTR;
}

Result<std::shared_ptr<FileWriter>> OrcFileFormat::MakeWriter(
    std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileWriteOptions> options,
    fs::FileLocator destination_locator) const {
  return Status::NotImplemented("ORC writer not yet implemented.");
}

}  // namespace dataset
}  // namespace arrow
