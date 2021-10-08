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

#include <memory>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/scanner.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace dataset {

namespace {

Result<std::unique_ptr<arrow::adapters::orc::ORCFileReader>> OpenORCReader(
    const FileSource& source,
    const std::shared_ptr<ScanOptions>& scan_options = nullptr) {
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());

  arrow::MemoryPool* pool;
  if (scan_options) {
    pool = scan_options->pool;
  } else {
    pool = default_memory_pool();
  }

  auto reader = arrow::adapters::orc::ORCFileReader::Open(std::move(input), pool);
  auto status = reader.status();
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
    struct Impl {
      static Result<RecordBatchIterator> Make(const FileSource& source,
                                              const FileFormat& format,
                                              const ScanOptions& scan_options) {
        ARROW_ASSIGN_OR_RAISE(
            auto reader,
            OpenORCReader(source, std::make_shared<ScanOptions>(scan_options)));
        int num_stripes = reader->NumberOfStripes();
        return RecordBatchIterator(Impl{std::move(reader), 0, num_stripes});
      }

      Result<std::shared_ptr<RecordBatch>> Next() {
        if (i_ == num_stripes_) {
          return nullptr;
        }
        std::shared_ptr<RecordBatch> batch;
        // TODO (https://issues.apache.org/jira/browse/ARROW-13797)
        // determine included fields from options_->MaterializedFields() to
        // optimize the column selection (see _column_index_lookup in python
        // orc.py for custom logic)
        // std::vector<int> included_fields;
        // TODO (https://issues.apache.org/jira/browse/ARROW-14153)
        // pass scan_options_->batch_size
        return reader_->ReadStripe(i_++);
      }

      std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader_;
      int i_;
      int num_stripes_;
    };

    return Impl::Make(source_, *checked_pointer_cast<FileFragment>(fragment_)->format(),
                      *options_);
  }

 private:
  FileSource source_;
};

class OrcScanTaskIterator {
 public:
  static Result<ScanTaskIterator> Make(std::shared_ptr<ScanOptions> options,
                                       std::shared_ptr<FileFragment> fragment) {
    return ScanTaskIterator(OrcScanTaskIterator(std::move(options), std::move(fragment)));
  }

  Result<std::shared_ptr<ScanTask>> Next() {
    if (once_) {
      // Iteration is done.
      return nullptr;
    }

    once_ = true;
    return std::shared_ptr<ScanTask>(new OrcScanTask(fragment_, options_));
  }

 private:
  OrcScanTaskIterator(std::shared_ptr<ScanOptions> options,
                      std::shared_ptr<FileFragment> fragment)
      : options_(std::move(options)), fragment_(std::move(fragment)) {}

  bool once_ = false;
  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<FileFragment> fragment_;
};

}  // namespace

Result<bool> OrcFileFormat::IsSupported(const FileSource& source) const {
  RETURN_NOT_OK(source.Open().status());
  return OpenORCReader(source).ok();
}

Result<std::shared_ptr<Schema>> OrcFileFormat::Inspect(const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenORCReader(source));
  return reader->ReadSchema();
}

Result<ScanTaskIterator> OrcFileFormat::ScanFile(
    const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<FileFragment>& fragment) const {
  return OrcScanTaskIterator::Make(options, fragment);
}

Future<util::optional<int64_t>> OrcFileFormat::CountRows(
    const std::shared_ptr<FileFragment>& file, compute::Expression predicate,
    const std::shared_ptr<ScanOptions>& options) {
  if (ExpressionHasFieldRefs(predicate)) {
    return Future<util::optional<int64_t>>::MakeFinished(util::nullopt);
  }
  auto self = checked_pointer_cast<OrcFileFormat>(shared_from_this());
  return DeferNotOk(options->io_context.executor()->Submit(
      [self, file]() -> Result<util::optional<int64_t>> {
        ARROW_ASSIGN_OR_RAISE(auto reader, OpenORCReader(file->source()));
        return reader->NumberOfRows();
      }));
}

// //
// // OrcFileWriter, OrcFileWriteOptions
// //

std::shared_ptr<FileWriteOptions> OrcFileFormat::DefaultWriteOptions() {
  // TODO (https://issues.apache.org/jira/browse/ARROW-13796)
  return nullptr;
}

Result<std::shared_ptr<FileWriter>> OrcFileFormat::MakeWriter(
    std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileWriteOptions> options,
    fs::FileLocator destination_locator) const {
  // TODO (https://issues.apache.org/jira/browse/ARROW-13796)
  return Status::NotImplemented("ORC writer not yet implemented.");
}

}  // namespace dataset
}  // namespace arrow
