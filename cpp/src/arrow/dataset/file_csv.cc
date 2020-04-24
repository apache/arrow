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

#include "arrow/dataset/file_csv.h"

#include <memory>
#include <string>

#include "arrow/csv/options.h"
#include "arrow/csv/reader.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/result.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

using internal::checked_cast;
using internal::checked_pointer_cast;

static inline csv::ReadOptions default_read_options() {
  auto defaults = csv::ReadOptions::Defaults();
  defaults.use_threads = false;
  return defaults;
}

static inline Result<std::shared_ptr<csv::StreamingReader>> OpenReader(
    const FileSource& source, const CsvFileFormat& format) {
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());

  auto maybe_reader = csv::StreamingReader::Make(
      default_memory_pool(), std::move(input), default_read_options(),
      format.parse_options, format.convert_options);
  if (!maybe_reader.ok()) {
    return maybe_reader.status().WithMessage("Could not open IPC input source '",
                                             source.path(), "': ", maybe_reader.status());
  }

  return std::move(maybe_reader).ValueOrDie();
}

/// \brief A ScanTask backed by an Csv file.
class CsvScanTask : public ScanTask {
 public:
  CsvScanTask(std::shared_ptr<const CsvFileFormat> format, FileSource source,
              std::shared_ptr<ScanOptions> options, std::shared_ptr<ScanContext> context)
      : ScanTask(std::move(options), std::move(context)),
        format_(std::move(format)),
        source_(std::move(source)) {}

  Result<RecordBatchIterator> Execute() override {
    ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source_, *format_));
    return IteratorFromReader(std::move(reader));
  }

 private:
  std::shared_ptr<const CsvFileFormat> format_;
  FileSource source_;
};

Result<bool> CsvFileFormat::IsSupported(const FileSource& source) const {
  RETURN_NOT_OK(source.Open().status());
  return OpenReader(source, *this).ok();
}

Result<std::shared_ptr<Schema>> CsvFileFormat::Inspect(const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source, *this));
  return reader->schema();
}

Result<ScanTaskIterator> CsvFileFormat::ScanFile(
    const FileSource& source, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<ScanContext> context) const {
  auto this_ = checked_pointer_cast<const CsvFileFormat>(shared_from_this());
  auto task = std::make_shared<CsvScanTask>(std::move(this_), source, std::move(options),
                                            std::move(context));

  return MakeVectorIterator<std::shared_ptr<ScanTask>>({std::move(task)});
}

}  // namespace dataset
}  // namespace arrow
