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

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/csv/reader.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

using internal::checked_cast;
using internal::checked_pointer_cast;

Result<std::unordered_set<std::string>> GetColumnNames(
    const csv::ParseOptions& parse_options, util::string_view first_block,
    MemoryPool* pool) {
  uint32_t parsed_size = 0;
  csv::BlockParser parser(pool, parse_options, /*num_cols=*/-1,
                          /*max_num_rows=*/1);

  RETURN_NOT_OK(parser.Parse(util::string_view{first_block}, &parsed_size));

  if (parser.num_rows() != 1) {
    return Status::Invalid(
        "Could not read first row from CSV file, either "
        "file is truncated or header is larger than block size");
  }

  if (parser.num_cols() == 0) {
    return Status::Invalid("No columns in CSV file");
  }

  std::unordered_set<std::string> column_names;

  RETURN_NOT_OK(
      parser.VisitLastRow([&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
        util::string_view view{reinterpret_cast<const char*>(data), size};
        if (column_names.emplace(view.to_string()).second) {
          return Status::OK();
        }
        return Status::Invalid("CSV file contained multiple columns named ", view);
      }));

  return column_names;
}

static inline Result<csv::ConvertOptions> GetConvertOptions(
    const CsvFileFormat& format, const std::shared_ptr<ScanOptions>& scan_options,
    const Buffer& first_block, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto column_names,
      GetColumnNames(format.parse_options, util::string_view{first_block}, pool));

  auto convert_options = csv::ConvertOptions::Defaults();

  for (const auto& field : scan_options->schema()->fields()) {
    if (column_names.find(field->name()) == column_names.end()) continue;
    convert_options.column_types[field->name()] = field->type();
    convert_options.include_columns.push_back(field->name());
  }

  // FIXME(bkietz) also acquire types of fields materialized but not projected.
  for (auto&& name : FieldsInExpression(scan_options->filter)) {
    ARROW_ASSIGN_OR_RAISE(auto match,
                          FieldRef(name).FindOneOrNone(*scan_options->schema()));
    if (match.indices().empty()) {
      convert_options.include_columns.push_back(std::move(name));
    }
  }
  return convert_options;
}

static inline csv::ReadOptions GetReadOptions(const CsvFileFormat& format) {
  auto read_options = csv::ReadOptions::Defaults();
  // Multithreaded conversion of individual files would lead to excessive thread
  // contention when ScanTasks are also executed in multiple threads, so we disable it
  // here.
  read_options.use_threads = false;
  return read_options;
}

static inline Result<std::shared_ptr<csv::StreamingReader>> OpenReader(
    const FileSource& source, const CsvFileFormat& format,
    const std::shared_ptr<ScanOptions>& scan_options = nullptr,
    MemoryPool* pool = default_memory_pool()) {
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());

  auto reader_options = GetReadOptions(format);
  ARROW_ASSIGN_OR_RAISE(auto first_block, input->ReadAt(0, reader_options.block_size));
  RETURN_NOT_OK(input->Seek(0));

  const auto& parse_options = format.parse_options;

  ARROW_ASSIGN_OR_RAISE(
      auto convert_options,
      scan_options == nullptr
          ? ToResult(csv::ConvertOptions::Defaults())
          : GetConvertOptions(format, scan_options, *first_block, pool));

  auto maybe_reader = csv::StreamingReader::Make(pool, std::move(input), reader_options,
                                                 parse_options, convert_options);
  if (!maybe_reader.ok()) {
    return maybe_reader.status().WithMessage("Could not open CSV input source '",
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
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          OpenReader(source_, *format_, options(), context()->pool));
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

Result<ScanTaskIterator> CsvFileFormat::ScanFile(std::shared_ptr<ScanOptions> options,
                                                 std::shared_ptr<ScanContext> context,
                                                 FileFragment* fragment) const {
  auto this_ = checked_pointer_cast<const CsvFileFormat>(shared_from_this());
  auto task = std::make_shared<CsvScanTask>(std::move(this_), fragment->source(),
                                            std::move(options), std::move(context));

  return MakeVectorIterator<std::shared_ptr<ScanTask>>({std::move(task)});
}

}  // namespace dataset
}  // namespace arrow
