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
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/io/buffered.h"
#include "arrow/io/compressed.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace dataset {

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::Executor;
using internal::SerialExecutor;
using RecordBatchGenerator = std::function<Future<std::shared_ptr<RecordBatch>>()>;

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
        if (column_names.emplace(std::string(view)).second) {
          return Status::OK();
        }
        return Status::Invalid("CSV file contained multiple columns named ", view);
      }));

  return column_names;
}

static inline Result<csv::ConvertOptions> GetConvertOptions(
    const CsvFileFormat& format, const std::shared_ptr<ScanOptions>& scan_options,
    const util::string_view first_block, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto column_names,
                        GetColumnNames(format.parse_options, first_block, pool));

  ARROW_ASSIGN_OR_RAISE(
      auto csv_scan_options,
      GetFragmentScanOptions<CsvFragmentScanOptions>(
          kCsvTypeName, scan_options.get(), format.default_fragment_scan_options));
  auto convert_options = csv_scan_options->convert_options;
  auto materialized = scan_options->MaterializedFields();
  std::unordered_set<std::string> materialized_fields(materialized.begin(),
                                                      materialized.end());
  for (auto field : scan_options->dataset_schema->fields()) {
    if (materialized_fields.find(field->name()) == materialized_fields.end()) continue;
    // Ignore virtual columns.
    if (column_names.find(field->name()) == column_names.end()) continue;
    // Only read the requested columns
    convert_options.include_columns.push_back(field->name());
    // Properly set conversion types
    convert_options.column_types[field->name()] = field->type();
  }
  return convert_options;
}

static inline Result<csv::ReadOptions> GetReadOptions(
    const CsvFileFormat& format, const std::shared_ptr<ScanOptions>& scan_options) {
  ARROW_ASSIGN_OR_RAISE(
      auto csv_scan_options,
      GetFragmentScanOptions<CsvFragmentScanOptions>(
          kCsvTypeName, scan_options.get(), format.default_fragment_scan_options));
  auto read_options = csv_scan_options->read_options;
  // Multithreaded conversion of individual files would lead to excessive thread
  // contention when ScanTasks are also executed in multiple threads, so we disable it
  // here.  Also, this is a no-op since the streaming CSV reader is currently serial
  read_options.use_threads = false;
  return read_options;
}

static inline Future<std::shared_ptr<csv::StreamingReader>> OpenReaderAsync(
    const FileSource& source, const CsvFileFormat& format,
    const std::shared_ptr<ScanOptions>& scan_options, internal::Executor* cpu_executor,
    MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto reader_options, GetReadOptions(format, scan_options));

  ARROW_ASSIGN_OR_RAISE(auto input, source.OpenCompressed());
  ARROW_ASSIGN_OR_RAISE(
      input, io::BufferedInputStream::Create(reader_options.block_size,
                                             default_memory_pool(), std::move(input)));

  // Grab the first block and use it to determine the schema and create a reader.  The
  // input->Peek call blocks so we run the whole thing on the I/O thread pool.
  return DeferNotOk(input->io_context().executor()->Submit(
      [=]() -> Future<std::shared_ptr<csv::StreamingReader>> {
        ARROW_ASSIGN_OR_RAISE(auto first_block, input->Peek(reader_options.block_size));
        const auto& parse_options = format.parse_options;
        auto convert_options = csv::ConvertOptions::Defaults();
        if (scan_options != nullptr) {
          ARROW_ASSIGN_OR_RAISE(convert_options, GetConvertOptions(format, scan_options,
                                                                   first_block, pool));
        }

        auto reader_fut = csv::StreamingReader::MakeAsync(
            io::default_io_context(), std::move(input), cpu_executor, reader_options,
            parse_options, convert_options);
        // Adds the filename to the error
        return reader_fut.Then(
            [](const std::shared_ptr<csv::StreamingReader>& maybe_reader)
                -> Result<std::shared_ptr<csv::StreamingReader>> { return maybe_reader; },
            [source](const Status& err) -> Result<std::shared_ptr<csv::StreamingReader>> {
              return err.WithMessage("Could not open CSV input source '", source.path(),
                                     "': ", err);
            });
      }));
}

static inline Result<std::shared_ptr<csv::StreamingReader>> OpenReader(
    const FileSource& source, const CsvFileFormat& format,
    const std::shared_ptr<ScanOptions>& scan_options = nullptr,
    MemoryPool* pool = default_memory_pool()) {
  auto open_reader_fut =
      OpenReaderAsync(source, format, scan_options, internal::GetCpuThreadPool(), pool);
  return open_reader_fut.result();
}

static RecordBatchGenerator GeneratorFromReader(
    const Future<std::shared_ptr<csv::StreamingReader>>& reader) {
  auto gen_fut = reader.Then(
      [](const std::shared_ptr<csv::StreamingReader>& reader) -> RecordBatchGenerator {
        return [reader]() { return reader->ReadNextAsync(); };
      });
  return MakeFromFuture(std::move(gen_fut));
}

/// \brief A ScanTask backed by an Csv file.
class CsvScanTask : public ScanTask {
 public:
  CsvScanTask(std::shared_ptr<const CsvFileFormat> format,
              std::shared_ptr<ScanOptions> options,
              std::shared_ptr<FileFragment> fragment)
      : ScanTask(std::move(options), fragment),
        format_(std::move(format)),
        source_(fragment->source()) {}

  Result<RecordBatchIterator> Execute() override {
    auto reader_fut = OpenReaderAsync(source_, *format_, options(),
                                      internal::GetCpuThreadPool(), options()->pool);
    auto reader_gen = GeneratorFromReader(std::move(reader_fut));
    return MakeGeneratorIterator(std::move(reader_gen));
  }

  Future<RecordBatchVector> SafeExecute(internal::Executor* executor) override {
    auto reader_fut =
        OpenReaderAsync(source_, *format_, options(), executor, options()->pool);
    auto reader_gen = GeneratorFromReader(std::move(reader_fut));
    return CollectAsyncGenerator(reader_gen);
  }

  Future<> SafeVisit(
      internal::Executor* executor,
      std::function<Status(std::shared_ptr<RecordBatch>)> visitor) override {
    auto reader_fut =
        OpenReaderAsync(source_, *format_, options(), executor, options()->pool);
    auto reader_gen = GeneratorFromReader(std::move(reader_fut));
    return VisitAsyncGenerator(reader_gen, visitor);
  }

 private:
  std::shared_ptr<const CsvFileFormat> format_;
  FileSource source_;
};

bool CsvFileFormat::Equals(const FileFormat& format) const {
  if (type_name() != format.type_name()) return false;

  const auto& other_parse_options =
      checked_cast<const CsvFileFormat&>(format).parse_options;

  return parse_options.delimiter == other_parse_options.delimiter &&
         parse_options.quoting == other_parse_options.quoting &&
         parse_options.quote_char == other_parse_options.quote_char &&
         parse_options.double_quote == other_parse_options.double_quote &&
         parse_options.escaping == other_parse_options.escaping &&
         parse_options.escape_char == other_parse_options.escape_char &&
         parse_options.newlines_in_values == other_parse_options.newlines_in_values &&
         parse_options.ignore_empty_lines == other_parse_options.ignore_empty_lines;
}

Result<bool> CsvFileFormat::IsSupported(const FileSource& source) const {
  RETURN_NOT_OK(source.Open().status());
  return OpenReader(source, *this).ok();
}

Result<std::shared_ptr<Schema>> CsvFileFormat::Inspect(const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source, *this));
  return reader->schema();
}

Result<ScanTaskIterator> CsvFileFormat::ScanFile(
    const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<FileFragment>& fragment) const {
  auto this_ = checked_pointer_cast<const CsvFileFormat>(shared_from_this());
  auto task = std::make_shared<CsvScanTask>(std::move(this_), options, fragment);

  return MakeVectorIterator<std::shared_ptr<ScanTask>>({std::move(task)});
}

}  // namespace dataset
}  // namespace arrow
