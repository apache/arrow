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
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>

#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/csv/reader.h"
#include "arrow/csv/writer.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/io/buffered.h"
#include "arrow/io/compressed.h"
#include "arrow/ipc/writer.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/utf8.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::Executor;
using internal::SerialExecutor;

namespace dataset {

using RecordBatchGenerator = std::function<Future<std::shared_ptr<RecordBatch>>()>;

Result<std::unordered_set<std::string>> GetColumnNames(
    const csv::ReadOptions& read_options, const csv::ParseOptions& parse_options,
    util::string_view first_block, MemoryPool* pool) {
  // Skip BOM when reading column names (ARROW-14644, ARROW-17382)
  auto size = first_block.length();
  const uint8_t* data = reinterpret_cast<const uint8_t*>(first_block.data());
  ARROW_ASSIGN_OR_RAISE(auto data_no_bom, util::SkipUTF8BOM(data, size));
  size = size - static_cast<uint32_t>(data_no_bom - data);
  first_block = util::string_view(reinterpret_cast<const char*>(data_no_bom), size);
  if (!read_options.column_names.empty()) {
    std::unordered_set<std::string> column_names;
    for (const auto& s : read_options.column_names) {
      if (!column_names.emplace(s).second) {
        return Status::Invalid("CSV file contained multiple columns named ", s);
      }
    }
    return column_names;
  }

  uint32_t parsed_size = 0;
  int32_t max_num_rows = read_options.skip_rows + 1;
  csv::BlockParser parser(pool, parse_options, /*num_cols=*/-1, /*first_row=*/1,
                          max_num_rows);

  RETURN_NOT_OK(parser.Parse(util::string_view{first_block}, &parsed_size));

  if (parser.num_rows() != max_num_rows) {
    return Status::Invalid("Could not read first ", max_num_rows,
                           " rows from CSV file, either file is truncated or"
                           " header is larger than block size");
  }

  if (parser.num_cols() == 0) {
    return Status::Invalid("No columns in CSV file");
  }

  std::unordered_set<std::string> column_names;

  if (read_options.autogenerate_column_names) {
    column_names.reserve(parser.num_cols());
    for (int32_t i = 0; i < parser.num_cols(); ++i) {
      std::stringstream ss;
      ss << "f" << i;
      column_names.emplace(ss.str());
    }
    return column_names;
  }

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
    const CsvFileFormat& format, const ScanOptions* scan_options,
    const util::string_view first_block) {
  ARROW_ASSIGN_OR_RAISE(
      auto csv_scan_options,
      GetFragmentScanOptions<CsvFragmentScanOptions>(
          kCsvTypeName, scan_options, format.default_fragment_scan_options));
  ARROW_ASSIGN_OR_RAISE(
      auto column_names,
      GetColumnNames(csv_scan_options->read_options, format.parse_options, first_block,
                     scan_options ? scan_options->pool : default_memory_pool()));

  auto convert_options = csv_scan_options->convert_options;

  if (!scan_options) return convert_options;

  auto field_refs = scan_options->MaterializedFields();
  std::unordered_set<std::string> materialized_fields;
  materialized_fields.reserve(field_refs.size());
  // Preprocess field refs. We try to avoid FieldRef::GetFoo here since that's
  // quadratic (and this is significant overhead with 1000+ columns)
  for (const auto& ref : field_refs) {
    if (const std::string* name = ref.name()) {
      // Common case
      materialized_fields.emplace(*name);
      continue;
    }
    // Currently CSV reader doesn't support reading any nested types, so this
    // path shouldn't be hit. However, implement it in the same way as IPC/ORC:
    // load the entire top-level field if a nested field is selected.
    ARROW_ASSIGN_OR_RAISE(auto field, ref.GetOneOrNone(*scan_options->dataset_schema));
    if (column_names.find(field->name()) == column_names.end()) continue;
    // Only read the requested columns
    convert_options.include_columns.push_back(field->name());
    // Properly set conversion types
    convert_options.column_types[field->name()] = field->type();
  }

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
    const std::shared_ptr<ScanOptions>& scan_options, Executor* cpu_executor) {
#ifdef ARROW_WITH_OPENTELEMETRY
  auto tracer = arrow::internal::tracing::GetTracer();
  auto span = tracer->StartSpan("arrow::dataset::CsvFileFormat::OpenReaderAsync");
#endif
  ARROW_ASSIGN_OR_RAISE(
      auto fragment_scan_options,
      GetFragmentScanOptions<CsvFragmentScanOptions>(
          kCsvTypeName, scan_options.get(), format.default_fragment_scan_options));
  ARROW_ASSIGN_OR_RAISE(auto reader_options, GetReadOptions(format, scan_options));
  ARROW_ASSIGN_OR_RAISE(auto input, source.OpenCompressed());
  if (fragment_scan_options->stream_transform_func) {
    ARROW_ASSIGN_OR_RAISE(input, fragment_scan_options->stream_transform_func(input));
  }
  const auto& path = source.path();
  ARROW_ASSIGN_OR_RAISE(
      input, io::BufferedInputStream::Create(reader_options.block_size,
                                             default_memory_pool(), std::move(input)));

  // Grab the first block and use it to determine the schema and create a reader.  The
  // input->Peek call blocks so we run the whole thing on the I/O thread pool.
  auto reader_fut = DeferNotOk(input->io_context().executor()->Submit(
      [=]() -> Future<std::shared_ptr<csv::StreamingReader>> {
        ARROW_ASSIGN_OR_RAISE(auto first_block, input->Peek(reader_options.block_size));
        const auto& parse_options = format.parse_options;
        ARROW_ASSIGN_OR_RAISE(
            auto convert_options,
            GetConvertOptions(format, scan_options ? scan_options.get() : nullptr,
                              first_block));
        return csv::StreamingReader::MakeAsync(io::default_io_context(), std::move(input),
                                               cpu_executor, reader_options,
                                               parse_options, convert_options);
      }));
  return reader_fut.Then(
      // Adds the filename to the error
      [=](const std::shared_ptr<csv::StreamingReader>& reader)
          -> Result<std::shared_ptr<csv::StreamingReader>> {
#ifdef ARROW_WITH_OPENTELEMETRY
        span->SetStatus(opentelemetry::trace::StatusCode::kOk);
        span->End();
#endif
        return reader;
      },
      [=](const Status& err) -> Result<std::shared_ptr<csv::StreamingReader>> {
#ifdef ARROW_WITH_OPENTELEMETRY
        arrow::internal::tracing::MarkSpan(err, span.get());
        span->End();
#endif
        return err.WithMessage("Could not open CSV input source '", path, "': ", err);
      });
}

static inline Result<std::shared_ptr<csv::StreamingReader>> OpenReader(
    const FileSource& source, const CsvFileFormat& format,
    const std::shared_ptr<ScanOptions>& scan_options = nullptr) {
  auto open_reader_fut = OpenReaderAsync(source, format, scan_options,
                                         ::arrow::internal::GetCpuThreadPool());
  return open_reader_fut.result();
}

static RecordBatchGenerator GeneratorFromReader(
    const Future<std::shared_ptr<csv::StreamingReader>>& reader, int64_t batch_size) {
  auto gen_fut = reader.Then(
      [batch_size](
          const std::shared_ptr<csv::StreamingReader>& reader) -> RecordBatchGenerator {
        auto batch_gen = [reader]() { return reader->ReadNextAsync(); };
        return MakeChunkedBatchGenerator(std::move(batch_gen), batch_size);
      });
  return MakeFromFuture(std::move(gen_fut));
}

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

Result<RecordBatchGenerator> CsvFileFormat::ScanBatchesAsync(
    const std::shared_ptr<ScanOptions>& scan_options,
    const std::shared_ptr<FileFragment>& file) const {
  auto this_ = checked_pointer_cast<const CsvFileFormat>(shared_from_this());
  auto source = file->source();
  auto reader_fut =
      OpenReaderAsync(source, *this, scan_options, ::arrow::internal::GetCpuThreadPool());
  auto generator = GeneratorFromReader(std::move(reader_fut), scan_options->batch_size);
  WRAP_ASYNC_GENERATOR_WITH_CHILD_SPAN(
      generator, "arrow::dataset::CsvFileFormat::ScanBatchesAsync::Next");
  return generator;
}

Future<util::optional<int64_t>> CsvFileFormat::CountRows(
    const std::shared_ptr<FileFragment>& file, compute::Expression predicate,
    const std::shared_ptr<ScanOptions>& options) {
  if (ExpressionHasFieldRefs(predicate)) {
    return Future<util::optional<int64_t>>::MakeFinished(util::nullopt);
  }
  auto self = checked_pointer_cast<CsvFileFormat>(shared_from_this());
  ARROW_ASSIGN_OR_RAISE(
      auto fragment_scan_options,
      GetFragmentScanOptions<CsvFragmentScanOptions>(
          kCsvTypeName, options.get(), self->default_fragment_scan_options));
  ARROW_ASSIGN_OR_RAISE(auto read_options, GetReadOptions(*self, options));
  ARROW_ASSIGN_OR_RAISE(auto input, file->source().OpenCompressed());
  if (fragment_scan_options->stream_transform_func) {
    ARROW_ASSIGN_OR_RAISE(input, fragment_scan_options->stream_transform_func(input));
  }
  return csv::CountRowsAsync(options->io_context, std::move(input),
                             ::arrow::internal::GetCpuThreadPool(), read_options,
                             self->parse_options)
      .Then([](int64_t count) { return util::make_optional<int64_t>(count); });
}

//
// CsvFileWriter, CsvFileWriteOptions
//

std::shared_ptr<FileWriteOptions> CsvFileFormat::DefaultWriteOptions() {
  std::shared_ptr<CsvFileWriteOptions> csv_options(
      new CsvFileWriteOptions(shared_from_this()));
  csv_options->write_options =
      std::make_shared<csv::WriteOptions>(csv::WriteOptions::Defaults());
  return csv_options;
}

Result<std::shared_ptr<FileWriter>> CsvFileFormat::MakeWriter(
    std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileWriteOptions> options,
    fs::FileLocator destination_locator) const {
  if (!Equals(*options->format())) {
    return Status::TypeError("Mismatching format/write options.");
  }
  auto csv_options = checked_pointer_cast<CsvFileWriteOptions>(options);
  ARROW_ASSIGN_OR_RAISE(
      auto writer, csv::MakeCSVWriter(destination, schema, *csv_options->write_options));
  return std::shared_ptr<FileWriter>(
      new CsvFileWriter(std::move(destination), std::move(writer), std::move(schema),
                        std::move(csv_options), std::move(destination_locator)));
}

CsvFileWriter::CsvFileWriter(std::shared_ptr<io::OutputStream> destination,
                             std::shared_ptr<ipc::RecordBatchWriter> writer,
                             std::shared_ptr<Schema> schema,
                             std::shared_ptr<CsvFileWriteOptions> options,
                             fs::FileLocator destination_locator)
    : FileWriter(std::move(schema), std::move(options), std::move(destination),
                 std::move(destination_locator)),
      batch_writer_(std::move(writer)) {}

Status CsvFileWriter::Write(const std::shared_ptr<RecordBatch>& batch) {
  return batch_writer_->WriteRecordBatch(*batch);
}

Future<> CsvFileWriter::FinishInternal() {
  // The CSV writer's Close() is a no-op, so just treat it as synchronous
  RETURN_NOT_OK(batch_writer_->Close());
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
