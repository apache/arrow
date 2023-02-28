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
#include "arrow/util/bit_util.h"
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

struct CsvInspectedFragment : public InspectedFragment {
  CsvInspectedFragment(std::vector<std::string> column_names,
                       std::shared_ptr<io::InputStream> input_stream, int64_t num_bytes)
      : InspectedFragment(std::move(column_names)),
        input_stream(std::move(input_stream)),
        num_bytes(num_bytes) {}
  // We need to start reading the file in order to figure out the column names and
  // so we save off the input stream
  std::shared_ptr<io::InputStream> input_stream;
  int64_t num_bytes;
};

class CsvFileScanner : public FragmentScanner {
 public:
  CsvFileScanner(std::shared_ptr<csv::StreamingReader> reader, int num_batches,
                 int64_t best_guess_bytes_per_batch)
      : reader_(std::move(reader)),
        num_batches_(num_batches),
        best_guess_bytes_per_batch_(best_guess_bytes_per_batch) {}

  Future<std::shared_ptr<RecordBatch>> ScanBatch(int batch_number) override {
    // This should be called in increasing order but let's verify that in case it changes.
    // It would be easy enough to handle out of order but no need for that complexity at
    // the moment.
    DCHECK_EQ(scanned_so_far_++, batch_number);
    return reader_->ReadNextAsync();
  }

  int64_t EstimatedDataBytes(int batch_number) override {
    return best_guess_bytes_per_batch_;
  }

  int NumBatches() override { return num_batches_; }

  static Result<csv::ConvertOptions> GetConvertOptions(
      const CsvFragmentScanOptions& csv_options, const FragmentScanRequest& scan_request,
      const CsvInspectedFragment& inspected_fragment) {
    // We use the convert options given from the user but override which columns we are
    // looking for.
    auto convert_options = csv_options.convert_options;
    std::vector<std::string> columns;
    std::unordered_map<std::string, std::shared_ptr<DataType>> column_types;
    for (const auto& scan_column : scan_request.fragment_selection->columns()) {
      if (scan_column.path.indices().size() != 1) {
        return Status::Invalid("CSV reader does not supported nested references");
      }
      const std::string& column_name =
          inspected_fragment.column_names[scan_column.path.indices()[0]];
      columns.push_back(column_name);
      column_types[column_name] = scan_column.requested_type->GetSharedPtr();
    }
    convert_options.include_columns = std::move(columns);
    convert_options.column_types = std::move(column_types);
    return std::move(convert_options);
  }

  static Future<std::shared_ptr<FragmentScanner>> Make(
      const CsvFragmentScanOptions& csv_options, const FragmentScanRequest& scan_request,
      const CsvInspectedFragment& inspected_fragment, Executor* cpu_executor) {
    auto read_options = csv_options.read_options;

    int num_batches = static_cast<int>(bit_util::CeilDiv(
        inspected_fragment.num_bytes, static_cast<int64_t>(read_options.block_size)));
    // Could be better, but a reasonable starting point.  CSV presumably takes up more
    // space than an in-memory format so this should be conservative.
    int64_t best_guess_bytes_per_batch = read_options.block_size;
    ARROW_ASSIGN_OR_RAISE(
        csv::ConvertOptions convert_options,
        GetConvertOptions(csv_options, scan_request, inspected_fragment));

    return csv::StreamingReader::MakeAsync(
               io::default_io_context(), inspected_fragment.input_stream, cpu_executor,
               read_options, csv_options.parse_options, convert_options)
        .Then([num_batches, best_guess_bytes_per_batch](
                  const std::shared_ptr<csv::StreamingReader>& reader)
                  -> std::shared_ptr<FragmentScanner> {
          return std::make_shared<CsvFileScanner>(reader, num_batches,
                                                  best_guess_bytes_per_batch);
        });
  }

 private:
  std::shared_ptr<csv::StreamingReader> reader_;
  int num_batches_;
  int64_t best_guess_bytes_per_batch_;

  int scanned_so_far_ = 0;
};

using RecordBatchGenerator = std::function<Future<std::shared_ptr<RecordBatch>>()>;

Result<std::vector<std::string>> GetOrderedColumnNames(
    const csv::ReadOptions& read_options, const csv::ParseOptions& parse_options,
    std::string_view first_block, MemoryPool* pool) {
  // Skip BOM when reading column names (ARROW-14644, ARROW-17382)
  auto size = first_block.length();
  const uint8_t* data = reinterpret_cast<const uint8_t*>(first_block.data());
  ARROW_ASSIGN_OR_RAISE(auto data_no_bom, util::SkipUTF8BOM(data, size));
  size = size - static_cast<uint32_t>(data_no_bom - data);
  first_block = std::string_view(reinterpret_cast<const char*>(data_no_bom), size);
  if (!read_options.column_names.empty()) {
    return read_options.column_names;
  }

  uint32_t parsed_size = 0;
  int32_t max_num_rows = read_options.skip_rows + 1;
  csv::BlockParser parser(pool, parse_options, /*num_cols=*/-1, /*first_row=*/1,
                          max_num_rows);

  RETURN_NOT_OK(parser.Parse(std::string_view{first_block}, &parsed_size));

  if (parser.num_rows() != max_num_rows) {
    return Status::Invalid("Could not read first ", max_num_rows,
                           " rows from CSV file, either file is truncated or"
                           " header is larger than block size");
  }

  if (parser.num_cols() == 0) {
    return Status::Invalid("No columns in CSV file");
  }

  std::vector<std::string> column_names;

  if (read_options.autogenerate_column_names) {
    column_names.reserve(parser.num_cols());
    for (int32_t i = 0; i < parser.num_cols(); ++i) {
      std::stringstream ss;
      ss << "f" << i;
      column_names.emplace_back(ss.str());
    }
    return column_names;
  }

  RETURN_NOT_OK(
      parser.VisitLastRow([&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
        std::string_view view{reinterpret_cast<const char*>(data), size};
        column_names.emplace_back(view);
        return Status::OK();
      }));

  return column_names;
}

Result<std::unordered_set<std::string>> GetColumnNames(
    const csv::ReadOptions& read_options, const csv::ParseOptions& parse_options,
    std::string_view first_block, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      std::vector<std::string> ordered_names,
      GetOrderedColumnNames(read_options, parse_options, first_block, pool));
  std::unordered_set<std::string> unordered_names;
  for (const auto& column : ordered_names) {
    if (!unordered_names.emplace(column).second) {
      return Status::Invalid("CSV file contained multiple columns named ", column);
    }
  }
  return unordered_names;
}

static inline Result<csv::ConvertOptions> GetConvertOptions(
    const CsvFileFormat& format, const ScanOptions* scan_options,
    const std::string_view first_block) {
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

CsvFileFormat::CsvFileFormat() : FileFormat(std::make_shared<CsvFragmentScanOptions>()) {}

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

Future<std::optional<int64_t>> CsvFileFormat::CountRows(
    const std::shared_ptr<FileFragment>& file, compute::Expression predicate,
    const std::shared_ptr<ScanOptions>& options) {
  if (ExpressionHasFieldRefs(predicate)) {
    return Future<std::optional<int64_t>>::MakeFinished(std::nullopt);
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
      .Then([](int64_t count) { return std::make_optional<int64_t>(count); });
}

Future<std::shared_ptr<FragmentScanner>> CsvFileFormat::BeginScan(
    const FragmentScanRequest& request, const InspectedFragment& inspected_fragment,
    const FragmentScanOptions* format_options, compute::ExecContext* exec_context) const {
  auto csv_options = static_cast<const CsvFragmentScanOptions*>(format_options);
  auto csv_fragment = static_cast<const CsvInspectedFragment&>(inspected_fragment);
  return CsvFileScanner::Make(*csv_options, request, csv_fragment,
                              exec_context->executor());
}

Result<std::shared_ptr<InspectedFragment>> DoInspectFragment(
    const FileSource& source, const CsvFragmentScanOptions& csv_options,
    compute::ExecContext* exec_context) {
  ARROW_ASSIGN_OR_RAISE(auto input, source.OpenCompressed());
  if (csv_options.stream_transform_func) {
    ARROW_ASSIGN_OR_RAISE(input, csv_options.stream_transform_func(input));
  }
  ARROW_ASSIGN_OR_RAISE(
      input, io::BufferedInputStream::Create(csv_options.read_options.block_size,
                                             default_memory_pool(), std::move(input)));

  ARROW_ASSIGN_OR_RAISE(std::string_view first_block,
                        input->Peek(csv_options.read_options.block_size));

  ARROW_ASSIGN_OR_RAISE(
      std::vector<std::string> column_names,
      GetOrderedColumnNames(csv_options.read_options, csv_options.parse_options,
                            first_block, exec_context->memory_pool()));
  return std::make_shared<CsvInspectedFragment>(std::move(column_names), std::move(input),
                                                source.Size());
}

Future<std::shared_ptr<InspectedFragment>> CsvFileFormat::InspectFragment(
    const FileSource& source, const FragmentScanOptions* format_options,
    compute::ExecContext* exec_context) const {
  auto csv_options = static_cast<const CsvFragmentScanOptions*>(format_options);
  Executor* io_executor;
  if (source.filesystem()) {
    io_executor = source.filesystem()->io_context().executor();
  } else {
    io_executor = exec_context->executor();
  }
  return DeferNotOk(io_executor->Submit([source, csv_options, exec_context]() {
    return DoInspectFragment(source, *csv_options, exec_context);
  }));
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
