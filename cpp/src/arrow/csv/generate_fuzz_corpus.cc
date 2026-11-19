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

// A command line executable that generates a bunch of valid CSV files
// containing example record batches.  Each file is prefixed with a serialized
// CSV fuzz header (see csv/fuzz_header_internal.h) encoding the reader
// options matching the dialect used for writing.  Those are used as fuzzing
// seeds to make fuzzing more efficient.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/util.h"
#include "arrow/compute/cast.h"
#include "arrow/csv/fuzz_header_internal.h"
#include "arrow/csv/options.h"
#include "arrow/csv/writer.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/json/from_string.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/testing/random.h"
#include "arrow/util/io_util.h"

namespace arrow::csv {

using ::arrow::internal::CreateDir;
using ::arrow::internal::PlatformFilename;
using ::arrow::json::ArrayFromJSONString;

Result<std::shared_ptr<Buffer>> WriteRecordBatch(
    const std::shared_ptr<RecordBatch>& batch, const WriteOptions& options) {
  ARROW_ASSIGN_OR_RAISE(auto sink, io::BufferOutputStream::Create(1024));
  ARROW_ASSIGN_OR_RAISE(auto writer, MakeCSVWriter(sink.get(), batch->schema(), options));
  RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  RETURN_NOT_OK(writer->Close());
  return sink->Finish();
}

Result<std::shared_ptr<RecordBatch>> MakeBatch(
    std::function<Result<std::shared_ptr<Array>>(int64_t length, double null_probability)>
        array_factory,
    int64_t length) {
  ArrayVector columns;
  FieldVector fields;

  struct ColumnSpec {
    std::string name;
    double null_probability;
  };
  for (auto spec : {ColumnSpec{"with_nulls", 0.2}, ColumnSpec{"without_nulls", 0.0}}) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> column,
                          array_factory(length, spec.null_probability));
    columns.push_back(column);
    fields.push_back(field(spec.name, column->type()));
  }
  return RecordBatch::Make(schema(std::move(fields)), length, std::move(columns));
}

Result<RecordBatchVector> Batches() {
  ::arrow::random::RandomArrayGenerator gen(/*seed=*/42);
  RecordBatchVector batches;

  auto append_batch = [&](auto array_factory, int64_t length) -> Status {
    ARROW_ASSIGN_OR_RAISE(auto batch, MakeBatch(array_factory, length));
    batches.push_back(batch);
    return Status::OK();
  };

  // Ideally, we should exercise all possible inference kinds (see inference_internal.h)
  auto make_nulls = [&](int64_t length, double null_probability) {
    return MakeArrayOfNull(null(), length);
  };
  auto make_ints = [&](int64_t length, double null_probability) {
    return gen.Int64(length, /*min=*/-1'000'000, /*max=*/1'000'000, null_probability);
  };
  auto make_floats = [&](int64_t length, double null_probability) {
    return gen.Float64(length, /*min=*/-100.0, /*max=*/100.0, null_probability);
  };
  auto make_booleans = [&](int64_t length, double null_probability) {
    return gen.Boolean(length, /*true_probability=*/0.8, null_probability);
  };
  auto make_dates = [&](int64_t length, double null_probability) {
    return gen.Date64(length, /*min=*/1, /*max=*/365 * 60, null_probability);
  };
  auto make_times = [&](int64_t length, double null_probability) {
    return gen.Int32(length, /*min=*/0, /*max=*/86399, null_probability)
        ->View(time32(TimeUnit::SECOND));
  };

  std::string timezone;
  auto make_timestamps = [&](int64_t length, double null_probability) {
    return gen.Int64(length, /*min=*/1, /*max=*/1764079190, null_probability)
        ->View(timestamp(TimeUnit::SECOND, timezone));
  };
  auto make_timestamps_ns = [&](int64_t length, double null_probability) {
    return gen
        .Int64(length, /*min=*/1, /*max=*/1764079190LL * 1'000'000'000, null_probability)
        ->View(timestamp(TimeUnit::NANO, timezone));
  };

  auto make_strings = [&](int64_t length, double null_probability) {
    return gen.String(length, /*min_length=*/3, /*max_length=*/15, null_probability);
  };
  auto make_string_with_repeats = [&](int64_t length, double null_probability) {
    // `unique` should be less than `auto_dict_max_cardinality` in fuzz target
    return gen.StringWithRepeats(length, /*unique=*/10, /*min_length=*/3,
                                 /*max_length=*/15, null_probability);
  };

  RETURN_NOT_OK(append_batch(make_nulls, /*length=*/2000));
  RETURN_NOT_OK(append_batch(make_ints, /*length=*/500));
  RETURN_NOT_OK(append_batch(make_floats, /*length=*/150));
  RETURN_NOT_OK(append_batch(make_booleans, /*length=*/500));

  RETURN_NOT_OK(append_batch(make_dates, /*length=*/200));
  RETURN_NOT_OK(append_batch(make_times, /*length=*/400));
  timezone = "";
  RETURN_NOT_OK(append_batch(make_timestamps, /*length=*/200));
  RETURN_NOT_OK(append_batch(make_timestamps_ns, /*length=*/100));
  // Will generate timestamps with a "Z" suffix
  timezone = "UTC";
  RETURN_NOT_OK(append_batch(make_timestamps, /*length=*/200));
  RETURN_NOT_OK(append_batch(make_timestamps_ns, /*length=*/100));
  // Will generate timestamps with a "+0100" or "+0200" suffix
  timezone = "Europe/Paris";
  RETURN_NOT_OK(append_batch(make_timestamps, /*length=*/200));
  RETURN_NOT_OK(append_batch(make_timestamps_ns, /*length=*/100));

  RETURN_NOT_OK(append_batch(make_strings, /*length=*/300));
  RETURN_NOT_OK(append_batch(make_string_with_repeats, /*length=*/300));
  // XXX Cannot add non-UTF8 binary as the CSV writer doesn't support writing it

  return batches;
}

namespace {

// Default fuzz header values, matching the default CSV reader options
// (see ParseOptions, ReadOptions and ConvertOptions in csv/options.h).
internal::FuzzCsvHeader MakeDefaultFuzzCsvHeader() {
  internal::FuzzCsvHeader header{};
  internal::SetFuzzCsvMagic(header);
  // --- ParseOptions defaults ---
  header.parse_1.delimiter_id = 0;  // ','
  header.parse_1.quoting = 1;
  header.parse_1.quote_char_id = 0;  // '"'
  header.parse_1.double_quote = 1;
  header.parse_1.escaping = 0;
  header.parse_1.escape_char_id = 0;  // '\\'
  header.parse_2.newlines_in_values = 0;
  header.parse_2.ignore_empty_lines = 1;
  header.parse_2.pad_short_rows = 0;
  header.parse_2.ignore_extra_columns = 0;
  // --- ReadOptions defaults ---
  header.read.block_size_id = 3;  // 1 << 20 (the default block size)
  header.read.use_threads = 1;
  header.read.skip_rows = 0;
  header.read.autogenerate_column_names = 0;
  // --- ConvertOptions defaults ---
  header.convert_1.strings_can_be_null = 0;
  header.convert_1.quoted_strings_can_be_null = 1;
  header.convert_1.auto_dict_encode = 0;
  header.convert_1.auto_dict_max_cardinality_id = 0;  // 50
  header.convert_1.decimal_point_is_comma = 0;        // '.'
  header.convert_1.check_utf8 = 1;
  return header;
}

// Index of `delimiter` in the candidate delimiters of the fuzz header
uint8_t FuzzDelimiterId(char delimiter) {
  auto it = std::find(internal::kFuzzCsvDelimiters.begin(),
                      internal::kFuzzCsvDelimiters.end(), delimiter);
  return static_cast<uint8_t>(it - internal::kFuzzCsvDelimiters.begin());
}

using WriteAndHeaderOptions = std::pair<WriteOptions, internal::FuzzCsvHeader>;

// Generate several dialect variants of each batch, matching the option
// rotations performed by the fuzz target (see csv/fuzz.cc).  For each
// variant, also compute the fuzz header encoding the reader options that
// match the writing dialect.
std::vector<WriteAndHeaderOptions> SampleVariants() {
  std::vector<WriteAndHeaderOptions> variants;
  auto append_variant = [&](std::function<void(WriteOptions&)> write_fn,
                            std::function<void(internal::FuzzCsvHeader&)> header_fn) {
    auto write_options = WriteOptions::Defaults();
    write_fn(write_options);
    auto validation_st = write_options.Validate();
    if (!validation_st.ok()) {
      std::cerr << "Skipping invalid write options: " << validation_st.ToString()
                << std::endl;
      return;
    }
    auto header = MakeDefaultFuzzCsvHeader();
    header_fn(header);
    variants.emplace_back(std::move(write_options), std::move(header));
  };
  // Default dialect
  append_variant([](WriteOptions&) {}, [](internal::FuzzCsvHeader&) {});
  // Alternate delimiters
  append_variant(
      [](WriteOptions& o) { o.delimiter = ';'; },
      [](internal::FuzzCsvHeader& h) { h.parse_1.delimiter_id = FuzzDelimiterId(';'); });
  append_variant(
      [](WriteOptions& o) { o.delimiter = '\t'; },
      [](internal::FuzzCsvHeader& h) { h.parse_1.delimiter_id = FuzzDelimiterId('\t'); });
  // Quote all values (the writer uses '"' as the quoting character)
  append_variant([](WriteOptions& o) { o.quoting_style = QuotingStyle::AllValid; },
                 [](internal::FuzzCsvHeader&) {});
  // No header row, and let the reader autogenerate column names
  append_variant(
      [](WriteOptions& o) { o.include_header = false; },
      [](internal::FuzzCsvHeader& h) { h.read.autogenerate_column_names = 1; });
  // Write nulls as "N/A", and let the reader parse them as nulls
  // ("N/A" is one of the default null spellings in ConvertOptions)
  append_variant([](WriteOptions& o) { o.null_string = "N/A"; },
                 [](internal::FuzzCsvHeader& h) { h.convert_1.strings_can_be_null = 1; });
  return variants;
}

}  // namespace

Status DoMain(const std::string& out_dir) {
  ARROW_ASSIGN_OR_RAISE(auto dir_fn, PlatformFilename::FromString(out_dir));
  RETURN_NOT_OK(CreateDir(dir_fn));

  int sample_num = 1;
  auto sample_name = [&]() -> std::string {
    return "csv-file-" + std::to_string(sample_num++);
  };

  ARROW_ASSIGN_OR_RAISE(auto batches, Batches());
  const auto option_variants = SampleVariants();

  for (const auto& batch : batches) {
    RETURN_NOT_OK(batch->ValidateFull());
    for (const auto& [options, header] : option_variants) {
      ARROW_ASSIGN_OR_RAISE(auto buffer, WriteRecordBatch(batch, options));
      const std::string header_str = internal::SerializeFuzzCsvHeader(header);

      ARROW_ASSIGN_OR_RAISE(auto sample_fn, dir_fn.Join(sample_name()));
      std::cerr << sample_fn.ToString() << std::endl;
      ARROW_ASSIGN_OR_RAISE(auto file, io::FileOutputStream::Open(sample_fn.ToString()));
      RETURN_NOT_OK(
          file->Write(header_str.data(), static_cast<int64_t>(header_str.size())));
      RETURN_NOT_OK(file->Write(buffer));
      RETURN_NOT_OK(file->Close());
    }
  }
  return Status::OK();
}

ARROW_NORETURN void Usage() {
  std::cerr << "Usage: arrow-csv-generate-fuzz-corpus "
            << "<output directory>" << std::endl;
  std::exit(2);
}

int Main(int argc, char** argv) {
  if (argc != 2) {
    Usage();
  }
  auto out_dir = std::string(argv[1]);

  Status st = DoMain(out_dir);
  if (!st.ok()) {
    std::cerr << st.ToString() << std::endl;
    return 1;
  }
  return 0;
}

}  // namespace arrow::csv

int main(int argc, char** argv) { return arrow::csv::Main(argc, argv); }
