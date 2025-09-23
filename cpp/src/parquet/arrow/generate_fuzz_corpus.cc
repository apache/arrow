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

// A command line executable that generates a bunch of valid Parquet files
// containing example record batches.  Those are used as fuzzing seeds
// to make fuzzing more efficient.

#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/util.h"
#include "arrow/io/file.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/random.h"
#include "arrow/util/compression.h"
#include "arrow/util/io_util.h"
#include "arrow/util/key_value_metadata.h"
#include "parquet/arrow/writer.h"

namespace arrow {

using ::arrow::internal::CreateDir;
using ::arrow::internal::PlatformFilename;
using ::arrow::util::Float16;
using ::parquet::ArrowWriterProperties;
using ::parquet::WriterProperties;

static constexpr int32_t kBatchSize = 1000;
// This will emit several row groups
static constexpr int32_t kChunkSize = kBatchSize * 3 / 8;

struct WriteConfig {
  std::shared_ptr<WriterProperties> writer_properties;
  std::shared_ptr<ArrowWriterProperties> arrow_writer_properties;
};

struct Column {
  std::string name;
  std::shared_ptr<Array> array;

  static std::function<std::string()> NameGenerator() {
    struct Gen {
      int num_col = 1;

      std::string operator()() {
        std::stringstream ss;
        ss << "col_" << num_col++;
        return std::move(ss).str();
      }
    };
    return Gen{};
  }
};

std::vector<WriteConfig> GetWriteConfigurations() {
  // clang-format off
  auto w_brotli = WriterProperties::Builder()
      .disable_dictionary("no_dict")
      ->compression("compressed", Compression::BROTLI)
      // Override current default of 1MB
      ->data_pagesize(20'000)
      // Reduce max dictionary page size so that less pages are dict-encoded.
      ->dictionary_pagesize_limit(1'000)
      // Emit various physical types for decimal columns
      ->enable_store_decimal_as_integer()
      ->build();
  // Store the Arrow schema so as to exercise more data types when reading
  auto a_default = ArrowWriterProperties::Builder{}
      .store_schema()
      ->build();
  // clang-format on

  std::vector<WriteConfig> configs;
  configs.push_back({w_brotli, a_default});
  return configs;
}

Result<std::shared_ptr<RecordBatch>> ExampleBatch1() {
  constexpr double kNullProbability = 0.2;

  random::RandomArrayGenerator gen(42);
  auto name_gen = Column::NameGenerator();

  auto field_for_array_named = [&](const std::shared_ptr<Array>& array,
                                   std::string name) {
    return field(std::move(name), array->type(), /*nullable=*/array->null_count() != 0);
  };
  auto field_for_array = [&](const std::shared_ptr<Array>& array) {
    return field_for_array_named(array, name_gen());
  };

  std::vector<Column> columns;

  auto int16_array = gen.Int16(kBatchSize, -30000, 30000, kNullProbability);
  auto int32_array = gen.Int32(kBatchSize, -2000000000, 2000000000, kNullProbability);
  auto int64_array = gen.Int64(kBatchSize, -9000000000000000000LL, 9000000000000000000LL,
                               kNullProbability);
  auto non_null_float64_array =
      gen.Float64(kBatchSize, -1e10, 1e10, /*null_probability=*/0.0);
  auto tiny_strings_array = gen.String(kBatchSize, 0, 3, kNullProbability);
  auto large_strings_array =
      gen.LargeString(kBatchSize, /*min_length=*/0, /*max_length=*/20, kNullProbability);
  auto string_view_array =
      gen.StringView(kBatchSize, /*min_length=*/8, /*max_length=*/30, kNullProbability);
  ARROW_ASSIGN_OR_RAISE(auto null_array, MakeArrayOfNull(null(), kBatchSize));

  // Null
  columns.push_back({name_gen(), null_array});
  // Numerics
  columns.push_back({name_gen(), int16_array});
  columns.push_back({name_gen(), non_null_float64_array});
  columns.push_back(
      {name_gen(), gen.Float16(kBatchSize, Float16::FromDouble(-1e4),
                               Float16::FromDouble(1e4), kNullProbability)});
  columns.push_back({name_gen(), int64_array});
  // Decimals
  columns.push_back(
      {name_gen(), gen.Decimal128(decimal128(24, 7), kBatchSize, kNullProbability)});
  columns.push_back(
      {name_gen(), gen.Decimal256(decimal256(43, 7), kBatchSize, kNullProbability)});
  columns.push_back(
      {name_gen(), gen.Decimal64(decimal64(12, 3), kBatchSize, kNullProbability)});
  columns.push_back(
      {name_gen(), gen.Decimal32(decimal32(7, 3), kBatchSize, kNullProbability)});

  // Timestamp
  for (auto unit : TimeUnit::values()) {
    ARROW_ASSIGN_OR_RAISE(auto timestamps, int64_array->View(timestamp(unit, "UTC")));
    columns.push_back({name_gen(), timestamps});
  }
  // Time32, time64
  ARROW_ASSIGN_OR_RAISE(
      auto time32_s,
      gen.Int32(kBatchSize, 0, 86399, kNullProbability)->View(time32(TimeUnit::SECOND)));
  columns.push_back({name_gen(), time32_s});
  ARROW_ASSIGN_OR_RAISE(auto time32_ms,
                        gen.Int32(kBatchSize, 0, 86399999, kNullProbability)
                            ->View(time32(TimeUnit::MILLI)));
  columns.push_back({name_gen(), time32_ms});
  ARROW_ASSIGN_OR_RAISE(auto time64_us,
                        gen.Int64(kBatchSize, 0, 86399999999LL, kNullProbability)
                            ->View(time64(TimeUnit::MICRO)));
  columns.push_back({name_gen(), time64_us});
  ARROW_ASSIGN_OR_RAISE(auto time64_ns,
                        gen.Int64(kBatchSize, 0, 86399999999999LL, kNullProbability)
                            ->View(time64(TimeUnit::NANO)));
  columns.push_back({name_gen(), time64_ns});
  // Date32, date64
  ARROW_ASSIGN_OR_RAISE(
      auto date32_array,
      gen.Int32(kBatchSize, -1000 * 365, 1000 * 365, kNullProbability)->View(date32()));
  columns.push_back({name_gen(), date32_array});
  columns.push_back(
      {name_gen(), gen.Date64(kBatchSize, -1000 * 365, 1000 * 365, kNullProbability)});

  // A column of tiny strings that will hopefully trigger dict encoding
  columns.push_back({name_gen(), tiny_strings_array});
  columns.push_back({name_gen(), large_strings_array});
  columns.push_back({name_gen(), string_view_array});
  columns.push_back(
      {name_gen(), gen.FixedSizeBinary(kBatchSize, /*byte_width=*/7, kNullProbability)});

  // A column of lists/large lists
  {
    auto values = gen.Int64(kBatchSize * 10, -10000, 10000, kNullProbability);
    auto offsets = gen.Offsets(kBatchSize + 1, 0, static_cast<int32_t>(values->length()));
    ARROW_ASSIGN_OR_RAISE(auto lists, ListArray::FromArrays(*offsets, *values));
    columns.push_back({name_gen(), lists});
    auto large_offsets = gen.LargeOffsets(kBatchSize + 1, 0, values->length());
    ARROW_ASSIGN_OR_RAISE(auto large_lists,
                          LargeListArray::FromArrays(*large_offsets, *values));
    columns.push_back({name_gen(), large_lists});
  }
  // A column of a repeated constant that will hopefully trigger RLE encoding
  {
    ARROW_ASSIGN_OR_RAISE(auto values, MakeArrayFromScalar(Int16Scalar(42), kBatchSize));
    columns.push_back({name_gen(), values});
  }
  // A column of lists of lists
  {
    auto inner_values = gen.Int64(kBatchSize * 9, -10000, 10000, kNullProbability);
    auto inner_offsets =
        gen.Offsets(kBatchSize * 3 + 1, 0, static_cast<int32_t>(inner_values->length()),
                    kNullProbability);
    ARROW_ASSIGN_OR_RAISE(auto inner_lists,
                          ListArray::FromArrays(*inner_offsets, *inner_values));
    auto offsets = gen.Offsets(
        kBatchSize + 1, 0, static_cast<int32_t>(inner_lists->length()), kNullProbability);
    ARROW_ASSIGN_OR_RAISE(auto lists, ListArray::FromArrays(*offsets, *inner_lists));
    columns.push_back({name_gen(), lists});
  }
  // A column of maps
  {
    constexpr auto kChildSize = kBatchSize * 3;
    auto keys = gen.String(kChildSize, /*min_length=*/4, /*max_length=*/7,
                           /*null_probability=*/0);
    auto values = gen.Float32(kChildSize, -1e10, 1e10, kNullProbability);
    columns.push_back({name_gen(), gen.Map(keys, values, kBatchSize, kNullProbability)});
  }
  // A column of nested non-nullable structs
  {
    ARROW_ASSIGN_OR_RAISE(auto inner_a,
                          StructArray::Make({int16_array, non_null_float64_array},
                                            {field_for_array(int16_array),
                                             field_for_array(non_null_float64_array)}));
    ARROW_ASSIGN_OR_RAISE(auto structs,
                          StructArray::Make({inner_a, tiny_strings_array},
                                            {field_for_array(inner_a),
                                             field_for_array(tiny_strings_array)}));
    columns.push_back({name_gen(), structs});
  }
  // A column of nested nullable structs
  {
    auto null_bitmap = gen.NullBitmap(kBatchSize, kNullProbability);
    ARROW_ASSIGN_OR_RAISE(auto inner_a,
                          StructArray::Make({int16_array, non_null_float64_array},
                                            {field_for_array(int16_array),
                                             field_for_array(non_null_float64_array)},
                                            std::move(null_bitmap)));
    null_bitmap = gen.NullBitmap(kBatchSize, kNullProbability);
    ARROW_ASSIGN_OR_RAISE(
        auto structs,
        StructArray::Make({inner_a, tiny_strings_array},
                          {field_for_array(inner_a), field_for_array(tiny_strings_array)},
                          std::move(null_bitmap)));
    columns.push_back({name_gen(), structs});
  }

  // TODO extension types: UUID, JSON, GEOMETRY, GEOGRAPHY

  // A non-dict-encoded column (see GetWriteConfigurations)
  columns.push_back({"no_dict", gen.String(kBatchSize, 0, 30, kNullProbability)});
  // A column that should be quite compressible (see GetWriteConfigurations)
  columns.push_back({"compressed", gen.Int64(kBatchSize, -10, 10, kNullProbability)});

  FieldVector fields;
  ArrayVector arrays;
  for (const auto& col : columns) {
    fields.push_back(field_for_array_named(col.array, col.name));
    arrays.push_back(col.array);
  }
  auto md = key_value_metadata({"key1", "key2"}, {"value1", ""});
  auto schema = ::arrow::schema(std::move(fields), std::move(md));
  return RecordBatch::Make(std::move(schema), kBatchSize, std::move(arrays));
}

Result<std::vector<std::shared_ptr<RecordBatch>>> Batches() {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  ARROW_ASSIGN_OR_RAISE(auto batch, ExampleBatch1());
  batches.push_back(batch);
  return batches;
}

Status DoMain(const std::string& out_dir) {
  ARROW_ASSIGN_OR_RAISE(auto dir_fn, PlatformFilename::FromString(out_dir));
  RETURN_NOT_OK(CreateDir(dir_fn));

  int sample_num = 1;
  auto sample_name = [&]() -> std::string {
    return "pq-table-" + std::to_string(sample_num++);
  };

  ARROW_ASSIGN_OR_RAISE(auto batches, Batches());

  auto write_configs = GetWriteConfigurations();

  for (const auto& batch : batches) {
    RETURN_NOT_OK(batch->ValidateFull());
    ARROW_ASSIGN_OR_RAISE(auto table, Table::FromRecordBatches({batch}));

    for (const auto& config : write_configs) {
      ARROW_ASSIGN_OR_RAISE(auto sample_fn, dir_fn.Join(sample_name()));
      std::cerr << sample_fn.ToString() << std::endl;
      ARROW_ASSIGN_OR_RAISE(auto file, io::FileOutputStream::Open(sample_fn.ToString()));
      RETURN_NOT_OK(::parquet::arrow::WriteTable(*table, default_memory_pool(), file,
                                                 kChunkSize, config.writer_properties,
                                                 config.arrow_writer_properties));
      RETURN_NOT_OK(file->Close());
    }
  }
  return Status::OK();
}

ARROW_NORETURN void Usage() {
  std::cerr << "Usage: parquet-arrow-generate-fuzz-corpus "
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

}  // namespace arrow

int main(int argc, char** argv) { return ::arrow::Main(argc, argv); }
