// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/io/memory.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include <arrow/testing/builder.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/range.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <iostream>

#include <arrow/util/future.h>
#include <cstdlib>
#include <random>
#include <string>

using parquet::IntervalRange;
using parquet::IntervalRanges;

std::string random_string() {
  static auto& chrs = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static std::mt19937 rg{std::random_device()()};
  static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);

  int length = std::rand() % 100;

  std::string s;
  s.reserve(length);
  while (length--) s += chrs[pick(rg)];

  return s;
}

/// The table looks like (with_nulls = false):
// {
// { a: {x: 0, y: 0}, b: {0, 0, 0}, c: "0<random_string>", d: 0},
// { a: {x: 1, y: 1}, b: {1, 1, 1}, c: "1<random_string>", d: 1},
// ...
// { a: {x: 99, y: 99}, b: {99, 99, 99}, c: "99<random_string>", d: 99}
// }
arrow::Result<std::shared_ptr<arrow::Table>> GetTable(bool with_nulls = false) {
  // if with_nulls, the generated table should null values
  // set first 10 rows and last 10 rows to null
  std::shared_ptr<arrow::Buffer> null_bitmap;
  std::vector flags(100, true);
  if (with_nulls) {
    std::fill_n(flags.begin(), 10, false);
    std::fill_n(flags.begin() + 90, 10, false);

    size_t length = flags.size();

    ARROW_ASSIGN_OR_RAISE(null_bitmap, arrow::AllocateEmptyBitmap(length));

    uint8_t* bitmap = null_bitmap->mutable_data();
    for (size_t i = 0; i < length; ++i) {
      if (flags[i]) {
        arrow::bit_util::SetBit(bitmap, i);
      }
    }
  }

  auto int32_builder = arrow::Int32Builder();

  // Struct col
  std::shared_ptr<arrow::Array> arr_a_x;
  std::shared_ptr<arrow::Array> arr_a_y;
  ARROW_RETURN_NOT_OK(int32_builder.AppendValues(arrow::internal::Iota(0, 100)));
  ARROW_RETURN_NOT_OK(int32_builder.Finish(&arr_a_x));
  ARROW_RETURN_NOT_OK(int32_builder.AppendValues(arrow::internal::Iota(0, 100)));
  ARROW_RETURN_NOT_OK(int32_builder.Finish(&arr_a_y));
  ARROW_ASSIGN_OR_RAISE(auto arr_a, arrow::StructArray::Make(
                                        {arr_a_x, arr_a_y},
                                        std::vector<std::string>{"x", "y"}, null_bitmap));

  // List<int> col
  std::shared_ptr<arrow::Array> arr_b_values;
  std::shared_ptr<arrow::Array> arr_b_offsets;
  std::vector<int> b_values;
  for (int i = 0; i < 100; ++i) {
    for (int j = 0; j < 3; ++j) {
      b_values.push_back(i);
    }
  }
  ARROW_RETURN_NOT_OK(int32_builder.AppendValues(b_values));
  ARROW_RETURN_NOT_OK(int32_builder.Finish(&arr_b_values));
  std::vector<int> offsets = arrow::internal::Iota(0, 101);
  std::transform(offsets.begin(), offsets.end(), offsets.begin(),
                 [](const int x) { return x * 3; });
  ARROW_RETURN_NOT_OK(int32_builder.AppendValues(offsets));
  ARROW_RETURN_NOT_OK(int32_builder.Finish(&arr_b_offsets));
  ARROW_ASSIGN_OR_RAISE(auto arr_b, arrow::ListArray::FromArrays(
                                        *arr_b_offsets, *arr_b_values,
                                        arrow::default_memory_pool(), null_bitmap));

  // string col
  auto string_builder = arrow::StringBuilder();
  std::shared_ptr<arrow::Array> arr_c;
  std::vector<std::string> strs;
  uint8_t valid_bytes[100];
  for (size_t i = 0; i < 100; i++) {
    // add more chars to make this column unaligned with other columns' page
    strs.push_back(std::to_string(i) + random_string());
    valid_bytes[i] = flags[i];
  }
  ARROW_RETURN_NOT_OK(string_builder.AppendValues(strs, &valid_bytes[0]));
  ARROW_RETURN_NOT_OK(string_builder.Finish(&arr_c));

  // int col
  std::shared_ptr<arrow::Array> arr_d;
  ARROW_RETURN_NOT_OK(int32_builder.AppendValues(arrow::internal::Iota(0, 100), flags));
  ARROW_RETURN_NOT_OK(int32_builder.Finish(&arr_d));

  auto schema = arrow::schema({
      // complex types prior to simple types
      field("a", arr_a->type()),
      field("b", list(arrow::int32())),
      field("c", arrow::utf8()),
      field("d", arrow::int32()),
  });

  return arrow::Table::Make(schema, {arr_a, arr_b, arr_c, arr_d});
}

arrow::Result<std::shared_ptr<arrow::Buffer>> WriteFullFile(
    const bool with_nulls = false) {
  using parquet::ArrowWriterProperties;
  using parquet::WriterProperties;

  ARROW_ASSIGN_OR_RAISE(const auto table, GetTable(with_nulls));

  const std::shared_ptr<WriterProperties> props =
      WriterProperties::Builder()
          .max_row_group_length(30)
          ->enable_write_page_index()
          ->disable_dictionary()
          ->write_batch_size(1)
          ->data_pagesize(30)  // small pages
          ->compression(arrow::Compression::SNAPPY)
          ->build();

  const std::shared_ptr<ArrowWriterProperties> arrow_props =
      ArrowWriterProperties::Builder().store_schema()->build();

  ARROW_ASSIGN_OR_RAISE(const auto out_stream, ::arrow::io::BufferOutputStream::Create());

  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table.get(),
                                                 arrow::default_memory_pool(), out_stream,
                                                 /*chunk_size=*/100, props, arrow_props));

  // {
  //   // output to a local file for debugging
  //   ARROW_ASSIGN_OR_RAISE(auto outfile, arrow::io::FileOutputStream::Open(
  //                                           "/tmp/range_reader_test.parquet"));
  //
  //   ARROW_RETURN_NOT_OK(
  //       parquet::arrow::WriteTable(*table.get(), arrow::default_memory_pool(), outfile,
  //                                  /*chunk_size=*/100, props, arrow_props));
  // }

  return out_stream->Finish();
}

bool checking_col(const std::string& col_name,
                  const std::vector<std::string>& column_names) {
  return std::find(column_names.begin(), column_names.end(), col_name) !=
         column_names.end();
}

void check_rb(std::unique_ptr<arrow::RecordBatchReader> rb_reader,
              const size_t expected_rows, const int64_t expected_sum) {
  const std::vector<std::string> column_names = rb_reader->schema()->field_names();

  size_t total_rows = 0;
  int64_t sum_a = 0;
  int64_t sum_b = 0;
  int64_t sum_c = 0;
  int64_t sum_d = 0;
  for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch : *rb_reader) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    total_rows += batch->num_rows();

    if (checking_col("a", column_names)) {
      auto a_array =
          std::dynamic_pointer_cast<arrow::StructArray>(batch->GetColumnByName("a"));
      auto a_x_array = std::dynamic_pointer_cast<arrow::Int32Array>(a_array->field(0));
      auto a_y_array = std::dynamic_pointer_cast<arrow::Int32Array>(a_array->field(1));
      for (auto iter = a_x_array->begin(); iter != a_x_array->end(); ++iter) {
        sum_a += (*iter).has_value() ? (*iter).value() : 0;
      }
      for (auto iter = a_y_array->begin(); iter != a_y_array->end(); ++iter) {
        sum_a += (*iter).has_value() ? (*iter).value() : 0;
      }
    }

    if (checking_col("b", column_names)) {
      auto b_array =
          std::dynamic_pointer_cast<arrow::ListArray>(batch->GetColumnByName("b"));
      ASSERT_OK_AND_ASSIGN(auto flatten_b_array, b_array->Flatten());
      auto b_array_values = std::dynamic_pointer_cast<arrow::Int32Array>(flatten_b_array);
      for (auto iter = b_array_values->begin(); iter != b_array_values->end(); ++iter) {
        sum_b += (*iter).has_value() ? (*iter).value() : 0;
      }
    }

    if (checking_col("c", column_names)) {
      auto c_array =
          std::dynamic_pointer_cast<arrow::StringArray>(batch->GetColumnByName("c"));
      for (auto iter = c_array->begin(); iter != c_array->end(); ++iter) {
        sum_c += std::stoi(std::string(
            (*iter).has_value()
                ? (*iter).value().substr(
                      0, std::distance(
                             (*iter).value().begin(),
                             std::find_if((*iter).value().begin(), (*iter).value().end(),
                                          [](char c) { return !std::isdigit(c); })))
                : "0"));
      }
    }

    if (checking_col("d", column_names)) {
      auto d_array =
          std::dynamic_pointer_cast<arrow::Int32Array>(batch->GetColumnByName("d"));
      for (auto iter = d_array->begin(); iter != d_array->end(); ++iter) {
        sum_d += (*iter).has_value() ? (*iter).value() : 0;
      }
    }
  }
  ASSERT_EQ(expected_rows, total_rows);

  if (checking_col("a", column_names)) {
    ASSERT_EQ(expected_sum * 2, sum_a);
  }
  if (checking_col("b", column_names)) {
    ASSERT_EQ(expected_sum * 3, sum_b);
  }
  if (checking_col("c", column_names)) {
    ASSERT_EQ(expected_sum, sum_c);
  }
  if (checking_col("d", column_names)) {
    ASSERT_EQ(expected_sum, sum_d);
  }
}

class CountingBytesBufferReader : public arrow::io::BufferReader {
 public:
  using BufferReader::BufferReader;

  arrow::Future<std::shared_ptr<arrow::Buffer>> ReadAsync(
      const arrow::io::IOContext& context, int64_t position, int64_t nbytes) override {
    read_bytes_ += nbytes;
    return BufferReader::ReadAsync(context, position, nbytes);
  }

  arrow::Future<int64_t> ReadAsync(const arrow::io::IOContext& ctx,
                                   std::vector<arrow::io::ReadRange>& ranges,
                                   void* out) override {
    read_bytes_ += std::accumulate(ranges.begin(), ranges.end(), 0,
                                   [](int64_t sum, const arrow::io::ReadRange& range) {
                                     return sum + range.length;
                                   });
    return RandomAccessFile::ReadAsync(ctx, ranges, out);
  }

  int64_t read_bytes() const { return read_bytes_; }

 private:
  int64_t read_bytes_ = 0;
};

class TestRecordBatchReaderWithRanges : public testing::TestWithParam<int> {
 public:
  void SetUp() {
    int mode = GetParam();

    ASSERT_OK_AND_ASSIGN(auto buffer, WriteFullFile());

    arrow::MemoryPool* pool = arrow::default_memory_pool();

    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(4096 * 4);
    reader_properties.enable_buffered_stream();

    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(10);  // default 64 * 1024
    if (mode != 0) {
      arrow_reader_props.set_pre_buffer(true);
    }

    if (mode == 2) {
      arrow::io::CacheOptions cache_options = arrow::io::CacheOptions::Defaults();
      cache_options.hole_size_limit = 0;
      cache_options.lazy = true;
      cache_options.prefetch_limit = 2;
      arrow_reader_props.set_cache_options(cache_options);
    }

    parquet::arrow::FileReaderBuilder reader_builder;
    in_file = std::make_shared<CountingBytesBufferReader>(buffer);
    ASSERT_OK(reader_builder.Open(in_file, /*memory_map=*/reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    ASSERT_OK_AND_ASSIGN(arrow_reader, reader_builder.Build());
  }

  void TearDown() {}

 protected:
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  std::shared_ptr<CountingBytesBufferReader> in_file;
};

TEST_P(TestRecordBatchReaderWithRanges, SelectOnePageForEachRG) {
  std::unique_ptr<arrow::RecordBatchReader> rb_reader;
  IntervalRanges rows{{{0, 9}, {40, 49}, {80, 89}, {90, 99}}};

  const std::vector column_indices{0, 1, 2, 3, 4};
  ASSERT_OK(arrow_reader->GetRecordBatchReader(rows, column_indices, &rb_reader));

  // (0+...+9) + (40+...+49) + (80+...+89) + (90+...+99) = 2280
  check_rb(std::move(rb_reader), 40, 2280);
}

TEST_P(TestRecordBatchReaderWithRanges, SelectSomePageForOneRG) {
  std::unique_ptr<arrow::RecordBatchReader> rb_reader;
  IntervalRanges rows{{IntervalRange{0, 7}, IntervalRange{16, 23}}};

  const std::vector column_indices{0, 1, 2, 3, 4};
  ASSERT_OK(arrow_reader->GetRecordBatchReader(rows, column_indices, &rb_reader));

  // (0+...+7) + (16+...+23) = 184
  check_rb(std::move(rb_reader), 16, 184);
}

TEST_P(TestRecordBatchReaderWithRanges, SelectAllRange) {
  std::unique_ptr<arrow::RecordBatchReader> rb_reader;
  IntervalRanges rows{{IntervalRange{0, 29}, IntervalRange{30, 59}, IntervalRange{60, 89},
                       IntervalRange{90, 99}}};

  const std::vector column_indices{0, 1, 2, 3, 4};
  ASSERT_OK(arrow_reader->GetRecordBatchReader(rows, column_indices, &rb_reader));

  // (0+...+99) = 4950
  check_rb(std::move(rb_reader), 100, 4950);
}

TEST_P(TestRecordBatchReaderWithRanges, CheckSkipIOEffective) {
  std::unique_ptr<arrow::RecordBatchReader> rb_reader;
  IntervalRanges rows{{IntervalRange{3, 3}}};

  const std::vector column_indices{0, 1, 2, 3, 4};
  ASSERT_OK(arrow_reader->GetRecordBatchReader(rows, column_indices, &rb_reader));

  check_rb(std::move(rb_reader), 1, 3);

  // only one page should be touched when we enable pre_buffer
  // the total read bytes should be small (the first RG is about 3000 bytes)
  auto mode = GetParam();
  if (mode == 1 || mode == 2) {
    ASSERT_LT(in_file->read_bytes(), 1000);
  }
}

TEST_P(TestRecordBatchReaderWithRanges, SelectEmptyRange) {
  std::unique_ptr<arrow::RecordBatchReader> rb_reader;
  IntervalRanges rows{};

  const std::vector column_indices{0, 1, 2, 3, 4};
  const auto status =
      arrow_reader->GetRecordBatchReader(rows, column_indices, &rb_reader);
  ASSERT_OK(status);
  check_rb(std::move(rb_reader), 0, 0);
}

TEST_P(TestRecordBatchReaderWithRanges, SelectOneRowSkipOneRow) {
  // case 1: only care about RG 0
  {
    std::unique_ptr<arrow::RecordBatchReader> rb_reader;
    std::vector<parquet::IntervalRange> ranges;
    for (int64_t i = 0; i < 30; i++) {
      if (i % 2 == 0) {
        ranges.push_back({i, i});
      }
    }
    const std::vector column_indices{0, 1, 2, 3, 4};
    ASSERT_OK(arrow_reader->GetRecordBatchReader(IntervalRanges(ranges), column_indices,
                                                 &rb_reader));

    check_rb(std::move(rb_reader), 15, 210);  // 0 + 2 + ... + 28 = 210
  }

  // case 2: care about RG 0 and 2
  {
    std::unique_ptr<arrow::RecordBatchReader> rb_reader;
    std::vector<parquet::IntervalRange> ranges;
    for (int64_t i = 0; i < 30; i++) {
      if (i % 2 == 0) {
        ranges.push_back({i, i});
      }
    }

    for (int64_t i = 60; i < 90; i++) {
      if (i % 2 == 0) {
        ranges.push_back({i, i});
      }
    }
    const std::vector column_indices{0, 1, 2, 3, 4};
    ASSERT_OK(arrow_reader->GetRecordBatchReader(IntervalRanges(ranges), column_indices,
                                                 &rb_reader));

    check_rb(std::move(rb_reader), 30,
             1320);  // (0 + 2 + ... + 28) + (60 + 62 ... + 88) = 1320
  }
}

TEST_P(TestRecordBatchReaderWithRanges, InvalidRanges) {
  std::unique_ptr<arrow::RecordBatchReader> rb_reader;
  {
    auto create_ranges = []() -> IntervalRanges {
      return IntervalRanges{{IntervalRange{-1, 5}}};
    };
    EXPECT_THROW(create_ranges(), parquet::ParquetException);
  }

  {
    auto create_ranges = []() -> IntervalRanges {
      return IntervalRanges{{{0, 4}, {2, 5}}};
    };
    EXPECT_THROW(create_ranges(), parquet::ParquetException);
  }
  {
    // will treat as {0,99}
    IntervalRanges rows{{IntervalRange{0, 100}}};
    const std::vector column_indices{0, 1, 2, 3, 4};
    const auto status =
        arrow_reader->GetRecordBatchReader(rows, column_indices, &rb_reader);
    ASSERT_NOT_OK(status);
    EXPECT_TRUE(status.message().find("The provided row range [(0, 100)] exceeds the "
                                      "number of rows in the file: 100") !=
                std::string::npos);
  }
}

// mode 0: normal read with pre_buffer = false
// mode 1: normal read with pre_buffer = true
// mode 2: with pre_buffer = true and set cache options
INSTANTIATE_TEST_SUITE_P(ParameterizedTestRecordBatchReaderWithRanges,
                         TestRecordBatchReaderWithRanges, testing::Values(0, 1, 2));

TEST(TestRecordBatchReaderWithRangesBadCases, NoPageIndex) {
  using parquet::ArrowWriterProperties;
  using parquet::WriterProperties;

  // write a file without page index
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Table> table, GetTable());
  std::shared_ptr<WriterProperties> props =
      WriterProperties::Builder()
          .max_row_group_length(30)
          ->disable_write_page_index()  // NO INDEX !!!!
          ->write_batch_size(13)
          ->data_pagesize(1)
          ->compression(arrow::Compression::SNAPPY)
          ->build();
  std::shared_ptr<ArrowWriterProperties> arrow_props =
      ArrowWriterProperties::Builder().store_schema()->build();
  ASSERT_OK_AND_ASSIGN(auto out_stream, ::arrow::io::BufferOutputStream::Create());
  ASSERT_OK(parquet::arrow::WriteTable(*table.get(), arrow::default_memory_pool(),
                                       out_stream,
                                       /*chunk_size=*/100, props, arrow_props));
  ASSERT_OK_AND_ASSIGN(auto buffer, out_stream->Finish());

  // try to read the file with Range
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto reader_properties = parquet::ReaderProperties(pool);
  reader_properties.set_buffer_size(4096 * 4);
  reader_properties.enable_buffered_stream();
  auto arrow_reader_props = parquet::ArrowReaderProperties();
  arrow_reader_props.set_batch_size(10);  // default 64 * 1024

  parquet::arrow::FileReaderBuilder reader_builder;
  auto in_file = std::make_shared<arrow::io::BufferReader>(buffer);
  ASSERT_OK(reader_builder.Open(in_file, /*memory_map=*/reader_properties));
  reader_builder.memory_pool(pool);
  reader_builder.properties(arrow_reader_props);
  ASSERT_OK_AND_ASSIGN(auto arrow_reader, reader_builder.Build());

  std::unique_ptr<arrow::RecordBatchReader> rb_reader;
  IntervalRanges rows{{IntervalRange{0, 29}}};
  std::vector column_indices{0, 1, 2, 3, 4};
  auto status = arrow_reader->GetRecordBatchReader(rows, column_indices, &rb_reader);
  ASSERT_NOT_OK(status);

  EXPECT_TRUE(
      status.message().find("Page index is required but not found for row group 0") !=
      std::string::npos);
}

class TestRecordBatchReaderWithRangesWithNulls : public testing::Test {
 public:
  void SetUp() {
    ASSERT_OK_AND_ASSIGN(auto buffer, WriteFullFile(true));

    arrow::MemoryPool* pool = arrow::default_memory_pool();

    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(4096 * 4);
    reader_properties.enable_buffered_stream();

    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(10);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    const auto in_file = std::make_shared<arrow::io::BufferReader>(buffer);
    ASSERT_OK(reader_builder.Open(in_file, /*memory_map=*/reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    ASSERT_OK_AND_ASSIGN(arrow_reader, reader_builder.Build());
  }

  void TearDown() {}

 protected:
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
};

TEST_F(TestRecordBatchReaderWithRangesWithNulls, SelectOneRowSkipOneRow) {
  {
    std::unique_ptr<arrow::RecordBatchReader> rb_reader;
    std::vector<parquet::IntervalRange> ranges;
    for (int64_t i = 0; i < 30; i++) {
      if (i % 2 == 0) {
        ranges.push_back({i, i});
      }
    }

    for (int64_t i = 60; i < 90; i++) {
      if (i % 2 == 0) {
        ranges.push_back({i, i});
      }
    }
    const std::vector column_indices{0, 1, 2, 3, 4};
    ASSERT_OK(arrow_reader->GetRecordBatchReader(IntervalRanges(ranges), column_indices,
                                                 &rb_reader));

    // 0-9 is masked as null, so the ramaining is:
    // (10 + 12 + ... + 28) + (60 + 62 ... + 88) = 1320
    check_rb(std::move(rb_reader), 30, 1300);
  }
}
