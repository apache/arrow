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

/// The table looks like (with_nulls = false):
// {
// { a: {x: 0, y: 0}, b: {0, 0, 0}, c: "0", d: 0},
// { a: {x: 1, y: 1}, b: {1, 1, 1}, c: "1", d: 1},
// ...
// { a: {x: 99, y: 99}, b: {99, 99, 99}, c: "99", d: 99}
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
    strs.push_back(std::to_string(i));
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
          ->write_batch_size(13)
          ->data_pagesize(1)  // this will cause every batch creating a page
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

void check_rb(std::shared_ptr<arrow::RecordBatchReader> rb_reader,
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
        sum_c += std::stoi(std::string((*iter).has_value() ? (*iter).value() : "0"));
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

  if (checking_col("a", column_names)) ASSERT_EQ(expected_sum * 2, sum_a);
  if (checking_col("b", column_names)) ASSERT_EQ(expected_sum * 3, sum_b);
  if (checking_col("c", column_names)) ASSERT_EQ(expected_sum, sum_c);
  if (checking_col("d", column_names)) ASSERT_EQ(expected_sum, sum_d);
}

class TestRecordBatchReaderWithRanges : public testing::Test {
 public:
  void SetUp() {
    ASSERT_OK_AND_ASSIGN(auto buffer, WriteFullFile());

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

TEST_F(TestRecordBatchReaderWithRanges, SelectSomePageForEachRG) {
  std::shared_ptr<arrow::RecordBatchReader> rb_reader;
  const auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
  row_ranges_map->insert({0, std::make_shared<parquet::RowRanges>(parquet::Range{0, 9})});
  row_ranges_map->insert(
      {1, std::make_shared<parquet::RowRanges>(parquet::Range{10, 19})});
  row_ranges_map->insert(
      {2, std::make_shared<parquet::RowRanges>(parquet::Range{20, 29})});
  row_ranges_map->insert({3, std::make_shared<parquet::RowRanges>(parquet::Range{0, 9})});

  const std::vector column_indices{0, 1, 2, 3, 4};
  ASSERT_OK(arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, column_indices,
                                               row_ranges_map, &rb_reader));

  // (0+...+9) + (40+...+49) + (80+...+89) + (90+...+99) = 2280
  check_rb(rb_reader, 40, 2280);
}

TEST_F(TestRecordBatchReaderWithRanges, SelectAllRange) {
  std::shared_ptr<arrow::RecordBatchReader> rb_reader;
  const auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
  row_ranges_map->insert(
      {0, std::make_shared<parquet::RowRanges>(parquet::Range{0, 29})});
  row_ranges_map->insert(
      {1, std::make_shared<parquet::RowRanges>(parquet::Range{0, 29})});
  row_ranges_map->insert(
      {2, std::make_shared<parquet::RowRanges>(parquet::Range{0, 29})});
  row_ranges_map->insert({3, std::make_shared<parquet::RowRanges>(parquet::Range{0, 9})});

  const std::vector column_indices{0, 1, 2, 3, 4};
  ASSERT_OK(arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, column_indices,
                                               row_ranges_map, &rb_reader));

  // (0+...+99) = 4950
  check_rb(rb_reader, 100, 4950);
}

TEST_F(TestRecordBatchReaderWithRanges, SelectEmptyRange) {
  std::shared_ptr<arrow::RecordBatchReader> rb_reader;
  const auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
  row_ranges_map->insert(
      {0, std::make_shared<parquet::RowRanges>(std::vector<parquet::Range>())});
  const std::vector column_indices{0, 1, 2, 3, 4};
  const auto status = arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, column_indices,
                                                         row_ranges_map, &rb_reader);
  ASSERT_NOT_OK(status);
  EXPECT_TRUE(status.message().find("The provided row range is invalid, keep it monotone "
                                    "and non-interleaving: []") != std::string::npos);
}

TEST_F(TestRecordBatchReaderWithRanges, SelectOneRowSkipOneRow) {
  // case 1: row_ranges_map contains only RG {0}, other RGs should be skipped
  {
    std::shared_ptr<arrow::RecordBatchReader> rb_reader;
    const auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
    std::vector<parquet::Range> ranges;
    for (int64_t i = 0; i < 30; i++) {
      if (i % 2 == 0) ranges.push_back({i, i});
    }
    row_ranges_map->insert({0, std::make_shared<parquet::RowRanges>(ranges)});
    const std::vector column_indices{0, 1, 2, 3, 4};
    ASSERT_OK(arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, column_indices,
                                                 row_ranges_map, &rb_reader));

    check_rb(rb_reader, 15, 210);  // 0 + 2 + ... + 28 = 210
  }

  // case 2: row_ranges_map contains only RG {0,2}, other RGs should be skipped
  {
    std::shared_ptr<arrow::RecordBatchReader> rb_reader;
    const auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
    std::vector<parquet::Range> ranges;
    for (int64_t i = 0; i < 30; i++) {
      if (i % 2 == 0) ranges.push_back({i, i});
    }
    row_ranges_map->insert({0, std::make_shared<parquet::RowRanges>(ranges)});
    row_ranges_map->insert({2, std::make_shared<parquet::RowRanges>(ranges)});
    const std::vector column_indices{0, 1, 2, 3, 4};
    ASSERT_OK(arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, column_indices,
                                                 row_ranges_map, &rb_reader));

    check_rb(rb_reader, 30, 1320);  // (0 + 2 + ... + 28) + (60 + 62 ... + 88) = 1320
  }
}

TEST_F(TestRecordBatchReaderWithRanges, InvalidRanges) {
  std::shared_ptr<arrow::RecordBatchReader> rb_reader;
  {
    const auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
    row_ranges_map->insert(
        {0, std::make_shared<parquet::RowRanges>(parquet::Range{-1, 5})});
    const std::vector column_indices{0, 1, 2, 3, 4};
    const auto status = arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, column_indices,
                                                           row_ranges_map, &rb_reader);
    ASSERT_NOT_OK(status);
    EXPECT_TRUE(status.message().find("The provided row range is invalid, keep it "
                                      "monotone and non-interleaving: [(-1, 5)]") !=
                std::string::npos);
  }

  {
    const auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
    row_ranges_map->insert({0, std::make_shared<parquet::RowRanges>(std::vector{
                                   parquet::Range{0, 4}, parquet::Range{2, 5}})});
    const std::vector column_indices{0, 1, 2, 3, 4};
    const auto status = arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, column_indices,
                                                           row_ranges_map, &rb_reader);
    ASSERT_NOT_OK(status);
    EXPECT_TRUE(
        status.message().find("The provided row range is invalid, keep it monotone and "
                              "non-interleaving: [(0, 4), (2, 5)]") != std::string::npos);
  }
  {
    const auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
    row_ranges_map->insert(
        {0, std::make_shared<parquet::RowRanges>(std::vector{parquet::Range{0, 30}})});
    const std::vector column_indices{0, 1, 2, 3, 4};
    const auto status = arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, column_indices,
                                                           row_ranges_map, &rb_reader);
    ASSERT_NOT_OK(status);
    EXPECT_TRUE(status.message().find(
                    "The provided row range [(0, 30)] exceeds last page :[26, 29]") !=
                std::string::npos);
  }
}

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

  std::shared_ptr<arrow::RecordBatchReader> rb_reader;
  auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
  row_ranges_map->insert(
      {0, std::make_shared<parquet::RowRanges>(parquet::Range{0, 29})});
  std::vector column_indices{0, 1, 2, 3, 4};
  auto status = arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, column_indices,
                                                   row_ranges_map, &rb_reader);
  ASSERT_NOT_OK(status);
  EXPECT_TRUE(status.message().find("Attempting to read with Ranges but Page Index is "
                                    "not found for Row Group: 0") != std::string::npos);
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
    std::shared_ptr<arrow::RecordBatchReader> rb_reader;
    const auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
    std::vector<parquet::Range> ranges;
    for (int64_t i = 0; i < 30; i++) {
      if (i % 2 == 0) ranges.push_back({i, i});
    }
    row_ranges_map->insert({0, std::make_shared<parquet::RowRanges>(ranges)});
    row_ranges_map->insert({2, std::make_shared<parquet::RowRanges>(ranges)});
    const std::vector column_indices{0, 1, 2, 3, 4};
    ASSERT_OK(arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, column_indices,
                                                 row_ranges_map, &rb_reader));

    // 0-9 is masked as null, so the ramaining is:
    // (10 + 12 + ... + 28) + (60 + 62 ... + 88) = 1320
    check_rb(rb_reader, 30, 1300);
  }
}