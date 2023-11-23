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

#include <arrow/testing/gtest_util.h>
#include <arrow/util/range.h>
#include <iostream>

arrow::Result<std::shared_ptr<arrow::Table>> GetTable() {
  auto builder = arrow::Int32Builder();

  std::shared_ptr<arrow::Array> arr_a_values;
  std::shared_ptr<arrow::Array> arr_a_offsets;
  std::vector<int> a_values;
  for (int i = 0; i < 100; ++i) {
    for (int j = 0; j < 3; ++j) {
      a_values.push_back(i);
    }
  }
  ARROW_RETURN_NOT_OK(builder.AppendValues(a_values));
  ARROW_RETURN_NOT_OK(builder.Finish(&arr_a_values));
  std::vector<int> offsets = arrow::internal::Iota(0, 101);
  std::transform(offsets.begin(), offsets.end(), offsets.begin(),
                 [](int x) { return x * 3; });
  ARROW_RETURN_NOT_OK(builder.AppendValues(offsets));
  ARROW_RETURN_NOT_OK(builder.Finish(&arr_a_offsets));
  ARROW_ASSIGN_OR_RAISE(auto arr_a,
                        arrow::ListArray::FromArrays(*arr_a_offsets, *arr_a_values));

  std::shared_ptr<arrow::Array> arr_b;
  ARROW_RETURN_NOT_OK(builder.AppendValues(arrow::internal::Iota(0, 100)));
  ARROW_RETURN_NOT_OK(builder.Finish(&arr_b));

  auto string_builder = arrow::StringBuilder();
  std::shared_ptr<arrow::Array> arr_c;
  std::vector<std::string> strs;
  for (size_t i = 0; i < 100; i++) {
    strs.push_back("" + std::to_string(i));
  }
  ARROW_RETURN_NOT_OK(string_builder.AppendValues(strs));
  ARROW_RETURN_NOT_OK(string_builder.Finish(&arr_c));

  auto schema = arrow::schema({
      arrow::field("a", arrow::list(arrow::int32())),
      arrow::field("b", arrow::int32()),
      arrow::field("c", arrow::utf8()),
  });

  return arrow::Table::Make(schema, {arr_a, arr_b, arr_c});
}

arrow::Result<std::shared_ptr<arrow::Buffer>> WriteFullFile() {
  using parquet::ArrowWriterProperties;
  using parquet::WriterProperties;

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, GetTable());

  std::shared_ptr<WriterProperties> props =
      WriterProperties::Builder()
          .max_row_group_length(30)
          ->enable_write_page_index()
          ->write_batch_size(13)
          ->data_pagesize(1)  // this will cause every batch creating a page
          ->compression(arrow::Compression::SNAPPY)
          ->build();

  std::shared_ptr<ArrowWriterProperties> arrow_props =
      ArrowWriterProperties::Builder().store_schema()->build();

  ARROW_ASSIGN_OR_RAISE(auto out_stream, ::arrow::io::BufferOutputStream::Create());

  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table.get(),
                                                 arrow::default_memory_pool(), out_stream,
                                                 /*chunk_size=*/100, props, arrow_props));

  // {
  //   // output to a local file for debugging
  //   ARROW_ASSIGN_OR_RAISE(auto outfile, arrow::io::FileOutputStream::Open(
  //                                           "/tmp/filtered_reader_test.parquet"));
  //
  //   ARROW_RETURN_NOT_OK(
  //       parquet::arrow::WriteTable(*table.get(), arrow::default_memory_pool(), outfile,
  //                                  /*chunk_size=*/100, props, arrow_props));
  // }

  return out_stream->Finish();
}

void check_rb(std::shared_ptr<arrow::RecordBatchReader> rb_reader, size_t expect_rows,
              int64_t expect_sum_of_b) {
  size_t total_rows = 0;
  int64_t sum_a = 0;
  int64_t sum_b = 0;
  int64_t sum_c = 0;
  for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch : *rb_reader) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    total_rows += batch->num_rows();

    auto a_array = std::dynamic_pointer_cast<arrow::ListArray>(batch->column(0));
    ASSERT_OK_AND_ASSIGN(auto flatten_a_array, a_array->Flatten());
    auto a_array_values = std::dynamic_pointer_cast<arrow::Int32Array>(flatten_a_array);
    for (auto iter = a_array_values->begin(); iter != a_array_values->end(); ++iter) {
      sum_a += (*iter).value();
    }

    auto b_array = std::dynamic_pointer_cast<arrow::Int32Array>(batch->column(1));
    for (auto iter = b_array->begin(); iter != b_array->end(); ++iter) {
      sum_b += (*iter).value();
    }

    auto c_array = std::dynamic_pointer_cast<arrow::StringArray>(batch->column(2));
    for (auto iter = c_array->begin(); iter != c_array->end(); ++iter) {
      sum_c += std::stoi(std::string((*iter).value()));
    }
  }
  ASSERT_EQ(expect_rows, total_rows);
  ASSERT_EQ(expect_sum_of_b * 3, sum_a);
  ASSERT_EQ(expect_sum_of_b, sum_b);
  ASSERT_EQ(expect_sum_of_b, sum_c);
}

class TestRecordBatchReaderWithRanges : public ::testing::Test {
public:
  void SetUp() {

  }

  void TearDown() {}

protected:

};

TEST(TestRecordBatchReaderWithRanges2, Normal) {
  ASSERT_OK_AND_ASSIGN(auto buffer, WriteFullFile());

  arrow::MemoryPool* pool = arrow::default_memory_pool();

  auto reader_properties = parquet::ReaderProperties(pool);
  reader_properties.set_buffer_size(4096 * 4);
  reader_properties.enable_buffered_stream();

  auto arrow_reader_props = parquet::ArrowReaderProperties();
  // arrow_reader_props.set_batch_size(64 * 1024);  // default 64 * 1024
  arrow_reader_props.set_batch_size(10);  // default 64 * 1024

  parquet::arrow::FileReaderBuilder reader_builder;
  auto in_file = std::make_shared<::arrow::io::BufferReader>(buffer);
  ASSERT_OK(reader_builder.Open(in_file, /*memory_map=*/reader_properties));
  reader_builder.memory_pool(pool);
  reader_builder.properties(arrow_reader_props);

  ASSERT_OK_AND_ASSIGN(auto arrow_reader, reader_builder.Build());

  // // case 1: row_ranges_map contains only RG {0}, other RGs should be skipped
  // {
  //   std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
  //   auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
  //   std::vector<parquet::Range> ranges;
  //   for (int64_t i = 0; i < 30; i++) {
  //     if (i % 2 == 0) ranges.push_back({i, i});
  //   }
  //   row_ranges_map->insert({0, std::make_shared<parquet::RowRanges>(ranges)});
  //   ASSERT_OK(arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, {0, 1, 2},
  //   row_ranges_map,
  //                                                &rb_reader));
  //
  //   check_rb(rb_reader, 15, 210);  // 0 + 2 + ... + 28 = 210
  // }

  // case 2: row_ranges_map contains only RG {0,2}, other RGs should be skipped
  {
    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();
    std::vector<parquet::Range> ranges;
    for (int64_t i = 0; i < 30; i++) {
      if (i % 2 == 0) ranges.push_back({i, i});
    }
    row_ranges_map->insert({0, std::make_shared<parquet::RowRanges>(ranges)});
    row_ranges_map->insert({2, std::make_shared<parquet::RowRanges>(ranges)});
    ASSERT_OK(arrow_reader->GetRecordBatchReader({0, 1, 2, 3}, {0, 1, 2}, row_ranges_map,
                                                 &rb_reader));

    check_rb(rb_reader, 30, 1320); // (0 + 2 + ... + 28) + (60 + 62 ... + 88) = 1320
  }
}
