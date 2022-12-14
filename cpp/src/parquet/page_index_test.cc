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

#include "parquet/page_index.h"

#include <gtest/gtest.h>

#include "arrow/io/file.h"
#include "parquet/file_reader.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"

namespace parquet {

TEST(PageIndex, ReadOffsetIndex) {
  std::string dir_string(parquet::test::get_data_dir());
  std::string path = dir_string + "/alltypes_tiny_pages.parquet";
  auto reader = ParquetFileReader::OpenFile(path, false);
  auto file_metadata = reader->metadata();

  // Get offset index location to column 0 of row group 0.
  const int row_group_id = 0;
  const int column_id = 0;
  ASSERT_LT(row_group_id, file_metadata->num_row_groups());
  ASSERT_LT(column_id, file_metadata->num_columns());
  auto index_location = file_metadata->RowGroup(row_group_id)
                            ->ColumnChunk(column_id)
                            ->GetOffsetIndexLocation();
  ASSERT_TRUE(index_location.has_value());

  // Read serialized offset index from the file.
  std::shared_ptr<::arrow::io::RandomAccessFile> source;
  PARQUET_ASSIGN_OR_THROW(source, ::arrow::io::ReadableFile::Open(path));
  PARQUET_ASSIGN_OR_THROW(auto buffer,
                          source->ReadAt(index_location->offset, index_location->length));
  PARQUET_THROW_NOT_OK(source->Close());

  // Deserialize offset index.
  auto properties = default_reader_properties();
  std::unique_ptr<OffsetIndex> offset_index = OffsetIndex::Make(
      buffer->data(), static_cast<uint32_t>(buffer->size()), properties);

  // Verify only partial data as it contains 325 pages in total.
  const size_t num_pages = 325;
  const std::vector<size_t> page_indices = {0, 100, 200, 300};
  const std::vector<PageLocation> page_locations = {
      PageLocation{4, 109, 0}, PageLocation{11480, 133, 2244},
      PageLocation{22980, 133, 4494}, PageLocation{34480, 133, 6744}};

  ASSERT_EQ(num_pages, offset_index->page_locations().size());
  for (size_t i = 0; i < page_indices.size(); ++i) {
    size_t page_id = page_indices.at(i);
    const auto& read_page_location = offset_index->page_locations().at(page_id);
    const auto& expected_page_location = page_locations.at(i);
    ASSERT_EQ(expected_page_location.offset, read_page_location.offset);
    ASSERT_EQ(expected_page_location.compressed_page_size,
              read_page_location.compressed_page_size);
    ASSERT_EQ(expected_page_location.first_row_index, read_page_location.first_row_index);
  }
}

template <typename DType, typename T = typename DType::c_type>
void TestReadTypedColumnIndex(const std::string& file_name, int column_id,
                              size_t num_pages, BoundaryOrder::type boundary_order,
                              const std::vector<size_t>& page_indices,
                              const std::vector<bool>& null_pages,
                              const std::vector<T>& min_values,
                              const std::vector<T>& max_values,
                              bool has_null_counts = false,
                              const std::vector<int64_t>& null_counts = {}) {
  std::string dir_string(parquet::test::get_data_dir());
  std::string path = dir_string + "/" + file_name;
  auto reader = ParquetFileReader::OpenFile(path, false);
  auto file_metadata = reader->metadata();

  // Get column index location to a specific column chunk.
  const int row_group_id = 0;
  ASSERT_LT(row_group_id, file_metadata->num_row_groups());
  ASSERT_LT(column_id, file_metadata->num_columns());
  auto index_location = file_metadata->RowGroup(row_group_id)
                            ->ColumnChunk(column_id)
                            ->GetColumnIndexLocation();
  ASSERT_TRUE(index_location.has_value());

  // Read serialized column index from the file.
  std::shared_ptr<::arrow::io::RandomAccessFile> source;
  PARQUET_ASSIGN_OR_THROW(source, ::arrow::io::ReadableFile::Open(path));
  PARQUET_ASSIGN_OR_THROW(auto buffer,
                          source->ReadAt(index_location->offset, index_location->length));
  PARQUET_THROW_NOT_OK(source->Close());

  // Deserialize column index.
  auto properties = default_reader_properties();
  auto descr = file_metadata->schema()->Column(column_id);
  std::unique_ptr<ColumnIndex> column_index = ColumnIndex::Make(
      *descr, buffer->data(), static_cast<uint32_t>(buffer->size()), properties);
  auto typed_column_index = dynamic_cast<TypedColumnIndex<DType>*>(column_index.get());
  ASSERT_TRUE(typed_column_index != nullptr);

  // Verify only partial data as there are too many pages.
  ASSERT_EQ(num_pages, column_index->null_pages().size());
  ASSERT_EQ(has_null_counts, column_index->has_null_counts());
  ASSERT_EQ(boundary_order, column_index->boundary_order());
  for (size_t i = 0; i < page_indices.size(); ++i) {
    size_t page_id = page_indices.at(i);
    ASSERT_EQ(null_pages.at(i), column_index->null_pages().at(page_id));
    if (has_null_counts) {
      ASSERT_EQ(null_counts.at(i), column_index->null_counts().at(page_id));
    }
    // min/max values are only meaningful for non-null pages.
    if (!null_pages.at(i)) {
      if constexpr (std::is_same_v<T, double>) {
        ASSERT_DOUBLE_EQ(min_values.at(i), typed_column_index->min_values().at(page_id));
        ASSERT_DOUBLE_EQ(max_values.at(i), typed_column_index->max_values().at(page_id));
      } else if constexpr (std::is_same_v<T, float>) {
        ASSERT_FLOAT_EQ(min_values.at(i), typed_column_index->min_values().at(page_id));
        ASSERT_FLOAT_EQ(max_values.at(i), typed_column_index->max_values().at(page_id));
      } else if constexpr (std::is_same_v<T, FLBA>) {
        auto len = descr->type_length();
        ASSERT_EQ(0, ::memcmp(min_values.at(i).ptr,
                              typed_column_index->min_values().at(page_id).ptr, len));
        ASSERT_EQ(0, ::memcmp(max_values.at(i).ptr,
                              typed_column_index->max_values().at(page_id).ptr, len));
      } else {
        ASSERT_EQ(min_values.at(i), typed_column_index->min_values().at(page_id));
        ASSERT_EQ(max_values.at(i), typed_column_index->max_values().at(page_id));
      }
    }
  }
}

TEST(PageIndex, ReadInt64ColumnIndex) {
  const int column_id = 5;
  const size_t num_pages = 528;
  const BoundaryOrder::type boundary_order = BoundaryOrder::Unordered;
  const std::vector<size_t> page_indices = {0, 99, 426, 520};
  const std::vector<bool> null_pages = {false, false, false, false};
  const bool has_null_counts = true;
  const std::vector<int64_t> null_counts = {0, 0, 0, 0};
  const std::vector<int64_t> min_values = {0, 10, 0, 0};
  const std::vector<int64_t> max_values = {90, 90, 80, 70};

  TestReadTypedColumnIndex<Int64Type>(
      "alltypes_tiny_pages.parquet", column_id, num_pages, boundary_order, page_indices,
      null_pages, min_values, max_values, has_null_counts, null_counts);
}

TEST(PageIndex, ReadDoubleColumnIndex) {
  const int column_id = 7;
  const size_t num_pages = 528;
  const BoundaryOrder::type boundary_order = BoundaryOrder::Unordered;
  const std::vector<size_t> page_indices = {0, 51, 212, 527};
  const std::vector<bool> null_pages = {false, false, false, false};
  const bool has_null_counts = true;
  const std::vector<int64_t> null_counts = {0, 0, 0, 0};
  const std::vector<double> min_values = {-0, 30.3, 10.1, 40.4};
  const std::vector<double> max_values = {90.9, 90.9, 90.9, 60.6};

  TestReadTypedColumnIndex<DoubleType>(
      "alltypes_tiny_pages.parquet", column_id, num_pages, boundary_order, page_indices,
      null_pages, min_values, max_values, has_null_counts, null_counts);
}

TEST(PageIndex, ReadByteArrayColumnIndex) {
  const int column_id = 9;
  const size_t num_pages = 352;
  const BoundaryOrder::type boundary_order = BoundaryOrder::Ascending;
  const std::vector<size_t> page_indices = {0, 128, 256};
  const std::vector<bool> null_pages = {false, false, false};
  const bool has_null_counts = true;
  const std::vector<int64_t> null_counts = {0, 0, 0};

  // All min values are "0" and max values are "9".
  const std::string_view min_value = "0";
  const std::string_view max_value = "9";
  const std::vector<ByteArray> min_values = {ByteArray{min_value}, ByteArray{min_value},
                                             ByteArray{min_value}};
  const std::vector<ByteArray> max_values = {ByteArray{max_value}, ByteArray{max_value},
                                             ByteArray{max_value}};

  TestReadTypedColumnIndex<ByteArrayType>(
      "alltypes_tiny_pages.parquet", column_id, num_pages, boundary_order, page_indices,
      null_pages, min_values, max_values, has_null_counts, null_counts);
}

TEST(PageIndex, ReadBoolColumnIndex) {
  const int column_id = 1;
  const size_t num_pages = 82;
  const BoundaryOrder::type boundary_order = BoundaryOrder::Ascending;
  const std::vector<size_t> page_indices = {0, 16, 64};
  const std::vector<bool> null_pages = {false, false, false};
  const bool has_null_counts = true;
  const std::vector<int64_t> null_counts = {0, 0, 0};
  const std::vector<bool> min_values = {false, false, false};
  const std::vector<bool> max_values = {true, true, true};

  TestReadTypedColumnIndex<BooleanType>(
      "alltypes_tiny_pages.parquet", column_id, num_pages, boundary_order, page_indices,
      null_pages, min_values, max_values, has_null_counts, null_counts);
}

TEST(PageIndex, ReadFixedLengthByteArrayColumnIndex) {
  auto to_flba = [](const char* ptr) {
    return FLBA{reinterpret_cast<const uint8_t*>(ptr)};
  };

  const int column_id = 0;
  const size_t num_pages = 10;
  const BoundaryOrder::type boundary_order = BoundaryOrder::Descending;
  const std::vector<size_t> page_indices = {0, 4, 8};
  const std::vector<bool> null_pages = {false, false, false};
  const bool has_null_counts = true;
  const std::vector<int64_t> null_counts = {9, 13, 9};
  const std::vector<const char*> min_literals = {"\x00\x00\x03\x85", "\x00\x00\x01\xF5",
                                                 "\x00\x00\x00\x65"};
  const std::vector<const char*> max_literals = {"\x00\x00\x03\xE8", "\x00\x00\x02\x58",
                                                 "\x00\x00\x00\xC8"};
  const std::vector<FLBA> min_values = {
      to_flba(min_literals[0]), to_flba(min_literals[1]), to_flba(min_literals[2])};
  const std::vector<FLBA> max_values = {
      to_flba(max_literals[0]), to_flba(max_literals[1]), to_flba(max_literals[2])};

  TestReadTypedColumnIndex<FLBAType>(
      "fixed_length_byte_array.parquet", column_id, num_pages, boundary_order,
      page_indices, null_pages, min_values, max_values, has_null_counts, null_counts);
}

TEST(PageIndex, ReadColumnIndexWithNullPage) {
  const int column_id = 0;
  const size_t num_pages = 10;
  const BoundaryOrder::type boundary_order = BoundaryOrder::Unordered;
  const std::vector<size_t> page_indices = {2, 4, 8};
  const std::vector<bool> null_pages = {true, false, false};
  const bool has_null_counts = true;
  const std::vector<int64_t> null_counts = {100, 16, 8};
  const std::vector<int32_t> min_values = {0, -2048691758, -2046900272};
  const std::vector<int32_t> max_values = {0, 2143189382, 2087168549};

  TestReadTypedColumnIndex<Int32Type>(
      "int32_with_null_pages.parquet", column_id, num_pages, boundary_order, page_indices,
      null_pages, min_values, max_values, has_null_counts, null_counts);
}

}  // namespace parquet
