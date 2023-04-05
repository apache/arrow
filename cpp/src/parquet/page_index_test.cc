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

struct PageIndexRanges {
  int64_t column_index_offset;
  int64_t column_index_length;
  int64_t offset_index_offset;
  int64_t offset_index_length;
};

using RowGroupRanges = std::vector<PageIndexRanges>;

/// Creates an FileMetaData object w/ single row group based on data in
/// 'row_group_ranges'. It sets the offsets and sizes of the column index and offset index
/// members of the row group. It doesn't set the member if the input value is -1.
std::shared_ptr<FileMetaData> ConstructFakeMetaData(
    const RowGroupRanges& row_group_ranges) {
  format::RowGroup row_group;
  for (auto& page_index_ranges : row_group_ranges) {
    format::ColumnChunk col_chunk;
    if (page_index_ranges.column_index_offset != -1) {
      col_chunk.__set_column_index_offset(page_index_ranges.column_index_offset);
    }
    if (page_index_ranges.column_index_length != -1) {
      col_chunk.__set_column_index_length(
          static_cast<int32_t>(page_index_ranges.column_index_length));
    }
    if (page_index_ranges.offset_index_offset != -1) {
      col_chunk.__set_offset_index_offset(page_index_ranges.offset_index_offset);
    }
    if (page_index_ranges.offset_index_length != -1) {
      col_chunk.__set_offset_index_length(
          static_cast<int32_t>(page_index_ranges.offset_index_length));
    }
    row_group.columns.push_back(col_chunk);
  }

  format::FileMetaData metadata;
  metadata.row_groups.push_back(row_group);

  metadata.schema.emplace_back();
  schema::NodeVector fields;
  for (size_t i = 0; i < row_group_ranges.size(); ++i) {
    fields.push_back(schema::Int64(std::to_string(i)));
    metadata.schema.emplace_back();
    fields.back()->ToParquet(&metadata.schema.back());
  }
  schema::GroupNode::Make("schema", Repetition::REPEATED, fields)
      ->ToParquet(&metadata.schema.front());

  auto sink = CreateOutputStream();
  ThriftSerializer{}.Serialize(&metadata, sink.get());
  auto buffer = sink->Finish().MoveValueUnsafe();
  uint32_t len = static_cast<uint32_t>(buffer->size());
  return FileMetaData::Make(buffer->data(), &len);
}

/// Validates that 'DeterminePageIndexRangesInRowGroup()' selects the expected file
/// offsets and sizes or returns false when the row group doesn't have a page index.
void ValidatePageIndexRange(const RowGroupRanges& row_group_ranges,
                            const std::vector<int32_t>& column_indices,
                            bool expected_has_column_index,
                            bool expected_has_offset_index, int expected_ci_start,
                            int expected_ci_size, int expected_oi_start,
                            int expected_oi_size) {
  auto file_metadata = ConstructFakeMetaData(row_group_ranges);
  auto read_range = PageIndexReader::DeterminePageIndexRangesInRowGroup(
      *file_metadata->RowGroup(0), column_indices);
  ASSERT_EQ(expected_has_column_index, read_range.column_index.has_value());
  ASSERT_EQ(expected_has_offset_index, read_range.offset_index.has_value());
  if (expected_has_column_index) {
    EXPECT_EQ(expected_ci_start, read_range.column_index->offset);
    EXPECT_EQ(expected_ci_size, read_range.column_index->length);
  }
  if (expected_has_offset_index) {
    EXPECT_EQ(expected_oi_start, read_range.offset_index->offset);
    EXPECT_EQ(expected_oi_size, read_range.offset_index->length);
  }
}

/// This test constructs a couple of artificial row groups with page index offsets in
/// them. Then it validates if PageIndexReader::DeterminePageIndexRangesInRowGroup()
/// properly computes the file range that contains the whole page index.
TEST(PageIndex, DeterminePageIndexRangesInRowGroup) {
  // No Column chunks
  ValidatePageIndexRange({}, {}, false, false, -1, -1, -1, -1);
  // No page index at all.
  ValidatePageIndexRange({{-1, -1, -1, -1}}, {}, false, false, -1, -1, -1, -1);
  // Page index for single column chunk.
  ValidatePageIndexRange({{10, 5, 15, 5}}, {}, true, true, 10, 5, 15, 5);
  // Page index for two column chunks.
  ValidatePageIndexRange({{10, 5, 30, 25}, {15, 15, 50, 20}}, {}, true, true, 10, 20, 30,
                         40);
  // Page index for second column chunk.
  ValidatePageIndexRange({{-1, -1, -1, -1}, {20, 10, 30, 25}}, {}, true, true, 20, 10, 30,
                         25);
  // Page index for first column chunk.
  ValidatePageIndexRange({{10, 5, 15, 5}, {-1, -1, -1, -1}}, {}, true, true, 10, 5, 15,
                         5);
  // Missing offset index for first column chunk. Gap in column index.
  ValidatePageIndexRange({{10, 5, -1, -1}, {20, 10, 30, 25}}, {}, true, true, 10, 20, 30,
                         25);
  // Missing offset index for second column chunk.
  ValidatePageIndexRange({{10, 5, 25, 5}, {20, 10, -1, -1}}, {}, true, true, 10, 20, 25,
                         5);
  // Four column chunks.
  ValidatePageIndexRange(
      {{100, 10, 220, 30}, {110, 25, 250, 10}, {140, 30, 260, 40}, {200, 10, 300, 100}},
      {}, true, true, 100, 110, 220, 180);
}

/// This test constructs a couple of artificial row groups with page index offsets in
/// them. Then it validates if PageIndexReader::DeterminePageIndexRangesInRowGroup()
/// properly computes the file range that contains the page index of selected columns.
TEST(PageIndex, DeterminePageIndexRangesInRowGroupWithPartialColumnsSelected) {
  // No page index at all.
  ValidatePageIndexRange({{-1, -1, -1, -1}}, {0}, false, false, -1, -1, -1, -1);
  // Page index for single column chunk.
  ValidatePageIndexRange({{10, 5, 15, 5}}, {0}, true, true, 10, 5, 15, 5);
  // Page index for the 1st column chunk.
  ValidatePageIndexRange({{10, 5, 30, 25}, {15, 15, 50, 20}}, {0}, true, true, 10, 5, 30,
                         25);
  // Page index for the 2nd column chunk.
  ValidatePageIndexRange({{10, 5, 30, 25}, {15, 15, 50, 20}}, {1}, true, true, 15, 15, 50,
                         20);
  // Only 2nd column is selected among four column chunks.
  ValidatePageIndexRange(
      {{100, 10, 220, 30}, {110, 25, 250, 10}, {140, 30, 260, 40}, {200, 10, 300, 100}},
      {1}, true, true, 110, 25, 250, 10);
  // Only 2nd and 3rd columns are selected among four column chunks.
  ValidatePageIndexRange(
      {{100, 10, 220, 30}, {110, 25, 250, 10}, {140, 30, 260, 40}, {200, 10, 300, 100}},
      {1, 2}, true, true, 110, 60, 250, 50);
  // Only 2nd and 4th columns are selected among four column chunks.
  ValidatePageIndexRange(
      {{100, 10, 220, 30}, {110, 25, 250, 10}, {140, 30, 260, 40}, {200, 10, 300, 100}},
      {1, 3}, true, true, 110, 100, 250, 150);
  // Only 1st, 2nd and 4th columns are selected among four column chunks.
  ValidatePageIndexRange(
      {{100, 10, 220, 30}, {110, 25, 250, 10}, {140, 30, 260, 40}, {200, 10, 300, 100}},
      {0, 1, 3}, true, true, 100, 110, 220, 180);
  // 3rd column is selected but not present in the row group.
  EXPECT_THROW(ValidatePageIndexRange({{10, 5, 30, 25}, {15, 15, 50, 20}}, {2}, false,
                                      false, -1, -1, -1, -1),
               ParquetException);
}

/// This test constructs a couple of artificial row groups with page index offsets in
/// them. Then it validates if PageIndexReader::DeterminePageIndexRangesInRowGroup()
/// properly detects if column index or offset index is missing.
TEST(PageIndex, DeterminePageIndexRangesInRowGroupWithMissingPageIndex) {
  // No column index at all.
  ValidatePageIndexRange({{-1, -1, 15, 5}}, {}, false, true, -1, -1, 15, 5);
  // No offset index at all.
  ValidatePageIndexRange({{10, 5, -1, -1}}, {}, true, false, 10, 5, -1, -1);
  // No column index at all among two column chunks.
  ValidatePageIndexRange({{-1, -1, 30, 25}, {-1, -1, 50, 20}}, {}, false, true, -1, -1,
                         30, 40);
  // No offset index at all among two column chunks.
  ValidatePageIndexRange({{10, 5, -1, -1}, {15, 15, -1, -1}}, {}, true, false, 10, 20, -1,
                         -1);
}

}  // namespace parquet
