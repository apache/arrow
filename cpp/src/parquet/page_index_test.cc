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
#include <memory>
#include <filesystem>

#include "arrow/io/file.h"
#include "arrow/util/float16.h"
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/encryption/encryption_internal.h"


/*
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include <iostream>
#include <list>
#include <chrono>
#include <random>
#include <vector>
#include <fstream>
#include <filesystem>
#include <iomanip>
#include <cassert>
#include <parquet/page_index.h>


using arrow::Status;

*/

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
  auto sz = static_cast<uint32_t>(buffer->size());
  std::unique_ptr<OffsetIndex> offset_index = OffsetIndex::Make(
      buffer->data(), &sz, properties);

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

TEST(PageIndex, WriteOffsetIndex) {
  /// Create offset index via the OffsetIndexBuilder interface.
  auto builder = OffsetIndexBuilder::Make();
  const size_t num_pages = 5;
  const std::vector<int64_t> offsets = {100, 200, 300, 400, 500};
  const std::vector<int32_t> page_sizes = {1024, 2048, 3072, 4096, 8192};
  const std::vector<int64_t> first_row_indices = {0, 10000, 20000, 30000, 40000};
  for (size_t i = 0; i < num_pages; ++i) {
    builder->AddPage(offsets[i], page_sizes[i], first_row_indices[i]);
  }
  const int64_t final_position = 4096;
  builder->Finish(final_position);

  std::vector<std::unique_ptr<OffsetIndex>> offset_indexes;
  /// 1st element is the offset index just built.
  offset_indexes.emplace_back(builder->Build());
  /// 2nd element is the offset index restored by serialize-then-deserialize round trip.
  auto sink = CreateOutputStream();
  builder->WriteTo(sink.get());
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
  auto sz = static_cast<uint32_t>(buffer->size());
  offset_indexes.emplace_back(OffsetIndex::Make(buffer->data(),
                                                &sz,
                                                default_reader_properties()));

  /// Verify the data of the offset index.
  for (const auto& offset_index : offset_indexes) {
    ASSERT_EQ(num_pages, offset_index->page_locations().size());
    for (size_t i = 0; i < num_pages; ++i) {
      const auto& page_location = offset_index->page_locations().at(i);
      ASSERT_EQ(offsets[i] + final_position, page_location.offset);
      ASSERT_EQ(page_sizes[i], page_location.compressed_page_size);
      ASSERT_EQ(first_row_indices[i], page_location.first_row_index);
    }
  }
}

void TestWriteTypedColumnIndex(schema::NodePtr node,
                               const std::vector<EncodedStatistics>& page_stats,
                               BoundaryOrder::type boundary_order, bool has_null_counts) {
  auto descr = std::make_unique<ColumnDescriptor>(node, /*max_definition_level=*/1, 0);

  auto builder = ColumnIndexBuilder::Make(descr.get());
  for (const auto& stats : page_stats) {
    builder->AddPage(stats);
  }
  ASSERT_NO_THROW(builder->Finish());

  std::vector<std::unique_ptr<ColumnIndex>> column_indexes;
  /// 1st element is the column index just built.
  column_indexes.emplace_back(builder->Build());
  /// 2nd element is the column index restored by serialize-then-deserialize round trip.
  auto sink = CreateOutputStream();
  builder->WriteTo(sink.get());
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
  column_indexes.emplace_back(ColumnIndex::Make(*descr, buffer->data(),
                                                static_cast<uint32_t>(buffer->size()),
                                                default_reader_properties()));

  /// Verify the data of the column index.
  for (const auto& column_index : column_indexes) {
    ASSERT_EQ(boundary_order, column_index->boundary_order());
    ASSERT_EQ(has_null_counts, column_index->has_null_counts());
    const size_t num_pages = column_index->null_pages().size();
    for (size_t i = 0; i < num_pages; ++i) {
      ASSERT_EQ(page_stats[i].all_null_value, column_index->null_pages()[i]);
      ASSERT_EQ(page_stats[i].min(), column_index->encoded_min_values()[i]);
      ASSERT_EQ(page_stats[i].max(), column_index->encoded_max_values()[i]);
      if (has_null_counts) {
        ASSERT_EQ(page_stats[i].null_count, column_index->null_counts()[i]);
      }
    }
  }
}

TEST(PageIndex, WriteInt32ColumnIndex) {
  auto encode = [=](int32_t value) {
    return std::string(reinterpret_cast<const char*>(&value), sizeof(int32_t));
  };

  // Integer values in the ascending order.
  std::vector<EncodedStatistics> page_stats(3);
  page_stats.at(0).set_null_count(1).set_min(encode(1)).set_max(encode(2));
  page_stats.at(1).set_null_count(2).set_min(encode(2)).set_max(encode(3));
  page_stats.at(2).set_null_count(3).set_min(encode(3)).set_max(encode(4));

  TestWriteTypedColumnIndex(schema::Int32("c1"), page_stats, BoundaryOrder::Ascending,
                            /*has_null_counts=*/true);
}

TEST(PageIndex, WriteInt64ColumnIndex) {
  auto encode = [=](int64_t value) {
    return std::string(reinterpret_cast<const char*>(&value), sizeof(int64_t));
  };

  // Integer values in the descending order.
  std::vector<EncodedStatistics> page_stats(3);
  page_stats.at(0).set_null_count(4).set_min(encode(-1)).set_max(encode(-2));
  page_stats.at(1).set_null_count(0).set_min(encode(-2)).set_max(encode(-3));
  page_stats.at(2).set_null_count(4).set_min(encode(-3)).set_max(encode(-4));

  TestWriteTypedColumnIndex(schema::Int64("c1"), page_stats, BoundaryOrder::Descending,
                            /*has_null_counts=*/true);
}

TEST(PageIndex, WriteFloatColumnIndex) {
  auto encode = [=](float value) {
    return std::string(reinterpret_cast<const char*>(&value), sizeof(float));
  };

  // Float values with no specific order.
  std::vector<EncodedStatistics> page_stats(3);
  page_stats.at(0).set_null_count(0).set_min(encode(2.2F)).set_max(encode(4.4F));
  page_stats.at(1).set_null_count(0).set_min(encode(1.1F)).set_max(encode(5.5F));
  page_stats.at(2).set_null_count(0).set_min(encode(3.3F)).set_max(encode(6.6F));

  TestWriteTypedColumnIndex(schema::Float("c1"), page_stats, BoundaryOrder::Unordered,
                            /*has_null_counts=*/true);
}

TEST(PageIndex, WriteDoubleColumnIndex) {
  auto encode = [=](double value) {
    return std::string(reinterpret_cast<const char*>(&value), sizeof(double));
  };

  // Double values with no specific order and without null count.
  std::vector<EncodedStatistics> page_stats(3);
  page_stats.at(0).set_min(encode(1.2)).set_max(encode(4.4));
  page_stats.at(1).set_min(encode(2.2)).set_max(encode(5.5));
  page_stats.at(2).set_min(encode(3.3)).set_max(encode(-6.6));

  TestWriteTypedColumnIndex(schema::Double("c1"), page_stats, BoundaryOrder::Unordered,
                            /*has_null_counts=*/false);
}

TEST(PageIndex, WriteByteArrayColumnIndex) {
  // Byte array values with identical min/max.
  std::vector<EncodedStatistics> page_stats(3);
  page_stats.at(0).set_min("bar").set_max("foo");
  page_stats.at(1).set_min("bar").set_max("foo");
  page_stats.at(2).set_min("bar").set_max("foo");

  TestWriteTypedColumnIndex(schema::ByteArray("c1"), page_stats, BoundaryOrder::Ascending,
                            /*has_null_counts=*/false);
}

TEST(PageIndex, WriteFLBAColumnIndex) {
  // FLBA values in the ascending order with some null pages
  std::vector<EncodedStatistics> page_stats(5);
  page_stats.at(0).set_min("abc").set_max("ABC");
  page_stats.at(1).all_null_value = true;
  page_stats.at(2).set_min("foo").set_max("FOO");
  page_stats.at(3).all_null_value = true;
  page_stats.at(4).set_min("xyz").set_max("XYZ");

  auto node =
      schema::PrimitiveNode::Make("c1", Repetition::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY,
                                  ConvertedType::NONE, /*length=*/3);
  TestWriteTypedColumnIndex(std::move(node), page_stats, BoundaryOrder::Ascending,
                            /*has_null_counts=*/false);
}

TEST(PageIndex, WriteFloat16ColumnIndex) {
  using ::arrow::util::Float16;
  auto encode = [](auto value) {
    auto bytes = Float16(value).ToLittleEndian();
    return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  };

  // Float16 (FLBA) values in the ascending order and without null count.
  std::vector<EncodedStatistics> page_stats(4);
  page_stats.at(0).set_min(encode(-1.3)).set_max(encode(+3.6));
  page_stats.at(1).set_min(encode(-0.2)).set_max(encode(+4.5));
  page_stats.at(2).set_min(encode(+1.1)).set_max(encode(+5.4));
  page_stats.at(3).set_min(encode(+2.0)).set_max(encode(+6.3));

  auto node = schema::PrimitiveNode::Make(
      "c1", Repetition::OPTIONAL, LogicalType::Float16(), Type::FIXED_LEN_BYTE_ARRAY,
      /*length=*/2);
  TestWriteTypedColumnIndex(std::move(node), page_stats, BoundaryOrder::Ascending,
                            /*has_null_counts=*/false);
}

TEST(PageIndex, WriteColumnIndexWithAllNullPages) {
  // All values are null.
  std::vector<EncodedStatistics> page_stats(3);
  page_stats.at(0).set_null_count(100).all_null_value = true;
  page_stats.at(1).set_null_count(100).all_null_value = true;
  page_stats.at(2).set_null_count(100).all_null_value = true;

  TestWriteTypedColumnIndex(schema::Int32("c1"), page_stats, BoundaryOrder::Unordered,
                            /*has_null_counts=*/true);
}

TEST(PageIndex, WriteColumnIndexWithInvalidNullCounts) {
  auto encode = [=](int32_t value) {
    return std::string(reinterpret_cast<const char*>(&value), sizeof(int32_t));
  };

  // Some pages do not provide null_count
  std::vector<EncodedStatistics> page_stats(3);
  page_stats.at(0).set_min(encode(1)).set_max(encode(2)).set_null_count(0);
  page_stats.at(1).set_min(encode(1)).set_max(encode(3));
  page_stats.at(2).set_min(encode(2)).set_max(encode(3)).set_null_count(0);

  TestWriteTypedColumnIndex(schema::Int32("c1"), page_stats, BoundaryOrder::Ascending,
                            /*has_null_counts=*/false);
}

TEST(PageIndex, WriteColumnIndexWithCorruptedStats) {
  auto encode = [=](int32_t value) {
    return std::string(reinterpret_cast<const char*>(&value), sizeof(int32_t));
  };

  // 2nd page does not set anything
  std::vector<EncodedStatistics> page_stats(3);
  page_stats.at(0).set_min(encode(1)).set_max(encode(2));
  page_stats.at(2).set_min(encode(3)).set_max(encode(4));

  ColumnDescriptor descr(schema::Int32("c1"), /*max_definition_level=*/1, 0);
  auto builder = ColumnIndexBuilder::Make(&descr);
  for (const auto& stats : page_stats) {
    builder->AddPage(stats);
  }
  ASSERT_NO_THROW(builder->Finish());
  ASSERT_EQ(nullptr, builder->Build());

  auto sink = CreateOutputStream();
  builder->WriteTo(sink.get());
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
  EXPECT_EQ(0, buffer->size());
}

TEST(PageIndex, TestPageIndexBuilderWithZeroRowGroup) {
  schema::NodeVector fields = {schema::Int32("c1"), schema::ByteArray("c2")};
  schema::NodePtr root = schema::GroupNode::Make("schema", Repetition::REPEATED, fields);
  SchemaDescriptor schema;
  schema.Init(root);

  auto builder = PageIndexBuilder::Make(&schema);

  // AppendRowGroup() is not called and expect throw.
  ASSERT_THROW(builder->GetColumnIndexBuilder(0), ParquetException);
  ASSERT_THROW(builder->GetOffsetIndexBuilder(0), ParquetException);

  // Finish the builder without calling AppendRowGroup().
  ASSERT_NO_THROW(builder->Finish());

  // Verify WriteTo does not write anything.
  auto sink = CreateOutputStream();
  PageIndexLocation location;
  builder->WriteTo(sink.get(), &location);
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
  ASSERT_EQ(0, buffer->size());
  ASSERT_TRUE(location.column_index_location.empty());
  ASSERT_TRUE(location.offset_index_location.empty());
}

class PageIndexBuilderTest : public ::testing::Test {
 public:
  void WritePageIndexes(int num_row_groups, int num_columns,
                        const std::vector<std::vector<EncodedStatistics>>& page_stats,
                        const std::vector<std::vector<PageLocation>>& page_locations,
                        int final_position) {
    auto builder = PageIndexBuilder::Make(&schema_);
    for (int row_group = 0; row_group < num_row_groups; ++row_group) {
      ASSERT_NO_THROW(builder->AppendRowGroup());

      for (int column = 0; column < num_columns; ++column) {
        if (static_cast<size_t>(column) < page_stats[row_group].size()) {
          auto column_index_builder = builder->GetColumnIndexBuilder(column);
          ASSERT_NO_THROW(column_index_builder->AddPage(page_stats[row_group][column]));
          ASSERT_NO_THROW(column_index_builder->Finish());
        }

        if (static_cast<size_t>(column) < page_locations[row_group].size()) {
          auto offset_index_builder = builder->GetOffsetIndexBuilder(column);
          ASSERT_NO_THROW(
              offset_index_builder->AddPage(page_locations[row_group][column]));
          ASSERT_NO_THROW(offset_index_builder->Finish(final_position));
        }
      }
    }
    ASSERT_NO_THROW(builder->Finish());

    auto sink = CreateOutputStream();
    builder->WriteTo(sink.get(), &page_index_location_);
    PARQUET_ASSIGN_OR_THROW(buffer_, sink->Finish());

    ASSERT_EQ(static_cast<size_t>(num_row_groups),
              page_index_location_.column_index_location.size());
    ASSERT_EQ(static_cast<size_t>(num_row_groups),
              page_index_location_.offset_index_location.size());
    for (int row_group = 0; row_group < num_row_groups; ++row_group) {
      ASSERT_EQ(static_cast<size_t>(num_columns),
                page_index_location_.column_index_location[row_group].size());
      ASSERT_EQ(static_cast<size_t>(num_columns),
                page_index_location_.offset_index_location[row_group].size());
    }
  }

  void CheckColumnIndex(int row_group, int column, const EncodedStatistics& stats) {
    auto column_index = ReadColumnIndex(row_group, column);
    ASSERT_NE(nullptr, column_index);
    ASSERT_EQ(size_t{1}, column_index->null_pages().size());
    ASSERT_EQ(stats.all_null_value, column_index->null_pages()[0]);
    ASSERT_EQ(stats.min(), column_index->encoded_min_values()[0]);
    ASSERT_EQ(stats.max(), column_index->encoded_max_values()[0]);
    ASSERT_EQ(stats.has_null_count, column_index->has_null_counts());
    if (stats.has_null_count) {
      ASSERT_EQ(stats.null_count, column_index->null_counts()[0]);
    }
  }

  void CheckOffsetIndex(int row_group, int column, const PageLocation& expected_location,
                        int64_t final_location) {
    auto offset_index = ReadOffsetIndex(row_group, column);
    ASSERT_NE(nullptr, offset_index);
    ASSERT_EQ(size_t{1}, offset_index->page_locations().size());
    const auto& location = offset_index->page_locations()[0];
    ASSERT_EQ(expected_location.offset + final_location, location.offset);
    ASSERT_EQ(expected_location.compressed_page_size, location.compressed_page_size);
    ASSERT_EQ(expected_location.first_row_index, location.first_row_index);
  }

 protected:
  std::unique_ptr<ColumnIndex> ReadColumnIndex(int row_group, int column) {
    auto location = page_index_location_.column_index_location[row_group][column];
    if (!location.has_value()) {
      return nullptr;
    }
    auto properties = default_reader_properties();
    return ColumnIndex::Make(*schema_.Column(column), buffer_->data() + location->offset,
                             static_cast<uint32_t>(location->length), properties);
  }

  std::unique_ptr<OffsetIndex> ReadOffsetIndex(int row_group, int column) {
    auto location = page_index_location_.offset_index_location[row_group][column];
    if (!location.has_value()) {
      return nullptr;
    }
    auto properties = default_reader_properties();
    auto sz = static_cast<uint32_t>(location->length);
    return OffsetIndex::Make(buffer_->data() + location->offset,
                             &sz, properties);
  }

  SchemaDescriptor schema_;
  std::shared_ptr<Buffer> buffer_;
  PageIndexLocation page_index_location_;
};

TEST_F(PageIndexBuilderTest, SingleRowGroup) {
  schema::NodePtr root = schema::GroupNode::Make(
      "schema", Repetition::REPEATED,
      {schema::ByteArray("c1"), schema::ByteArray("c2"), schema::ByteArray("c3")});
  schema_.Init(root);

  // Prepare page stats and page locations for single row group.
  // Note that the 3rd column does not have any stats and its page index is disabled.
  const int num_row_groups = 1;
  const int num_columns = 3;
  const std::vector<std::vector<EncodedStatistics>> page_stats = {
      /*row_group_id=0*/
      {/*column_id=0*/ EncodedStatistics().set_null_count(0).set_min("a").set_max("b"),
       /*column_id=1*/ EncodedStatistics().set_null_count(0).set_min("A").set_max("B")}};
  const std::vector<std::vector<PageLocation>> page_locations = {
      /*row_group_id=0*/
      {/*column_id=0*/ {/*offset=*/128, /*compressed_page_size=*/512,
                        /*first_row_index=*/0},
       /*column_id=1*/ {/*offset=*/1024, /*compressed_page_size=*/512,
                        /*first_row_index=*/0}}};
  const int64_t final_position = 200;

  WritePageIndexes(num_row_groups, num_columns, page_stats, page_locations,
                   final_position);

  // Verify that first two columns have good page indexes.
  for (int column = 0; column < 2; ++column) {
    CheckColumnIndex(/*row_group=*/0, column, page_stats[0][column]);
    CheckOffsetIndex(/*row_group=*/0, column, page_locations[0][column], final_position);
  }

  // Verify the 3rd column does not have page indexes.
  ASSERT_EQ(nullptr, ReadColumnIndex(/*row_group=*/0, /*column=*/2));
  ASSERT_EQ(nullptr, ReadOffsetIndex(/*row_group=*/0, /*column=*/2));
}

TEST_F(PageIndexBuilderTest, TwoRowGroups) {
  schema::NodePtr root = schema::GroupNode::Make(
      "schema", Repetition::REPEATED, {schema::ByteArray("c1"), schema::ByteArray("c2")});
  schema_.Init(root);

  // Prepare page stats and page locations for two row groups.
  // Note that the 2nd column in the 2nd row group has corrupted stats.
  const int num_row_groups = 2;
  const int num_columns = 2;
  const std::vector<std::vector<EncodedStatistics>> page_stats = {
      /*row_group_id=0*/
      {/*column_id=0*/ EncodedStatistics().set_min("a").set_max("b"),
       /*column_id=1*/ EncodedStatistics().set_null_count(0).set_min("A").set_max("B")},
      /*row_group_id=1*/
      {/*column_id=0*/ EncodedStatistics() /* corrupted stats */,
       /*column_id=1*/ EncodedStatistics().set_null_count(0).set_min("bar").set_max(
           "foo")}};
  const std::vector<std::vector<PageLocation>> page_locations = {
      /*row_group_id=0*/
      {/*column_id=0*/ {/*offset=*/128, /*compressed_page_size=*/512,
                        /*first_row_index=*/0},
       /*column_id=1*/ {/*offset=*/1024, /*compressed_page_size=*/512,
                        /*first_row_index=*/0}},
      /*row_group_id=0*/
      {/*column_id=0*/ {/*offset=*/128, /*compressed_page_size=*/512,
                        /*first_row_index=*/0},
       /*column_id=1*/ {/*offset=*/1024, /*compressed_page_size=*/512,
                        /*first_row_index=*/0}}};
  const int64_t final_position = 200;

  WritePageIndexes(num_row_groups, num_columns, page_stats, page_locations,
                   final_position);

  // Verify that all columns have good column indexes except the 2nd column in the 2nd row
  // group.
  CheckColumnIndex(/*row_group=*/0, /*column=*/0, page_stats[0][0]);
  CheckColumnIndex(/*row_group=*/0, /*column=*/1, page_stats[0][1]);
  CheckColumnIndex(/*row_group=*/1, /*column=*/1, page_stats[1][1]);
  ASSERT_EQ(nullptr, ReadColumnIndex(/*row_group=*/1, /*column=*/0));

  // Verify that two columns have good offset indexes.
  CheckOffsetIndex(/*row_group=*/0, /*column=*/0, page_locations[0][0], final_position);
  CheckOffsetIndex(/*row_group=*/0, /*column=*/1, page_locations[0][1], final_position);
  CheckOffsetIndex(/*row_group=*/1, /*column=*/0, page_locations[1][0], final_position);
  CheckOffsetIndex(/*row_group=*/1, /*column=*/1, page_locations[1][1], final_position);
}

using ::arrow::Status;

std::shared_ptr<::arrow::Table> GetTable(int nColumns, int nRows)
{
  std::random_device dev;
  std::vector<std::shared_ptr<::arrow::Array>> arrays;
  std::vector<std::shared_ptr<::arrow::Field>> fields;

  // For simplicity, we'll create int32 columns. You can expand this to handle other types.
  for (int i = 0; i < nColumns; i++)
  {
    ::arrow::FloatBuilder builder;

    for (int j = 0; j < nRows; j++)
    {
      int reps = j / 10;
      if (j % 10 < reps) {
        // repeat first value reps times
        // do this to force varying length encoding dictionary
        float val = (j / 10) * 10;
        assert(builder.Append(val).ok());
      } else {
        // keep going with ascending values
        assert(builder.Append(static_cast<float>(j)).ok());
      }
    }

    std::shared_ptr<::arrow::Array> array;
    assert(builder.Finish(&array).ok());

    arrays.push_back(array);
    fields.push_back(::arrow::field("c_" + std::to_string(i), ::arrow::float32(), false));
  }

  auto table = ::arrow::Table::Make(::arrow::schema(fields), arrays);
  return table;
}

void WriteTableToParquet(size_t nColumns, size_t nRows, const std::string &filename, int64_t chunkSize, bool use_dict)
{
  if(std::filesystem::exists(filename)) {
    return;
  }

  auto table = GetTable(nColumns, nRows);
  auto result = ::arrow::io::FileOutputStream::Open(filename);
  auto outfile = result.ValueOrDie();

  parquet::WriterProperties::Builder builder;
  builder.enable_write_page_index()
          ->max_row_group_length(chunkSize);
  if(!use_dict) {
    builder.disable_dictionary();
  }
  auto properties = builder.build();
  assert(parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(),
                                    outfile, chunkSize, properties) == Status::OK());
}

// Read page indexes one piece at a time via full metadata and PageIndexReader
std::vector<ColumnOffsets> ReadPageIndexes(const std::string &filename) {
  auto read_properties = parquet::default_reader_properties();
  auto reader = parquet::ParquetFileReader::OpenFile(filename, false, read_properties);
  auto metadata = reader->metadata();
  std::shared_ptr<parquet::PageIndexReader> page_index_reader = reader->GetPageIndexReader();

  std::vector<ColumnOffsets> rowgroup_offsets;
  for (int rg = 0; rg < metadata->num_row_groups(); ++rg) {
    std::shared_ptr<parquet::RowGroupPageIndexReader> row_group_index_reader = page_index_reader->RowGroup(rg);
    auto row_group_reader = reader->RowGroup(rg);
    ColumnOffsets offset_indexes;
    for (int col = 0; col < metadata->num_columns(); ++col) {
      auto offset_index = row_group_index_reader->GetOffsetIndex(col);
      offset_indexes.push_back(offset_index);
    }
    rowgroup_offsets.push_back(offset_indexes);
  }
  return rowgroup_offsets;
}

// Read all page indexes as one chunk from parquet file
std::vector<ColumnOffsets> ReadPageIndexesDirect(const std::string &filename, std::shared_ptr<parquet::FileMetaData> metadata) {
  auto read_properties = parquet::default_reader_properties();
  auto reader = parquet::ParquetFileReader::OpenFile(filename, false, read_properties, metadata);
  std::shared_ptr<parquet::PageIndexReader> page_index_reader = reader->GetPageIndexReader();

  std::vector<ColumnOffsets> rowgroup_offsets = page_index_reader->GetAllOffsets();
  return rowgroup_offsets;
}

std::string ReadIndexedRow(const std::string &filename, std::vector<int> indicies,
                               std::shared_ptr<parquet::FileMetaData> metadata,
                               bool string_result = true) {
  auto readerProperties = parquet::default_reader_properties();
  parquet::arrow::FileReaderBuilder fileReaderBuilder;
  assert(fileReaderBuilder.OpenFile(filename, false, readerProperties, metadata) == Status::OK());
  auto reader = fileReaderBuilder.Build();

  // Read table layout + data to string
  std::shared_ptr<::arrow::Table> parquet_table;
  assert(reader->get()->ReadTable(indicies, &parquet_table) == Status::OK());
  if(string_result) {
    std::string values = parquet_table->ToString();
    return values;
  }
  return "";
}

void ReadColumnsUsingOffsetIndex(const std::string &filename, std::vector<int> indicies)
{
  std::shared_ptr<::arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile, ::arrow::io::ReadableFile::Open(filename));

  ReaderProperties props = parquet::default_reader_properties();
  props.set_read_only_rowgroup_0(true);
  auto metadata_row_0 = parquet::ReadMetaData(infile, props); // only row group 0

  auto metadata_all_rows = parquet::ReadMetaData(infile);
  // PrintSchema(metadata->schema()->schema_root().get(), std::cout);

  // Retrieve and check page offsets
  auto rowgroup_offsets = ReadPageIndexesDirect(filename, metadata_row_0);
  auto expected_rowgroup_offsets = ReadPageIndexes(filename);
  for(int rg=0; rg < metadata_all_rows->num_row_groups(); ++rg) {
    for(int col=0; col < metadata_all_rows->num_columns(); ++col) {
      auto expected = expected_rowgroup_offsets[rg][col];
      auto actual = rowgroup_offsets[rg][col];
      ASSERT_EQ(expected->page_locations(), actual->page_locations()) << "The retrieved page index locations are incorrect";
    }
  }

  // Use indexes to read and check rows
  std::vector<int> row_group_test({0, 3, 9});
  for(auto row_group: row_group_test) {
    auto expected_metadata = metadata_all_rows->Subset({row_group});
    std::string expected_read = ReadIndexedRow(filename, indicies, expected_metadata);

    metadata_row_0->IndexTo(row_group, rowgroup_offsets);
    std::string indexed_read = ReadIndexedRow(filename, indicies, metadata_row_0);

    if(false) {
      std::cerr << "Row group: " << row_group << "\n";
      std::cerr << "Correct read:\n";
      std::cerr << expected_read;
      std::cerr << "\n\nOffset based read:\n";
      std::cerr << indexed_read;
      std::cerr << "\n\n";
      std::cerr << "Expected metadata:\n";
      PrintSchema(expected_metadata->schema()->schema_root().get(), std::cerr);
      std::cerr << "Metadata row 0:\n";
      PrintSchema(metadata_row_0->schema()->schema_root().get(), std::cerr);
    }

    ASSERT_EQ(expected_read, indexed_read);
  }
}

TEST_F(PageIndexBuilderTest, OffsetReader) {
  std::string dir_string(parquet::test::get_data_dir());
  std::string path_no_dict = dir_string + "/index_reader_test.parquet";
  std::string path_with_dict = dir_string + "/index_reader_test_dict.parquet";

  int nColumn = 10;
  int nRow = 95;
  int chunk_size = 10;
  std::vector<int> indicies(nColumn/2);
  std::iota(indicies.begin(), indicies.end(), 0);

  // Test w/0 dictionary
  WriteTableToParquet(nColumn, nRow, path_no_dict.c_str(), chunk_size, false);
  ReadColumnsUsingOffsetIndex(path_no_dict.c_str(), indicies);

  // Test with dictionary
  WriteTableToParquet(nColumn, nRow, path_with_dict.c_str(), chunk_size, true);
  ReadColumnsUsingOffsetIndex(path_with_dict.c_str(), indicies);
}


void BenchmarkRegularRead(const std::string &filename, std::vector<int> &indicies, std::chrono::microseconds *tm_std,
                          std::vector<int> &row_group_test,
                          const std::shared_ptr<::arrow::io::ReadableFile> &infile) {// Benchmark regular read
// Slightly cheating.
// First to read pays a penalty for file buffering
  auto std_begin = std::chrono::steady_clock::now();
  auto metadata_all_rows = ReadMetaData(infile);
  for(auto row_group: row_group_test) {
    auto expected_metadata = metadata_all_rows->Subset({row_group});
    ReadIndexedRow(filename, indicies, expected_metadata, false);
  }
  auto std_end = std::chrono::steady_clock::now();
  *tm_std = std::chrono::duration_cast<std::chrono::microseconds>(std_end - std_begin);
}

void BenchmarkIndexedRead(const std::string &filename, std::vector<int> &indicies, std::chrono::microseconds *tm_index,
                          std::vector<int> &row_group_test,
                          const std::shared_ptr<::arrow::io::ReadableFile> &infile) {// Benchmark indexed read
  auto index_begin = std::chrono::steady_clock::now();
  ReaderProperties props = default_reader_properties();
  props.set_read_only_rowgroup_0(true);
  auto metadata_row_0 = ReadMetaData(infile, props); // only row group 0
  auto rowgroup_offsets = ReadPageIndexesDirect(filename, metadata_row_0);
  for(auto row_group: row_group_test) {
    metadata_row_0->IndexTo(row_group, rowgroup_offsets);
    ReadIndexedRow(filename, indicies, metadata_row_0, false);
  }
  auto index_end = std::chrono::steady_clock::now();
  *tm_index = std::chrono::duration_cast<std::chrono::microseconds>(index_end - index_begin);
}

void BenchmarkReadColumnsUsingOffsetIndex(const std::string &filename, std::vector<int> indicies,
                                          std::chrono::microseconds *tm_index, std::chrono::microseconds *tm_std)
{
  std::vector<int> row_group_test({3, 9});
  std::shared_ptr<::arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile, ::arrow::io::ReadableFile::Open(filename));
  BenchmarkRegularRead(filename, indicies, tm_std, row_group_test, infile);
  BenchmarkIndexedRead(filename, indicies, tm_index, row_group_test, infile);

}

TEST_F(PageIndexBuilderTest, BenchmarkReader) {
  std::string dir_string(parquet::test::get_data_dir());
  std::string path = dir_string + "/index_reader_bm.parquet";

  int nColumn = 6000;
  int nRow = 1000;
  int chunk_size = 10;
  std::vector<int> indicies(nColumn/2);
  std::iota(indicies.begin(), indicies.end(), 0);
  WriteTableToParquet(nColumn, nRow, path.c_str(), chunk_size, false);
  std::chrono::microseconds tm_index, tm_std;
  BenchmarkReadColumnsUsingOffsetIndex(path.c_str(), indicies, &tm_index, &tm_std);

  std::cerr << "Benchmark Results: \n"
            << "(nColumn=" << nColumn << ", nRow=" << nRow << ")"
            << ", chunk_size=" << chunk_size
            << ", time with index=" << tm_index.count()/1e+6 << "s"
          << ", time with full metadata=" << tm_std.count()/1e+6 << "s"
          << std::endl;

  /*
  Benchmark Results:
  (nColumn=6000, nRow=1000), chunk_size=10, time with index=0.813804s, time with full metadata=1.94265s
  */
}


}  // namespace parquet
