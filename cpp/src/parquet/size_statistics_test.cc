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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <algorithm>
#include <random>

#include "arrow/buffer.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/span.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "parquet/column_writer.h"
#include "parquet/file_writer.h"
#include "parquet/page_index.h"
#include "parquet/schema.h"
#include "parquet/size_statistics.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"
#include "parquet/types.h"

namespace parquet {

TEST(SizeStatistics, ThriftSerDe) {
  const std::vector<int64_t> kDefLevels = {128, 64, 32, 16};
  const std::vector<int64_t> kRepLevels = {100, 80, 60, 40, 20};
  constexpr int64_t kUnencodedByteArrayDataBytes = 1234;

  for (const auto& descr :
       {std::make_unique<ColumnDescriptor>(schema::Int32("a"), /*max_def_level=*/3,
                                           /*max_rep_level=*/4),
        std::make_unique<ColumnDescriptor>(schema::ByteArray("a"), /*max_def_level=*/3,
                                           /*max_rep_level=*/4)}) {
    auto size_statistics = SizeStatistics::Make(descr.get());
    size_statistics->repetition_level_histogram = kRepLevels;
    size_statistics->definition_level_histogram = kDefLevels;
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      size_statistics->IncrementUnencodedByteArrayDataBytes(kUnencodedByteArrayDataBytes);
    }
    auto thrift_statistics = ToThrift(*size_statistics);
    auto restored_statistics = FromThrift(thrift_statistics);
    EXPECT_EQ(restored_statistics.definition_level_histogram, kDefLevels);
    EXPECT_EQ(restored_statistics.repetition_level_histogram, kRepLevels);
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      EXPECT_TRUE(restored_statistics.unencoded_byte_array_data_bytes.has_value());
      EXPECT_EQ(restored_statistics.unencoded_byte_array_data_bytes.value(),
                kUnencodedByteArrayDataBytes);
    } else {
      EXPECT_FALSE(restored_statistics.unencoded_byte_array_data_bytes.has_value());
    }
  }
}

bool operator==(const SizeStatistics& lhs, const SizeStatistics& rhs) {
  return lhs.repetition_level_histogram == rhs.repetition_level_histogram &&
         lhs.definition_level_histogram == rhs.definition_level_histogram &&
         lhs.unencoded_byte_array_data_bytes == rhs.unencoded_byte_array_data_bytes;
}

struct PageSizeStatistics {
  std::vector<int64_t> def_levels;
  std::vector<int64_t> rep_levels;
  std::vector<int64_t> byte_array_bytes;
  bool operator==(const PageSizeStatistics& other) const {
    return def_levels == other.def_levels && rep_levels == other.rep_levels &&
           byte_array_bytes == other.byte_array_bytes;
  }
};

class SizeStatisticsRoundTripTest : public ::testing::Test {
 public:
  void WriteFile(SizeStatisticsLevel level,
                 const std::shared_ptr<::arrow::Table>& table) {
    auto writer_properties = WriterProperties::Builder()
                                 .max_row_group_length(2) /* every row group has 2 rows */
                                 ->data_pagesize(1)       /* every page has 1 row */
                                 ->enable_write_page_index()
                                 ->enable_statistics()
                                 ->set_size_statistics_level(level)
                                 ->build();

    // Get schema from table.
    auto schema = table->schema();
    std::shared_ptr<SchemaDescriptor> parquet_schema;
    auto arrow_writer_properties = default_arrow_writer_properties();
    ASSERT_OK_NO_THROW(arrow::ToParquetSchema(schema.get(), *writer_properties,
                                              *arrow_writer_properties, &parquet_schema));
    auto schema_node =
        std::static_pointer_cast<schema::GroupNode>(parquet_schema->schema_root());

    // Write table to buffer.
    auto sink = CreateOutputStream();
    auto pool = ::arrow::default_memory_pool();
    auto writer = ParquetFileWriter::Open(sink, schema_node, writer_properties);
    std::unique_ptr<arrow::FileWriter> arrow_writer;
    ASSERT_OK(arrow::FileWriter::Make(pool, std::move(writer), schema,
                                      arrow_writer_properties, &arrow_writer));
    ASSERT_OK_NO_THROW(arrow_writer->WriteTable(*table));
    ASSERT_OK_NO_THROW(arrow_writer->Close());
    ASSERT_OK_AND_ASSIGN(buffer_, sink->Finish());
  }

  void ReadSizeStatistics() {
    auto read_properties = default_arrow_reader_properties();
    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer_));

    // Read row group size statistics in order.
    auto metadata = reader->metadata();
    for (int i = 0; i < metadata->num_row_groups(); ++i) {
      auto row_group_metadata = metadata->RowGroup(i);
      for (int j = 0; j < metadata->num_columns(); ++j) {
        auto column_metadata = row_group_metadata->ColumnChunk(j);
        auto size_stats = column_metadata->size_statistics();
        row_group_stats_.push_back(size_stats ? *size_stats : SizeStatistics{});
      }
    }

    // Read page size statistics in order.
    auto page_index_reader = reader->GetPageIndexReader();
    ASSERT_NE(page_index_reader, nullptr);

    for (int i = 0; i < metadata->num_row_groups(); ++i) {
      auto row_group_index_reader = page_index_reader->RowGroup(i);
      ASSERT_NE(row_group_index_reader, nullptr);

      for (int j = 0; j < metadata->num_columns(); ++j) {
        PageSizeStatistics page_stats;

        auto column_index = row_group_index_reader->GetColumnIndex(j);
        if (column_index != nullptr) {
          if (column_index->has_definition_level_histograms()) {
            page_stats.def_levels = column_index->definition_level_histograms();
          }
          if (column_index->has_repetition_level_histograms()) {
            page_stats.rep_levels = column_index->repetition_level_histograms();
          }
        }

        auto offset_index = row_group_index_reader->GetOffsetIndex(j);
        if (offset_index != nullptr) {
          page_stats.byte_array_bytes = offset_index->unencoded_byte_array_data_bytes();
        }

        page_stats_.emplace_back(std::move(page_stats));
      }
    }
  }

  void Reset() {
    buffer_.reset();
    row_group_stats_.clear();
    page_stats_.clear();
  }

 protected:
  std::shared_ptr<Buffer> buffer_;
  std::vector<SizeStatistics> row_group_stats_;
  std::vector<PageSizeStatistics> page_stats_;
  inline static const SizeStatistics kEmptyRowGroupStats{};
  inline static const PageSizeStatistics kEmptyPageStats{};
};

TEST_F(SizeStatisticsRoundTripTest, EnableSizeStats) {
  auto schema = ::arrow::schema({
      ::arrow::field("a", ::arrow::list(::arrow::list(::arrow::int32()))),
      ::arrow::field("b", ::arrow::list(::arrow::list(::arrow::utf8()))),
  });
  // First two rows are in one row group, and the other two rows are in another row group.
  auto table = ::arrow::TableFromJSON(schema, {R"([
      [ [[1],[1,1],[1,1,1]], [["a"],["a","a"],["a","a","a"]] ],
      [ [[0,1,null]],        [["foo","bar",null]]            ],
      [ [],                  []                              ],
      [ [[],[null],null],    [[],[null],null]                ]
    ])"});

  for (auto size_stats_level :
       {SizeStatisticsLevel::None, SizeStatisticsLevel::ColumnChunk,
        SizeStatisticsLevel::PageAndColumnChunk}) {
    WriteFile(size_stats_level, table);
    ReadSizeStatistics();

    if (size_stats_level == SizeStatisticsLevel::None) {
      EXPECT_THAT(row_group_stats_,
                  ::testing::ElementsAre(kEmptyRowGroupStats, kEmptyRowGroupStats,
                                         kEmptyRowGroupStats, kEmptyRowGroupStats));
    } else {
      EXPECT_THAT(row_group_stats_, ::testing::ElementsAre(
                                        SizeStatistics{/*def_levels=*/{0, 0, 0, 0, 1, 8},
                                                       /*rep_levels=*/{2, 2, 5},
                                                       /*byte_array_bytes=*/std::nullopt},
                                        SizeStatistics{/*def_levels=*/{0, 0, 0, 0, 1, 8},
                                                       /*rep_levels=*/{2, 2, 5},
                                                       /*byte_array_bytes=*/12},
                                        SizeStatistics{/*def_levels=*/{0, 1, 1, 1, 1, 0},
                                                       /*rep_levels=*/{2, 2, 0},
                                                       /*byte_array_bytes=*/std::nullopt},
                                        SizeStatistics{/*def_levels=*/{0, 1, 1, 1, 1, 0},
                                                       /*rep_levels=*/{2, 2, 0},
                                                       /*byte_array_bytes=*/0}));
    }

    if (size_stats_level == SizeStatisticsLevel::PageAndColumnChunk) {
      EXPECT_THAT(
          page_stats_,
          ::testing::ElementsAre(
              PageSizeStatistics{/*def_levels=*/{0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 1, 2},
                                 /*rep_levels=*/{1, 2, 3, 1, 0, 2},
                                 /*byte_array_bytes=*/{}},
              PageSizeStatistics{/*def_levels=*/{0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 1, 2},
                                 /*rep_levels=*/{1, 2, 3, 1, 0, 2},
                                 /*byte_array_bytes=*/{6, 6}},
              PageSizeStatistics{/*def_levels=*/{0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0},
                                 /*rep_levels=*/{1, 0, 0, 1, 2, 0},
                                 /*byte_array_bytes=*/{}},
              PageSizeStatistics{/*def_levels=*/{0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0},
                                 /*rep_levels=*/{1, 0, 0, 1, 2, 0},
                                 /*byte_array_bytes=*/{0, 0}}));
    } else {
      EXPECT_THAT(page_stats_, ::testing::ElementsAre(kEmptyPageStats, kEmptyPageStats,
                                                      kEmptyPageStats, kEmptyPageStats));
    }

    Reset();
  }
}

TEST_F(SizeStatisticsRoundTripTest, WriteDictionaryArray) {
  auto schema = ::arrow::schema(
      {::arrow::field("a", ::arrow::dictionary(::arrow::int16(), ::arrow::utf8()))});
  WriteFile(
      SizeStatisticsLevel::PageAndColumnChunk,
      ::arrow::TableFromJSON(schema, {R"([["aa"],["aaa"],[null],["a"],["aaa"],["a"]])"}));

  ReadSizeStatistics();
  EXPECT_THAT(row_group_stats_,
              ::testing::ElementsAre(SizeStatistics{/*def_levels=*/{0, 2},
                                                    /*rep_levels=*/{2},
                                                    /*byte_array_bytes=*/5},
                                     SizeStatistics{/*def_levels=*/{1, 1},
                                                    /*rep_levels=*/{2},
                                                    /*byte_array_bytes=*/1},
                                     SizeStatistics{/*def_levels=*/{0, 2},
                                                    /*rep_levels=*/{2},
                                                    /*byte_array_bytes=*/4}));
  EXPECT_THAT(page_stats_,
              ::testing::ElementsAre(PageSizeStatistics{/*def_levels=*/{0, 2},
                                                        /*rep_levels=*/{2},
                                                        /*byte_array_bytes=*/{5}},
                                     PageSizeStatistics{/*def_levels=*/{1, 1},
                                                        /*rep_levels=*/{2},
                                                        /*byte_array_bytes=*/{1}},
                                     PageSizeStatistics{/*def_levels=*/{0, 2},
                                                        /*rep_levels=*/{2},
                                                        /*byte_array_bytes=*/{4}}));
}

}  // namespace parquet
