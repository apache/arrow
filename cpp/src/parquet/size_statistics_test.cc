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

TEST(SizeStatistics, WriteBatchLevels) {
  std::vector<int64_t> expected_def_level_histogram = {256, 128, 64, 32, 16, 8, 4, 2, 2};
  std::vector<int64_t> expected_rep_level_histogram = {256, 128, 64, 32, 32};
  constexpr int16_t kMaxDefLevel = 8;
  constexpr int16_t kMaxRefLevel = 4;
  auto descr =
      std::make_unique<ColumnDescriptor>(schema::Int32("a"), kMaxDefLevel, kMaxRefLevel);
  auto builder = SizeStatisticsBuilder::Make(descr.get());

  auto write_batch_levels =
      [&](const std::vector<int64_t>& histogram,
          const std::function<void(SizeStatisticsBuilder*,
                                   ::arrow::util::span<const int16_t>)>&
              write_levels_func) {
        std::vector<int16_t> levels;
        for (int16_t level = 0; level < static_cast<int16_t>(histogram.size()); level++) {
          levels.insert(levels.end(), histogram[level], level);
        }

        auto rng = std::default_random_engine{};
        std::shuffle(std::begin(levels), std::end(levels), rng);

        constexpr size_t kBatchSize = 64;
        for (size_t i = 0; i < levels.size(); i += kBatchSize) {
          auto batch_size = static_cast<int64_t>(std::min(kBatchSize, levels.size() - i));
          write_levels_func(builder.get(),
                            {levels.data() + i, static_cast<size_t>(batch_size)});
        }
      };

  write_batch_levels(expected_def_level_histogram,
                     &SizeStatisticsBuilder::AddDefinitionLevels);
  write_batch_levels(expected_rep_level_histogram,
                     &SizeStatisticsBuilder::AddRepetitionLevels);
  auto size_statistics = builder->Build();
  EXPECT_EQ(size_statistics->definition_level_histogram(), expected_def_level_histogram);
  EXPECT_EQ(size_statistics->repetition_level_histogram(), expected_rep_level_histogram);
}

TEST(SizeStatistics, WriteRepeatedLevels) {
  constexpr int16_t kMaxDefLevel = 2;
  constexpr int16_t kMaxRepLevel = 3;
  auto descr =
      std::make_unique<ColumnDescriptor>(schema::Int32("a"), kMaxDefLevel, kMaxRepLevel);
  auto builder = SizeStatisticsBuilder::Make(descr.get());

  constexpr int64_t kNumRounds = 10;
  for (int64_t round = 1; round <= kNumRounds; round++) {
    for (int16_t def_level = 0; def_level <= kMaxDefLevel; def_level++) {
      builder->AddDefinitionLevel(/*num_levels=*/round + def_level, def_level);
    }
    for (int16_t rep_level = 0; rep_level <= kMaxRepLevel; rep_level++) {
      builder->AddRepetitionLevel(/*num_levels=*/round + rep_level * rep_level,
                                  rep_level);
    }
  }

  auto size_statistics = builder->Build();
  EXPECT_EQ(size_statistics->definition_level_histogram(),
            std::vector<int64_t>({55, 65, 75}));
  EXPECT_EQ(size_statistics->repetition_level_histogram(),
            std::vector<int64_t>({55, 65, 95, 145}));
}

TEST(SizeStatistics, WriteDenseByteArrayValues) {
  constexpr std::string_view kValue = "foo";
  constexpr int kNumValues = 1000;
  constexpr int kBatchSize = 64;
  const std::vector<parquet::ByteArray> values(kNumValues, kValue);

  auto descr = std::make_unique<ColumnDescriptor>(
      schema::ByteArray("a"), /*max_def_level=*/0, /*max_rep_level=*/0);
  auto builder = SizeStatisticsBuilder::Make(descr.get());
  for (int i = 0; i < kNumValues; i += kBatchSize) {
    auto batch_size = std::min(kBatchSize, kNumValues - i);
    builder->AddValues(values.data() + i, batch_size);
  }

  auto size_statistics = builder->Build();
  EXPECT_EQ(size_statistics->unencoded_byte_array_data_bytes().value_or(-1),
            kNumValues * kValue.size());
}

TEST(SizeStatistics, WriteSpacedByteArrayValues) {
  constexpr std::string_view kValue = "foo";
  constexpr int kNumValues = 1000;
  constexpr int kBatchSize = 63;
  const std::vector<parquet::ByteArray> values(kNumValues, kValue);
  ASSERT_OK_AND_ASSIGN(auto not_null_bitmap, ::arrow::AllocateBitmap(kNumValues));
  int not_null_count = 0;
  for (int i = 0; i < kNumValues; i++) {
    if (i % 3 == 0) {
      ::arrow::bit_util::ClearBit(not_null_bitmap->mutable_data(), i);
    } else {
      ::arrow::bit_util::SetBit(not_null_bitmap->mutable_data(), i);
      not_null_count++;
    }
  }

  auto descr = std::make_unique<ColumnDescriptor>(
      schema::ByteArray("a"), /*max_def_level=*/1, /*max_rep_level=*/0);
  auto builder = SizeStatisticsBuilder::Make(descr.get());
  for (int i = 0; i < kNumValues; i += kBatchSize) {
    auto batch_size = std::min(kBatchSize, kNumValues - i);
    builder->AddValuesSpaced(values.data() + i, not_null_bitmap->data(), i, batch_size);
  }

  auto size_statistics = builder->Build();
  EXPECT_EQ(size_statistics->unencoded_byte_array_data_bytes().value_or(-1),
            not_null_count * kValue.size());
}

TEST(SizeStatistics, WriteBinaryArray) {
  std::vector<std::shared_ptr<::arrow::Array>> arrays = {
      ::arrow::ArrayFromJSON(::arrow::binary(), R"(["foo", null, "bar", "baz"])"),
      ::arrow::ArrayFromJSON(::arrow::large_binary(), R"(["foo", null, "bar", "baz"])"),
  };
  for (const auto& array : arrays) {
    auto descr = std::make_unique<ColumnDescriptor>(
        schema::ByteArray("a"), /*max_def_level=*/1, /*max_rep_level=*/0);
    auto builder = SizeStatisticsBuilder::Make(descr.get());
    builder->AddValues(*array);
    auto size_statistics = builder->Build();
    EXPECT_EQ(size_statistics->unencoded_byte_array_data_bytes().value_or(-1), 9);
  }
}

TEST(SizeStatistics, MergeStatistics) {
  constexpr int kNumValues = 16;
  const std::array<int16_t, kNumValues> def_levels = {0, 0, 0, 0, 1, 1, 1, 1,
                                                      2, 2, 2, 2, 3, 3, 3, 3};
  const std::array<int16_t, kNumValues> rep_levels = {0, 1, 2, 3, 0, 1, 2, 3,
                                                      0, 1, 2, 3, 0, 1, 2, 3};
  const std::vector<int64_t> expected_histogram = {8, 8, 8, 8};
  constexpr std::string_view kByteArrayValue = "foo";
  const std::vector<parquet::ByteArray> values(kNumValues,
                                               parquet::ByteArray{kByteArrayValue});

  for (const auto& descr :
       {std::make_unique<ColumnDescriptor>(schema::Int32("a"), /*max_def_level=*/3,
                                           /*max_rep_level=*/3),
        std::make_unique<ColumnDescriptor>(schema::ByteArray("a"), /*max_def_level=*/3,
                                           /*max_rep_level=*/3)}) {
    auto builder = SizeStatisticsBuilder::Make(descr.get());
    builder->AddRepetitionLevels(rep_levels);
    builder->AddDefinitionLevels(def_levels);
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      builder->AddValues(values.data(), kNumValues);
    }
    auto size_statistics_1 = builder->Build();

    builder->Reset();
    builder->AddRepetitionLevels(rep_levels);
    builder->AddDefinitionLevels(def_levels);
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      builder->AddValues(values.data(), kNumValues);
    }
    auto size_statistics_2 = builder->Build();

    size_statistics_1->Merge(*size_statistics_2);
    EXPECT_EQ(size_statistics_1->definition_level_histogram(), expected_histogram);
    EXPECT_EQ(size_statistics_1->repetition_level_histogram(), expected_histogram);
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      EXPECT_TRUE(size_statistics_1->unencoded_byte_array_data_bytes().has_value());
      EXPECT_EQ(size_statistics_1->unencoded_byte_array_data_bytes().value(),
                kByteArrayValue.size() * kNumValues * 2);
    } else {
      EXPECT_FALSE(size_statistics_1->unencoded_byte_array_data_bytes().has_value());
    }
  }
}

TEST(SizeStatistics, ThriftSerDe) {
  constexpr int kNumValues = 16;
  const std::array<int16_t, kNumValues> def_levels = {0, 0, 0, 0, 1, 1, 1, 1,
                                                      2, 2, 2, 2, 3, 3, 3, 3};
  const std::array<int16_t, kNumValues> rep_levels = {0, 1, 2, 3, 0, 1, 2, 3,
                                                      0, 1, 2, 3, 0, 1, 2, 3};
  const std::vector<int64_t> expected_histogram = {4, 4, 4, 4};
  constexpr std::string_view kByteArrayValue = "foo";
  const std::vector<parquet::ByteArray> values(kNumValues,
                                               parquet::ByteArray{kByteArrayValue});

  for (const auto& descr :
       {std::make_unique<ColumnDescriptor>(schema::Int32("a"), /*max_def_level=*/3,
                                           /*max_rep_level=*/3),
        std::make_unique<ColumnDescriptor>(schema::ByteArray("a"), /*max_def_level=*/3,
                                           /*max_rep_level=*/3)}) {
    auto builder = SizeStatisticsBuilder::Make(descr.get());
    builder->AddRepetitionLevels(rep_levels);
    builder->AddDefinitionLevels(def_levels);
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      builder->AddValues(values.data(), kNumValues);
    }
    auto size_statistics = builder->Build();
    auto thrift_statistics = ToThrift(*size_statistics);
    auto restored_statistics = SizeStatistics::Make(&thrift_statistics, descr.get());
    EXPECT_EQ(restored_statistics->definition_level_histogram(), expected_histogram);
    EXPECT_EQ(restored_statistics->repetition_level_histogram(), expected_histogram);
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      EXPECT_TRUE(restored_statistics->unencoded_byte_array_data_bytes().has_value());
      EXPECT_EQ(restored_statistics->unencoded_byte_array_data_bytes().value(),
                kByteArrayValue.size() * kNumValues);
    } else {
      EXPECT_FALSE(restored_statistics->unencoded_byte_array_data_bytes().has_value());
    }
  }
}

struct RowGroupSizeStatistics {
  std::vector<int64_t> ref_levels;
  std::vector<int64_t> def_levels;
  std::optional<int64_t> byte_array_bytes;
  bool operator==(const RowGroupSizeStatistics& other) const {
    return ref_levels == other.ref_levels && def_levels == other.def_levels &&
           byte_array_bytes == other.byte_array_bytes;
  }
};

struct PageSizeStatistics {
  std::vector<int64_t> ref_levels;
  std::vector<int64_t> def_levels;
  std::vector<int64_t> byte_array_bytes;
  bool operator==(const PageSizeStatistics& other) const {
    return ref_levels == other.ref_levels && def_levels == other.def_levels &&
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
        RowGroupSizeStatistics row_group_stats;
        if (size_stats != nullptr) {
          row_group_stats = {size_stats->repetition_level_histogram(),
                             size_stats->definition_level_histogram(),
                             size_stats->unencoded_byte_array_data_bytes()};
        }
        row_group_stats_.emplace_back(std::move(row_group_stats));
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
          if (column_index->has_repetition_level_histograms()) {
            page_stats.ref_levels = column_index->repetition_level_histograms();
          }
          if (column_index->has_definition_level_histograms()) {
            page_stats.def_levels = column_index->definition_level_histograms();
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

 protected:
  std::shared_ptr<Buffer> buffer_;
  std::vector<RowGroupSizeStatistics> row_group_stats_;
  std::vector<PageSizeStatistics> page_stats_;
  inline static const RowGroupSizeStatistics kEmptyRowGroupStats{};
  inline static const PageSizeStatistics kEmptyPageStats{};
};

TEST_F(SizeStatisticsRoundTripTest, DisableSizeStats) {
  auto schema = ::arrow::schema({
      ::arrow::field("a", ::arrow::list(::arrow::list(::arrow::int32()))),
      ::arrow::field("b", ::arrow::list(::arrow::list(::arrow::utf8()))),
  });
  WriteFile(SizeStatisticsLevel::None, ::arrow::TableFromJSON(schema, {R"([
      [ [[1],[1,1],[1,1,1]], [["a"],["a","a"],["a","a","a"]] ],
      [ [[0,1,null]],        [["foo","bar",null]]            ],
      [ [],                  []                              ],
      [ [[],[null],null],    [[],[null],null]                ]
    ])"}));

  ReadSizeStatistics();
  EXPECT_THAT(row_group_stats_,
              ::testing::ElementsAre(kEmptyRowGroupStats, kEmptyRowGroupStats,
                                     kEmptyRowGroupStats, kEmptyRowGroupStats));
  EXPECT_THAT(page_stats_, ::testing::ElementsAre(kEmptyPageStats, kEmptyPageStats,
                                                  kEmptyPageStats, kEmptyPageStats));
}

TEST_F(SizeStatisticsRoundTripTest, EnableColumnChunkSizeStats) {
  auto schema = ::arrow::schema({
      ::arrow::field("a", ::arrow::list(::arrow::list(::arrow::int32()))),
      ::arrow::field("b", ::arrow::list(::arrow::list(::arrow::utf8()))),
  });
  WriteFile(SizeStatisticsLevel::ColumnChunk, ::arrow::TableFromJSON(schema, {R"([
      [ [[1],[1,1],[1,1,1]], [["a"],["a","a"],["a","a","a"]] ],
      [ [[0,1,null]],        [["foo","bar",null]]            ],
      [ [],                  []                              ],
      [ [[],[null],null],    [[],[null],null]                ]
    ])"}));

  ReadSizeStatistics();
  EXPECT_THAT(
      row_group_stats_,
      ::testing::ElementsAre(RowGroupSizeStatistics{/*ref_levels=*/{2, 2, 5},
                                                    /*def_levels=*/{0, 0, 0, 0, 1, 8},
                                                    /*byte_array_bytes=*/std::nullopt},
                             RowGroupSizeStatistics{/*ref_levels=*/{2, 2, 5},
                                                    /*def_levels=*/{0, 0, 0, 0, 1, 8},
                                                    /*byte_array_bytes=*/12},
                             RowGroupSizeStatistics{/*ref_levels=*/{2, 2, 0},
                                                    /*def_levels=*/{0, 1, 1, 1, 1, 0},
                                                    /*byte_array_bytes=*/std::nullopt},
                             RowGroupSizeStatistics{/*ref_levels=*/{2, 2, 0},
                                                    /*def_levels=*/{0, 1, 1, 1, 1, 0},
                                                    /*byte_array_bytes=*/0}));
  EXPECT_THAT(page_stats_, ::testing::ElementsAre(kEmptyPageStats, kEmptyPageStats,
                                                  kEmptyPageStats, kEmptyPageStats));
}

TEST_F(SizeStatisticsRoundTripTest, EnablePageSizeStats) {
  auto schema = ::arrow::schema({
      ::arrow::field("a", ::arrow::list(::arrow::list(::arrow::int32()))),
      ::arrow::field("b", ::arrow::list(::arrow::list(::arrow::utf8()))),
  });
  WriteFile(SizeStatisticsLevel::Page, ::arrow::TableFromJSON(schema, {R"([
      [ [[1],[1,1],[1,1,1]], [["a"],["a","a"],["a","a","a"]] ],
      [ [[0,1,null]],        [["foo","bar",null]]            ],
      [ [],                  []                              ],
      [ [[],[null],null],    [[],[null],null]                ]
    ])"}));

  ReadSizeStatistics();
  EXPECT_THAT(
      row_group_stats_,
      ::testing::ElementsAre(RowGroupSizeStatistics{/*ref_levels=*/{2, 2, 5},
                                                    /*def_levels=*/{0, 0, 0, 0, 1, 8},
                                                    /*byte_array_bytes=*/std::nullopt},
                             RowGroupSizeStatistics{/*ref_levels=*/{2, 2, 5},
                                                    /*def_levels=*/{0, 0, 0, 0, 1, 8},
                                                    /*byte_array_bytes=*/12},
                             RowGroupSizeStatistics{/*ref_levels=*/{2, 2, 0},
                                                    /*def_levels=*/{0, 1, 1, 1, 1, 0},
                                                    /*byte_array_bytes=*/std::nullopt},
                             RowGroupSizeStatistics{/*ref_levels=*/{2, 2, 0},
                                                    /*def_levels=*/{0, 1, 1, 1, 1, 0},
                                                    /*byte_array_bytes=*/0}));
  EXPECT_THAT(page_stats_,
              ::testing::ElementsAre(
                  PageSizeStatistics{/*ref_levels=*/{1, 2, 3, 1, 0, 2},
                                     /*def_levels=*/{0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 1, 2},
                                     /*byte_array_bytes=*/{}},
                  PageSizeStatistics{/*ref_levels=*/{1, 2, 3, 1, 0, 2},
                                     /*def_levels=*/{0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 1, 2},
                                     /*byte_array_bytes=*/{6, 6}},
                  PageSizeStatistics{/*ref_levels=*/{1, 0, 0, 1, 2, 0},
                                     /*def_levels=*/{0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0},
                                     /*byte_array_bytes=*/{}},
                  PageSizeStatistics{/*ref_levels=*/{1, 0, 0, 1, 2, 0},
                                     /*def_levels=*/{0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0},
                                     /*byte_array_bytes=*/{0, 0}}));
}

TEST_F(SizeStatisticsRoundTripTest, WriteDictionaryArray) {
  std::shared_ptr<::arrow::Array> dict;
  std::vector<std::string> dict_values = {"a", "aa", "aaa"};
  ::arrow::ArrayFromVector<::arrow::StringType, std::string>(dict_values, &dict);
  std::shared_ptr<::arrow::DataType> dict_type =
      ::arrow::dictionary(::arrow::int16(), ::arrow::utf8());

  std::shared_ptr<::arrow::Array> indices;
  std::vector<bool> is_valid = {true, true, false, true, true, true};
  std::vector<int16_t> indices_values = {1, 2, -1, 0, 2, 0};
  ::arrow::ArrayFromVector<::arrow::Int16Type, int16_t>(is_valid, indices_values,
                                                        &indices);
  auto array = std::make_shared<::arrow::DictionaryArray>(dict_type, indices, dict);

  auto schema = ::arrow::schema({::arrow::field("a", dict_type)});
  auto table = ::arrow::Table::Make(schema, {array}, indices->length());
  WriteFile(SizeStatisticsLevel::Page, table);

  ReadSizeStatistics();
  EXPECT_THAT(row_group_stats_,
              ::testing::ElementsAre(RowGroupSizeStatistics{/*ref_levels=*/{2},
                                                            /*def_levels=*/{0, 2},
                                                            /*byte_array_bytes=*/5},
                                     RowGroupSizeStatistics{/*ref_levels=*/{2},
                                                            /*def_levels=*/{1, 1},
                                                            /*byte_array_bytes=*/1},
                                     RowGroupSizeStatistics{/*ref_levels=*/{2},
                                                            /*def_levels=*/{0, 2},
                                                            /*byte_array_bytes=*/4}));
  EXPECT_THAT(page_stats_,
              ::testing::ElementsAre(PageSizeStatistics{/*ref_levels=*/{2},
                                                        /*def_levels=*/{0, 2},
                                                        /*byte_array_bytes=*/{5}},
                                     PageSizeStatistics{/*ref_levels=*/{2},
                                                        /*def_levels=*/{1, 1},
                                                        /*byte_array_bytes=*/{1}},
                                     PageSizeStatistics{/*ref_levels=*/{2},
                                                        /*def_levels=*/{0, 2},
                                                        /*byte_array_bytes=*/{4}}));
}

}  // namespace parquet
