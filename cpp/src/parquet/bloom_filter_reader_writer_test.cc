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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"

#include "parquet/bloom_filter.h"
#include "parquet/bloom_filter_reader.h"
#include "parquet/bloom_filter_writer.h"
#include "parquet/file_reader.h"
#include "parquet/test_util.h"

namespace parquet::test {

TEST(BloomFilterReader, ReadBloomFilter) {
  std::vector<std::string> files = {"data_index_bloom_encoding_stats.parquet",
                                    "data_index_bloom_encoding_with_length.parquet"};
  for (const auto& test_file : files) {
    std::string dir_string(get_data_dir());
    std::string path = dir_string + "/" + test_file;
    auto reader = ParquetFileReader::OpenFile(path, /*memory_map=*/false);
    auto file_metadata = reader->metadata();
    EXPECT_FALSE(file_metadata->is_encryption_algorithm_set());
    auto& bloom_filter_reader = reader->GetBloomFilterReader();
    auto row_group_0 = bloom_filter_reader.RowGroup(0);
    ASSERT_NE(nullptr, row_group_0);
    EXPECT_THROW_THAT(
        [&]() { bloom_filter_reader.RowGroup(1); }, ParquetException,
        ::testing::Property(&ParquetException::what,
                            ::testing::HasSubstr("Invalid row group ordinal")));
    auto bloom_filter = row_group_0->GetColumnBloomFilter(0);
    ASSERT_NE(nullptr, bloom_filter);
    EXPECT_THROW_THAT([&]() { row_group_0->GetColumnBloomFilter(1); }, ParquetException,
                      ::testing::Property(&ParquetException::what,
                                          ::testing::HasSubstr(
                                              "Invalid column index at column ordinal")));

    // assert exists
    {
      std::string_view sv = "Hello";
      ByteArray ba{sv};
      EXPECT_TRUE(bloom_filter->FindHash(bloom_filter->Hash(&ba)));
    }

    // no exists
    {
      std::string_view sv = "NOT_EXISTS";
      ByteArray ba{sv};
      EXPECT_FALSE(bloom_filter->FindHash(bloom_filter->Hash(&ba)));
    }
  }
}

TEST(BloomFilterReader, FileNotHaveBloomFilter) {
  // Can still get a BloomFilterReader and a RowGroupBloomFilter
  // reader, but cannot get a non-null BloomFilter.
  std::string dir_string(get_data_dir());
  std::string path = dir_string + "/alltypes_plain.parquet";
  auto reader = ParquetFileReader::OpenFile(path, false);
  auto file_metadata = reader->metadata();
  EXPECT_FALSE(file_metadata->is_encryption_algorithm_set());
  auto& bloom_filter_reader = reader->GetBloomFilterReader();
  auto row_group_0 = bloom_filter_reader.RowGroup(0);
  ASSERT_NE(nullptr, row_group_0);
  EXPECT_THROW(bloom_filter_reader.RowGroup(1), ParquetException);
  auto bloom_filter = row_group_0->GetColumnBloomFilter(0);
  ASSERT_EQ(nullptr, bloom_filter);
}

// <c1:BYTE_ARRAY, c2:BYTE_ARRAY>, c1 has enabled bloom filter.
TEST(BloomFilterBuilder, BasicRoundTrip) {
  SchemaDescriptor schema;
  schema::NodePtr root = schema::GroupNode::Make(
      "schema", Repetition::REPEATED, {schema::ByteArray("c1"), schema::ByteArray("c2")});
  schema.Init(root);

  BloomFilterOptions bloom_filter_options{100, 0.05};
  const auto bitset_size = BlockSplitBloomFilter::OptimalNumOfBytes(
      bloom_filter_options.ndv, bloom_filter_options.fpp);
  WriterProperties::Builder properties_builder;
  properties_builder.enable_bloom_filter(bloom_filter_options, "c1");
  auto writer_properties = properties_builder.build();
  auto bloom_filter_builder = BloomFilterBuilder::Make(&schema, writer_properties.get());

  auto write_row_group_bloom_filter = [&](const std::vector<uint64_t>& hash_values) {
    bloom_filter_builder->AppendRowGroup();
    auto bloom_filter =
        bloom_filter_builder->GetOrCreateBloomFilter(/*column_ordinal=*/0);
    ASSERT_NE(bloom_filter, nullptr);
    ASSERT_EQ(bloom_filter->GetBitsetSize(), bitset_size);
    for (uint64_t hash_value : hash_values) {
      bloom_filter->InsertHash(hash_value);
    }
  };

  write_row_group_bloom_filter({100, 200});
  write_row_group_bloom_filter({300, 400});

  auto sink = CreateOutputStream();
  IndexLocations bloom_filter_location;
  bloom_filter_builder->WriteTo(sink.get(), &bloom_filter_location);
  ASSERT_EQ(bloom_filter_location.locations.size(), 2);
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  struct RowGroupBloomFilterCase {
    int32_t row_group_id;
    std::vector<uint64_t> existing_values;
    std::vector<uint64_t> non_existing_values;
  };

  for (const auto& c : std::vector<RowGroupBloomFilterCase>{
           RowGroupBloomFilterCase{/*row_group_id=*/0, /*existing_values=*/{100, 200},
                                   /*non_existing_values=*/{300, 400}},
           RowGroupBloomFilterCase{/*row_group_id=*/1, /*existing_values=*/{300, 400},
                                   /*non_existing_values=*/{100, 200}}}) {
    auto row_group_bloom_filter = bloom_filter_location.locations.find(c.row_group_id);
    ASSERT_NE(row_group_bloom_filter, bloom_filter_location.locations.cend());

    auto bloom_filter_location = row_group_bloom_filter->second.find(0);
    ASSERT_NE(bloom_filter_location, row_group_bloom_filter->second.cend());
    int64_t bloom_filter_offset = bloom_filter_location->second.offset;
    int32_t bloom_filter_length = bloom_filter_location->second.length;

    ReaderProperties reader_properties;
    ::arrow::io::BufferReader reader(
        ::arrow::SliceBuffer(buffer, bloom_filter_offset, bloom_filter_length));
    auto filter = parquet::BlockSplitBloomFilter::Deserialize(reader_properties, &reader);
    for (uint64_t hash : c.existing_values) {
      EXPECT_TRUE(filter.FindHash(hash));
    }
    for (uint64_t hash : c.non_existing_values) {
      EXPECT_FALSE(filter.FindHash(hash));
    }
  }
}

TEST(BloomFilterBuilder, InvalidOperations) {
  SchemaDescriptor schema;
  schema::NodePtr root = schema::GroupNode::Make(
      "schema", Repetition::REPEATED, {schema::ByteArray("c1"), schema::Boolean("c2")});
  schema.Init(root);

  WriterProperties::Builder properties_builder;
  BloomFilterOptions bloom_filter_options{100, 0.05};
  properties_builder.enable_bloom_filter(bloom_filter_options, "c1");
  properties_builder.enable_bloom_filter(bloom_filter_options, "c2");
  auto properties = properties_builder.build();
  auto bloom_filter_builder = BloomFilterBuilder::Make(&schema, properties.get());

  // AppendRowGroup() is not called yet.
  EXPECT_THROW_THAT(
      [&]() { bloom_filter_builder->GetOrCreateBloomFilter(/*column_ordinal=*/0); },
      ParquetException,
      ::testing::Property(
          &ParquetException::what,
          ::testing::HasSubstr("No row group appended to BloomFilterBuilder")));

  // Column ordinal is out of bound.
  bloom_filter_builder->AppendRowGroup();
  EXPECT_THROW_THAT([&]() { bloom_filter_builder->GetOrCreateBloomFilter(2); },
                    ParquetException,
                    ::testing::Property(&ParquetException::what,
                                        ::testing::HasSubstr("Invalid column ordinal")));

  // Boolean type is not supported.
  EXPECT_THROW_THAT(
      [&]() { bloom_filter_builder->GetOrCreateBloomFilter(1); }, ParquetException,
      ::testing::Property(
          &ParquetException::what,
          ::testing::HasSubstr("BloomFilterBuilder does not support boolean type")));

  // Get a created bloom filter should succeed.
  auto bloom_filter = bloom_filter_builder->GetOrCreateBloomFilter(0);
  ASSERT_EQ(bloom_filter_builder->GetOrCreateBloomFilter(0), bloom_filter);

  auto sink = CreateOutputStream();
  IndexLocations location;
  ASSERT_NO_FATAL_FAILURE(bloom_filter_builder->WriteTo(sink.get(), &location));
  ASSERT_EQ(location.locations.size(), 1);

  // WriteTo() has been called already.
  EXPECT_THROW_THAT(
      [&]() { bloom_filter_builder->WriteTo(sink.get(), &location); }, ParquetException,
      ::testing::Property(
          &ParquetException::what,
          ::testing::HasSubstr("Cannot write a finished BloomFilterBuilder")));
}

}  // namespace parquet::test
