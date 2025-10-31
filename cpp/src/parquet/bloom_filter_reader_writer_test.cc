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
#include "parquet/bloom_filter_builder_internal.h"
#include "parquet/bloom_filter_reader.h"
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

// <c1:BYTE_ARRAY, c2:BYTE_ARRAY>, c1 has bloom filter.
TEST(BloomFilterBuilder, BasicRoundTrip) {
  SchemaDescriptor schema;
  schema::NodePtr root = schema::GroupNode::Make(
      "schema", Repetition::REPEATED, {schema::ByteArray("c1"), schema::ByteArray("c2")});
  schema.Init(root);
  WriterProperties::Builder properties_builder;
  BloomFilterOptions bloom_filter_options;
  bloom_filter_options.ndv = 100;
  properties_builder.enable_bloom_filter_options(bloom_filter_options, "c1");
  auto writer_properties = properties_builder.build();
  auto builder = BloomFilterBuilder::Make(&schema, writer_properties.get());

  auto append_values_to_bloom_filter = [&](const std::vector<uint64_t>& insert_hashes) {
    builder->AppendRowGroup();
    auto bloom_filter = builder->GetOrCreateBloomFilter(0);
    ASSERT_NE(nullptr, bloom_filter);
    ASSERT_EQ(bloom_filter->GetBitsetSize(),
              BlockSplitBloomFilter::OptimalNumOfBytes(bloom_filter_options.ndv,
                                                       bloom_filter_options.fpp));
    for (uint64_t hash : insert_hashes) {
      bloom_filter->InsertHash(hash);
    }
  };
  // First row-group
  append_values_to_bloom_filter({100, 200});
  // Second row-group
  append_values_to_bloom_filter({300, 400});
  auto sink = CreateOutputStream();
  BloomFilterLocation location;
  builder->WriteTo(sink.get(), &location);
  EXPECT_EQ(2, location.bloom_filter_location.size());
  for (auto& [row_group_id, row_group_bloom_filter] : location.bloom_filter_location) {
    EXPECT_EQ(1, row_group_bloom_filter.size());
    EXPECT_TRUE(row_group_bloom_filter.find(0) != row_group_bloom_filter.end());
    EXPECT_FALSE(row_group_bloom_filter.find(1) != row_group_bloom_filter.end());
  }

  struct RowGroupBloomFilterCase {
    int32_t row_group_id;
    std::vector<uint64_t> exists_hashes;
    std::vector<uint64_t> unexists_hashes;
  };

  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  std::vector<RowGroupBloomFilterCase> cases = {
      RowGroupBloomFilterCase{/*row_group_id=*/0, /*exists_hashes=*/{100, 200},
                              /*unexists_hashes=*/{300, 400}},
      RowGroupBloomFilterCase{/*row_group_id=*/1, /*exists_hashes=*/{300, 400},
                              /*unexists_hashes=*/{100, 200}}};
  for (const auto& c : cases) {
    auto& bloom_filter_location = location.bloom_filter_location[c.row_group_id];
    int64_t bloom_filter_offset = bloom_filter_location[0].offset;
    int32_t bloom_filter_length = bloom_filter_location[0].length;

    ReaderProperties reader_properties;
    ::arrow::io::BufferReader reader(
        ::arrow::SliceBuffer(buffer, bloom_filter_offset, bloom_filter_length));
    auto filter = parquet::BlockSplitBloomFilter::Deserialize(reader_properties, &reader);
    for (uint64_t hash : c.exists_hashes) {
      EXPECT_TRUE(filter.FindHash(hash));
    }
    for (uint64_t hash : c.unexists_hashes) {
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
  BloomFilterOptions bloom_filter_options;
  bloom_filter_options.ndv = 100;
  properties_builder.enable_bloom_filter_options(bloom_filter_options, "c1");
  properties_builder.enable_bloom_filter_options(bloom_filter_options, "c2");
  auto properties = properties_builder.build();
  auto builder = BloomFilterBuilder::Make(&schema, properties.get());
  // AppendRowGroup() is not called and expect throw.
  EXPECT_THROW_THAT(
      [&]() { builder->GetOrCreateBloomFilter(0); }, ParquetException,
      ::testing::Property(
          &ParquetException::what,
          ::testing::HasSubstr("No row group appended to BloomFilterBuilder")));

  builder->AppendRowGroup();
  // GetOrCreateBloomFilter() with wrong column ordinal expect throw.
  EXPECT_THROW_THAT([&]() { builder->GetOrCreateBloomFilter(2); }, ParquetException,
                    ::testing::Property(&ParquetException::what,
                                        ::testing::HasSubstr("Invalid column ordinal")));
  // GetOrCreateBloomFilter() with boolean expect throw.
  EXPECT_THROW_THAT(
      [&]() { builder->GetOrCreateBloomFilter(1); }, ParquetException,
      ::testing::Property(
          &ParquetException::what,
          ::testing::HasSubstr("BloomFilterBuilder does not support boolean type")));
  auto filter = builder->GetOrCreateBloomFilter(0);
  // Call GetOrCreateBloomFilter the second time it is actually a cached version.
  EXPECT_EQ(filter, builder->GetOrCreateBloomFilter(0));
  auto sink = CreateOutputStream();
  BloomFilterLocation location;
  builder->WriteTo(sink.get(), &location);
  EXPECT_EQ(1, location.bloom_filter_location.size());
  // Multiple WriteTo() expect throw.
  EXPECT_THROW_THAT(
      [&]() { builder->WriteTo(sink.get(), &location); }, ParquetException,
      ::testing::Property(
          &ParquetException::what,
          ::testing::HasSubstr("Cannot write a finished BloomFilterBuilder")));
}

}  // namespace parquet::test
