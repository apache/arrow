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

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"

#include "parquet/bloom_filter.h"
#include "parquet/bloom_filter_builder.h"
#include "parquet/bloom_filter_reader.h"
#include "parquet/file_reader.h"
#include "parquet/test_util.h"

namespace parquet::test {

TEST(BloomFilterReader, ReadBloomFilter) {
  std::string dir_string(parquet::test::get_data_dir());
  std::string path = dir_string + "/data_index_bloom_encoding_stats.parquet";
  auto reader = ParquetFileReader::OpenFile(path, false);
  auto file_metadata = reader->metadata();
  EXPECT_FALSE(file_metadata->is_encryption_algorithm_set());
  auto& bloom_filter_reader = reader->GetBloomFilterReader();
  auto row_group_0 = bloom_filter_reader.RowGroup(0);
  ASSERT_NE(nullptr, row_group_0);
  EXPECT_THROW(bloom_filter_reader.RowGroup(1), ParquetException);
  auto bloom_filter = row_group_0->GetColumnBloomFilter(0);
  ASSERT_NE(nullptr, bloom_filter);
  EXPECT_THROW(row_group_0->GetColumnBloomFilter(1), ParquetException);

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

TEST(BloomFilterReader, FileNotHaveBloomFilter) {
  // Can still get a BloomFilterReader and a RowGroupBloomFilter
  // reader, but cannot get a non-null BloomFilter.
  std::string dir_string(parquet::test::get_data_dir());
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
TEST(BloomFilterBuilderTest, BasicRoundTrip) {
  SchemaDescriptor schema;
  schema::NodePtr root = schema::GroupNode::Make(
      "schema", Repetition::REPEATED, {schema::ByteArray("c1"), schema::ByteArray("c2")});
  schema.Init(root);
  WriterProperties::Builder properties_builder;
  BloomFilterOptions bloom_filter_options;
  bloom_filter_options.ndv = 100;
  properties_builder.enable_bloom_filter_options(bloom_filter_options, "c1");
  auto writer_properties = properties_builder.build();
  auto builder = BloomFilterBuilder::Make(&schema, *writer_properties);
  builder->AppendRowGroup();
  auto bloom_filter = builder->GetOrCreateBloomFilter(0);
  ASSERT_NE(nullptr, bloom_filter);
  ASSERT_EQ(bloom_filter->GetBitsetSize(),
            BlockSplitBloomFilter::OptimalNumOfBytes(bloom_filter_options.ndv,
                                                     bloom_filter_options.fpp));
  std::vector<uint64_t> insert_hashes = {100, 200};
  for (uint64_t hash : insert_hashes) {
    bloom_filter->InsertHash(hash);
  }
  builder->Finish();
  auto sink = CreateOutputStream();
  BloomFilterLocation location;
  builder->WriteTo(sink.get(), &location);
  EXPECT_EQ(1, location.bloom_filter_location.size());
  EXPECT_EQ(2, location.bloom_filter_location[0].size());
  EXPECT_TRUE(location.bloom_filter_location[0][0].has_value());
  EXPECT_FALSE(location.bloom_filter_location[0][1].has_value());

  int64_t bloom_filter_offset = location.bloom_filter_location[0][0]->offset;
  int32_t bloom_filter_length = location.bloom_filter_location[0][0]->length;

  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());
  ReaderProperties reader_properties;
  ::arrow::io::BufferReader reader(
      ::arrow::SliceBuffer(buffer, bloom_filter_offset, bloom_filter_length));
  auto filter = parquet::BlockSplitBloomFilter::Deserialize(reader_properties, &reader);
  for (uint64_t hash : insert_hashes) {
    EXPECT_TRUE(bloom_filter->FindHash(hash));
  }
  EXPECT_FALSE(filter.FindHash(300));
}

TEST(BloomFilterBuilderTest, InvalidOperations) {
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
  auto builder = BloomFilterBuilder::Make(&schema, *properties);
  // AppendRowGroup() is not called and expect throw.
  ASSERT_THROW(builder->GetOrCreateBloomFilter(0), ParquetException);

  builder->AppendRowGroup();
  // GetOrCreateBloomFilter() with wrong column ordinal expect throw.
  ASSERT_THROW(builder->GetOrCreateBloomFilter(2), ParquetException);
  // GetOrCreateBloomFilter() with boolean expect throw.
  ASSERT_THROW(builder->GetOrCreateBloomFilter(1), ParquetException);
  builder->GetOrCreateBloomFilter(0);
  auto sink = CreateOutputStream();
  BloomFilterLocation location;
  // WriteTo() before Finish() expect throw.
  ASSERT_THROW(builder->WriteTo(sink.get(), &location), ParquetException);
  builder->Finish();
  builder->WriteTo(sink.get(), &location);
  EXPECT_EQ(1, location.bloom_filter_location.size());
}

}  // namespace parquet::test
