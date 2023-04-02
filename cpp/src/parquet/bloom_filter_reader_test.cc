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

#include "parquet/bloom_filter.h"
#include "parquet/bloom_filter_reader.h"
#include "parquet/file_reader.h"
#include "parquet/test_util.h"

namespace parquet {
namespace test {

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

}  // namespace test
}  // namespace parquet
