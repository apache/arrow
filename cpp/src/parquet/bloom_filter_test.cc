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

#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"

#include "parquet/bloom_filter.h"
#include "parquet/exception.h"
#include "parquet/murmur3.h"
#include "parquet/platform.h"
#include "parquet/test_util.h"
#include "parquet/types.h"
#include "parquet/xxhash.h"

namespace parquet {
namespace test {

TEST(Murmur3Test, TestBloomFilter) {
  uint64_t result;
  const uint8_t bitset[8] = {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7};
  ByteArray byteArray(8, bitset);
  MurmurHash3 murmur3;
  result = murmur3.Hash(&byteArray);
  EXPECT_EQ(result, UINT64_C(913737700387071329));
}

TEST(ConstructorTest, TestBloomFilter) {
  BlockSplitBloomFilter bloom_filter;
  EXPECT_NO_THROW(bloom_filter.Init(1000));

  // It throws because the length cannot be zero
  std::unique_ptr<uint8_t[]> bitset1(new uint8_t[1024]());
  EXPECT_THROW(bloom_filter.Init(bitset1.get(), 0), ParquetException);

  // It throws because the number of bytes of Bloom filter bitset must be a power of 2.
  std::unique_ptr<uint8_t[]> bitset2(new uint8_t[1024]());
  EXPECT_THROW(bloom_filter.Init(bitset2.get(), 1023), ParquetException);
}

// The BasicTest is used to test basic operations including InsertHash, FindHash and
// serializing and de-serializing.
TEST(BasicTest, TestBloomFilter) {
  BlockSplitBloomFilter bloom_filter;
  bloom_filter.Init(1024);

  for (int i = 0; i < 10; i++) {
    bloom_filter.InsertHash(bloom_filter.Hash(i));
  }

  for (int i = 0; i < 10; i++) {
    EXPECT_TRUE(bloom_filter.FindHash(bloom_filter.Hash(i)));
  }

  // Serialize Bloom filter to memory output stream
  auto sink = CreateOutputStream();
  bloom_filter.WriteTo(sink.get());

  // Deserialize Bloom filter from memory
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());
  ::arrow::io::BufferReader source(buffer);

  BlockSplitBloomFilter de_bloom = BlockSplitBloomFilter::Deserialize(&source);

  for (int i = 0; i < 10; i++) {
    EXPECT_TRUE(de_bloom.FindHash(de_bloom.Hash(i)));
  }
}

// Helper function to generate random string.
std::string GetRandomString(uint32_t length) {
  // Character set used to generate random string
  const std::string charset =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  std::default_random_engine gen(42);
  std::uniform_int_distribution<uint32_t> dist(0, static_cast<int>(charset.size() - 1));
  std::string ret(length, 'x');

  for (uint32_t i = 0; i < length; i++) {
    ret[i] = charset[dist(gen)];
  }
  return ret;
}

TEST(FPPTest, TestBloomFilter) {
  // It counts the number of times FindHash returns true.
  int exist = 0;

  // Total count of elements that will be used
#ifdef PARQUET_VALGRIND
  const int total_count = 5000;
#else
  const int total_count = 100000;
#endif

  // Bloom filter fpp parameter
  const double fpp = 0.01;

  std::vector<std::string> members;
  BlockSplitBloomFilter bloom_filter;
  bloom_filter.Init(BlockSplitBloomFilter::OptimalNumOfBits(total_count, fpp));

  // Insert elements into the Bloom filter
  for (int i = 0; i < total_count; i++) {
    // Insert random string which length is 8
    std::string tmp = GetRandomString(8);
    const ByteArray byte_array(8, reinterpret_cast<const uint8_t*>(tmp.c_str()));
    members.push_back(tmp);
    bloom_filter.InsertHash(bloom_filter.Hash(&byte_array));
  }

  for (int i = 0; i < total_count; i++) {
    const ByteArray byte_array1(8, reinterpret_cast<const uint8_t*>(members[i].c_str()));
    ASSERT_TRUE(bloom_filter.FindHash(bloom_filter.Hash(&byte_array1)));
    std::string tmp = GetRandomString(7);
    const ByteArray byte_array2(7, reinterpret_cast<const uint8_t*>(tmp.c_str()));

    if (bloom_filter.FindHash(bloom_filter.Hash(&byte_array2))) {
      exist++;
    }
  }

  // The exist should be probably less than 1000 according default FPP 0.01.
  EXPECT_LT(exist, total_count * fpp);
}

// The CompatibilityTest is used to test cross compatibility with parquet-mr, it reads
// the Bloom filter binary generated by the Bloom filter class in the parquet-mr project
// and tests whether the values inserted before could be filtered or not.

// The Bloom filter binary is generated by three steps in from Parquet-mr.
// Step 1: Construct a Bloom filter with 1024 bytes bitset.
// Step 2: Insert "hello", "parquet", "bloom", "filter" to Bloom filter.
// Step 3: Call writeTo API to write to File.
//
// TODO(mwish): The test is deprecated, because the bloom_filter.bin breaks
//  the current bloom filter standard.
/*
TEST(CompatibilityTest, TestBloomFilter) {
  const std::string test_string[4] = {"hello", "parquet", "bloom", "filter"};
  const std::string bloom_filter_test_binary =
      std::string(test::get_data_dir()) + "/bloom_filter.bin";

  PARQUET_ASSIGN_OR_THROW(auto handle,
                          ::arrow::io::ReadableFile::Open(bloom_filter_test_binary));
  PARQUET_ASSIGN_OR_THROW(int64_t size, handle->GetSize());

  // 1024 bytes (bitset) + 4 bytes (hash) + 4 bytes (algorithm) + 4 bytes (length)
  EXPECT_EQ(size, 1036);

  std::unique_ptr<uint8_t[]> bitset(new uint8_t[size]());
  PARQUET_ASSIGN_OR_THROW(auto buffer, handle->Read(size));

  ::arrow::io::BufferReader source(buffer);
  BlockSplitBloomFilter bloom_filter1 = BlockSplitBloomFilter::Deserialize(&source);

  for (int i = 0; i < 4; i++) {
    const ByteArray tmp(static_cast<uint32_t>(test_string[i].length()),
                        reinterpret_cast<const uint8_t*>(test_string[i].c_str()));
    EXPECT_TRUE(bloom_filter1.FindHash(bloom_filter1.Hash(&tmp)));
  }

  // The following is used to check whether the new created Bloom filter in parquet-cpp is
  // byte-for-byte identical to file at bloom_data_path which is created from parquet-mr
  // with same inserted hashes.
  BlockSplitBloomFilter bloom_filter2;
  bloom_filter2.Init(bloom_filter1.GetBitsetSize());
  for (int i = 0; i < 4; i++) {
    const ByteArray byte_array(static_cast<uint32_t>(test_string[i].length()),
                               reinterpret_cast<const uint8_t*>(test_string[i].c_str()));
    bloom_filter2.InsertHash(bloom_filter2.Hash(&byte_array));
  }

  // Serialize Bloom filter to memory output stream
  auto sink = CreateOutputStream();
  bloom_filter2.WriteTo(sink.get());
  PARQUET_ASSIGN_OR_THROW(auto buffer1, sink->Finish());

  PARQUET_THROW_NOT_OK(handle->Seek(0));
  PARQUET_ASSIGN_OR_THROW(size, handle->GetSize());
  PARQUET_ASSIGN_OR_THROW(auto buffer2, handle->Read(size));

  EXPECT_TRUE((*buffer1).Equals(*buffer2));
}
*/

// OptimalValueTest is used to test whether OptimalNumOfBits returns expected
// numbers according to formula:
//     num_of_bits = -8.0 * ndv / log(1 - pow(fpp, 1.0 / 8.0))
// where ndv is the number of distinct values and fpp is the false positive probability.
// Also it is used to test whether OptimalNumOfBits returns value between
// [MINIMUM_BLOOM_FILTER_SIZE, MAXIMUM_BLOOM_FILTER_SIZE].
TEST(OptimalValueTest, TestBloomFilter) {
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(256, 0.01), UINT32_C(4096));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(512, 0.01), UINT32_C(8192));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(1024, 0.01), UINT32_C(16384));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(2048, 0.01), UINT32_C(32768));

  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(200, 0.01), UINT32_C(2048));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(300, 0.01), UINT32_C(4096));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(700, 0.01), UINT32_C(8192));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(1500, 0.01), UINT32_C(16384));

  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(200, 0.025), UINT32_C(2048));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(300, 0.025), UINT32_C(4096));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(700, 0.025), UINT32_C(8192));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(1500, 0.025), UINT32_C(16384));

  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(200, 0.05), UINT32_C(2048));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(300, 0.05), UINT32_C(4096));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(700, 0.05), UINT32_C(8192));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(1500, 0.05), UINT32_C(16384));

  // Boundary check
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(4, 0.01), UINT32_C(256));
  EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(4, 0.25), UINT32_C(256));

  EXPECT_EQ(
      BlockSplitBloomFilter::OptimalNumOfBits(std::numeric_limits<uint32_t>::max(), 0.01),
      UINT32_C(1073741824));
  EXPECT_EQ(
      BlockSplitBloomFilter::OptimalNumOfBits(std::numeric_limits<uint32_t>::max(), 0.25),
      UINT32_C(1073741824));
}

int64_t HASHES_OF_LOOPING_BYTES_WITH_SEED_0[32] = {
    -1205034819632174695L, -1642502924627794072L, 5216751715308240086L,
    -1889335612763511331L, -13835840860730338L,   -2521325055659080948L,
    4867868962443297827L,  1498682999415010002L,  -8626056615231480947L,
    7482827008138251355L,  -617731006306969209L,  7289733825183505098L,
    4776896707697368229L,  1428059224718910376L,  6690813482653982021L,
    -6248474067697161171L, 4951407828574235127L,  6198050452789369270L,
    5776283192552877204L,  -626480755095427154L,  -6637184445929957204L,
    8370873622748562952L,  -1705978583731280501L, -7898818752540221055L,
    -2516210193198301541L, 8356900479849653862L,  -4413748141896466000L,
    -6040072975510680789L, 1451490609699316991L,  -7948005844616396060L,
    8567048088357095527L,  -4375578310507393311L};

int64_t HASHES_OF_LOOPING_BYTES_WITH_SEED_42[32] = {
    -7444071767201028348L, -8959994473701255385L, 7116559933691734543L,
    6019482000716350659L,  -6625277557348586272L, -5507563483608914162L,
    1540412690865189709L,  4522324563441226749L,  -7143238906056518746L,
    -7989831429045113014L, -7103973673268129917L, -2319060423616348937L,
    -7576144055863289344L, -8903544572546912743L, 6376815151655939880L,
    5913754614426879871L,  6466567997237536608L,  -869838547529805462L,
    -2416009472486582019L, -3059673981515537339L, 4211239092494362041L,
    1414635639471257331L,  166863084165354636L,   -3761330575439628223L,
    3524931906845391329L,  6070229753198168844L,  -3740381894759773016L,
    -1268276809699008557L, 1518581707938531581L,  7988048690914090770L,
    -4510281763783422346L, -8988936099728967847L};

TEST(XxHashTest, TestBloomFilter) {
  uint8_t bytes[32] = {};

  for (int i = 0; i < 32; i++) {
    ByteArray byteArray(i, bytes);
    bytes[i] = i;

    auto hasher_seed_0 = std::make_unique<XxHash>();
    EXPECT_EQ(HASHES_OF_LOOPING_BYTES_WITH_SEED_0[i], hasher_seed_0->Hash(&byteArray))
        << "Hash with seed 0 Error: " << i;

    auto hasher_seed_42 = std::make_unique<XxHash>(42);
    EXPECT_EQ(HASHES_OF_LOOPING_BYTES_WITH_SEED_42[i], hasher_seed_42->Hash(&byteArray))
        << "Hash with seed 42 Error: " << i;
  }
}

}  // namespace test

}  // namespace parquet
