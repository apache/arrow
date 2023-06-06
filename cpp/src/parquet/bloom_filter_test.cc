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
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

#include "parquet/bloom_filter.h"
#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/test_util.h"
#include "parquet/types.h"
#include "parquet/xxhasher.h"

namespace parquet {
namespace test {

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
  const std::vector<uint32_t> kBloomFilterSizes = {32, 64, 128, 256, 512, 1024, 2048};
  const std::vector<int32_t> kIntInserts = {1, 2,  3,  5,  6,       7,      8,
                                            9, 10, 42, -1, 1 << 29, 1 << 30};
  const std::vector<double> kFloatInserts = {1.5,     -1.5, 3.0, 6.0, 0.0,
                                             123.456, 1e6,  1e7, 1e8};
  const std::vector<int32_t> kNegativeIntLookups = {0,  11, 12,      13,     -2,
                                                    -3, 43, 1 << 27, 1 << 28};

  for (const auto bloom_filter_bytes : kBloomFilterSizes) {
    BlockSplitBloomFilter bloom_filter;
    bloom_filter.Init(bloom_filter_bytes);

    // Empty bloom filter deterministically returns false
    for (const auto v : kIntInserts) {
      EXPECT_FALSE(bloom_filter.FindHash(bloom_filter.Hash(v)));
    }
    for (const auto v : kFloatInserts) {
      EXPECT_FALSE(bloom_filter.FindHash(bloom_filter.Hash(v)));
    }

    // Insert all values
    for (const auto v : kIntInserts) {
      bloom_filter.InsertHash(bloom_filter.Hash(v));
    }
    for (const auto v : kFloatInserts) {
      bloom_filter.InsertHash(bloom_filter.Hash(v));
    }

    // They should always lookup successfully
    for (const auto v : kIntInserts) {
      EXPECT_TRUE(bloom_filter.FindHash(bloom_filter.Hash(v)));
    }
    for (const auto v : kFloatInserts) {
      EXPECT_TRUE(bloom_filter.FindHash(bloom_filter.Hash(v)));
    }

    // Values not inserted in the filter should only rarely lookup successfully
    int false_positives = 0;
    for (const auto v : kNegativeIntLookups) {
      false_positives += bloom_filter.FindHash(bloom_filter.Hash(v));
    }
    // (this is a crude check, see FPPTest below for a more rigorous formula)
    EXPECT_LE(false_positives, 2);

    // Serialize Bloom filter to memory output stream
    auto sink = CreateOutputStream();
    bloom_filter.WriteTo(sink.get());

    // Deserialize Bloom filter from memory
    ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());
    ::arrow::io::BufferReader source(buffer);

    ReaderProperties reader_properties;
    BlockSplitBloomFilter de_bloom =
        BlockSplitBloomFilter::Deserialize(reader_properties, &source);

    // Lookup previously inserted values
    for (const auto v : kIntInserts) {
      EXPECT_TRUE(de_bloom.FindHash(de_bloom.Hash(v)));
    }
    for (const auto v : kFloatInserts) {
      EXPECT_TRUE(de_bloom.FindHash(de_bloom.Hash(v)));
    }
    false_positives = 0;
    for (const auto v : kNegativeIntLookups) {
      false_positives += de_bloom.FindHash(de_bloom.Hash(v));
    }
    EXPECT_LE(false_positives, 2);
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
  bloom_filter.Init(BlockSplitBloomFilter::OptimalNumOfBytes(total_count, fpp));

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
TEST(CompatibilityTest, TestBloomFilter) {
  const std::string test_string[4] = {"hello", "parquet", "bloom", "filter"};
  const std::string bloom_filter_test_binary =
      std::string(test::get_data_dir()) + "/bloom_filter.xxhash.bin";

  PARQUET_ASSIGN_OR_THROW(auto handle,
                          ::arrow::io::ReadableFile::Open(bloom_filter_test_binary));
  PARQUET_ASSIGN_OR_THROW(int64_t size, handle->GetSize());

  // 16 bytes (thrift header) + 1024 bytes (bitset)
  EXPECT_EQ(size, 1040);

  std::unique_ptr<uint8_t[]> bitset(new uint8_t[size]());
  PARQUET_ASSIGN_OR_THROW(auto buffer, handle->Read(size));

  ::arrow::io::BufferReader source(buffer);
  ReaderProperties reader_properties;
  BlockSplitBloomFilter bloom_filter1 =
      BlockSplitBloomFilter::Deserialize(reader_properties, &source);

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

// OptimalValueTest is used to test whether OptimalNumOfBits returns expected
// numbers according to formula:
//     num_of_bits = -8.0 * ndv / log(1 - pow(fpp, 1.0 / 8.0))
// where ndv is the number of distinct values and fpp is the false positive probability.
// Also it is used to test whether OptimalNumOfBits returns value between
// [MINIMUM_BLOOM_FILTER_SIZE, MAXIMUM_BLOOM_FILTER_SIZE].
TEST(OptimalValueTest, TestBloomFilter) {
  auto testOptimalNumEstimation = [](uint32_t ndv, double fpp, uint32_t num_bits) {
    EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(ndv, fpp), num_bits);
    EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBytes(ndv, fpp), num_bits / 8);
  };

  testOptimalNumEstimation(256, 0.01, UINT32_C(4096));
  testOptimalNumEstimation(512, 0.01, UINT32_C(8192));
  testOptimalNumEstimation(1024, 0.01, UINT32_C(16384));
  testOptimalNumEstimation(2048, 0.01, UINT32_C(32768));

  testOptimalNumEstimation(200, 0.01, UINT32_C(2048));
  testOptimalNumEstimation(300, 0.01, UINT32_C(4096));
  testOptimalNumEstimation(700, 0.01, UINT32_C(8192));
  testOptimalNumEstimation(1500, 0.01, UINT32_C(16384));

  testOptimalNumEstimation(200, 0.025, UINT32_C(2048));
  testOptimalNumEstimation(300, 0.025, UINT32_C(4096));
  testOptimalNumEstimation(700, 0.025, UINT32_C(8192));
  testOptimalNumEstimation(1500, 0.025, UINT32_C(16384));

  testOptimalNumEstimation(200, 0.05, UINT32_C(2048));
  testOptimalNumEstimation(300, 0.05, UINT32_C(4096));
  testOptimalNumEstimation(700, 0.05, UINT32_C(8192));
  testOptimalNumEstimation(1500, 0.05, UINT32_C(16384));

  // Boundary check
  testOptimalNumEstimation(4, 0.01, BlockSplitBloomFilter::kMinimumBloomFilterBytes * 8);
  testOptimalNumEstimation(4, 0.25, BlockSplitBloomFilter::kMinimumBloomFilterBytes * 8);

  testOptimalNumEstimation(std::numeric_limits<uint32_t>::max(), 0.01,
                           BlockSplitBloomFilter::kMaximumBloomFilterBytes * 8);
  testOptimalNumEstimation(std::numeric_limits<uint32_t>::max(), 0.25,
                           BlockSplitBloomFilter::kMaximumBloomFilterBytes * 8);
}

// The test below is plainly copied from parquet-mr and serves as a basic sanity
// check of our XXH64 wrapper.
const int64_t HASHES_OF_LOOPING_BYTES_WITH_SEED_0[32] = {
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

/**
 * Test data is output of the following program with xxHash implementation
 * from https://github.com/Cyan4973/xxHash with commit
 * c8c4cc0f812719ce1f5b2c291159658980e7c255
 *
 * #define XXH_INLINE_ALL
 * #include "xxhash.h"
 * #include <stdlib.h>
 * #include <stdio.h>
 * int main()
 * {
 *     char* src = (char*) malloc(32);
 *     const int N = 32;
 *     for (int i = 0; i < N; i++) {
 *         src[i] = (char) i;
 *     }
 *
 *     printf("without seed\n");
 *     for (int i = 0; i <= N; i++) {
 *        printf("%lldL,\n", (long long) XXH64(src, i, 0));
 *     }
 * }
 */
TEST(XxHashTest, TestBloomFilter) {
  constexpr int kNumValues = 32;
  uint8_t bytes[kNumValues] = {};

  for (int i = 0; i < kNumValues; i++) {
    ByteArray byte_array(i, bytes);
    bytes[i] = i;

    auto hasher_seed_0 = std::make_unique<XxHasher>();
    EXPECT_EQ(HASHES_OF_LOOPING_BYTES_WITH_SEED_0[i], hasher_seed_0->Hash(&byte_array))
        << "Hash with seed 0 Error: " << i;
  }
}

// Same as TestBloomFilter but using Batch interface
TEST(XxHashTest, TestBloomFilterHashes) {
  constexpr int kNumValues = 32;
  uint8_t bytes[kNumValues] = {};

  std::vector<ByteArray> byte_array_vector;
  for (int i = 0; i < kNumValues; i++) {
    bytes[i] = i;
    byte_array_vector.emplace_back(i, bytes);
  }
  auto hasher_seed_0 = std::make_unique<XxHasher>();
  std::vector<uint64_t> hashes;
  hashes.resize(kNumValues);
  hasher_seed_0->Hashes(byte_array_vector.data(),
                        static_cast<int>(byte_array_vector.size()), hashes.data());
  for (int i = 0; i < kNumValues; i++) {
    EXPECT_EQ(HASHES_OF_LOOPING_BYTES_WITH_SEED_0[i], hashes[i])
        << "Hash with seed 0 Error: " << i;
  }
}

template <typename DType>
class TestBatchBloomFilter : public testing::Test {
 public:
  constexpr static int kTestDataSize = 64;

  // GenerateTestData with size 64.
  std::vector<typename DType::c_type> GenerateTestData();

  // The Lifetime owner for Test data
  std::vector<uint8_t> members;
};

template <typename DType>
std::vector<typename DType::c_type> TestBatchBloomFilter<DType>::GenerateTestData() {
  std::vector<typename DType::c_type> values(kTestDataSize);
  GenerateData(kTestDataSize, values.data(), &members);
  return values;
}

// Note: BloomFilter doesn't support BooleanType.
using BloomFilterTestTypes = ::testing::Types<Int32Type, Int64Type, FloatType, DoubleType,
                                              Int96Type, FLBAType, ByteArrayType>;

TYPED_TEST_SUITE(TestBatchBloomFilter, BloomFilterTestTypes);

TYPED_TEST(TestBatchBloomFilter, Basic) {
  using Type = typename TypeParam::c_type;
  std::vector<Type> test_data = TestFixture::GenerateTestData();
  BlockSplitBloomFilter batch_insert_filter;
  BlockSplitBloomFilter filter;

  // Bloom filter fpp parameter
  const double fpp = 0.05;
  filter.Init(BlockSplitBloomFilter::OptimalNumOfBytes(TestFixture::kTestDataSize, fpp));
  batch_insert_filter.Init(
      BlockSplitBloomFilter::OptimalNumOfBytes(TestFixture::kTestDataSize, fpp));

  std::vector<uint64_t> hashes;
  for (const Type& value : test_data) {
    uint64_t hash = 0;
    if constexpr (std::is_same_v<Type, FLBA>) {
      hash = filter.Hash(&value, kGenerateDataFLBALength);
    } else {
      hash = filter.Hash(&value);
    }
    hashes.push_back(hash);
  }

  std::vector<uint64_t> batch_hashes(test_data.size());
  if constexpr (std::is_same_v<Type, FLBA>) {
    batch_insert_filter.Hashes(test_data.data(), kGenerateDataFLBALength,
                               static_cast<int>(test_data.size()), batch_hashes.data());
  } else {
    batch_insert_filter.Hashes(test_data.data(), static_cast<int>(test_data.size()),
                               batch_hashes.data());
  }

  EXPECT_EQ(hashes, batch_hashes);

  std::shared_ptr<Buffer> buffer;
  std::shared_ptr<Buffer> batch_insert_buffer;
  {
    auto sink = CreateOutputStream();
    filter.WriteTo(sink.get());
    ASSERT_OK_AND_ASSIGN(buffer, sink->Finish());
  }
  {
    auto sink = CreateOutputStream();
    batch_insert_filter.WriteTo(sink.get());
    ASSERT_OK_AND_ASSIGN(batch_insert_buffer, sink->Finish());
  }
  AssertBufferEqual(*buffer, *batch_insert_buffer);
}

}  // namespace test
}  // namespace parquet
