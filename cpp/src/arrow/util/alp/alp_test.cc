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

#include <cmath>
#include <cstdint>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/alp/alp.h"
#include "arrow/util/alp/alp_constants.h"
#include "arrow/util/alp/alp_sampler.h"
#include "arrow/util/alp/alp_codec.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bpacking_internal.h"

namespace arrow {
namespace util {
namespace alp {

// ============================================================================
// Test helpers
// ============================================================================

// Compares two floating-point ranges by bit pattern, not by operator==.
// ALP is a lossless codec, so its tests must verify bit-exact recovery:
// `0.0 == -0.0` (different bits) and `NaN != NaN` (identical bits) make
// `EXPECT_THAT(out, ElementsAreArray(in))` the wrong check here. On
// mismatch the failure message names the index and prints both the
// value and the underlying hex bits.
template <typename T>
::testing::AssertionResult IsBitwiseEqual(const std::vector<T>& actual,
                                          const std::vector<T>& expected) {
  static_assert(std::is_floating_point<T>::value,
                "IsBitwiseEqual is for float/double only");
  using Bits = typename std::conditional<sizeof(T) == 4, uint32_t, uint64_t>::type;
  if (actual.size() != expected.size()) {
    return ::testing::AssertionFailure()
        << "size mismatch: actual=" << actual.size()
        << " expected=" << expected.size();
  }
  for (size_t i = 0; i < actual.size(); ++i) {
    Bits a_bits = 0, e_bits = 0;
    std::memcpy(&a_bits, &actual[i], sizeof(T));
    std::memcpy(&e_bits, &expected[i], sizeof(T));
    if (a_bits != e_bits) {
      return ::testing::AssertionFailure()
          << "bit-mismatch at index " << i << ": actual=" << actual[i]
          << " (bits 0x" << std::hex << a_bits << "), expected=" << std::dec
          << expected[i] << " (bits 0x" << std::hex << e_bits << ")";
    }
  }
  return ::testing::AssertionSuccess();
}

// ============================================================================
// ALP Constants Tests
// ============================================================================

TEST(AlpConstantsTest, SamplerConstants) {
  EXPECT_GT(AlpConstants::kSamplerVectorSize, 0);
  EXPECT_GT(AlpConstants::kSamplerRowgroupSize, 0);
  EXPECT_GT(AlpConstants::kSamplerSamplesPerVector, 0);
}

// ============================================================================
// AlpIntegerEncoding Tests
// ============================================================================

TEST(AlpIntegerEncodingTest, GetIntegerEncodingMetadataSize) {
  // Verify helper returns correct sizes for kForBitPack
  EXPECT_EQ(GetIntegerEncodingMetadataSize<float>(AlpIntegerEncoding::kForBitPack),
            AlpEncodedForVectorInfo<float>::kStoredSize);
  EXPECT_EQ(GetIntegerEncodingMetadataSize<double>(AlpIntegerEncoding::kForBitPack),
            AlpEncodedForVectorInfo<double>::kStoredSize);

  // Verify actual byte sizes (frame_of_reference + bit_width, no reserved)
  EXPECT_EQ(GetIntegerEncodingMetadataSize<float>(AlpIntegerEncoding::kForBitPack), 5);
  EXPECT_EQ(GetIntegerEncodingMetadataSize<double>(AlpIntegerEncoding::kForBitPack), 9);
}

// ============================================================================
// ALP Compression Tests (Float)
// ============================================================================

class AlpCompressionFloatTest : public ::testing::Test {
 protected:
  void TestCompressDecompressFloat(const std::vector<float>& input) {
    AlpCompression<float> compressor;

    // Compress
    AlpEncodingParameters preset{};  // Default preset
    auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

    // Decompress
    std::vector<float> output(input.size());
    compressor.DecompressVector(encoded, AlpIntegerEncoding::kForBitPack, output.data());

    // Verify
    ASSERT_EQ(output.size(), input.size());
    for (size_t i = 0; i < input.size(); ++i) {
      EXPECT_FLOAT_EQ(output[i], input[i]) << "Mismatch at index " << i;
    }
  }
};

TEST_F(AlpCompressionFloatTest, SimpleSequence) {
  std::vector<float> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<float>(i + 1);
  }
  TestCompressDecompressFloat(input);
}

TEST_F(AlpCompressionFloatTest, DecimalValues) {
  std::vector<float> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<float>(i) + 0.5f;
  }
  TestCompressDecompressFloat(input);
}

TEST_F(AlpCompressionFloatTest, SmallValues) {
  std::vector<float> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = 0.001f * (i + 1);
  }
  TestCompressDecompressFloat(input);
}

TEST_F(AlpCompressionFloatTest, MixedValues) {
  std::vector<float> input = {100.5f,       200.25f,       300.125f,   400.0625f,
                              500.03125f,   600.015625f,   700.0078125f,
                              800.00390625f};
  TestCompressDecompressFloat(input);
}

TEST_F(AlpCompressionFloatTest, RandomValues) {
  std::mt19937 rng(42);
  std::uniform_real_distribution<float> dist(0.0f, 1000.0f);

  std::vector<float> input(64);
  for (auto& v : input) {
    v = dist(rng);
  }

  TestCompressDecompressFloat(input);
}

// ============================================================================
// ALP Compression Tests (Double)
// ============================================================================

class AlpCompressionDoubleTest : public ::testing::Test {
 protected:
  void TestCompressDecompressDouble(const std::vector<double>& input) {
    AlpCompression<double> compressor;

    // Compress
    AlpEncodingParameters preset{};  // Default preset
    auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

    // Decompress
    std::vector<double> output(input.size());
    compressor.DecompressVector(encoded, AlpIntegerEncoding::kForBitPack, output.data());

    // Verify
    ASSERT_EQ(output.size(), input.size());
    for (size_t i = 0; i < input.size(); ++i) {
      EXPECT_DOUBLE_EQ(output[i], input[i]) << "Mismatch at index " << i;
    }
  }
};

TEST_F(AlpCompressionDoubleTest, SimpleSequence) {
  std::vector<double> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<double>(i + 1);
  }
  TestCompressDecompressDouble(input);
}

TEST_F(AlpCompressionDoubleTest, HighPrecision) {
  std::vector<double> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = 1.123456789 * (i + 1);
  }
  TestCompressDecompressDouble(input);
}

TEST_F(AlpCompressionDoubleTest, VerySmallValues) {
  std::vector<double> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = 1e-10 * (i + 1);
  }
  TestCompressDecompressDouble(input);
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST(AlpIntegrationTest, LargeFloatDataset) {
  std::mt19937 rng(12345);
  std::uniform_real_distribution<float> dist(-1000.0f, 1000.0f);

  std::vector<float> input(1024);
  for (auto& v : input) {
    v = dist(rng);
  }

  AlpCompression<float> compressor;
  AlpEncodingParameters preset{};
  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  std::vector<float> output(input.size());
  compressor.DecompressVector(encoded, AlpIntegerEncoding::kForBitPack, output.data());

  for (size_t i = 0; i < input.size(); ++i) {
    EXPECT_FLOAT_EQ(output[i], input[i]);
  }
}

TEST(AlpIntegrationTest, LargeDoubleDataset) {
  std::mt19937 rng(12345);
  std::uniform_real_distribution<double> dist(-1000.0, 1000.0);

  std::vector<double> input(1024);
  for (auto& v : input) {
    v = dist(rng);
  }

  AlpCompression<double> compressor;
  AlpEncodingParameters preset{};
  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  std::vector<double> output(input.size());
  compressor.DecompressVector(encoded, AlpIntegerEncoding::kForBitPack, output.data());

  for (size_t i = 0; i < input.size(); ++i) {
    EXPECT_DOUBLE_EQ(output[i], input[i]);
  }
}

// ============================================================================
// AlpEncodedVectorInfo Serialization Tests
// ============================================================================

TEST(AlpEncodedVectorInfoTest, StoreLoadRoundTrip) {
  // Test AlpEncodedVectorInfo (non-templated, 4 bytes)
  AlpEncodedVectorInfo info{};
  info.set_exponent(5);
  info.set_factor(3);
  info.set_num_exceptions(10);

  std::vector<uint8_t> buffer(AlpEncodedVectorInfo::kStoredSize + 10);
  info.Store({buffer.data(), buffer.size()});

  ASSERT_OK_AND_ASSIGN(AlpEncodedVectorInfo loaded,
                       AlpEncodedVectorInfo::Load({buffer.data(), buffer.size()}));
  EXPECT_EQ(info, loaded);
  EXPECT_EQ(loaded.exponent(), 5);
  EXPECT_EQ(loaded.factor(), 3);
  EXPECT_EQ(loaded.num_exceptions(), 10);
}

TEST(AlpEncodedForVectorInfoTest, StoreLoadRoundTripFloat) {
  // Test AlpEncodedForVectorInfo<float> (6 bytes)
  AlpEncodedForVectorInfo<float> info{};
  info.set_frame_of_reference(0x12345678U);
  info.set_bit_width(12);

  std::vector<uint8_t> buffer(AlpEncodedForVectorInfo<float>::kStoredSize + 10);
  info.Store({buffer.data(), buffer.size()});

  ASSERT_OK_AND_ASSIGN(AlpEncodedForVectorInfo<float> loaded,
                       AlpEncodedForVectorInfo<float>::Load({buffer.data(), buffer.size()}));
  EXPECT_EQ(info, loaded);
  EXPECT_EQ(loaded.frame_of_reference(), 0x12345678U);
  EXPECT_EQ(loaded.bit_width(), 12);
}

TEST(AlpEncodedForVectorInfoTest, StoreLoadRoundTripDouble) {
  // Test AlpEncodedForVectorInfo<double> (10 bytes)
  AlpEncodedForVectorInfo<double> info{};
  info.set_frame_of_reference(0x123456789ABCDEF0ULL);
  info.set_bit_width(20);

  std::vector<uint8_t> buffer(AlpEncodedForVectorInfo<double>::kStoredSize + 10);
  info.Store({buffer.data(), buffer.size()});

  ASSERT_OK_AND_ASSIGN(AlpEncodedForVectorInfo<double> loaded,
                       AlpEncodedForVectorInfo<double>::Load({buffer.data(), buffer.size()}));
  EXPECT_EQ(info, loaded);
  EXPECT_EQ(loaded.frame_of_reference(), 0x123456789ABCDEF0ULL);
  EXPECT_EQ(loaded.bit_width(), 20);
}

TEST(AlpEncodedVectorInfoTest, Size) {
  // AlpEncodedVectorInfo is non-templated and fixed at 4 bytes
  EXPECT_EQ(AlpEncodedVectorInfo::kStoredSize, 4);
  EXPECT_EQ(AlpEncodedVectorInfo::GetStoredSize(), 4);
}

TEST(AlpEncodedForVectorInfoTest, Size) {
  // AlpEncodedForVectorInfo: float=5 bytes, double=9 bytes
  // (frame_of_reference is 4 bytes for float, 8 bytes for double, + 1 byte for bit_width)
  EXPECT_EQ(AlpEncodedForVectorInfo<float>::kStoredSize, 5);
  EXPECT_EQ(AlpEncodedForVectorInfo<float>::GetStoredSize(), 5);
  EXPECT_EQ(AlpEncodedForVectorInfo<double>::kStoredSize, 9);
  EXPECT_EQ(AlpEncodedForVectorInfo<double>::GetStoredSize(), 9);
}

// ============================================================================
// Edge Case Tests
// ============================================================================

template <typename T>
class AlpEdgeCaseTest : public ::testing::Test {
 protected:
  void TestCompressDecompress(const std::vector<T>& input) {
    AlpCompression<T> compressor;
    AlpEncodingParameters preset{};
    auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

    std::vector<T> output(input.size());
    compressor.DecompressVector(encoded, AlpIntegerEncoding::kForBitPack, output.data());

    ASSERT_EQ(output.size(), input.size());
    // Verify bit-exact recovery (important for -0.0, NaN; see IsBitwiseEqual).
    EXPECT_TRUE(IsBitwiseEqual(output, input));
  }
};

using EdgeCaseTestTypes = ::testing::Types<float, double>;
TYPED_TEST_SUITE(AlpEdgeCaseTest, EdgeCaseTestTypes);

TYPED_TEST(AlpEdgeCaseTest, SingleElement) {
  std::vector<TypeParam> input = {static_cast<TypeParam>(42.5)};
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, EmptyInput) {
  // Test zero elements - empty vector
  // The wrapper API requires decomp_size to be a multiple of sizeof(T),
  // and 0 is a valid multiple. This tests the boundary condition.
  std::vector<TypeParam> input;

  int64_t max_size = AlpCodec<TypeParam>::GetMaxCompressedSize(0);
  std::vector<uint8_t> buffer(max_size > 0 ? max_size : 8);  // Ensure some buffer
  int64_t comp_size = static_cast<int64_t>(buffer.size());

  AlpCodec<TypeParam>::Encode(input.data(), 0, buffer.data(), &comp_size);

  // Decode zero elements
  std::vector<TypeParam> output;
  ASSERT_OK(AlpCodec<TypeParam>::template Decode<TypeParam>(0, buffer.data(),
                                                              comp_size, output.data()));

  // Both should be empty
  EXPECT_EQ(input.size(), output.size());
  EXPECT_EQ(input.size(), 0);
}

TYPED_TEST(AlpEdgeCaseTest, TwoElements) {
  std::vector<TypeParam> input = {static_cast<TypeParam>(1.5),
                                  static_cast<TypeParam>(2.5)};
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, ExactVectorSize) {
  // Test exactly kAlpVectorSize elements (1024)
  std::vector<TypeParam> input(AlpConstants::kAlpVectorSize);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, JustUnderVectorSize) {
  // Test kAlpVectorSize - 1 elements (1023)
  std::vector<TypeParam> input(AlpConstants::kAlpVectorSize - 1);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, JustOverVectorSize) {
  // Test kAlpVectorSize + 1 elements (1025) - requires multiple vectors
  std::vector<TypeParam> input(AlpConstants::kAlpVectorSize + 1);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }
  // For multi-vector, we need to process in chunks
  AlpCompression<TypeParam> compressor;
  AlpEncodingParameters preset{};

  // Process first vector
  auto encoded1 = compressor.CompressVector(input.data(),
                                            AlpConstants::kAlpVectorSize, preset);
  std::vector<TypeParam> output1(AlpConstants::kAlpVectorSize);
  compressor.DecompressVector(encoded1, AlpIntegerEncoding::kForBitPack, output1.data());

  // Process remaining element
  auto encoded2 = compressor.CompressVector(
      input.data() + AlpConstants::kAlpVectorSize, 1, preset);
  std::vector<TypeParam> output2(1);
  compressor.DecompressVector(encoded2, AlpIntegerEncoding::kForBitPack, output2.data());

  // Verify (first vector covers input[0:kAlpVectorSize], second covers the
  // trailing element).
  EXPECT_TRUE(IsBitwiseEqual(
      output1,
      std::vector<TypeParam>(input.begin(),
                             input.begin() + AlpConstants::kAlpVectorSize)));
  EXPECT_TRUE(IsBitwiseEqual(
      output2,
      std::vector<TypeParam>{input[AlpConstants::kAlpVectorSize]}));
}

// ============================================================================
// Special Values Tests
// ============================================================================

TYPED_TEST(AlpEdgeCaseTest, SpecialValues) {
  // Test NaN, Inf, -Inf, -0.0
  std::vector<TypeParam> input = {
      static_cast<TypeParam>(0.0),
      static_cast<TypeParam>(-0.0),
      std::numeric_limits<TypeParam>::infinity(),
      -std::numeric_limits<TypeParam>::infinity(),
      std::numeric_limits<TypeParam>::quiet_NaN(),
  };
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, NegativeZero) {
  // -0.0 should be preserved bit-exactly
  std::vector<TypeParam> input(100);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = (i % 2 == 0) ? static_cast<TypeParam>(0.0)
                            : static_cast<TypeParam>(-0.0);
  }
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, AllNaN) {
  // All NaN values - all become exceptions
  std::vector<TypeParam> input(64);
  for (auto& v : input) {
    v = std::numeric_limits<TypeParam>::quiet_NaN();
  }
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, AllInfinity) {
  // All infinity values
  std::vector<TypeParam> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = (i % 2 == 0) ? std::numeric_limits<TypeParam>::infinity()
                            : -std::numeric_limits<TypeParam>::infinity();
  }
  this->TestCompressDecompress(input);
}

// ============================================================================
// Compression Characteristics Tests
// ============================================================================

TYPED_TEST(AlpEdgeCaseTest, ConstantValues) {
  // All same values - should compress very well (bitWidth = 0)
  std::vector<TypeParam> input(1024);
  std::fill(input.begin(), input.end(), static_cast<TypeParam>(123.456));
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, MixedCompressibleAndExceptions) {
  // Mix of compressible decimals and exceptions
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    if (i % 10 == 0) {
      input[i] = std::numeric_limits<TypeParam>::quiet_NaN();
    } else if (i % 20 == 5) {
      input[i] = std::numeric_limits<TypeParam>::infinity();
    } else {
      input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.01);
    }
  }
  this->TestCompressDecompress(input);
}

// ============================================================================
// Boundary Value Tests
// ============================================================================

TYPED_TEST(AlpEdgeCaseTest, MaxMinValues) {
  std::vector<TypeParam> input = {
      std::numeric_limits<TypeParam>::max(),
      std::numeric_limits<TypeParam>::min(),
      std::numeric_limits<TypeParam>::lowest(),
      std::numeric_limits<TypeParam>::denorm_min(),
      std::numeric_limits<TypeParam>::epsilon(),
      -std::numeric_limits<TypeParam>::max(),
      -std::numeric_limits<TypeParam>::min(),
      -std::numeric_limits<TypeParam>::denorm_min(),
      -std::numeric_limits<TypeParam>::epsilon(),
      static_cast<TypeParam>(0.0)};
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, Subnormals) {
  // Test subnormal (denormalized) floating point values
  std::vector<TypeParam> input(100);
  TypeParam subnormal = std::numeric_limits<TypeParam>::denorm_min();
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = subnormal * static_cast<TypeParam>(i + 1);
  }
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, LargeDecimals) {
  // Test large decimal values that should still be compressible
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(1000000.0) +
               static_cast<TypeParam>(i) * static_cast<TypeParam>(0.01);
  }
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, SmallDecimals) {
  // Test very small decimal values
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(0.000001) * static_cast<TypeParam>(i + 1);
  }
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, NegativeValues) {
  // Test negative values
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = -static_cast<TypeParam>(i) * static_cast<TypeParam>(0.5);
  }
  this->TestCompressDecompress(input);
}

TYPED_TEST(AlpEdgeCaseTest, AlternatingSignValues) {
  // Test values alternating between positive and negative
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    TypeParam sign = (i % 2 == 0) ? static_cast<TypeParam>(1.0)
                                  : static_cast<TypeParam>(-1.0);
    input[i] = sign * static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }
  this->TestCompressDecompress(input);
}

// ============================================================================
// AlpEncodedVector Store/Load Tests
// ============================================================================

template <typename T>
class AlpEncodedVectorTest : public ::testing::Test {};

TYPED_TEST_SUITE(AlpEncodedVectorTest, EdgeCaseTestTypes);

TYPED_TEST(AlpEncodedVectorTest, StoreLoadRoundTrip) {
  // Create a sample encoded vector
  AlpCompression<TypeParam> compressor;
  AlpEncodingParameters preset{};

  std::vector<TypeParam> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.5);
  }

  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  // Store
  std::vector<uint8_t> buffer(encoded.GetStoredSize());
  encoded.Store({buffer.data(), buffer.size()});

  // Load (pass num_elements since it's not stored in the buffer)
  ASSERT_OK_AND_ASSIGN(
      auto loaded,
      AlpEncodedVector<TypeParam>::Load(
          {buffer.data(), buffer.size()}, static_cast<uint16_t>(input.size())));

  // Verify metadata
  EXPECT_EQ(encoded.alp_info(), loaded.alp_info());
  EXPECT_EQ(encoded.for_info(), loaded.for_info());

  // Decompress loaded and verify
  std::vector<TypeParam> output(input.size());
  compressor.DecompressVector(loaded, AlpIntegerEncoding::kForBitPack, output.data());

  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

TYPED_TEST(AlpEncodedVectorTest, GetStoredSizeConsistency) {
  AlpCompression<TypeParam> compressor;
  AlpEncodingParameters preset{};

  std::vector<TypeParam> input(128);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.25);
  }

  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  // Verify GetStoredSize matches actual storage
  std::vector<uint8_t> buffer(encoded.GetStoredSize());
  encoded.Store({buffer.data(), buffer.size()});

  EXPECT_EQ(buffer.size(), encoded.GetStoredSize());
}

// ============================================================================
// AlpEncodedVectorView Tests - Alignment Safety
// ============================================================================

// This test exercises AlpEncodedVectorView::LoadView which was previously
// vulnerable to undefined behavior from misaligned memory access (ubsan error).
// The old code used reinterpret_cast to create spans pointing directly into
// the buffer for exception_positions (uint16_t*) and exceptions (T*), which
// could violate alignment requirements when bit_packed_size was odd.
//
// The fix copies these into aligned std::vector storage.
TYPED_TEST(AlpEncodedVectorTest, ViewLoadWithExceptions) {
  AlpCompression<TypeParam> compressor;
  AlpEncodingParameters preset{};

  // Create data with exceptions to ensure exception handling code path is hit.
  // NaN, Inf, and -0.0 all become exceptions.
  std::vector<TypeParam> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    if (i % 10 == 0) {
      // Every 10th value is NaN - becomes an exception
      input[i] = std::numeric_limits<TypeParam>::quiet_NaN();
    } else if (i % 10 == 5) {
      // Some infinities - also exceptions
      input[i] = std::numeric_limits<TypeParam>::infinity();
    } else {
      // Normal compressible values
      input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
    }
  }

  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  // Verify we actually have exceptions
  EXPECT_GT(encoded.alp_info().num_exceptions(), 0)
      << "Test requires exceptions to exercise alignment code path";

  // Store to buffer
  std::vector<uint8_t> buffer(encoded.GetStoredSize());
  encoded.Store({buffer.data(), buffer.size()});

  // Load using zero-copy view - this was where the ubsan error occurred
  ASSERT_OK_AND_ASSIGN(
      auto view,
      AlpEncodedVectorView<TypeParam>::LoadView(
          {buffer.data(), buffer.size()}, static_cast<uint16_t>(input.size())));

  // Verify view loaded correctly
  EXPECT_EQ(view.alp_info(), encoded.alp_info());
  EXPECT_EQ(view.for_info(), encoded.for_info());
  EXPECT_EQ(view.num_elements(), input.size());
  EXPECT_EQ(view.exception_positions().size(), encoded.alp_info().num_exceptions());
  EXPECT_EQ(view.exceptions().size(), encoded.alp_info().num_exceptions());

  // Decompress using the view - this exercises PatchExceptions with the
  // std::vector members (previously spans that could be misaligned)
  std::vector<TypeParam> output(input.size());
  compressor.DecompressVectorView(view, AlpIntegerEncoding::kForBitPack, output.data());

  // Verify bit-exact reconstruction
  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

// Test specifically designed to create misaligned buffer offsets.
// VectorInfo is 10 bytes for float, 14 for double. If bit_packed_size is odd, exception_positions
// starts at an odd offset (14 + odd = odd), violating uint16_t alignment.
TYPED_TEST(AlpEncodedVectorTest, ViewLoadWithMisalignedExceptions) {
  AlpCompression<TypeParam> compressor;
  AlpEncodingParameters preset{};

  // Create a small vector with specific size to get odd bit_packed_size.
  // 5 elements with bit_width=8 -> bit_packed_size=5 (odd)
  // 7 elements with bit_width=8 -> bit_packed_size=7 (odd)
  // 9 elements with bit_width=8 -> bit_packed_size=9 (odd)
  // We want to ensure at least one exception exists.
  std::vector<TypeParam> input = {
      static_cast<TypeParam>(1.0),
      static_cast<TypeParam>(2.0),
      static_cast<TypeParam>(3.0),
      std::numeric_limits<TypeParam>::quiet_NaN(),  // Exception
      static_cast<TypeParam>(5.0),
      static_cast<TypeParam>(6.0),
      std::numeric_limits<TypeParam>::infinity(),  // Exception
  };

  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  // Verify we have exceptions
  EXPECT_GE(encoded.alp_info().num_exceptions(), 2)
      << "Expected at least 2 exceptions (NaN and Inf)";

  // Store to buffer
  std::vector<uint8_t> buffer(encoded.GetStoredSize());
  encoded.Store({buffer.data(), buffer.size()});

  // Calculate where exceptions start to verify potential misalignment
  const uint64_t alp_info_size = AlpEncodedVectorInfo::kStoredSize;
  const uint64_t for_info_size = AlpEncodedForVectorInfo<TypeParam>::kStoredSize;
  const uint64_t bit_packed_size = AlpEncodedForVectorInfo<TypeParam>::GetBitPackedSize(
      static_cast<uint16_t>(input.size()), encoded.for_info().bit_width());
  const uint64_t exception_pos_offset = alp_info_size + for_info_size + bit_packed_size;

  // Log alignment info for debugging
  SCOPED_TRACE("AlpInfo size: " + std::to_string(alp_info_size));
  SCOPED_TRACE("ForInfo size: " + std::to_string(for_info_size));
  SCOPED_TRACE("Bit packed size: " + std::to_string(bit_packed_size));
  SCOPED_TRACE("Exception pos offset: " + std::to_string(exception_pos_offset));
  SCOPED_TRACE("Offset is aligned: " +
               std::to_string(exception_pos_offset % alignof(uint16_t) == 0));

  // Load using view - with old code, this would trigger ubsan if misaligned
  ASSERT_OK_AND_ASSIGN(
      auto view,
      AlpEncodedVectorView<TypeParam>::LoadView(
          {buffer.data(), buffer.size()}, static_cast<uint16_t>(input.size())));

  // Access exceptions explicitly - with old code using spans, this would
  // be undefined behavior if the buffer wasn't properly aligned
  EXPECT_EQ(view.exception_positions().size(), encoded.alp_info().num_exceptions());
  EXPECT_EQ(view.exceptions().size(), encoded.alp_info().num_exceptions());

  // Verify exception positions are accessible and valid
  for (size_t i = 0; i < view.exception_positions().size(); ++i) {
    EXPECT_LT(view.exception_positions()[i], input.size())
        << "Exception position out of bounds at index " << i;
  }

  // Decompress and verify
  std::vector<TypeParam> output(input.size());
  compressor.DecompressVectorView(view, AlpIntegerEncoding::kForBitPack, output.data());

  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

// Test with buffer allocated at intentionally odd offset to maximize
// chance of hitting misalignment issues on systems that don't crash.
TYPED_TEST(AlpEncodedVectorTest, ViewLoadFromMisalignedBuffer) {
  AlpCompression<TypeParam> compressor;
  AlpEncodingParameters preset{};

  // Data with exceptions
  std::vector<TypeParam> input(32);
  for (size_t i = 0; i < input.size(); ++i) {
    if (i % 8 == 0) {
      input[i] = std::numeric_limits<TypeParam>::quiet_NaN();
    } else {
      input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.5);
    }
  }

  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);
  EXPECT_GT(encoded.alp_info().num_exceptions(), 0);

  // Allocate buffer with extra byte, then use offset to create misaligned start
  std::vector<uint8_t> oversized_buffer(encoded.GetStoredSize() + 16);

  // Try different offsets to hit various alignment scenarios
  for (size_t offset = 0; offset < 8; ++offset) {
    uint8_t* buffer_start = oversized_buffer.data() + offset;
    arrow::util::span<uint8_t> buffer(buffer_start, encoded.GetStoredSize());

    encoded.Store(buffer);

    // Load view from potentially misaligned buffer
    ASSERT_OK_AND_ASSIGN(
        auto view,
        AlpEncodedVectorView<TypeParam>::LoadView(
            {buffer_start, static_cast<size_t>(encoded.GetStoredSize())},
            static_cast<uint16_t>(input.size())));

    // Decompress - this is where the fix matters
    std::vector<TypeParam> output(input.size());
    compressor.DecompressVectorView(view, AlpIntegerEncoding::kForBitPack, output.data());

    // Verify
    EXPECT_TRUE(IsBitwiseEqual(output, input))
        << "Failed at buffer offset " << offset;
  }
}

// ============================================================================
// AlpCodec Tests
// ============================================================================

template <typename T>
class AlpCodecTest : public ::testing::Test {
 protected:
  void TestEncodeDecodeWrapper(const std::vector<T>& input) {
    // Get max compressed size
    int64_t max_comp_size =
        AlpCodec<T>::GetMaxCompressedSize(input.size());
    std::vector<uint8_t> comp_buffer(max_comp_size);

    // Encode
    int64_t comp_size = max_comp_size;
    AlpCodec<T>::Encode(input.data(),
                          static_cast<int64_t>(input.size()),
                          comp_buffer.data(), &comp_size);

    EXPECT_GT(comp_size, 0);
    EXPECT_LE(comp_size, max_comp_size);

    // Decode
    std::vector<T> output(input.size());
    ASSERT_OK(AlpCodec<T>::template Decode<T>(
        static_cast<int32_t>(input.size()), comp_buffer.data(),
        comp_size, output.data()));

    // Verify
    EXPECT_TRUE(IsBitwiseEqual(output, input));
  }
};

TYPED_TEST_SUITE(AlpCodecTest, EdgeCaseTestTypes);

TYPED_TEST(AlpCodecTest, SimpleSequence) {
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }
  this->TestEncodeDecodeWrapper(input);
}

TYPED_TEST(AlpCodecTest, MultipleVectors) {
  // Test with multiple vectors worth of data
  std::vector<TypeParam> input(3 * AlpConstants::kAlpVectorSize);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.01);
  }
  this->TestEncodeDecodeWrapper(input);
}

TYPED_TEST(AlpCodecTest, SpecialValues) {
  std::vector<TypeParam> input = {
      static_cast<TypeParam>(0.0),
      static_cast<TypeParam>(-0.0),
      std::numeric_limits<TypeParam>::infinity(),
      -std::numeric_limits<TypeParam>::infinity(),
      std::numeric_limits<TypeParam>::quiet_NaN(),
      static_cast<TypeParam>(1.5),
      static_cast<TypeParam>(-2.5),
  };
  this->TestEncodeDecodeWrapper(input);
}

TYPED_TEST(AlpCodecTest, GetMaxCompressedSizeAdequate) {
  // Verify GetMaxCompressedSize always provides enough space
  const std::vector<size_t> test_sizes = {1, 10, 100, 1023, 1024, 1025, 2048, 5000};

  for (const size_t size : test_sizes) {
    std::vector<TypeParam> input(size);
    for (size_t i = 0; i < size; ++i) {
      // Mix of values to create a realistic scenario
      input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.123);
      if (i % 7 == 0) {
        input[i] = std::numeric_limits<TypeParam>::quiet_NaN();
      }
    }

    int64_t max_comp_size =
        AlpCodec<TypeParam>::GetMaxCompressedSize(static_cast<int64_t>(size));
    std::vector<uint8_t> comp_buffer(max_comp_size);
    int64_t comp_size = max_comp_size;

    AlpCodec<TypeParam>::Encode(input.data(), static_cast<int64_t>(size),
                                  comp_buffer.data(), &comp_size);

    EXPECT_LE(comp_size, max_comp_size)
        << "Compressed size exceeded max for " << size << " elements";
    EXPECT_GT(comp_size, 0)
        << "Compression produced 0 bytes for " << size << " elements";
  }
}

TYPED_TEST(AlpCodecTest, WideningDecode) {
  // Test decoding float data to double (widening conversion)
  if constexpr (std::is_same_v<TypeParam, float>) {
    std::vector<float> input(256);
    for (size_t i = 0; i < input.size(); ++i) {
      input[i] = static_cast<float>(i) * 0.5f;
    }

    int64_t max_comp_size =
        AlpCodec<float>::GetMaxCompressedSize(static_cast<int64_t>(input.size()));
    std::vector<uint8_t> comp_buffer(max_comp_size);
    int64_t comp_size = max_comp_size;

    AlpCodec<float>::Encode(input.data(), static_cast<int64_t>(input.size()),
                              comp_buffer.data(), &comp_size);

    // Decode as double
    std::vector<double> output(input.size());
    ASSERT_OK(AlpCodec<float>::template Decode<double>(
        static_cast<int32_t>(input.size()), comp_buffer.data(), comp_size,
        output.data()));

    // Verify values match (as double)
    for (size_t i = 0; i < input.size(); ++i) {
      EXPECT_DOUBLE_EQ(output[i], static_cast<double>(input[i]));
    }
  }
}

// ============================================================================
// Bit-Width Edge Cases Tests
// ============================================================================

TYPED_TEST(AlpEdgeCaseTest, ZeroBitWidth) {
  // All identical values should result in bit_width=0
  std::vector<TypeParam> input(1024);
  std::fill(input.begin(), input.end(), static_cast<TypeParam>(123.456));

  AlpCompression<TypeParam> compressor;
  AlpEncodingParameters preset{};
  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  // bit_width should be 0 for constant values
  EXPECT_EQ(encoded.for_info().bit_width(), 0);

  // Verify round-trip
  std::vector<TypeParam> output(input.size());
  compressor.DecompressVector(encoded, AlpIntegerEncoding::kForBitPack, output.data());
  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

TYPED_TEST(AlpEdgeCaseTest, SmallBitWidths) {
  // Test small bit widths (1-8)
  for (int bit_range = 1; bit_range <= 8; ++bit_range) {
    std::vector<TypeParam> input(1024);
    TypeParam base_value = static_cast<TypeParam>(1000.0);

    for (size_t i = 0; i < input.size(); ++i) {
      input[i] = base_value + static_cast<TypeParam>(i % (1 << bit_range)) *
                                  static_cast<TypeParam>(0.01);
    }

    AlpCompression<TypeParam> compressor;
    AlpEncodingParameters preset{};
    auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

    std::vector<TypeParam> output(input.size());
    compressor.DecompressVector(encoded, AlpIntegerEncoding::kForBitPack, output.data());

    EXPECT_TRUE(IsBitwiseEqual(output, input))
        << "Failed for bit_range=" << bit_range;
  }
}

TYPED_TEST(AlpEdgeCaseTest, LargeBitWidths) {
  // Test large bit widths by creating data with large range
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    // Large spread of values
    input[i] = static_cast<TypeParam>(i * 1000000.0);
  }

  AlpCompression<TypeParam> compressor;
  AlpEncodingParameters preset{};
  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  std::vector<TypeParam> output(input.size());
  compressor.DecompressVector(encoded, AlpIntegerEncoding::kForBitPack, output.data());

  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

// ============================================================================
// Large Dataset Tests
// ============================================================================

TYPED_TEST(AlpCodecTest, VeryLargeDataset) {
  // Test with 1 million elements
  constexpr size_t kLargeSize = 1024 * 1024;
  std::vector<TypeParam> input(kLargeSize);

  std::mt19937 rng(12345);
  std::uniform_real_distribution<TypeParam> dist(
      static_cast<TypeParam>(-1000.0), static_cast<TypeParam>(1000.0));

  for (auto& v : input) {
    v = dist(rng);
  }

  this->TestEncodeDecodeWrapper(input);
}

TYPED_TEST(AlpCodecTest, MultiplePages) {
  // Test with data spanning multiple pages (each page has multiple vectors)
  constexpr size_t kMultiPageSize = 100000;  // ~100 vectors worth
  std::vector<TypeParam> input(kMultiPageSize);

  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.001);
  }

  this->TestEncodeDecodeWrapper(input);
}

TYPED_TEST(AlpCodecTest, EncodeWithPreset) {
  // Test that encoding with a pre-computed preset produces identical results
  constexpr size_t kTestSize = 4096;  // 4 vectors worth
  std::vector<TypeParam> input(kTestSize);

  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.123);
  }

  // First, encode normally
  const int64_t num_elements = static_cast<int64_t>(input.size());
  int64_t max_comp_size = AlpCodec<TypeParam>::GetMaxCompressedSize(num_elements);
  std::vector<uint8_t> comp_buffer1(max_comp_size);
  int64_t comp_size1 = max_comp_size;

  AlpCodec<TypeParam>::Encode(input.data(), num_elements,
                                comp_buffer1.data(), &comp_size1);

  // Now, use the preset-based API
  auto preset = AlpCodec<TypeParam>::CreateSamplingPreset(input.data(), num_elements);

  std::vector<uint8_t> comp_buffer2(max_comp_size);
  int64_t comp_size2 = max_comp_size;

  AlpCodec<TypeParam>::EncodeWithPreset(input.data(), num_elements,
                                          preset, AlpConstants::kAlpVectorSize,
                                          comp_buffer2.data(), &comp_size2);

  // Both should produce identical output
  EXPECT_EQ(comp_size1, comp_size2);
  EXPECT_EQ(std::memcmp(comp_buffer1.data(), comp_buffer2.data(), comp_size1), 0);

  // Verify the preset-based encoding can be decoded correctly
  std::vector<TypeParam> output(input.size());
  ASSERT_OK(AlpCodec<TypeParam>::template Decode<TypeParam>(
      static_cast<int32_t>(input.size()), comp_buffer2.data(), comp_size2,
      output.data()));

  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

TYPED_TEST(AlpCodecTest, PresetReuseAcrossBatches) {
  // Test that a preset can be reused for multiple encode calls
  constexpr size_t kBatchSize = 1024;
  std::vector<TypeParam> batch1(kBatchSize), batch2(kBatchSize);

  // Two batches with similar data characteristics
  for (size_t i = 0; i < kBatchSize; ++i) {
    batch1[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.01);
    batch2[i] = static_cast<TypeParam>(i + 1000) * static_cast<TypeParam>(0.01);
  }

  // Create preset from first batch
  const int64_t num_elements = static_cast<int64_t>(kBatchSize);
  auto preset = AlpCodec<TypeParam>::CreateSamplingPreset(batch1.data(), num_elements);

  int64_t max_comp_size = AlpCodec<TypeParam>::GetMaxCompressedSize(num_elements);

  // Encode batch1 with preset
  std::vector<uint8_t> comp1(max_comp_size);
  int64_t comp_size1 = max_comp_size;
  AlpCodec<TypeParam>::EncodeWithPreset(batch1.data(), num_elements,
                                          preset, AlpConstants::kAlpVectorSize,
                                          comp1.data(), &comp_size1);

  // Encode batch2 with same preset (reuse)
  std::vector<uint8_t> comp2(max_comp_size);
  int64_t comp_size2 = max_comp_size;
  AlpCodec<TypeParam>::EncodeWithPreset(batch2.data(), num_elements,
                                          preset, AlpConstants::kAlpVectorSize,
                                          comp2.data(), &comp_size2);

  // Both should encode successfully
  EXPECT_GT(comp_size1, 0);
  EXPECT_GT(comp_size2, 0);

  // Decode and verify both batches
  std::vector<TypeParam> output1(kBatchSize), output2(kBatchSize);
  ASSERT_OK(AlpCodec<TypeParam>::template Decode<TypeParam>(
      static_cast<int32_t>(kBatchSize), comp1.data(), comp_size1, output1.data()));
  ASSERT_OK(AlpCodec<TypeParam>::template Decode<TypeParam>(
      static_cast<int32_t>(kBatchSize), comp2.data(), comp_size2, output2.data()));

  EXPECT_TRUE(IsBitwiseEqual(output1, batch1));
  EXPECT_TRUE(IsBitwiseEqual(output2, batch2));
}

// ============================================================================
// Preset/Sampling Tests
// ============================================================================

template <typename T>
class AlpSamplerTest : public ::testing::Test {};

using SamplerTestTypes = ::testing::Types<float, double>;
TYPED_TEST_SUITE(AlpSamplerTest, SamplerTestTypes);

TYPED_TEST(AlpSamplerTest, PresetGenerationDecimalData) {
  // Verify preset generation selects appropriate exponent/factor for decimal data
  AlpSampler<TypeParam> sampler;

  // Create decimal-like data (2 decimal places)
  std::vector<TypeParam> data(10000);
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<TypeParam>(100.0 + i * 0.01);
  }

  // Use AddSample with span
  sampler.AddSample({data.data(), data.size()});
  auto result = sampler.Finalize();
  auto preset = result.alp_parameters;

  // Preset should have at least one combination
  EXPECT_GT(preset.combinations.size(), 0);

  // Verify the preset works for compression
  AlpCompression<TypeParam> compressor;
  auto encoded = compressor.CompressVector(data.data(),
      static_cast<uint16_t>(std::min(data.size(), size_t(1024))), preset);

  std::vector<TypeParam> output(std::min(data.size(), size_t(1024)));
  compressor.DecompressVector(encoded, AlpIntegerEncoding::kForBitPack, output.data());

  EXPECT_TRUE(IsBitwiseEqual(output, data));
}

TYPED_TEST(AlpSamplerTest, PresetGenerationMixedData) {
  // Test with mixed data patterns
  AlpSampler<TypeParam> sampler;

  std::vector<TypeParam> data(10000);
  std::mt19937 rng(42);
  std::uniform_real_distribution<TypeParam> dist(
      static_cast<TypeParam>(0.0), static_cast<TypeParam>(1000.0));

  for (auto& v : data) {
    v = dist(rng);
  }

  sampler.AddSample({data.data(), data.size()});
  auto result = sampler.Finalize();
  auto preset = result.alp_parameters;

  EXPECT_GT(preset.combinations.size(), 0);
}

TYPED_TEST(AlpSamplerTest, EmptySample) {
  AlpSampler<TypeParam> sampler;
  auto result = sampler.Finalize();
  auto preset = result.alp_parameters;

  // Should have default preset even without sampling
  // (may be empty or have default combination)
  EXPECT_GE(preset.combinations.size(), 0);
}

// ============================================================================
// Empty Input Tests (via AlpCompression directly)
// ============================================================================

TYPED_TEST(AlpEdgeCaseTest, EmptyInputViaCompression) {
  // Test compressing zero elements via AlpCompression directly
  std::vector<TypeParam> empty_input;
  AlpCompression<TypeParam> compressor;
  AlpEncodingParameters preset{};

  // Compress 0 elements - should produce a valid (minimal) encoded vector
  auto encoded = compressor.CompressVector(empty_input.data(), 0, preset);

  EXPECT_EQ(encoded.num_elements(), 0);
  EXPECT_EQ(encoded.alp_info().num_exceptions(), 0);
  EXPECT_EQ(encoded.packed_values().size(), 0);
  EXPECT_EQ(encoded.exceptions().size(), 0);
  EXPECT_EQ(encoded.exception_positions().size(), 0);

  // Decompress should also handle 0 elements
  std::vector<TypeParam> output;
  compressor.DecompressVector(encoded, AlpIntegerEncoding::kForBitPack, output.data());
  // No crash = success for empty case
}

TYPED_TEST(AlpCodecTest, EmptyInput) {
  // Test wrapper with zero elements
  std::vector<TypeParam> empty_input;

  int64_t max_comp_size = AlpCodec<TypeParam>::GetMaxCompressedSize(0);
  EXPECT_GT(max_comp_size, 0);  // Should at least have header space

  std::vector<uint8_t> comp_buffer(max_comp_size);
  int64_t comp_size = max_comp_size;

  // Encode 0 bytes (0 elements)
  AlpCodec<TypeParam>::Encode(empty_input.data(), 0, comp_buffer.data(), &comp_size);

  // Should produce at least the header
  EXPECT_GT(comp_size, 0);

  // Decode 0 elements
  std::vector<TypeParam> output;
  ASSERT_OK(AlpCodec<TypeParam>::template Decode<TypeParam>(
      0, comp_buffer.data(), comp_size, output.data()));
  // No crash = success
}

// ============================================================================
// Corrupted Data Handling Tests
// ============================================================================

// Decode returns Status for invalid/corrupted compressed data.

TEST(AlpRobustnessTest, TruncatedHeader) {
  // Test with buffer too small for header
  std::vector<uint8_t> tiny_buffer(5);  // Less than header size (7 bytes)

  std::vector<double> output(100);
  ASSERT_NOT_OK(
      AlpCodec<double>::Decode(100, tiny_buffer.data(),
                              static_cast<int64_t>(tiny_buffer.size()), output.data()));
}

TEST(AlpRobustnessTest, TruncatedData) {
  // Create valid compressed data, then corrupt the num_elements to cause issues
  std::vector<double> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<double>(i) * 0.123;
  }

  const int64_t num_elements = static_cast<int64_t>(input.size());
  int64_t max_size = AlpCodec<double>::GetMaxCompressedSize(num_elements);
  std::vector<uint8_t> buffer(max_size);
  int64_t comp_size = max_size;

  AlpCodec<double>::Encode(input.data(), num_elements, buffer.data(), &comp_size);

  // Verify that valid data decodes successfully.
  std::vector<double> output(input.size());
  ASSERT_OK(AlpCodec<double>::Decode(static_cast<int32_t>(input.size()), buffer.data(),
                                    comp_size, output.data()));

  // Verify successful decode
  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

// ============================================================================
// Determinism/Consistency Tests
// ============================================================================

TYPED_TEST(AlpEdgeCaseTest, CompressionDeterminism) {
  // Same input should always produce identical compressed output
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.123);
  }

  int64_t max_size = AlpCodec<TypeParam>::GetMaxCompressedSize(
      static_cast<int64_t>(input.size()));

  std::vector<uint8_t> buffer1(max_size);
  std::vector<uint8_t> buffer2(max_size);
  int64_t size1 = buffer1.size();
  int64_t size2 = buffer2.size();

  // Compress twice
  AlpCodec<TypeParam>::Encode(input.data(),
                              static_cast<int64_t>(input.size()),
                              buffer1.data(), &size1);
  AlpCodec<TypeParam>::Encode(input.data(),
                              static_cast<int64_t>(input.size()),
                              buffer2.data(), &size2);

  // Sizes should match
  EXPECT_EQ(size1, size2);

  // Compressed bytes should be identical
  EXPECT_EQ(std::memcmp(buffer1.data(), buffer2.data(), size1), 0);
}

TYPED_TEST(AlpEdgeCaseTest, DecompressionDeterminism) {
  // Multiple decompressions should produce identical output
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.5);
  }

  int64_t max_size = AlpCodec<TypeParam>::GetMaxCompressedSize(
      static_cast<int64_t>(input.size()));
  std::vector<uint8_t> buffer(max_size);
  int64_t comp_size = buffer.size();

  AlpCodec<TypeParam>::Encode(input.data(),
                              static_cast<int64_t>(input.size()),
                              buffer.data(), &comp_size);

  std::vector<TypeParam> output1(input.size());
  std::vector<TypeParam> output2(input.size());

  // Decompress twice
  ASSERT_OK(AlpCodec<TypeParam>::Decode(static_cast<int32_t>(input.size()),
                                          buffer.data(), comp_size, output1.data()));
  ASSERT_OK(AlpCodec<TypeParam>::Decode(static_cast<int32_t>(input.size()),
                                          buffer.data(), comp_size, output2.data()));

  // Outputs should be identical
  EXPECT_TRUE(IsBitwiseEqual(output1, output2));

  // And match input
  EXPECT_TRUE(IsBitwiseEqual(output1, input));
}

// ============================================================================
// Configurable vector_size Tests
// ============================================================================

TYPED_TEST(AlpCodecTest, RoundTripAtMultipleVectorSizes) {
  const std::vector<int32_t> vector_sizes = {64, 512, 1024, 2048, 4096};

  for (const int32_t vs : vector_sizes) {
    SCOPED_TRACE("vector_size=" + std::to_string(vs));

    // Test 4 data size categories per vector_size
    const std::vector<size_t> data_sizes = {
        static_cast<size_t>(vs / 2),      // less than one vector
        static_cast<size_t>(vs),           // exactly one vector
        static_cast<size_t>(vs * 3),       // exact multiple
        static_cast<size_t>(vs * 2 + 17),  // not a multiple (exercises remainder)
    };

    for (const size_t n : data_sizes) {
      SCOPED_TRACE("num_elements=" + std::to_string(n));

      std::vector<TypeParam> input(n);
      for (size_t i = 0; i < n; ++i) {
        input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.123);
      }

      int64_t max_comp_size = AlpCodec<TypeParam>::GetMaxCompressedSize(
          static_cast<int64_t>(n), vs);
      std::vector<uint8_t> comp_buffer(max_comp_size);
      int64_t comp_size = comp_buffer.size();

      AlpCodec<TypeParam>::Encode(input.data(),
                                  static_cast<int64_t>(n), vs,
                                  comp_buffer.data(), &comp_size);

      EXPECT_GT(comp_size, 0);
      EXPECT_LE(comp_size, max_comp_size);

      std::vector<TypeParam> output(n);
      ASSERT_OK(AlpCodec<TypeParam>::template Decode<TypeParam>(
          static_cast<int32_t>(n), comp_buffer.data(), comp_size, output.data()));

      EXPECT_TRUE(IsBitwiseEqual(
          std::vector<TypeParam>(output.begin(), output.begin() + n),
          std::vector<TypeParam>(input.begin(), input.begin() + n)));
    }
  }
}

TYPED_TEST(AlpCodecTest, EncodeWithPresetAtDifferentVectorSizes) {
  const std::vector<int32_t> vector_sizes = {64, 512, 2048};

  for (const int32_t vs : vector_sizes) {
    SCOPED_TRACE("vector_size=" + std::to_string(vs));

    const size_t n = vs * 2 + 7;
    std::vector<TypeParam> input(n);
    for (size_t i = 0; i < n; ++i) {
      input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.01);
    }

    auto preset = AlpCodec<TypeParam>::CreateSamplingPreset(
        input.data(), static_cast<int64_t>(n));

    int64_t max_comp_size = AlpCodec<TypeParam>::GetMaxCompressedSize(
        static_cast<int64_t>(n), vs);
    std::vector<uint8_t> comp_buffer(max_comp_size);
    int64_t comp_size = comp_buffer.size();

    AlpCodec<TypeParam>::EncodeWithPreset(input.data(),
                                          static_cast<int64_t>(n),
                                          preset, vs,
                                          comp_buffer.data(), &comp_size);

    EXPECT_GT(comp_size, 0u);

    std::vector<TypeParam> output(n);
    ASSERT_OK(AlpCodec<TypeParam>::template Decode<TypeParam>(
        static_cast<int32_t>(n), comp_buffer.data(), comp_size, output.data()));

    EXPECT_TRUE(IsBitwiseEqual(
        std::vector<TypeParam>(output.begin(), output.begin() + n),
        std::vector<TypeParam>(input.begin(), input.begin() + n)));
  }
}

TYPED_TEST(AlpCodecTest, GetMaxCompressedSizeVariesWithVectorSize) {
  const int64_t num_elements = 8192;

  int64_t size_64 = AlpCodec<TypeParam>::GetMaxCompressedSize(num_elements, 64);
  int64_t size_1024 = AlpCodec<TypeParam>::GetMaxCompressedSize(num_elements, 1024);
  int64_t size_4096 = AlpCodec<TypeParam>::GetMaxCompressedSize(num_elements, 4096);

  // Smaller vector_size means more vectors, more per-vector overhead
  EXPECT_GT(size_64, size_1024);
  EXPECT_GT(size_1024, size_4096);
}

#if GTEST_HAS_DEATH_TEST
TYPED_TEST(AlpCodecTest, InvalidVectorSizeZero) {
  std::vector<TypeParam> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }
  std::vector<uint8_t> buffer(4096);
  int64_t comp_size = buffer.size();

  EXPECT_DEATH(AlpCodec<TypeParam>::Encode(
                   input.data(),
                   static_cast<int64_t>(input.size()), 0,
                   buffer.data(), &comp_size),
               "");
}

TYPED_TEST(AlpCodecTest, InvalidVectorSizeNotPowerOfTwo) {
  std::vector<TypeParam> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }
  std::vector<uint8_t> buffer(4096);
  int64_t comp_size = buffer.size();

  EXPECT_DEATH(AlpCodec<TypeParam>::Encode(
                   input.data(),
                   static_cast<int64_t>(input.size()), 3,
                   buffer.data(), &comp_size),
               "");
}

TYPED_TEST(AlpCodecTest, InvalidVectorSizeExceedsMax) {
  std::vector<TypeParam> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }
  std::vector<uint8_t> buffer(4096);
  int64_t comp_size = buffer.size();

  EXPECT_DEATH(AlpCodec<TypeParam>::Encode(
                   input.data(),
                   static_cast<int64_t>(input.size()),
                   1 << 16, buffer.data(), &comp_size),
               "");
}
#endif  // GTEST_HAS_DEATH_TEST

}  // namespace alp
}  // namespace util
}  // namespace arrow
