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
#include "arrow/util/alp/alp_wrapper.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bpacking_internal.h"

namespace arrow {
namespace util {
namespace alp {

// ============================================================================
// ALP Constants Tests
// ============================================================================

TEST(AlpConstantsTest, SamplerConstants) {
  EXPECT_GT(AlpConstants::kSamplerVectorSize, 0);
  EXPECT_GT(AlpConstants::kSamplerRowgroupSize, 0);
  EXPECT_GT(AlpConstants::kSamplerSamplesPerVector, 0);
  EXPECT_EQ(AlpConstants::kAlpVersion, 1);
}

// ============================================================================
// ALP Compression Tests (Float)
// ============================================================================

class AlpCompressionFloatTest : public ::testing::Test {
 protected:
  void TestCompressDecompressFloat(const std::vector<float>& input) {
    AlpCompression<float> compressor;

    // Compress
    AlpEncodingPreset preset{};  // Default preset
    auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

    // Decompress
    std::vector<float> output(input.size());
    compressor.DecompressVector(encoded, AlpBitPackLayout::kNormal, output.data());

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
    AlpEncodingPreset preset{};  // Default preset
    auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

    // Decompress
    std::vector<double> output(input.size());
    compressor.DecompressVector(encoded, AlpBitPackLayout::kNormal, output.data());

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
  AlpEncodingPreset preset{};
  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  std::vector<float> output(input.size());
  compressor.DecompressVector(encoded, AlpBitPackLayout::kNormal, output.data());

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
  AlpEncodingPreset preset{};
  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  std::vector<double> output(input.size());
  compressor.DecompressVector(encoded, AlpBitPackLayout::kNormal, output.data());

  for (size_t i = 0; i < input.size(); ++i) {
    EXPECT_DOUBLE_EQ(output[i], input[i]);
  }
}

// ============================================================================
// AlpEncodedVectorInfo Serialization Tests
// ============================================================================

TEST(AlpEncodedVectorInfoTest, StoreLoadRoundTrip) {
  AlpEncodedVectorInfo info{};
  info.frame_of_reference = 0x123456789ABCDEF0ULL;
  info.exponent_and_factor = {5, 3};
  info.bit_width = 12;
  info.reserved = 0;
  info.num_elements = 1024;
  info.num_exceptions = 10;
  info.bit_packed_size = 1536;

  std::vector<char> buffer(AlpEncodedVectorInfo::GetStoredSize() + 10);
  info.Store({buffer.data(), buffer.size()});

  AlpEncodedVectorInfo loaded =
      AlpEncodedVectorInfo::Load({buffer.data(), buffer.size()});
  EXPECT_EQ(info, loaded);
  EXPECT_EQ(loaded.frame_of_reference, 0x123456789ABCDEF0ULL);
  EXPECT_EQ(loaded.exponent_and_factor.exponent, 5);
  EXPECT_EQ(loaded.exponent_and_factor.factor, 3);
  EXPECT_EQ(loaded.bit_width, 12);
  EXPECT_EQ(loaded.num_elements, 1024);
  EXPECT_EQ(loaded.num_exceptions, 10);
  EXPECT_EQ(loaded.bit_packed_size, 1536);
}

TEST(AlpEncodedVectorInfoTest, Size) {
  // Verify the stored size is as expected (18 bytes data + padding)
  EXPECT_EQ(AlpEncodedVectorInfo::GetStoredSize(), sizeof(AlpEncodedVectorInfo));
}

// ============================================================================
// Edge Case Tests
// ============================================================================

template <typename T>
class AlpEdgeCaseTest : public ::testing::Test {
 protected:
  void TestCompressDecompress(const std::vector<T>& input) {
    AlpCompression<T> compressor;
    AlpEncodingPreset preset{};
    auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

    std::vector<T> output(input.size());
    compressor.DecompressVector(encoded, AlpBitPackLayout::kNormal, output.data());

    ASSERT_EQ(output.size(), input.size());
    // Use memcmp for bit-exact comparison (important for -0.0, NaN)
    EXPECT_EQ(std::memcmp(output.data(), input.data(), input.size() * sizeof(T)),
              0);
  }
};

using EdgeCaseTestTypes = ::testing::Types<float, double>;
TYPED_TEST_SUITE(AlpEdgeCaseTest, EdgeCaseTestTypes);

TYPED_TEST(AlpEdgeCaseTest, SingleElement) {
  std::vector<TypeParam> input = {static_cast<TypeParam>(42.5)};
  this->TestCompressDecompress(input);
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
  AlpEncodingPreset preset{};

  // Process first vector
  auto encoded1 = compressor.CompressVector(input.data(),
                                            AlpConstants::kAlpVectorSize, preset);
  std::vector<TypeParam> output1(AlpConstants::kAlpVectorSize);
  compressor.DecompressVector(encoded1, AlpBitPackLayout::kNormal, output1.data());

  // Process remaining element
  auto encoded2 = compressor.CompressVector(
      input.data() + AlpConstants::kAlpVectorSize, 1, preset);
  std::vector<TypeParam> output2(1);
  compressor.DecompressVector(encoded2, AlpBitPackLayout::kNormal, output2.data());

  // Verify
  EXPECT_EQ(std::memcmp(output1.data(), input.data(),
                        AlpConstants::kAlpVectorSize * sizeof(TypeParam)),
            0);
  EXPECT_EQ(std::memcmp(output2.data(),
                        input.data() + AlpConstants::kAlpVectorSize,
                        sizeof(TypeParam)),
            0);
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
  AlpEncodingPreset preset{};

  std::vector<TypeParam> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.5);
  }

  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  // Store
  std::vector<char> buffer(encoded.GetStoredSize());
  encoded.Store({buffer.data(), buffer.size()});

  // Load
  auto loaded =
      AlpEncodedVector<TypeParam>::Load({buffer.data(), buffer.size()});

  // Verify metadata
  EXPECT_EQ(encoded.vector_info, loaded.vector_info);

  // Decompress loaded and verify
  std::vector<TypeParam> output(input.size());
  compressor.DecompressVector(loaded, AlpBitPackLayout::kNormal, output.data());

  EXPECT_EQ(std::memcmp(output.data(), input.data(), input.size() * sizeof(TypeParam)),
            0);
}

TYPED_TEST(AlpEncodedVectorTest, GetStoredSizeConsistency) {
  AlpCompression<TypeParam> compressor;
  AlpEncodingPreset preset{};

  std::vector<TypeParam> input(128);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.25);
  }

  auto encoded = compressor.CompressVector(input.data(), input.size(), preset);

  // Verify GetStoredSize matches actual storage
  std::vector<char> buffer(encoded.GetStoredSize());
  encoded.Store({buffer.data(), buffer.size()});

  EXPECT_EQ(buffer.size(), encoded.GetStoredSize());
}

// ============================================================================
// AlpWrapper Tests
// ============================================================================

template <typename T>
class AlpWrapperTest : public ::testing::Test {
 protected:
  void TestEncodeDecodeWrapper(const std::vector<T>& input) {
    // Get max compressed size
    uint64_t max_comp_size =
        AlpWrapper<T>::GetMaxCompressedSize(input.size() * sizeof(T));
    std::vector<char> comp_buffer(max_comp_size);

    // Encode
    size_t comp_size = comp_buffer.size();
    AlpWrapper<T>::Encode(input.data(), input.size() * sizeof(T),
                          comp_buffer.data(), &comp_size);

    EXPECT_GT(comp_size, 0);
    EXPECT_LE(comp_size, max_comp_size);

    // Decode
    std::vector<T> output(input.size());
    AlpWrapper<T>::template Decode<T>(output.data(), input.size(),
                                      comp_buffer.data(), comp_size);

    // Verify
    EXPECT_EQ(std::memcmp(output.data(), input.data(), input.size() * sizeof(T)),
              0);
  }
};

TYPED_TEST_SUITE(AlpWrapperTest, EdgeCaseTestTypes);

TYPED_TEST(AlpWrapperTest, SimpleSequence) {
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }
  this->TestEncodeDecodeWrapper(input);
}

TYPED_TEST(AlpWrapperTest, MultipleVectors) {
  // Test with multiple vectors worth of data
  std::vector<TypeParam> input(3 * AlpConstants::kAlpVectorSize);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.01);
  }
  this->TestEncodeDecodeWrapper(input);
}

TYPED_TEST(AlpWrapperTest, SpecialValues) {
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

TYPED_TEST(AlpWrapperTest, GetMaxCompressedSizeAdequate) {
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

    uint64_t max_comp_size =
        AlpWrapper<TypeParam>::GetMaxCompressedSize(size * sizeof(TypeParam));
    std::vector<char> comp_buffer(max_comp_size);
    size_t comp_size = comp_buffer.size();

    AlpWrapper<TypeParam>::Encode(input.data(), size * sizeof(TypeParam),
                                  comp_buffer.data(), &comp_size);

    EXPECT_LE(comp_size, max_comp_size)
        << "Compressed size exceeded max for " << size << " elements";
    EXPECT_GT(comp_size, 0)
        << "Compression produced 0 bytes for " << size << " elements";
  }
}

TYPED_TEST(AlpWrapperTest, WideningDecode) {
  // Test decoding float data to double (widening conversion)
  if constexpr (std::is_same_v<TypeParam, float>) {
    std::vector<float> input(256);
    for (size_t i = 0; i < input.size(); ++i) {
      input[i] = static_cast<float>(i) * 0.5f;
    }

    uint64_t max_comp_size =
        AlpWrapper<float>::GetMaxCompressedSize(input.size() * sizeof(float));
    std::vector<char> comp_buffer(max_comp_size);
    size_t comp_size = comp_buffer.size();

    AlpWrapper<float>::Encode(input.data(), input.size() * sizeof(float),
                              comp_buffer.data(), &comp_size);

    // Decode as double
    std::vector<double> output(input.size());
    AlpWrapper<float>::template Decode<double>(output.data(), input.size(),
                                               comp_buffer.data(), comp_size);

    // Verify values match (as double)
    for (size_t i = 0; i < input.size(); ++i) {
      EXPECT_DOUBLE_EQ(output[i], static_cast<double>(input[i]));
    }
  }
}

}  // namespace alp
}  // namespace util
}  // namespace arrow
