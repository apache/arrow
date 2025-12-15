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

}  // namespace alp
}  // namespace util
}  // namespace arrow
