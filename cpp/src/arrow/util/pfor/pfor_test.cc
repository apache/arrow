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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <numeric>
#include <random>
#include <span>
#include <vector>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/fastlanes/fastlanes_kernels.h"
#include "arrow/util/pfor/pfor.h"
#include "arrow/util/pfor/pfor_wrapper.h"

namespace arrow::util::pfor {

// ======================================================================
// Constants Tests

TEST(PforConstantsTest, VectorSizeIsPowerOfTwo) {
  EXPECT_EQ(PforConstants::kPforVectorSize, 1024);
  EXPECT_EQ(1 << PforConstants::kDefaultLogVectorSize,
            PforConstants::kPforVectorSize);
}

TEST(PforConstantsTest, VectorInfoSizes) {
  EXPECT_EQ(PforTypeTraits<int32_t>::kVectorInfoSize, 7);
  EXPECT_EQ(PforTypeTraits<int64_t>::kVectorInfoSize, 11);
}

// ======================================================================
// BitsRequired Tests

TEST(PforBitsRequiredTest, Int32) {
  EXPECT_EQ(PforTypeTraits<int32_t>::BitsRequired(0), 0);
  EXPECT_EQ(PforTypeTraits<int32_t>::BitsRequired(1), 1);
  EXPECT_EQ(PforTypeTraits<int32_t>::BitsRequired(2), 2);
  EXPECT_EQ(PforTypeTraits<int32_t>::BitsRequired(3), 2);
  EXPECT_EQ(PforTypeTraits<int32_t>::BitsRequired(255), 8);
  EXPECT_EQ(PforTypeTraits<int32_t>::BitsRequired(256), 9);
  EXPECT_EQ(PforTypeTraits<int32_t>::BitsRequired(0xFFFFFFFF), 32);
}

TEST(PforBitsRequiredTest, Int64) {
  EXPECT_EQ(PforTypeTraits<int64_t>::BitsRequired(0), 0);
  EXPECT_EQ(PforTypeTraits<int64_t>::BitsRequired(1), 1);
  EXPECT_EQ(PforTypeTraits<int64_t>::BitsRequired(0xFFFFFFFFFFFFFFFFULL), 64);
}

// ======================================================================
// VectorInfo Serialization Tests

TEST(PforVectorInfoTest, Int32RoundTrip) {
  PforVectorInfo<int32_t> info;
  info.set_frame_of_reference(-42);
  info.set_bit_width(17);
  info.set_num_exceptions(300);

  uint8_t buf[7];
  info.Store(std::span<uint8_t>(buf, 7));
  ASSERT_OK_AND_ASSIGN(auto loaded,
                       PforVectorInfo<int32_t>::Load(std::span<const uint8_t>(buf, 7)));

  EXPECT_EQ(loaded.frame_of_reference(), -42);
  EXPECT_EQ(loaded.bit_width(), 17);
  EXPECT_EQ(loaded.num_exceptions(), 300);
}

TEST(PforVectorInfoTest, Int64RoundTrip) {
  PforVectorInfo<int64_t> info;
  info.set_frame_of_reference(-123456789012345LL);
  info.set_bit_width(48);
  info.set_num_exceptions(30000);

  uint8_t buf[11];
  info.Store(std::span<uint8_t>(buf, 11));
  ASSERT_OK_AND_ASSIGN(auto loaded,
                       PforVectorInfo<int64_t>::Load(std::span<const uint8_t>(buf, 11)));

  EXPECT_EQ(loaded.frame_of_reference(), -123456789012345LL);
  EXPECT_EQ(loaded.bit_width(), 48);
  EXPECT_EQ(loaded.num_exceptions(), 30000);
}

// ======================================================================
// Cost Model Tests

TEST(PforCostModelTest, AllIdentical) {
  // All deltas are 0 => bit_width should be 0, no exceptions
  std::vector<uint32_t> deltas(100, 0);
  auto result = PforCompression<int32_t>::FindOptimalBitWidth(deltas.data(), 100);  // NOLINT
  EXPECT_EQ(result.bit_width, 0);
  EXPECT_EQ(result.num_exceptions, 0);
}

TEST(PforCostModelTest, SingleOutlier) {
  // 99 values fit in 3 bits, 1 outlier needs 16 bits
  std::vector<uint32_t> deltas(100, 5);  // all fit in 3 bits
  deltas[50] = 50000;                    // outlier: 16 bits
  auto result = PforCompression<int32_t>::FindOptimalBitWidth(deltas.data(), 100);
  // Cost at bit_width=3: 100*3 + 1*(16+32) = 300 + 48 = 348
  // Cost at bit_width=16: 100*16 + 0 = 1600
  // 348 < 1600, so should pick 3 with 1 exception
  EXPECT_EQ(result.bit_width, 3);
  EXPECT_EQ(result.num_exceptions, 1);
}

TEST(PforCostModelTest, NoOutliers) {
  // All values fit in 8 bits
  std::vector<uint32_t> deltas(100);
  for (int32_t i = 0; i < 100; ++i) deltas[i] = i * 2;
  auto result = PforCompression<int32_t>::FindOptimalBitWidth(deltas.data(), 100);
  EXPECT_EQ(result.num_exceptions, 0);
  EXPECT_LE(result.bit_width, 8);
}

// ======================================================================
// Vector Encode/Decode Round-Trip Tests

TEST(PforVectorTest, Int32SimpleSequence) {
  std::vector<int32_t> values(64);
  std::iota(values.begin(), values.end(), 100);

  auto encoded = PforCompression<int32_t>::EncodeVector(values.data(), 64);
  EXPECT_EQ(encoded.info().frame_of_reference(), 100);
  EXPECT_EQ(encoded.info().num_exceptions(), 0);

  // Serialize then decode
  size_t serialized_size =
      PforCompression<int32_t>::SerializedVectorSize(encoded, 64);
  std::vector<uint8_t> buffer(serialized_size);
  PforCompression<int32_t>::SerializeVector(encoded, 64, buffer);

  std::vector<int32_t> decoded(64);
  ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 64));

  EXPECT_EQ(values, decoded);
}

TEST(PforVectorTest, Int32WithOutlier) {
  std::vector<int32_t> values = {100, 102, 101, 103, 100, 99, 50000, 104};

  auto encoded = PforCompression<int32_t>::EncodeVector(values.data(), 8);
  EXPECT_EQ(encoded.info().frame_of_reference(), 99);
  EXPECT_GT(encoded.info().num_exceptions(), 0);

  size_t serialized_size =
      PforCompression<int32_t>::SerializedVectorSize(encoded, 8);
  std::vector<uint8_t> buffer(serialized_size);
  PforCompression<int32_t>::SerializeVector(encoded, 8, buffer);

  std::vector<int32_t> decoded(8);
  ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 8));

  EXPECT_EQ(values, decoded);
}

TEST(PforVectorTest, Int32AllIdentical) {
  std::vector<int32_t> values(100, 42);

  auto encoded = PforCompression<int32_t>::EncodeVector(values.data(), 100);
  EXPECT_EQ(encoded.info().bit_width(), 0);
  EXPECT_EQ(encoded.info().num_exceptions(), 0);

  size_t serialized_size =
      PforCompression<int32_t>::SerializedVectorSize(encoded, 100);
  std::vector<uint8_t> buffer(serialized_size);
  PforCompression<int32_t>::SerializeVector(encoded, 100, buffer);

  std::vector<int32_t> decoded(100);
  ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 100));

  EXPECT_EQ(values, decoded);
}

TEST(PforVectorTest, Int32NegativeValues) {
  std::vector<int32_t> values = {-100, -50, -200, -1, -150};

  auto encoded = PforCompression<int32_t>::EncodeVector(values.data(), 5);
  EXPECT_EQ(encoded.info().frame_of_reference(), -200);

  size_t serialized_size =
      PforCompression<int32_t>::SerializedVectorSize(encoded, 5);
  std::vector<uint8_t> buffer(serialized_size);
  PforCompression<int32_t>::SerializeVector(encoded, 5, buffer);

  std::vector<int32_t> decoded(5);
  ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 5));

  EXPECT_EQ(values, decoded);
}

TEST(PforVectorTest, Int32MinMaxEdge) {
  std::vector<int32_t> values = {std::numeric_limits<int32_t>::min(),
                                  std::numeric_limits<int32_t>::max(), 0, -1, 1};

  auto encoded = PforCompression<int32_t>::EncodeVector(values.data(), 5);

  size_t serialized_size =
      PforCompression<int32_t>::SerializedVectorSize(encoded, 5);
  std::vector<uint8_t> buffer(serialized_size);
  PforCompression<int32_t>::SerializeVector(encoded, 5, buffer);

  std::vector<int32_t> decoded(5);
  ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 5));

  EXPECT_EQ(values, decoded);
}

TEST(PforVectorTest, Int64SimpleSequence) {
  std::vector<int64_t> values(64);
  std::iota(values.begin(), values.end(), 1000000000LL);

  auto encoded = PforCompression<int64_t>::EncodeVector(values.data(), 64);

  size_t serialized_size =
      PforCompression<int64_t>::SerializedVectorSize(encoded, 64);
  std::vector<uint8_t> buffer(serialized_size);
  PforCompression<int64_t>::SerializeVector(encoded, 64, buffer);

  std::vector<int64_t> decoded(64);
  ASSERT_OK(PforCompression<int64_t>::DecodeVector(decoded.data(), buffer, 64));

  EXPECT_EQ(values, decoded);
}

TEST(PforVectorTest, Int64WithOutlier) {
  std::vector<int64_t> values(100, 5000000LL);
  values[42] = 999999999999LL;  // Outlier

  auto encoded = PforCompression<int64_t>::EncodeVector(values.data(), 100);
  EXPECT_GT(encoded.info().num_exceptions(), 0);

  size_t serialized_size =
      PforCompression<int64_t>::SerializedVectorSize(encoded, 100);
  std::vector<uint8_t> buffer(serialized_size);
  PforCompression<int64_t>::SerializeVector(encoded, 100, buffer);

  std::vector<int64_t> decoded(100);
  ASSERT_OK(PforCompression<int64_t>::DecodeVector(decoded.data(), buffer, 100));

  EXPECT_EQ(values, decoded);
}

// ======================================================================
// Page-Level Wrapper Tests

TEST(PforWrapperTest, Int32SmallPage) {
  std::vector<int32_t> values = {10, 20, 30, 40, 50};

  int64_t max_size = PforWrapper<int32_t>::GetMaxCompressedSize(5);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int32_t>::Encode(values.data(), 5, compressed.data(), &comp_size);
  EXPECT_GT(comp_size, 0);

  std::vector<int32_t> decoded(5);
  ASSERT_OK(PforWrapper<int32_t>::Decode(decoded.data(), 5, compressed.data(), comp_size));

  EXPECT_EQ(values, decoded);
}

TEST(PforWrapperTest, Int32ExactOneVector) {
  std::vector<int32_t> values(1024);
  std::iota(values.begin(), values.end(), 0);

  int64_t max_size = PforWrapper<int32_t>::GetMaxCompressedSize(1024);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int32_t>::Encode(values.data(), 1024, compressed.data(), &comp_size);

  std::vector<int32_t> decoded(1024);
  ASSERT_OK(PforWrapper<int32_t>::Decode(decoded.data(), 1024, compressed.data(), comp_size));

  EXPECT_EQ(values, decoded);
}

TEST(PforWrapperTest, Int32MultipleVectors) {
  // 2.5 vectors worth of data
  const int32_t n = 2560;
  std::vector<int32_t> values(n);
  std::mt19937 rng(42);
  std::uniform_int_distribution<int32_t> dist(0, 1000);
  for (auto& v : values) v = dist(rng);

  int64_t max_size = PforWrapper<int32_t>::GetMaxCompressedSize(n);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int32_t>::Encode(values.data(), n, compressed.data(), &comp_size);

  std::vector<int32_t> decoded(n);
  ASSERT_OK(PforWrapper<int32_t>::Decode(decoded.data(), n, compressed.data(), comp_size));

  EXPECT_EQ(values, decoded);
}

TEST(PforWrapperTest, Int32WithOutliers) {
  std::vector<int32_t> values(1024, 100);
  // Sprinkle outliers
  values[0] = -999999;
  values[100] = 888888;
  values[500] = 777777;
  values[1023] = -123456;

  int64_t max_size = PforWrapper<int32_t>::GetMaxCompressedSize(1024);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int32_t>::Encode(values.data(), 1024, compressed.data(), &comp_size);

  std::vector<int32_t> decoded(1024);
  ASSERT_OK(PforWrapper<int32_t>::Decode(decoded.data(), 1024, compressed.data(), comp_size));

  EXPECT_EQ(values, decoded);
}

TEST(PforWrapperTest, Int64MultipleVectors) {
  const int32_t n = 3000;
  std::vector<int64_t> values(n);
  std::mt19937 rng(123);
  std::uniform_int_distribution<int64_t> dist(0, 100000);
  for (auto& v : values) v = dist(rng);
  // Add outliers
  values[0] = 9999999999999LL;
  values[1500] = -9999999999999LL;

  int64_t max_size = PforWrapper<int64_t>::GetMaxCompressedSize(n);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int64_t>::Encode(values.data(), n, compressed.data(), &comp_size);

  std::vector<int64_t> decoded(n);
  ASSERT_OK(PforWrapper<int64_t>::Decode(decoded.data(), n, compressed.data(), comp_size));

  EXPECT_EQ(values, decoded);
}

TEST(PforWrapperTest, Int32SingleElement) {
  std::vector<int32_t> values = {42};

  int64_t max_size = PforWrapper<int32_t>::GetMaxCompressedSize(1);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int32_t>::Encode(values.data(), 1, compressed.data(), &comp_size);

  std::vector<int32_t> decoded(1);
  ASSERT_OK(PforWrapper<int32_t>::Decode(decoded.data(), 1, compressed.data(), comp_size));

  EXPECT_EQ(values, decoded);
}

TEST(PforWrapperTest, Int32AllZeros) {
  std::vector<int32_t> values(1024, 0);

  int64_t max_size = PforWrapper<int32_t>::GetMaxCompressedSize(1024);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int32_t>::Encode(values.data(), 1024, compressed.data(), &comp_size);

  // Should compress very well (bit_width = 0)
  EXPECT_LT(comp_size, 100);

  std::vector<int32_t> decoded(1024);
  ASSERT_OK(PforWrapper<int32_t>::Decode(decoded.data(), 1024, compressed.data(), comp_size));

  EXPECT_EQ(values, decoded);
}

TEST(PforWrapperTest, Int32LargeRandom) {
  const int32_t n = 10000;
  std::vector<int32_t> values(n);
  std::mt19937 rng(99);
  std::uniform_int_distribution<int32_t> dist(
      std::numeric_limits<int32_t>::min(),
      std::numeric_limits<int32_t>::max());
  for (auto& v : values) v = dist(rng);

  int64_t max_size = PforWrapper<int32_t>::GetMaxCompressedSize(n);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int32_t>::Encode(values.data(), n, compressed.data(), &comp_size);

  std::vector<int32_t> decoded(n);
  ASSERT_OK(PforWrapper<int32_t>::Decode(decoded.data(), n, compressed.data(), comp_size));

  EXPECT_EQ(values, decoded);
}

// ======================================================================
// Compression Ratio Test

TEST(PforCompressionRatioTest, ClusteredDataCompresses) {
  // Data clustered around 1000 with one outlier
  std::vector<int32_t> values(1024);
  std::mt19937 rng(42);
  std::uniform_int_distribution<int32_t> dist(1000, 1255);
  for (auto& v : values) v = dist(rng);
  values[500] = 999999;  // One outlier

  int64_t max_size = PforWrapper<int32_t>::GetMaxCompressedSize(1024);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int32_t>::Encode(values.data(), 1024, compressed.data(), &comp_size);

  // PLAIN would be 4096 bytes. PFOR should be much smaller.
  size_t plain_size = 1024 * sizeof(int32_t);
  EXPECT_LT(comp_size, plain_size / 2);
}

// ============================================================================
// FastLanes packing-mode tests
// ============================================================================
//
// All round-trips must produce flat output identical to the input — the
// FastLanes scatter on decode is the inverse of the gather on encode, so the
// PFOR contract (flat output, no permutation visible to the caller) holds.

TEST(PforPackingModeTest, VectorRoundTripIdentityForBothModes) {
  std::vector<int32_t> values(1024);
  std::mt19937 rng(7);
  std::uniform_int_distribution<int32_t> dist(1000, 1500);
  for (auto& v : values) v = dist(rng);
  values[123] = 99999;  // outlier — exercises the exception path
  values[800] = -50;    // another, below FOR

  for (PackingMode mode : {PackingMode::BitPack, PackingMode::FastLanes}) {
    SCOPED_TRACE(testing::Message() << "mode=" << static_cast<int>(mode));
    auto encoded =
        PforCompression<int32_t>::EncodeVector(values.data(), 1024, mode);
    EXPECT_EQ(encoded.info().packing_mode(), mode);

    int64_t sz = PforCompression<int32_t>::SerializedVectorSize(encoded, 1024);
    std::vector<uint8_t> buffer(sz);
    PforCompression<int32_t>::SerializeVector(encoded, 1024, buffer);

    std::vector<int32_t> decoded(1024);
    ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 1024));

    EXPECT_EQ(values, decoded);
  }
}

TEST(PforPackingModeTest, FastLanesFallsBackOnPartialTail) {
  // 700-element vector — smaller than the FastLanes block size. Encoder
  // must fall back to BitPack regardless of the requested mode so the
  // legacy bit-packer handles the partial tail.
  std::vector<int32_t> values(700);
  std::mt19937 rng(99);
  std::uniform_int_distribution<int32_t> dist(0, 100);
  for (auto& v : values) v = dist(rng);

  auto encoded =
      PforCompression<int32_t>::EncodeVector(values.data(), 700, PackingMode::FastLanes);
  EXPECT_EQ(encoded.info().packing_mode(), PackingMode::BitPack);

  int64_t sz = PforCompression<int32_t>::SerializedVectorSize(encoded, 700);
  std::vector<uint8_t> buffer(sz);
  PforCompression<int32_t>::SerializeVector(encoded, 700, buffer);

  std::vector<int32_t> decoded(700);
  ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 700));
  EXPECT_EQ(values, decoded);
}

TEST(PforPackingModeTest, WrapperRoundTripFastLanes) {
  // PforWrapper covers multiple vectors plus the offset array.
  constexpr int32_t kN = 5 * 1024;  // 5 full FastLanes vectors
  std::vector<int32_t> values(kN);
  std::mt19937 rng(2026);
  std::uniform_int_distribution<int32_t> dist(-100, 100);
  for (auto& v : values) v = dist(rng);

  int64_t max_size = PforWrapper<int32_t>::GetMaxCompressedSize(kN);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int32_t>::Encode(values.data(), kN, compressed.data(), &comp_size,
                                PackingMode::FastLanes);

  std::vector<int32_t> decoded(kN);
  ASSERT_OK(PforWrapper<int32_t>::Decode(decoded.data(), kN, compressed.data(),
                                          comp_size));
  EXPECT_EQ(values, decoded);
}

TEST(PforPackingModeTest, WrapperRoundTripMixedTail) {
  // 5*1024 + 700 elements: 5 full vectors (FastLanes-packed when requested)
  // + 1 tail vector (falls back to BitPack inside EncodeVector). Both modes
  // round-trip via the per-vector flag.
  constexpr int32_t kN = 5 * 1024 + 700;
  std::vector<int32_t> values(kN);
  std::mt19937 rng(123);
  std::uniform_int_distribution<int32_t> dist(50000, 60000);
  for (auto& v : values) v = dist(rng);

  int64_t max_size = PforWrapper<int32_t>::GetMaxCompressedSize(kN);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int32_t>::Encode(values.data(), kN, compressed.data(), &comp_size,
                                PackingMode::FastLanes);

  std::vector<int32_t> decoded(kN);
  ASSERT_OK(PforWrapper<int32_t>::Decode(decoded.data(), kN, compressed.data(),
                                          comp_size));
  EXPECT_EQ(values, decoded);
}

TEST(PforPackingModeTest, ConstantVectorBothModes) {
  // bit_width = 0 path: FOR captures everything, no packed payload. Should
  // round-trip identically under either mode (the FastLanes kernel is never
  // entered because bit_width == 0).
  std::vector<int32_t> values(1024, 42);
  for (PackingMode mode : {PackingMode::BitPack, PackingMode::FastLanes}) {
    SCOPED_TRACE(testing::Message() << "mode=" << static_cast<int>(mode));
    auto encoded =
        PforCompression<int32_t>::EncodeVector(values.data(), 1024, mode);
    EXPECT_EQ(encoded.info().bit_width(), 0);

    int64_t sz = PforCompression<int32_t>::SerializedVectorSize(encoded, 1024);
    std::vector<uint8_t> buffer(sz);
    PforCompression<int32_t>::SerializeVector(encoded, 1024, buffer);

    std::vector<int32_t> decoded(1024);
    ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 1024));
    EXPECT_EQ(values, decoded);
  }
}

// ============================================================================
// OutputOrder::Transposed tests — verify the transposed-output decode path
// matches the FL_ORDER permutation of the input for FastLanes vectors.
// ============================================================================

TEST(PforOutputOrderTest, TransposedMatchesFLOrder) {
  std::vector<int32_t> values(1024);
  std::mt19937 rng(11);
  std::uniform_int_distribution<int32_t> dist(1000, 1500);
  for (auto& v : values) v = dist(rng);
  values[7]  = 99999;   // outlier — exercises the exception path under transposed
  values[412] = -50;

  auto encoded = PforCompression<int32_t>::EncodeVector(
      values.data(), 1024, PackingMode::FastLanes);
  ASSERT_EQ(encoded.info().packing_mode(), PackingMode::FastLanes);

  int64_t sz = PforCompression<int32_t>::SerializedVectorSize(encoded, 1024);
  std::vector<uint8_t> buffer(sz);
  PforCompression<int32_t>::SerializeVector(encoded, 1024, buffer);

  std::vector<int32_t> decoded(1024);
  ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 1024,
                                                    OutputOrder::Transposed));

  // out[t] == input[fromTransposed32(t)] for every stream position t.
  for (size_t t = 0; t < 1024; ++t) {
    ASSERT_EQ(decoded[t],
              values[fastlanes::fromTransposed32(t)])
        << "t=" << t;
  }
}

TEST(PforOutputOrderTest, TransposedThenInvertEqualsInput) {
  // Belt-and-suspenders: the user code path is "decode transposed, then
  // apply fromTransposed32 myself when I need original-order positions".
  // Confirm that inverting the permutation manually produces the input.
  std::vector<int32_t> values(1024);
  std::mt19937 rng(31);
  std::uniform_int_distribution<int32_t> dist(-200, 200);
  for (auto& v : values) v = dist(rng);

  auto encoded = PforCompression<int32_t>::EncodeVector(
      values.data(), 1024, PackingMode::FastLanes);

  int64_t sz = PforCompression<int32_t>::SerializedVectorSize(encoded, 1024);
  std::vector<uint8_t> buffer(sz);
  PforCompression<int32_t>::SerializeVector(encoded, 1024, buffer);

  std::vector<int32_t> transposed(1024);
  ASSERT_OK(PforCompression<int32_t>::DecodeVector(transposed.data(), buffer, 1024,
                                                    OutputOrder::Transposed));

  std::vector<int32_t> flat(1024);
  for (size_t t = 0; t < 1024; ++t) {
    flat[fastlanes::fromTransposed32(t)] = transposed[t];
  }
  EXPECT_EQ(values, flat);
}

TEST(PforOutputOrderTest, BitPackIgnoresTransposedRequest) {
  // BitPack mode has no transposition; OutputOrder::Transposed must be
  // ignored (output is still flat / identity).
  std::vector<int32_t> values(1024);
  std::iota(values.begin(), values.end(), 5000);

  auto encoded = PforCompression<int32_t>::EncodeVector(
      values.data(), 1024, PackingMode::BitPack);
  ASSERT_EQ(encoded.info().packing_mode(), PackingMode::BitPack);

  int64_t sz = PforCompression<int32_t>::SerializedVectorSize(encoded, 1024);
  std::vector<uint8_t> buffer(sz);
  PforCompression<int32_t>::SerializeVector(encoded, 1024, buffer);

  std::vector<int32_t> decoded(1024);
  ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 1024,
                                                    OutputOrder::Transposed));
  EXPECT_EQ(values, decoded);
}

TEST(PforOutputOrderTest, WrapperTransposedAcrossManyVectors) {
  constexpr int32_t kN = 5 * 1024;
  std::vector<int32_t> values(kN);
  std::mt19937 rng(909);
  std::uniform_int_distribution<int32_t> dist(-1000, 1000);
  for (auto& v : values) v = dist(rng);

  int64_t max_size = PforWrapper<int32_t>::GetMaxCompressedSize(kN);
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;

  PforWrapper<int32_t>::Encode(values.data(), kN, compressed.data(), &comp_size,
                                PackingMode::FastLanes);

  std::vector<int32_t> decoded(kN);
  ASSERT_OK(PforWrapper<int32_t>::Decode(decoded.data(), kN, compressed.data(),
                                          comp_size, OutputOrder::Transposed));

  // Verify per-block: decoded[block*1024 + t] == values[block*1024 + fromTransposed32(t)]
  for (int32_t block = 0; block < 5; ++block) {
    for (size_t t = 0; t < 1024; ++t) {
      const int32_t base = block * 1024;
      ASSERT_EQ(decoded[base + t],
                values[base + fastlanes::fromTransposed32(t)])
          << "block=" << block << " t=" << t;
    }
  }
}

TEST(PforOutputOrderTest, TransposedFullWidthVector) {
  // Exercises the bit_width == 32 path (UnpackBlock's std::memcpy branch)
  // under transposed output. Endpoints at INT32_MIN/MAX force min-FOR to
  // span the full 32-bit delta range, so the cost model selects bit_width 32.
  std::vector<int32_t> values(1024);
  std::mt19937 rng(1234);
  std::uniform_int_distribution<int32_t> dist(std::numeric_limits<int32_t>::min(),
                                              std::numeric_limits<int32_t>::max());
  for (auto& v : values) v = dist(rng);
  values[0] = std::numeric_limits<int32_t>::min();
  values[1] = std::numeric_limits<int32_t>::max();

  auto encoded = PforCompression<int32_t>::EncodeVector(
      values.data(), 1024, PackingMode::FastLanes);
  ASSERT_EQ(encoded.info().packing_mode(), PackingMode::FastLanes);
  ASSERT_EQ(encoded.info().bit_width(), 32);

  int64_t sz = PforCompression<int32_t>::SerializedVectorSize(encoded, 1024);
  std::vector<uint8_t> buffer(sz);
  PforCompression<int32_t>::SerializeVector(encoded, 1024, buffer);

  std::vector<int32_t> decoded(1024);
  ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 1024,
                                                    OutputOrder::Transposed));
  for (size_t t = 0; t < 1024; ++t) {
    ASSERT_EQ(decoded[t], values[fastlanes::fromTransposed32(t)]) << "t=" << t;
  }
}

TEST(PforOutputOrderTest, TransposedConstantVector) {
  // Exercises the bit_width == 0 constant fast-path under transposed output:
  // every value equals the frame-of-reference, so transposed and flat outputs
  // are identical (the permutation of a constant is itself).
  std::vector<int32_t> values(1024, -7);

  auto encoded = PforCompression<int32_t>::EncodeVector(
      values.data(), 1024, PackingMode::FastLanes);
  ASSERT_EQ(encoded.info().bit_width(), 0);

  int64_t sz = PforCompression<int32_t>::SerializedVectorSize(encoded, 1024);
  std::vector<uint8_t> buffer(sz);
  PforCompression<int32_t>::SerializeVector(encoded, 1024, buffer);

  std::vector<int32_t> decoded(1024);
  ASSERT_OK(PforCompression<int32_t>::DecodeVector(decoded.data(), buffer, 1024,
                                                    OutputOrder::Transposed));
  for (size_t t = 0; t < 1024; ++t) {
    ASSERT_EQ(decoded[t], values[fastlanes::fromTransposed32(t)]) << "t=" << t;
  }
}

}  // namespace arrow::util::pfor
