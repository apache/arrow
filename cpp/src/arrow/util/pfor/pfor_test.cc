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
#include <cstring>
#include <limits>
#include <numeric>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/pfor/pfor_internal.h"
#include "arrow/util/pfor/pfor_wrapper_internal.h"

namespace arrow::util::pfor {

// ======================================================================
// Test fixture
//
// PFOR is instantiated for int32_t and int64_t, and the two differ in the
// widths of the frame of reference, the packed deltas and the exception values,
// so nearly every test here belongs to both. The helpers hide the width
// arithmetic; tests that poke at particular byte offsets derive them from
// sizeof(T) and kStoredSize rather than writing them out.

template <typename T>
class PforTest : public ::testing::Test {
 protected:
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;

  /// Encode, serialize and decode a single vector, checking that it round-trips.
  ///
  /// \param[out] encoded_out the encoded form, for tests that assert on its
  ///             metadata; may be null
  void RoundTripVector(const std::vector<T>& values,
                       PforEncodedVector<T>* encoded_out = nullptr) {
    const auto num_elements = static_cast<int32_t>(values.size());
    auto encoded = PforCompression<T>::EncodeVector(values.data(), num_elements);

    const int64_t needed =
        PforCompression<T>::SerializedVectorSize(encoded, num_elements);
    std::vector<uint8_t> buffer(needed);
    ASSERT_OK_AND_ASSIGN(const int64_t written, PforCompression<T>::SerializeVector(
                                                    encoded, num_elements, buffer));
    ASSERT_EQ(needed, written);

    std::vector<T> decoded(values.size());
    ASSERT_OK(PforCompression<T>::DecodeVector(buffer, num_elements, decoded.data()));
    ASSERT_EQ(values, decoded);

    if (encoded_out != nullptr) {
      *encoded_out = std::move(encoded);
    }
  }

  /// Encode and decode a whole page, checking that it round-trips and stays
  /// inside the bound the encoder allocates from.
  ///
  /// \param[out] comp_size_out the compressed size, for tests that assert on
  ///             the compression ratio; may be null
  void RoundTripPage(const std::vector<T>& values, int64_t* comp_size_out = nullptr) {
    const auto num_values = static_cast<int32_t>(values.size());
    ASSERT_OK_AND_ASSIGN(const int64_t max_size,
                         PforWrapper<T>::GetMaxCompressedSize(num_values));
    std::vector<uint8_t> compressed(max_size);
    int64_t comp_size = max_size;
    ASSERT_OK(
        PforWrapper<T>::Encode(values.data(), num_values, compressed.data(), &comp_size));
    ASSERT_GT(comp_size, 0);
    ASSERT_LE(comp_size, max_size);

    std::vector<T> decoded(values.size());
    ASSERT_OK(
        PforWrapper<T>::Decode(compressed.data(), comp_size, num_values, decoded.data()));
    ASSERT_EQ(values, decoded);

    if (comp_size_out != nullptr) {
      *comp_size_out = comp_size;
    }
  }

  /// Encode `values` and return the compressed bytes, sized exactly.
  static std::vector<uint8_t> EncodePage(const std::vector<T>& values) {
    const auto num_values = static_cast<int32_t>(values.size());
    int64_t comp_size = PforWrapper<T>::GetMaxCompressedSize(num_values).ValueOrDie();
    std::vector<uint8_t> compressed(comp_size);
    ARROW_CHECK_OK(
        PforWrapper<T>::Encode(values.data(), num_values, compressed.data(), &comp_size));
    compressed.resize(comp_size);
    return compressed;
  }

  /// Offset of the first vector in a single-vector page: header, then the
  /// one-entry offset array.
  static constexpr int64_t kFirstVectorOffset =
      PforConstants::kHeaderSize +
      static_cast<int64_t>(sizeof(PforConstants::OffsetType));

  /// Offsets of the PforVectorInfo fields, relative to the start of a vector.
  static constexpr int64_t kBitWidthOffset = static_cast<int64_t>(sizeof(T));
  static constexpr int64_t kNumExceptionsOffset = kBitWidthOffset + 1;

  static std::vector<T> RandomValues(int32_t num_values, T min, T max, uint32_t seed) {
    std::vector<T> values(num_values);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<T> dist(min, max);
    for (auto& v : values) v = dist(rng);
    return values;
  }
};

using PforTypes = ::testing::Types<int32_t, int64_t>;
TYPED_TEST_SUITE(PforTest, PforTypes);

namespace {

void StoreLE32(uint8_t* dest, uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    dest[i] = static_cast<uint8_t>(value >> (8 * i));
  }
}

}  // namespace

// ======================================================================
// Constants Tests

TEST(PforConstantsTest, VectorSizeIsPowerOfTwo) {
  EXPECT_EQ(PforConstants::kPforVectorSize, 1024);
  EXPECT_EQ(1 << PforConstants::kDefaultLogVectorSize, PforConstants::kPforVectorSize);
}

TEST(PforConstantsTest, VectorInfoSizes) {
  EXPECT_EQ(PforTypeTraits<int32_t>::kVectorInfoSize, 7);
  EXPECT_EQ(PforTypeTraits<int64_t>::kVectorInfoSize, 11);
}

// ======================================================================
// BitsRequired Tests

TYPED_TEST(PforTest, BitsRequired) {
  using T = TypeParam;
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;

  EXPECT_EQ(PforTypeTraits<T>::BitsRequired(0), 0);
  EXPECT_EQ(PforTypeTraits<T>::BitsRequired(1), 1);
  EXPECT_EQ(PforTypeTraits<T>::BitsRequired(2), 2);
  EXPECT_EQ(PforTypeTraits<T>::BitsRequired(3), 2);
  EXPECT_EQ(PforTypeTraits<T>::BitsRequired(255), 8);
  EXPECT_EQ(PforTypeTraits<T>::BitsRequired(256), 9);
  EXPECT_EQ(PforTypeTraits<T>::BitsRequired(std::numeric_limits<UnsignedT>::max()),
            PforTypeTraits<T>::kMaxBitWidth);
}

// ======================================================================
// VectorInfo Serialization Tests

TYPED_TEST(PforTest, VectorInfoRoundTrip) {
  using T = TypeParam;
  PforVectorInfo<T> info;
  info.set_frame_of_reference(-42);
  info.set_bit_width(17);
  info.set_num_exceptions(300);

  std::vector<uint8_t> buf(PforVectorInfo<T>::kStoredSize);
  info.Store(buf);
  ASSERT_OK_AND_ASSIGN(auto loaded, PforVectorInfo<T>::Load(buf));

  EXPECT_EQ(loaded.frame_of_reference(), -42);
  EXPECT_EQ(loaded.bit_width(), 17);
  EXPECT_EQ(loaded.num_exceptions(), 300);
}

// The extremes of every field at once: the widest frame of reference the type
// can hold and the full bit width. A six-bit width field could not store 64,
// and reported such a vector as constant.
TYPED_TEST(PforTest, VectorInfoRoundTripAtFieldExtremes) {
  using T = TypeParam;
  constexpr T kFrameOfReference = std::numeric_limits<T>::min();
  PforVectorInfo<T> info(kFrameOfReference, PforTypeTraits<T>::kMaxBitWidth, 30000);

  std::vector<uint8_t> buf(PforVectorInfo<T>::kStoredSize);
  info.Store(buf);
  ASSERT_OK_AND_ASSIGN(auto loaded, PforVectorInfo<T>::Load(buf));

  EXPECT_EQ(loaded.frame_of_reference(), kFrameOfReference);
  EXPECT_EQ(loaded.bit_width(), PforTypeTraits<T>::kMaxBitWidth);
  EXPECT_EQ(loaded.num_exceptions(), 30000);
}

// Every element of a maximum-size vector can be an exception, so the count has
// to reach kMaxVectorSize. A signed 16-bit field stopped one short of that.
TYPED_TEST(PforTest, VectorInfoNumExceptionsAtMaxVectorSize) {
  using T = TypeParam;
  PforVectorInfo<T> info;
  info.set_bit_width(3);
  info.set_num_exceptions(PforConstants::kMaxVectorSize);

  std::vector<uint8_t> buf(PforVectorInfo<T>::kStoredSize);
  info.Store(buf);
  ASSERT_OK_AND_ASSIGN(auto loaded, PforVectorInfo<T>::Load(buf));

  EXPECT_EQ(loaded.num_exceptions(), PforConstants::kMaxVectorSize);
}

TYPED_TEST(PforTest, VectorInfoRejectsNumExceptionsAboveMaxVectorSize) {
  using T = TypeParam;
  // [FOR] [bit_width 1B] [num_exceptions 2B], with a count no vector can have.
  std::vector<uint8_t> buf(PforVectorInfo<T>::kStoredSize, 0);
  buf[TestFixture::kBitWidthOffset] = 3;
  buf[TestFixture::kNumExceptionsOffset] = 0xFF;
  buf[TestFixture::kNumExceptionsOffset + 1] = 0xFF;
  ASSERT_RAISES(Invalid, PforVectorInfo<T>::Load(buf));
}

TYPED_TEST(PforTest, VectorInfoRejectsUndersizedBuffer) {
  using T = TypeParam;
  std::vector<uint8_t> buf(PforVectorInfo<T>::kStoredSize - 1, 0);
  ASSERT_RAISES(Invalid, PforVectorInfo<T>::Load(buf));
}

// ======================================================================
// Cost Model Tests

TYPED_TEST(PforTest, CostModelAllIdentical) {
  using T = TypeParam;
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;
  // All deltas are 0 => bit_width should be 0, no exceptions
  std::vector<UnsignedT> deltas(100, 0);
  auto result = PforCompression<T>::FindOptimalBitWidth(deltas.data(), 100);
  EXPECT_EQ(result.bit_width, 0);
  EXPECT_EQ(result.num_exceptions, 0);
}

TYPED_TEST(PforTest, CostModelSingleOutlier) {
  using T = TypeParam;
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;
  // 99 values fit in 3 bits, 1 outlier needs 16 bits
  std::vector<UnsignedT> deltas(100, 5);  // all fit in 3 bits
  deltas[50] = 50000;                     // outlier: 16 bits
  auto result = PforCompression<T>::FindOptimalBitWidth(deltas.data(), 100);
  // An exception costs a 16-bit position plus a full-width value, so at
  // bit_width=3 the vector costs 100*3 + 1*(16 + 8*sizeof(T)) bits, against
  // 100*16 at bit_width=16. Patching the one outlier wins for both widths of T.
  EXPECT_EQ(result.bit_width, 3);
  EXPECT_EQ(result.num_exceptions, 1);
}

TYPED_TEST(PforTest, CostModelNoOutliers) {
  using T = TypeParam;
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;
  // All values fit in 8 bits
  std::vector<UnsignedT> deltas(100);
  for (int32_t i = 0; i < 100; ++i) deltas[i] = i * 2;
  auto result = PforCompression<T>::FindOptimalBitWidth(deltas.data(), 100);
  EXPECT_EQ(result.num_exceptions, 0);
  EXPECT_LE(result.bit_width, 8);
}

// ======================================================================
// Vector Encode/Decode Round-Trip Tests

// An ascending run is what the delta mode is for, so this no longer measures
// the frame of reference: the packed stream holds the differences, all of them
// 1, and the frame belongs to those. The first value travels separately.
TYPED_TEST(PforTest, VectorSimpleSequence) {
  using T = TypeParam;
  std::vector<T> values(64);
  std::iota(values.begin(), values.end(), 100);

  PforEncodedVector<T> encoded;
  this->RoundTripVector(values, &encoded);
  EXPECT_TRUE(encoded.info().is_delta());
  EXPECT_EQ(encoded.start_value(), 100);
}

// The same shape with the differences made unpredictable, which is what leaves
// the values themselves as the cheaper thing to pack. Here the frame is the
// minimum and nothing is patched, as it was before there was a delta mode.
TYPED_TEST(PforTest, VectorSimpleSequenceWithoutDelta) {
  using T = TypeParam;
  std::vector<T> values = this->RandomValues(64, 100, 163, 11);

  PforEncodedVector<T> encoded;
  this->RoundTripVector(values, &encoded);
  EXPECT_FALSE(encoded.info().is_delta());
  EXPECT_EQ(encoded.info().frame_of_reference(),
            *std::min_element(values.begin(), values.end()));
  EXPECT_EQ(encoded.info().num_exceptions(), 0);
}

TYPED_TEST(PforTest, VectorWithOutlier) {
  using T = TypeParam;
  std::vector<T> values = {100, 102, 101, 103, 100, 99, 50000, 104};

  PforEncodedVector<T> encoded;
  this->RoundTripVector(values, &encoded);
  EXPECT_EQ(encoded.info().frame_of_reference(), 99);
  EXPECT_GT(encoded.info().num_exceptions(), 0);
}

TYPED_TEST(PforTest, VectorAllIdentical) {
  using T = TypeParam;
  std::vector<T> values(100, 42);

  PforEncodedVector<T> encoded;
  this->RoundTripVector(values, &encoded);
  EXPECT_EQ(encoded.info().bit_width(), 0);
  EXPECT_EQ(encoded.info().num_exceptions(), 0);
}

TYPED_TEST(PforTest, VectorNegativeValues) {
  using T = TypeParam;
  std::vector<T> values = {-100, -50, -200, -1, -150};

  PforEncodedVector<T> encoded;
  this->RoundTripVector(values, &encoded);
  EXPECT_EQ(encoded.info().frame_of_reference(), -200);
}

// The frame of reference is subtracted in the unsigned domain, so a vector
// spanning the whole range of T has a delta that only wraps to the right answer.
TYPED_TEST(PforTest, VectorMinMaxEdge) {
  using T = TypeParam;
  std::vector<T> values = {std::numeric_limits<T>::min(), std::numeric_limits<T>::max(),
                           0, -1, 1};
  this->RoundTripVector(values);
}

TYPED_TEST(PforTest, VectorSingleElement) {
  using T = TypeParam;
  this->RoundTripVector(std::vector<T>{42});
}

// ======================================================================
// Page-Level Wrapper Tests

TYPED_TEST(PforTest, PageSmall) { this->RoundTripPage({10, 20, 30, 40, 50}); }

TYPED_TEST(PforTest, PageExactOneVector) {
  using T = TypeParam;
  std::vector<T> values(PforConstants::kPforVectorSize);
  std::iota(values.begin(), values.end(), 0);
  this->RoundTripPage(values);
}

TYPED_TEST(PforTest, PageMultipleVectors) {
  // 2.5 vectors worth of data
  this->RoundTripPage(TestFixture::RandomValues(2560, 0, 1000, /*seed=*/42));
}

TYPED_TEST(PforTest, PageWithOutliers) {
  using T = TypeParam;
  std::vector<T> values(1024, 100);
  values[0] = -999999;
  values[100] = 888888;
  values[500] = 777777;
  values[1023] = -123456;
  this->RoundTripPage(values);
}

TYPED_TEST(PforTest, PagePartialTrailingVector) {
  using T = TypeParam;
  auto values = TestFixture::RandomValues(3000, 0, 100000, /*seed=*/123);
  values[0] = std::numeric_limits<T>::max() / 2;
  values[1500] = std::numeric_limits<T>::min() / 2;
  this->RoundTripPage(values);
}

TYPED_TEST(PforTest, PageSingleElement) {
  using T = TypeParam;
  this->RoundTripPage(std::vector<T>{42});
}

TYPED_TEST(PforTest, PageAllZeros) {
  using T = TypeParam;
  std::vector<T> values(1024, 0);
  int64_t comp_size = 0;
  this->RoundTripPage(values, &comp_size);
  // bit_width is 0, so the page is header, offset array and vector info only.
  EXPECT_LT(comp_size, 100);
}

TYPED_TEST(PforTest, PageLargeRandom) {
  using T = TypeParam;
  this->RoundTripPage(TestFixture::RandomValues(10000, std::numeric_limits<T>::min(),
                                                std::numeric_limits<T>::max(),
                                                /*seed=*/99));
}

// Covers the FOR==0 fast path (bit_width > 0 with frame_of_reference == 0):
// decode unpacks straight into the output, skipping the scratch buffer and the
// add-FOR pass. Forces min == 0, a range that needs bit_width > 0, outliers to
// exercise the exception patch on that path, and multiple vectors.
TYPED_TEST(PforTest, PageZeroMinWithExceptions) {
  auto values = TestFixture::RandomValues(3000, 0, 500, /*seed=*/2024);
  values[0] = 0;            // force min == 0 -> FOR == 0
  values[100] = 1'000'000;  // outliers -> exceptions on the FOR==0 path
  values[1500] = 999'999;
  values[2999] = 500'000;
  this->RoundTripPage(values);
}

// ======================================================================
// Encode Argument Validation

TYPED_TEST(PforTest, EncodeZeroValuesWritesABareHeader) {
  using T = TypeParam;
  // An all-null page holds no values, and the reader still has to be able to load
  // a header from it, so zero values encodes to the header and nothing else. The
  // pointer is null because the buffer the values would come from is empty.
  std::vector<uint8_t> compressed(64);
  int64_t comp_size = 64;
  ASSERT_OK(PforWrapper<T>::Encode(nullptr, 0, compressed.data(), &comp_size));
  ASSERT_EQ(PforConstants::kHeaderSize, comp_size);

  ASSERT_OK_AND_ASSIGN(const int32_t count,
                       PforWrapper<T>::DecodeElementCount(compressed.data(), comp_size));
  ASSERT_EQ(0, count);
  ASSERT_OK(PforWrapper<T>::Decode(compressed.data(), comp_size, 0, nullptr));
}

TYPED_TEST(PforTest, EncodeRejectsNegativeCount) {
  using T = TypeParam;
  std::vector<T> values = {1, 2, 3};
  std::vector<uint8_t> compressed(64);
  int64_t comp_size = 64;
  ASSERT_RAISES(Invalid,
                PforWrapper<T>::Encode(values.data(), -1, compressed.data(), &comp_size));
}

TYPED_TEST(PforTest, EncodeRejectsNullInputWithValuesToEncode) {
  using T = TypeParam;
  std::vector<uint8_t> compressed(64);
  int64_t comp_size = 64;
  ASSERT_RAISES(Invalid,
                PforWrapper<T>::Encode(nullptr, 3, compressed.data(), &comp_size));
}

TYPED_TEST(PforTest, EncodeRejectsUnrepresentableVectorSize) {
  using T = TypeParam;
  std::vector<T> values = {1, 2, 3};
  std::vector<uint8_t> compressed(64);

  // Not a power of two.
  int64_t comp_size = 64;
  ASSERT_RAISES(Invalid, PforWrapper<T>::Encode(values.data(), 3, 1000, compressed.data(),
                                                &comp_size));

  // A power of two, but the header's log_vector_size field cannot describe it.
  comp_size = 64;
  ASSERT_RAISES(
      Invalid, PforWrapper<T>::Encode(values.data(), 3, PforConstants::kMaxVectorSize * 2,
                                      compressed.data(), &comp_size));
}

// ======================================================================
// Compression Ratio Test

TYPED_TEST(PforTest, ClusteredDataCompresses) {
  using T = TypeParam;
  // Data clustered around 1000 with one outlier
  auto values = TestFixture::RandomValues(1024, 1000, 1255, /*seed=*/42);
  values[500] = 999999;

  int64_t comp_size = 0;
  this->RoundTripPage(values, &comp_size);

  const size_t plain_size = 1024 * sizeof(T);
  EXPECT_LT(comp_size, static_cast<int64_t>(plain_size / 2));
}

// ======================================================================
// Corrupt Page Tests
//
// A PFOR page can come from anywhere, so Decode has to reject a header that
// disagrees with the buffer it arrived in rather than trusting it and reading
// or writing out of bounds.

namespace {

// A tight cluster plus one far outlier, so the cost model packs narrow and
// patches the outlier. The outlier is 31 bits out rather than 16: an exception
// costs a position plus a full-width value, so a 16-bit outlier is cheaper to
// pack than to patch once the values are 64 bits wide, and the tests below need
// a vector that actually has an exception section.
template <typename T>
std::vector<T> ClusterWithOutlier() {
  return {10, 20, 30, 40, 2'000'000'000};
}

}  // namespace

TYPED_TEST(PforTest, DecodeElementCountReadsThePageHeader) {
  using T = TypeParam;
  std::vector<T> values(2048);
  std::iota(values.begin(), values.end(), 0);
  auto compressed = TestFixture::EncodePage(values);

  ASSERT_OK_AND_ASSIGN(const int32_t count, PforWrapper<T>::DecodeElementCount(
                                                compressed.data(), compressed.size()));
  EXPECT_EQ(count, static_cast<int32_t>(values.size()));
}

TYPED_TEST(PforTest, DecodeElementCountRejectsMalformedInput) {
  using T = TypeParam;
  const std::vector<T> values = {10, 20, 30, 40, 50};
  auto compressed = TestFixture::EncodePage(values);

  ASSERT_RAISES(Invalid, PforWrapper<T>::DecodeElementCount(
                             compressed.data(), PforConstants::kHeaderSize - 1));
  ASSERT_RAISES(Invalid, PforWrapper<T>::DecodeElementCount(nullptr, compressed.size()));
  ASSERT_RAISES(Invalid, PforWrapper<T>::DecodeElementCount(compressed.data(), -1));
}

TYPED_TEST(PforTest, CorruptBufferTooSmallForHeader) {
  using T = TypeParam;
  const std::vector<T> values = {10, 20, 30, 40, 50};
  auto compressed = TestFixture::EncodePage(values);

  std::vector<T> decoded(values.size());
  ASSERT_RAISES(Invalid,
                PforWrapper<T>::Decode(compressed.data(), PforConstants::kHeaderSize - 1,
                                       5, decoded.data()));
}

TYPED_TEST(PforTest, CorruptElementCountExceedsOutputCapacity) {
  using T = TypeParam;
  const std::vector<T> values = {10, 20, 30, 40, 50};
  auto compressed = TestFixture::EncodePage(values);

  // num_elements lives at header byte 3.
  StoreLE32(compressed.data() + 3, 6);

  std::vector<T> decoded(values.size());
  ASSERT_RAISES(Invalid, PforWrapper<T>::Decode(compressed.data(), compressed.size(), 5,
                                                decoded.data()));
}

TYPED_TEST(PforTest, CorruptElementCountBelowOutputCapacity) {
  using T = TypeParam;
  const std::vector<T> values = {10, 20, 30, 40, 50};
  auto compressed = TestFixture::EncodePage(values);

  // A count short of the caller's capacity would fill part of the output and
  // leave the rest holding whatever it held before.
  StoreLE32(compressed.data() + 3, 4);

  std::vector<T> decoded(values.size());
  ASSERT_RAISES(Invalid, PforWrapper<T>::Decode(compressed.data(), compressed.size(), 5,
                                                decoded.data()));
}

TYPED_TEST(PforTest, CorruptOffsetArrayTruncated) {
  using T = TypeParam;
  // Four vectors, so the offset array is 16 bytes; hand Decode a buffer with
  // room for only two of them.
  std::vector<T> values(4096);
  std::iota(values.begin(), values.end(), 0);
  auto compressed = TestFixture::EncodePage(values);

  std::vector<T> decoded(values.size());
  ASSERT_RAISES(Invalid,
                PforWrapper<T>::Decode(compressed.data(), PforConstants::kHeaderSize + 8,
                                       4096, decoded.data()));
}

TYPED_TEST(PforTest, CorruptVectorOffsetPastEndOfBuffer) {
  using T = TypeParam;
  std::vector<T> values(2048);
  std::iota(values.begin(), values.end(), 0);
  auto compressed = TestFixture::EncodePage(values);

  // Point the second vector past the end of the buffer.
  StoreLE32(compressed.data() + TestFixture::kFirstVectorOffset,
            static_cast<uint32_t>(compressed.size()));

  std::vector<T> decoded(values.size());
  ASSERT_RAISES(Invalid, PforWrapper<T>::Decode(compressed.data(), compressed.size(),
                                                2048, decoded.data()));
}

TYPED_TEST(PforTest, CorruptFirstVectorOffsetSkipsTheOffsetArray) {
  using T = TypeParam;
  const std::vector<T> values = {10, 20, 30, 40, 50};
  auto compressed = TestFixture::EncodePage(values);

  // One vector, so its data starts right after the four byte offset array.
  StoreLE32(compressed.data() + PforConstants::kHeaderSize, 5);

  std::vector<T> decoded(values.size());
  ASSERT_RAISES(Invalid, PforWrapper<T>::Decode(compressed.data(), compressed.size(), 5,
                                                decoded.data()));
}

TYPED_TEST(PforTest, CorruptVectorOffsetsRunBackwards) {
  using T = TypeParam;
  std::vector<T> values(2048);
  std::iota(values.begin(), values.end(), 0);
  auto compressed = TestFixture::EncodePage(values);

  // Point the second vector at the first one's data. Both offsets are inside the
  // buffer, so only reading them as a chain catches it.
  StoreLE32(compressed.data() + TestFixture::kFirstVectorOffset,
            static_cast<uint32_t>(2 * sizeof(PforConstants::OffsetType)));

  std::vector<T> decoded(values.size());
  ASSERT_RAISES(Invalid, PforWrapper<T>::Decode(compressed.data(), compressed.size(),
                                                2048, decoded.data()));
}

TYPED_TEST(PforTest, CorruptExceptionPositionPastEndOfVector) {
  using T = TypeParam;
  const std::vector<T> values = ClusterWithOutlier<T>();
  auto compressed = TestFixture::EncodePage(values);

  // Locate the stored exception position: the vector info, then the packed
  // deltas at whatever width the cost model chose.
  const int64_t vector_start = TestFixture::kFirstVectorOffset;
  ASSERT_GT(compressed[vector_start + TestFixture::kNumExceptionsOffset], 0)
      << "the cost model packed this vector without exceptions, so there is no "
         "exception position to corrupt";
  const uint8_t bit_width = compressed[vector_start + TestFixture::kBitWidthOffset];
  const int64_t packed_bytes = (5 * bit_width + 7) / 8;
  const int64_t position_offset =
      vector_start + PforVectorInfo<T>::kStoredSize + packed_bytes;

  // Point the exception at index 100 of a five-element vector.
  compressed[position_offset] = 100;
  compressed[position_offset + 1] = 0;

  std::vector<T> decoded(5);
  ASSERT_RAISES(Invalid, PforWrapper<T>::Decode(compressed.data(), compressed.size(), 5,
                                                decoded.data()));
}

TYPED_TEST(PforTest, CorruptExceptionCountExceedsVectorLength) {
  using T = TypeParam;
  const std::vector<T> values = ClusterWithOutlier<T>();
  auto compressed = TestFixture::EncodePage(values);

  const int64_t vector_start = TestFixture::kFirstVectorOffset;
  compressed[vector_start + TestFixture::kNumExceptionsOffset] = 6;
  compressed[vector_start + TestFixture::kNumExceptionsOffset + 1] = 0;

  std::vector<T> decoded(5);
  ASSERT_RAISES(Invalid, PforWrapper<T>::Decode(compressed.data(), compressed.size(), 5,
                                                decoded.data()));
}

TYPED_TEST(PforTest, CorruptExceptionDataTruncated) {
  using T = TypeParam;
  const std::vector<T> values = ClusterWithOutlier<T>();
  auto compressed = TestFixture::EncodePage(values);

  // Drop the trailing exception value, leaving its position behind.
  ASSERT_GT(
      compressed[TestFixture::kFirstVectorOffset + TestFixture::kNumExceptionsOffset], 0);
  std::vector<T> decoded(5);
  ASSERT_RAISES(
      Invalid, PforWrapper<T>::Decode(compressed.data(),
                                      static_cast<int64_t>(compressed.size() - sizeof(T)),
                                      5, decoded.data()));
}

// ======================================================================
// Output sizing

TYPED_TEST(PforTest, MaxCompressedSizeRejectsInvalidArguments) {
  using T = TypeParam;
  ASSERT_RAISES(Invalid, PforWrapper<T>::GetMaxCompressedSize(-1));
  // A zero vector_size would divide by zero on the way to the answer.
  for (const int32_t vector_size : {0, 3, 1000, PforConstants::kMaxVectorSize * 2}) {
    SCOPED_TRACE("vector_size=" + std::to_string(vector_size));
    ASSERT_RAISES(Invalid, PforWrapper<T>::GetMaxCompressedSize(1024, vector_size));
    std::vector<T> values(16, 0);
    int64_t comp_size = 1 << 20;
    std::vector<uint8_t> compressed(comp_size);
    ASSERT_RAISES(Invalid, PforWrapper<T>::Encode(values.data(), 16, vector_size,
                                                  compressed.data(), &comp_size));
  }
}

TYPED_TEST(PforTest, EncodeRejectsUndersizedOutputBuffer) {
  using T = TypeParam;
  std::vector<T> values(1024);
  std::iota(values.begin(), values.end(), 0);
  ASSERT_OK_AND_ASSIGN(const int64_t max_size,
                       PforWrapper<T>::GetMaxCompressedSize(1024));

  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size - 1;
  ASSERT_RAISES(Invalid, PforWrapper<T>::Encode(values.data(), 1024, compressed.data(),
                                                &comp_size));
}

TYPED_TEST(PforTest, SerializeVectorRejectsUndersizedDestination) {
  using T = TypeParam;
  std::vector<T> values(64);
  std::iota(values.begin(), values.end(), 0);
  const auto encoded = PforCompression<T>::EncodeVector(values.data(), 64);
  const int64_t needed = PforCompression<T>::SerializedVectorSize(encoded, 64);

  std::vector<uint8_t> buffer(needed);
  ASSERT_OK_AND_ASSIGN(const int64_t written,
                       PforCompression<T>::SerializeVector(encoded, 64, buffer));
  ASSERT_EQ(needed, written);
  ASSERT_RAISES(Invalid, PforCompression<T>::SerializeVector(
                             encoded, 64, std::span<uint8_t>(buffer).subspan(1)));
}

// SerializedVectorSize is derived from the vector info, while the copy that
// follows it takes its lengths from the sections, so a vector whose info and
// sections disagree has to be refused rather than written past the size the
// caller reserved.
TYPED_TEST(PforTest, SerializeVectorRejectsInfoInconsistentWithSections) {
  using T = TypeParam;
  std::vector<T> values(64);
  std::iota(values.begin(), values.end(), 0);
  const auto encoded = PforCompression<T>::EncodeVector(values.data(), 64);
  std::vector<uint8_t> buffer(PforCompression<T>::SerializedVectorSize(encoded, 64) + 64);

  {
    // A bit width that does not account for the packed bytes present.
    auto corrupted = encoded;
    corrupted.mutable_info().set_bit_width(
        static_cast<uint8_t>(encoded.info().bit_width() + 1));
    ASSERT_RAISES(Invalid, PforCompression<T>::SerializeVector(corrupted, 64, buffer));
  }
  {
    // An exception count with nothing behind it. Taken relative to the real
    // count, which need not be zero: the frame search buys a narrower width
    // with patches, so even a plain ascending run can arrive with one.
    auto corrupted = encoded;
    corrupted.mutable_info().set_num_exceptions(
        static_cast<PforConstants::ExceptionCountType>(encoded.info().num_exceptions() +
                                                       1));
    ASSERT_RAISES(Invalid, PforCompression<T>::SerializeVector(corrupted, 64, buffer));
  }
}

// GetMaxCompressedSize is what the encoder allocates, and it is derived from the
// cost model rather than from a worst case that assumes every value is an
// exception, so inputs that push the model in different directions all have to
// land inside it.
TYPED_TEST(PforTest, MaxCompressedSizeBoundHoldsForAdversarialInputs) {
  using T = TypeParam;
  constexpr int32_t kNumValues = 3000;
  std::mt19937 rng(7);
  std::uniform_int_distribution<T> full_range(std::numeric_limits<T>::min(),
                                              std::numeric_limits<T>::max());

  std::vector<std::vector<T>> inputs;
  // Incompressible: every delta needs the full width, so the model picks it.
  inputs.emplace_back(kNumValues);
  for (auto& v : inputs.back()) v = full_range(rng);
  // A narrow cluster with a scatter of far-away values, which is the case that
  // trades bit width against exception count.
  inputs.emplace_back(kNumValues, 1000);
  for (int32_t i = 0; i < kNumValues; i += 3) {
    inputs.back()[i] = full_range(rng);
  }
  // Alternating extremes, so the frame of reference cannot help.
  inputs.emplace_back(kNumValues);
  for (int32_t i = 0; i < kNumValues; ++i) {
    inputs.back()[i] =
        (i % 2 == 0) ? std::numeric_limits<T>::min() : std::numeric_limits<T>::max();
  }

  for (size_t i = 0; i < inputs.size(); ++i) {
    SCOPED_TRACE("input " + std::to_string(i));
    this->RoundTripPage(inputs[i]);
  }
}

// ======================================================================
// Delta Mode and Frame Search Tests

// The mode has to survive every width, not just the ones a plausible column
// produces, so this drives it through all of them: a cumulative sum whose
// differences are exactly w bits wide leaves the encoder no cheaper option than
// differencing, since the values themselves span 1024 times as much.
TYPED_TEST(PforTest, DeltaRoundTripsAtEveryBitWidth) {
  using T = TypeParam;
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;
  constexpr int32_t kNumValues = 1024;

  for (uint8_t width = 0; width <= PforTypeTraits<T>::kMaxBitWidth; ++width) {
    SCOPED_TRACE("delta width " + std::to_string(width));
    // Differences that need exactly `width` bits, and whose top bit is set so
    // the width cannot be mistaken for a narrower one.
    const UnsignedT step =
        width == 0 ? UnsignedT{0} : static_cast<UnsignedT>(UnsignedT{1} << (width - 1));

    std::vector<T> values(kNumValues);
    auto acc = static_cast<UnsignedT>(0);
    for (int32_t i = 0; i < kNumValues; ++i) {
      std::memcpy(&values[i], &acc, sizeof(T));
      acc += step;
    }
    this->RoundTripVector(values);
  }
}

// The negative control for the sweep above. If the encoder stopped choosing the
// delta mode -- a cost-model change, a gate that reads the wrong thing -- every
// round-trip test would still pass while testing nothing, so assert here that
// the path is reached at all, and that it is not reached for data it would only
// make bigger.
TYPED_TEST(PforTest, DeltaIsChosenForRunsAndDeclinedForNoise) {
  using T = TypeParam;
  constexpr int32_t kNumValues = 1024;

  std::vector<T> ascending(kNumValues);
  std::iota(ascending.begin(), ascending.end(), static_cast<T>(1'000'000));
  PforEncodedVector<T> encoded;
  this->RoundTripVector(ascending, &encoded);
  EXPECT_TRUE(encoded.info().is_delta());

  // Independent draws from a narrow band: the values fit in 10 bits, while
  // their differences take 11 and swing both ways.
  const std::vector<T> noise = this->RandomValues(kNumValues, 0, 1000, 42);
  this->RoundTripVector(noise, &encoded);
  EXPECT_FALSE(encoded.info().is_delta());
}

// The property the mode exists for: a delta vector carries its own first value,
// so it decodes without the vector before it. Decoding vector 1 of a page
// straight out of the middle is what a reader doing block-level random access
// would do, and it has to produce that vector's values and no others.
TYPED_TEST(PforTest, DeltaVectorDecodesWithoutThePrecedingVector) {
  using T = TypeParam;
  constexpr int32_t kVectorSize = static_cast<int32_t>(PforConstants::kPforVectorSize);
  constexpr int32_t kNumValues = 3 * kVectorSize;

  std::vector<T> values(kNumValues);
  std::iota(values.begin(), values.end(), static_cast<T>(500));
  const std::vector<uint8_t> page = this->EncodePage(values);

  // The offset array follows the header, one entry per vector, each counted
  // from the start of the array itself.
  const uint8_t* offsets = page.data() + PforConstants::kHeaderSize;
  const auto second = util::SafeLoadAs<PforConstants::OffsetType>(
      offsets + sizeof(PforConstants::OffsetType));
  const uint8_t* vector = offsets + second;

  std::vector<T> decoded(kVectorSize);
  ASSERT_OK(PforCompression<T>::DecodeVector(
      std::span<const uint8_t>(vector, page.data() + page.size() - vector), kVectorSize,
      decoded.data()));
  EXPECT_EQ(
      std::vector<T>(values.begin() + kVectorSize, values.begin() + 2 * kVectorSize),
      decoded);
}

// Two-sided patching. A tight cluster with one value far below it is the case
// the old frame could not handle: at the minimum, the packed window has to span
// the whole gap and every value in the cluster pays for it. The frame belongs
// above the low value, which then becomes an exception like any other.
TYPED_TEST(PforTest, FrameSitsAboveTheMinimumToPatchALowOutlier) {
  using T = TypeParam;
  constexpr int32_t kNumValues = 1024;

  std::vector<T> values = this->RandomValues(kNumValues, 1'000'000, 1'000'063, 5);
  values[500] = 0;  // one value four orders of magnitude below the cluster

  PforEncodedVector<T> encoded;
  this->RoundTripVector(values, &encoded);
  EXPECT_GT(encoded.info().frame_of_reference(),
            *std::min_element(values.begin(), values.end()));
  EXPECT_GE(encoded.info().num_exceptions(), 1);
  // Six bits covers the cluster; spanning the gap would take twenty. The bound
  // is loose because the search picks the frame from bucketed counts and then
  // lowers it to the smallest value it actually covers, which lands on the
  // cluster minimum only when the cluster falls inside one bucket.
  EXPECT_LE(encoded.info().bit_width(), 8);
}

// The two features together, on the shape that motivated them: a counter that
// climbs steadily and resets, whose differences are a run of small positive
// values with a handful of large negative ones. The differences want a frame
// above their minimum, and the drops want patching -- neither alone is enough,
// and this is where both of the encoder's bugs showed up during development.
TYPED_TEST(PforTest, SawtoothPacksAtNearlyNoCost) {
  using T = TypeParam;
  constexpr int32_t kNumValues = 8192;

  std::vector<T> values(kNumValues);
  T level = 0;
  for (int32_t i = 0; i < kNumValues; ++i) {
    values[i] = level;
    level = (i % 200 == 199) ? 0 : static_cast<T>(level + 12);
  }

  int64_t comp_size = 0;
  this->RoundTripPage(values, &comp_size);
  // The differences pack at width 0 with one patch per reset. Anything that
  // falls back to a width wide enough to hold the climb costs at least 12 bits
  // a value, so a bound of one is far from the boundary.
  const double bits_per_value = 8.0 * comp_size / kNumValues;
  EXPECT_LT(bits_per_value, 1.0) << "compressed to " << bits_per_value << " bits/value";
}

// An exception in a delta vector is itself a difference, so the decoder has to
// patch before it sums. Summing first would carry the placeholder zero into
// every value after the patch, which round-trips as a plausible-looking
// sequence rather than an obvious corruption.
TYPED_TEST(PforTest, DeltaExceptionsArePatchedBeforeTheSum) {
  using T = TypeParam;
  constexpr int32_t kNumValues = 1024;

  // A steady climb with occasional jumps far larger than the packed width can
  // hold, so the jumps become exceptions and everything after one depends on
  // the patch having landed.
  std::vector<T> values(kNumValues);
  T level = 1000;
  for (int32_t i = 0; i < kNumValues; ++i) {
    values[i] = level;
    level = static_cast<T>(level + ((i % 137 == 100) ? 1'000'000 : 3));
  }

  PforEncodedVector<T> encoded;
  this->RoundTripVector(values, &encoded);
  EXPECT_TRUE(encoded.info().is_delta());
  EXPECT_GE(encoded.info().num_exceptions(), 1);
}

// The delta flag shares its byte with the bit width, and the width goes up to
// 64, which needs all seven of the bits left over. A vector whose differences
// span the type is the case that would catch the two fields overlapping.
TYPED_TEST(PforTest, DeltaAtFullWidthRoundTrips) {
  using T = TypeParam;
  constexpr int32_t kNumValues = 1024;
  std::mt19937_64 rng(3);

  // Values whose differences are full-width, built by accumulating full-width
  // steps, so the sequence wraps the type as it goes.
  std::vector<T> values(kNumValues);
  auto acc = static_cast<typename PforTypeTraits<T>::UnsignedType>(0);
  for (int32_t i = 0; i < kNumValues; ++i) {
    std::memcpy(&values[i], &acc, sizeof(T));
    acc += static_cast<typename PforTypeTraits<T>::UnsignedType>(rng());
  }
  this->RoundTripVector(values);
}

// The same overlap, tested at the metadata directly rather than through data
// that happens to produce it, at every width the type allows.
TYPED_TEST(PforTest, VectorInfoKeepsTheDeltaFlagOutOfTheWidth) {
  using T = TypeParam;
  std::vector<uint8_t> buffer(PforVectorInfo<T>::kStoredSize);

  for (uint8_t width = 0; width <= PforTypeTraits<T>::kMaxBitWidth; ++width) {
    for (bool is_delta : {false, true}) {
      SCOPED_TRACE("width " + std::to_string(width) + " delta " +
                   std::to_string(is_delta));
      const PforVectorInfo<T> info(static_cast<T>(-7), width, 123, is_delta);
      info.Store(buffer);
      ASSERT_OK_AND_ASSIGN(const auto loaded, PforVectorInfo<T>::Load(buffer));
      EXPECT_EQ(loaded.bit_width(), width);
      EXPECT_EQ(loaded.is_delta(), is_delta);
      EXPECT_EQ(loaded.frame_of_reference(), static_cast<T>(-7));
      EXPECT_EQ(loaded.num_exceptions(), 123);
      EXPECT_EQ(loaded.stored_bytes(),
                PforVectorInfo<T>::kStoredSize +
                    (is_delta ? static_cast<int64_t>(sizeof(T)) : 0));
    }
  }
}

// Differencing is done in the unsigned domain because signed overflow is
// undefined, and a column spanning the type's range will overflow. The bits
// have to round-trip anyway.
TYPED_TEST(PforTest, DeltaRoundTripsAcrossTheTypeExtremes) {
  using T = TypeParam;
  const T lo = std::numeric_limits<T>::min();
  const T hi = std::numeric_limits<T>::max();

  std::vector<std::vector<T>> inputs = {
      {lo, hi, lo, hi, lo, hi, lo, hi},
      {lo, static_cast<T>(lo + 1), static_cast<T>(lo + 2), hi, static_cast<T>(hi - 1)},
      {hi, static_cast<T>(hi - 1), static_cast<T>(hi - 2), lo, static_cast<T>(lo + 1)},
  };
  // A ramp from the bottom of the type that runs off the top of it.
  inputs.emplace_back(1024);
  auto acc = static_cast<typename PforTypeTraits<T>::UnsignedType>(lo);
  for (auto& v : inputs.back()) {
    std::memcpy(&v, &acc, sizeof(T));
    acc += static_cast<typename PforTypeTraits<T>::UnsignedType>(hi / 100);
  }

  for (size_t i = 0; i < inputs.size(); ++i) {
    SCOPED_TRACE("input " + std::to_string(i));
    this->RoundTripVector(inputs[i]);
  }
}

// Small vectors are where the start value is expensive: the format allows eight
// values to a vector, and eight bytes across eight of them is a byte each. The
// cost model has to account for it rather than differencing whenever the
// differences happen to be narrower.
TYPED_TEST(PforTest, StartValueIsPaidForAtEveryVectorSize) {
  using T = TypeParam;
  for (int32_t log_size = PforConstants::kMinLogVectorSize;
       log_size <= PforConstants::kMaxLogVectorSize; ++log_size) {
    const int32_t vector_size = 1 << log_size;
    SCOPED_TRACE("vector size " + std::to_string(vector_size));

    std::vector<T> values(static_cast<size_t>(vector_size) * 3);
    std::iota(values.begin(), values.end(), static_cast<T>(77));

    const auto num_values = static_cast<int32_t>(values.size());
    ASSERT_OK_AND_ASSIGN(const int64_t max_size,
                         PforWrapper<T>::GetMaxCompressedSize(num_values, vector_size));
    std::vector<uint8_t> compressed(max_size);
    int64_t comp_size = max_size;
    ASSERT_OK(PforWrapper<T>::Encode(values.data(), num_values, vector_size,
                                     compressed.data(), &comp_size));
    ASSERT_LE(comp_size, max_size);

    std::vector<T> decoded(values.size());
    ASSERT_OK(
        PforWrapper<T>::Decode(compressed.data(), comp_size, num_values, decoded.data()));
    EXPECT_EQ(values, decoded);
  }
}

// A one-element vector has no difference to take, and a two-element one has
// exactly one. Both are inside the range the format allows for a trailing
// partial vector.
TYPED_TEST(PforTest, DeltaHandlesVectorsTooShortToDifference) {
  using T = TypeParam;
  this->RoundTripVector(std::vector<T>{42});
  this->RoundTripVector(std::vector<T>{42, 43});
  this->RoundTripVector(std::vector<T>{std::numeric_limits<T>::max()});
  this->RoundTripVector(
      std::vector<T>{std::numeric_limits<T>::max(), std::numeric_limits<T>::min()});
}

namespace {

/// A named column shape, for the gate test below.
template <typename T>
struct NamedColumn {
  std::string name;
  std::vector<T> values;
};

/// The distributions in pfor_benchmark.cc, plus the extremes, as one battery.
/// Anything that changes the encoder's mind about differencing has to be seen
/// on one of these or it is not being tested.
template <typename T>
std::vector<NamedColumn<T>> DeltaDecisionColumns() {
  constexpr int32_t kN = static_cast<int32_t>(PforConstants::kPforVectorSize);
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;
  std::vector<NamedColumn<T>> columns;
  auto add = [&columns](std::string name, std::vector<T> values) {
    columns.push_back({std::move(name), std::move(values)});
  };
  auto make = [](auto fn) {
    std::vector<T> v(kN);
    for (int32_t i = 0; i < kN; ++i) v[i] = fn(i);
    return v;
  };

  add("constant", std::vector<T>(kN, static_cast<T>(-7)));
  add("ascending", make([](int32_t i) { return static_cast<T>(1'000'000 + i); }));
  add("descending", make([](int32_t i) { return static_cast<T>(1'000'000 - i); }));
  add("monotonic_with_gaps", make([](int32_t i) {
        return static_cast<T>(500'000 + i * 3 + (i % 64 == 0 ? 4096 : 0));
      }));
  add("timestamps",
      make([](int32_t i) { return static_cast<T>(1'700'000'000 + i * 17 + (i % 7)); }));
  // A ramp that resets: small positive differences with a handful of large
  // negative ones. The shape the two-sided frame exists for.
  add("sawtooth",
      make([](int32_t i) { return static_cast<T>((i % 200) * 13 + 100'000); }));
  add("cluster_far_from_zero",
      make([](int32_t i) { return static_cast<T>(1'000'000 + (i % 37)); }));
  add("two_clusters", make([](int32_t i) {
        return static_cast<T>(i % 2 == 0 ? 100 + (i % 8) : 900'000 + (i % 8));
      }));
  add("cluster_with_outliers", make([](int32_t i) {
        return static_cast<T>(i % 97 == 0 ? 1'000'000 + i : 500 + (i % 11));
      }));
  add("type_extremes", make([](int32_t i) {
        return i % 2 == 0 ? std::numeric_limits<T>::lowest() + static_cast<T>(i)
                          : std::numeric_limits<T>::max() - static_cast<T>(i);
      }));
  // A random walk, and independent draws at several spreads. The walk is where
  // differencing pays; the draws are where it must be declined.
  for (uint32_t seed : {1u, 7u, 42u, 1234u}) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int32_t> step(-40, 40);
    auto walk = static_cast<UnsignedT>(0);
    std::vector<T> values(kN);
    for (int32_t i = 0; i < kN; ++i) {
      walk += static_cast<UnsignedT>(static_cast<T>(step(rng)));
      std::memcpy(&values[i], &walk, sizeof(T));
    }
    add("random_walk_" + std::to_string(seed), std::move(values));
  }
  for (T spread : {static_cast<T>(1), static_cast<T>(1000), static_cast<T>(1'000'000)}) {
    for (uint32_t seed : {3u, 99u}) {
      std::vector<T> values(kN);
      std::mt19937 rng(seed);
      std::uniform_int_distribution<T> dist(static_cast<T>(0), spread);
      for (auto& v : values) v = dist(rng);
      add("noise_" + std::to_string(static_cast<int64_t>(spread)) + "_" +
              std::to_string(seed),
          std::move(values));
    }
  }
  return columns;
}

/// What ChooseVectorPlan would decide with no gate in front of the delta
/// search: cost both modes properly and keep the cheaper.
template <typename T>
PforVectorPlan<T> ChooseVectorPlanUngated(const T* values, int32_t num_elements,
                                          T* delta_scratch) {
  const FrameChoice<T> raw = ChooseFrameAndWidth<T>(values, num_elements);
  PforVectorPlan<T> plan;
  plan.frame_of_reference = raw.frame_of_reference;
  plan.bit_width = raw.bit_width;
  plan.num_exceptions = raw.num_exceptions;
  plan.cost_bits = raw.cost_bits;
  if (num_elements < 2 || raw.bit_width == 0) return plan;

  MinMax<T> delta_bounds;
  ComputeDeltas<T>(values, num_elements, delta_scratch, &delta_bounds);
  const FrameChoice<T> delta =
      ChooseFrameAndWidth<T>(delta_scratch, num_elements, delta_bounds);
  const int64_t delta_cost = delta.cost_bits + static_cast<int64_t>(sizeof(T)) * 8;
  if (delta_cost < plan.cost_bits) {
    plan.delta = true;
    plan.frame_of_reference = delta.frame_of_reference;
    plan.start_value = values[0];
    plan.bit_width = delta.bit_width;
    plan.num_exceptions = delta.num_exceptions;
    plan.cost_bits = delta_cost;
  }
  return plan;
}

}  // namespace

// The gate in front of the delta search is an estimate, so it can in principle
// decline a vector the search would have won. Pin that it does not, on every
// shape the encoder is meant to handle: same mode, same width, same cost as the
// ungated chooser. A change that makes the estimate too pessimistic shows up
// here as a lost delta rather than as a silent ratio regression.
TYPED_TEST(PforTest, DeltaGateAgreesWithTheUngatedChooser) {
  using T = TypeParam;
  int delta_chosen = 0;
  for (const auto& column : DeltaDecisionColumns<T>()) {
    SCOPED_TRACE(column.name);
    const auto n = static_cast<int32_t>(column.values.size());
    std::vector<T> scratch(n);
    const PforVectorPlan<T> gated =
        ChooseVectorPlan<T>(column.values.data(), n, scratch.data(),
                            /*delta_enabled=*/true);
    const PforVectorPlan<T> ungated =
        ChooseVectorPlanUngated<T>(column.values.data(), n, scratch.data());

    EXPECT_EQ(ungated.delta, gated.delta);
    EXPECT_EQ(ungated.cost_bits, gated.cost_bits);
    EXPECT_EQ(ungated.bit_width, gated.bit_width);
    EXPECT_EQ(ungated.frame_of_reference, gated.frame_of_reference);
    EXPECT_EQ(ungated.num_exceptions, gated.num_exceptions);
    if (gated.delta) ++delta_chosen;
  }
  // The battery has to exercise both answers, or the agreement above is only
  // evidence that the gate declines everything.
  EXPECT_GT(delta_chosen, 0);
}

// The estimate is the point of the gate, so check it reaches the shape it was
// built for. A gate reading the spread of the differences rather than their
// distribution declines the sawtooth, whose differences are as widely spread as
// its values and far cheaper to pack.
TYPED_TEST(PforTest, DeltaGateAdmitsTheSawtooth) {
  using T = TypeParam;
  constexpr int32_t kN = static_cast<int32_t>(PforConstants::kPforVectorSize);
  std::vector<T> values(kN);
  for (int32_t i = 0; i < kN; ++i) {
    values[i] = static_cast<T>((i % 200) * 13 + 100'000);
  }
  std::vector<T> scratch(kN);
  const PforVectorPlan<T> plan =
      ChooseVectorPlan<T>(values.data(), kN, scratch.data(), /*delta_enabled=*/true);
  EXPECT_TRUE(plan.delta);
  // Five resets in 1024 values, patched, leaving a very narrow packed window.
  EXPECT_LE(plan.bit_width, 5);
}

}  // namespace arrow::util::pfor
