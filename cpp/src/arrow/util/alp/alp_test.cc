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

#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <random>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/alp/alp_codec_internal.h"
#include "arrow/util/alp/alp_compression_internal.h"
#include "arrow/util/alp/alp_constants_internal.h"
#include "arrow/util/alp/alp_metadata_internal.h"
#include "arrow/util/alp/alp_sampler_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/ubsan.h"

namespace arrow::util::alp {

// Test helpers

// Compares two ranges by bit pattern. ALP must be lossless, and `==` cannot show
// that: `0.0 == -0.0` and `NaN != NaN`. A mismatch reports the index and the bits.
template <typename T>
::testing::AssertionResult IsBitwiseEqual(const std::vector<T>& actual,
                                          const std::vector<T>& expected) {
  static_assert(std::is_floating_point<T>::value,
                "IsBitwiseEqual is for float/double only");
  using Bits = typename std::conditional<sizeof(T) == 4, uint32_t, uint64_t>::type;
  if (actual.size() != expected.size()) {
    return ::testing::AssertionFailure() << "size mismatch: actual=" << actual.size()
                                         << " expected=" << expected.size();
  }
  for (size_t i = 0; i < actual.size(); ++i) {
    Bits a_bits = 0, e_bits = 0;
    std::memcpy(&a_bits, &actual[i], sizeof(T));
    std::memcpy(&e_bits, &expected[i], sizeof(T));
    if (a_bits != e_bits) {
      return ::testing::AssertionFailure()
             << "bit-mismatch at index " << i << ": actual=" << actual[i] << " (bits 0x"
             << std::hex << a_bits << "), expected=" << std::dec << expected[i]
             << " (bits 0x" << std::hex << e_bits << ")";
    }
  }
  return ::testing::AssertionSuccess();
}

template <typename T, typename TargetType = T>
void DecodeEncodedVector(const AlpEncodedVector<T>& encoded,
                         std::vector<TargetType>* output) {
  output->resize(encoded.num_elements());

  std::vector<uint8_t> buffer(static_cast<size_t>(encoded.GetStoredSize()));
  encoded.Store({buffer.data(), buffer.size()});
  ASSERT_OK_AND_ASSIGN(auto view,
                       AlpEncodedVectorView<T>::Load({buffer.data(), buffer.size()},
                                                     encoded.num_elements(),
                                                     arrow::default_memory_pool()));

  std::vector<typename AlpCompression<T>::EncodedUnsigned> scratch(
      encoded.num_elements());
  AlpCompression<T>::Decompress(view, std::span<TargetType>(*output), scratch);
}

template <typename T>
void RoundTripVector(const std::vector<T>& input) {
  ASSERT_OK_AND_ASSIGN(const AlpEncodingPreset preset, AlpCodec<T>::MakePreset(input));
  const auto encoded = AlpCompression<T>::Compress(input, preset);

  std::vector<T> output(input.size());
  DecodeEncodedVector(encoded, &output);

  ASSERT_EQ(output.size(), input.size());
  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

// Spec page and vector corruption helpers

// Encode a small page and return the compressed bytes, for tests that corrupt a
// single field and check that loading rejects the result.
template <typename T>
std::vector<uint8_t> EncodeSmallPage(std::vector<T>* input) {
  input->resize(64);
  for (size_t i = 0; i < input->size(); ++i) {
    (*input)[i] = static_cast<T>(i) * static_cast<T>(0.1);
  }
  EXPECT_OK_AND_ASSIGN(
      int64_t max_comp_size,
      AlpCodec<T>::GetMaxCompressedSize(static_cast<int64_t>(input->size()),
                                        AlpFormatConstants::kDefaultVectorSize));
  std::vector<uint8_t> comp_buffer(max_comp_size);
  EXPECT_OK_AND_ASSIGN(
      const int64_t comp_size,
      AlpCodec<T>::Encode(*input, AlpFormatConstants::kDefaultVectorSize, comp_buffer));
  comp_buffer.resize(comp_size);
  return comp_buffer;
}

// Encode one small vector and return its stored bytes, for tests that corrupt a
// single metadata field and check that loading rejects the result.
template <typename T>
std::vector<uint8_t> EncodeSmallVector(std::vector<T>* input) {
  input->resize(64);
  for (size_t i = 0; i < input->size(); ++i) {
    (*input)[i] = static_cast<T>(i) * static_cast<T>(0.1);
  }
  const auto encoded =
      AlpCompression<T>::Compress(*input, AlpEncodingPreset::MakeDefault());
  std::vector<uint8_t> buffer(static_cast<size_t>(encoded.GetStoredSize()));
  encoded.Store(buffer);
  return buffer;
}

// Types used by the templated tests below.
using FloatingTestTypes = ::testing::Types<float, double>;

// AlpEncodingPreset Tests

// A preset the codec cannot use must be rejected with a status: an empty
// combination list, an unknown integer encoding, an exponent past the
// power-of-ten table, or a factor larger than the exponent.
TEST(AlpEncodingPresetTest, RejectsInvalidPreset) {
  const std::vector<float> input = {1.0f, 2.0f, 3.0f};
  std::vector<uint8_t> output(256);
  const std::vector<AlpExponentAndFactor> identity = {{0, 0}};
  const uint8_t too_large_exponent =
      static_cast<uint8_t>(AlpTypedConstants<float>::kMaxExponent + 1);

  const std::vector<std::pair<AlpEncodingPreset, const char*>> cases = {
      {{}, "at least one combination"},
      {{identity, 0, static_cast<AlpIntegerEncoding>(99)},
       "unsupported integer encoding"},
      {{{AlpExponentAndFactor{too_large_exponent, 0}},
        0,
        AlpIntegerEncoding::kForBitPack},
       "ALP preset exponent"},
      {{{AlpExponentAndFactor{1, 2}}, 0, AlpIntegerEncoding::kForBitPack},
       "ALP preset factor"},
  };

  for (const auto& [preset, expected_message] : cases) {
    SCOPED_TRACE(expected_message);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr(expected_message),
        AlpCodec<float>::Encode(input, preset, AlpFormatConstants::kDefaultVectorSize,
                                output));
  }
}
// AlpInfo Serialization Tests

TEST(AlpInfoTest, StoreLoadRoundTrip) {
  // AlpInfo is 4 bytes.
  AlpInfo info{};
  info.SetExponent(5);
  info.SetFactor(3);
  info.SetNumExceptions(10);

  std::vector<uint8_t> buffer(AlpInfo::kStoredSize + 10);
  info.Store({buffer.data(), buffer.size()});

  ASSERT_OK_AND_ASSIGN(AlpInfo loaded, AlpInfo::Load({buffer.data(), buffer.size()}));
  EXPECT_EQ(info, loaded);
  EXPECT_EQ(loaded.exponent(), 5);
  EXPECT_EQ(loaded.factor(), 3);
  EXPECT_EQ(loaded.num_exceptions(), 10);
}

TEST(AlpForInfoTest, StoreLoadRoundTripFloat) {
  // AlpForInfo<float> is 5 bytes.
  AlpForInfo<float> info{};
  info.SetFrameOfReference(0x12345678U);
  info.SetBitWidth(12);

  std::vector<uint8_t> buffer(AlpForInfo<float>::kStoredSize + 10);
  info.Store({buffer.data(), buffer.size()});

  ASSERT_OK_AND_ASSIGN(AlpForInfo<float> loaded,
                       AlpForInfo<float>::Load({buffer.data(), buffer.size()}));
  EXPECT_EQ(info, loaded);
  EXPECT_EQ(loaded.frame_of_reference(), 0x12345678U);
  EXPECT_EQ(loaded.bit_width(), 12);
}

TEST(AlpForInfoTest, StoreLoadRoundTripDouble) {
  // AlpForInfo<double> is 9 bytes.
  AlpForInfo<double> info{};
  info.SetFrameOfReference(0x123456789ABCDEF0ULL);
  info.SetBitWidth(20);

  std::vector<uint8_t> buffer(AlpForInfo<double>::kStoredSize + 10);
  info.Store({buffer.data(), buffer.size()});

  ASSERT_OK_AND_ASSIGN(AlpForInfo<double> loaded,
                       AlpForInfo<double>::Load({buffer.data(), buffer.size()}));
  EXPECT_EQ(info, loaded);
  EXPECT_EQ(loaded.frame_of_reference(), 0x123456789ABCDEF0ULL);
  EXPECT_EQ(loaded.bit_width(), 20);
}

// AlpVector Tests

template <typename T>
class AlpVectorTest : public ::testing::Test {};

TYPED_TEST_SUITE(AlpVectorTest, FloatingTestTypes);

TYPED_TEST(AlpVectorTest, ValuePatterns) {
  using MakeValue = std::function<TypeParam(size_t)>;
  const std::vector<std::pair<std::string, MakeValue>> patterns = {
      {"integers", [](size_t i) { return static_cast<TypeParam>(i + 1); }},
      {"halves",
       [](size_t i) { return static_cast<TypeParam>(i) + static_cast<TypeParam>(0.5); }},
      {"milli decimals",
       [](size_t i) {
         return static_cast<TypeParam>(0.001) * static_cast<TypeParam>(i + 1);
       }},
      {"micro decimals",
       [](size_t i) {
         return static_cast<TypeParam>(1e-6) * static_cast<TypeParam>(i + 1);
       }},
      {"nano decimals",
       [](size_t i) {
         return static_cast<TypeParam>(1e-10) * static_cast<TypeParam>(i + 1);
       }},
      {"large decimals",
       [](size_t i) {
         return static_cast<TypeParam>(1000000.0) +
                static_cast<TypeParam>(i) * static_cast<TypeParam>(0.01);
       }},
      {"high precision",
       [](size_t i) {
         return static_cast<TypeParam>(1.123456789) * static_cast<TypeParam>(i + 1);
       }},
      {"negative ramp",
       [](size_t i) { return -static_cast<TypeParam>(i) * static_cast<TypeParam>(0.5); }},
      {"alternating sign",
       [](size_t i) {
         const auto sign = (i % 2 == 0) ? TypeParam{1} : TypeParam{-1};
         return sign * static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
       }},
  };

  for (const auto& [name, make_value] : patterns) {
    SCOPED_TRACE(name);
    std::vector<TypeParam> input(1024);
    for (size_t i = 0; i < input.size(); ++i) {
      input[i] = make_value(i);
    }

    ASSERT_OK_AND_ASSIGN(const AlpEncodingPreset preset,
                         AlpCodec<TypeParam>::MakePreset(input));
    const auto encoded = AlpCompression<TypeParam>::Compress(input, preset);
    EXPECT_LT(encoded.alp_info().num_exceptions(), static_cast<int32_t>(input.size()));

    std::vector<TypeParam> output(input.size());
    DecodeEncodedVector(encoded, &output);
    EXPECT_TRUE(IsBitwiseEqual(output, input));
  }
}

TYPED_TEST(AlpVectorTest, ExplicitPreset) {
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }

  const AlpEncodingPreset preset{{AlpExponentAndFactor{1, 0}}, 0};
  const auto encoded = AlpCompression<TypeParam>::Compress(input, preset);

  EXPECT_EQ(encoded.alp_info().exponent(), 1);
  EXPECT_EQ(encoded.alp_info().factor(), 0);
  EXPECT_EQ(encoded.alp_info().num_exceptions(), 0);

  std::vector<TypeParam> output(input.size());
  DecodeEncodedVector(encoded, &output);
  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

// A preset holding several candidates makes the encoder search them. (0, 0)
// cannot represent 0.1 at all and (2, 0) needs a wider range, so (1, 0) is the
// one that compresses this vector best.
TYPED_TEST(AlpVectorTest, PicksBestCombination) {
  std::vector<TypeParam> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }

  const AlpEncodingPreset preset{{AlpExponentAndFactor{0, 0}, AlpExponentAndFactor{1, 0},
                                  AlpExponentAndFactor{2, 0}},
                                 0};
  const auto encoded = AlpCompression<TypeParam>::Compress(input, preset);

  EXPECT_EQ(encoded.alp_info().exponent(), 1);
  EXPECT_EQ(encoded.alp_info().factor(), 0);
  EXPECT_EQ(encoded.alp_info().num_exceptions(), 0);

  std::vector<TypeParam> output(input.size());
  DecodeEncodedVector(encoded, &output);
  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

// A wide random range plus a few extremes, so values land on both sides of the
// ALP window and the exception path gets used.
TYPED_TEST(AlpVectorTest, RandomAndExtremes) {
  std::mt19937 rng(12345);
  std::uniform_real_distribution<TypeParam> dist(static_cast<TypeParam>(-1e20),
                                                 static_cast<TypeParam>(1e20));

  std::vector<TypeParam> input(1024);
  for (auto& v : input) {
    v = dist(rng);
  }

  const std::array<TypeParam, 10> extremes = {
      std::numeric_limits<TypeParam>::lowest(),
      std::numeric_limits<TypeParam>::max(),
      std::numeric_limits<TypeParam>::min(),
      std::numeric_limits<TypeParam>::denorm_min(),
      static_cast<TypeParam>(0.0),
      static_cast<TypeParam>(-0.0),
      static_cast<TypeParam>(1e-30),
      static_cast<TypeParam>(-1e30),
      static_cast<TypeParam>(1.234567890123456789),
      static_cast<TypeParam>(0.1) + static_cast<TypeParam>(0.2),
  };
  for (size_t i = 0; i < extremes.size(); ++i) {
    input[i * 64] = extremes[i];
  }

  RoundTripVector(input);
}

// Sizes below the 8-element batch, so only the scalar tail runs.
TYPED_TEST(AlpVectorTest, SmallInputs) {
  const std::vector<TypeParam> one = {static_cast<TypeParam>(42.5)};
  const std::vector<TypeParam> two = {static_cast<TypeParam>(1.5),
                                      static_cast<TypeParam>(2.5)};

  for (const auto* input : {&one, &two}) {
    RoundTripVector(*input);
  }
}

// Special Values Tests

TYPED_TEST(AlpVectorTest, NegativeZero) {
  // -0.0 should be preserved bit-exactly
  std::vector<TypeParam> input(100);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = (i % 2 == 0) ? static_cast<TypeParam>(0.0) : static_cast<TypeParam>(-0.0);
  }
  RoundTripVector(input);
}

// Every value is an exception, so nothing is bit-packed.
TYPED_TEST(AlpVectorTest, AllExceptions) {
  std::vector<TypeParam> nans(64, std::numeric_limits<TypeParam>::quiet_NaN());
  std::vector<TypeParam> infinities(64);
  for (size_t i = 0; i < infinities.size(); ++i) {
    infinities[i] = (i % 2 == 0) ? std::numeric_limits<TypeParam>::infinity()
                                 : -std::numeric_limits<TypeParam>::infinity();
  }

  for (const auto* input : {&nans, &infinities}) {
    RoundTripVector(*input);
  }
}

TYPED_TEST(AlpVectorTest, MixedCompressibleAndExceptions) {
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
  RoundTripVector(input);
}

// FLOAT vectors can be decoded straight into double. Finite floats are exactly
// representable as double, so the result must match bit for bit.
TEST(AlpVectorTest, WideningDecode) {
  std::vector<float> input(256);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<float>(i) * 0.5f;
  }

  const auto encoded =
      AlpCompression<float>::Compress(input, AlpEncodingPreset::MakeDefault());

  std::vector<double> output(input.size());
  DecodeEncodedVector(encoded, &output);

  EXPECT_TRUE(IsBitwiseEqual(output, std::vector<double>(input.begin(), input.end())));
}

// Extremes of the floating type, the subnormal ramp just above zero, and the
// bounds of the integer type the encoder scales into.
TYPED_TEST(AlpVectorTest, BoundaryValues) {
  using Exact = typename AlpTypedConstants<TypeParam>::EncodedSigned;
  constexpr auto kExactMax = std::numeric_limits<Exact>::max();
  constexpr auto kExactMin = std::numeric_limits<Exact>::lowest();

  // A ramp of subnormals: the smallest magnitudes the encoder ever sees.
  std::vector<TypeParam> subnormal_ramp(100);
  const TypeParam smallest = std::numeric_limits<TypeParam>::denorm_min();
  for (size_t i = 0; i < subnormal_ramp.size(); ++i) {
    subnormal_ramp[i] = smallest * static_cast<TypeParam>(i + 1);
  }

  const std::vector<std::vector<TypeParam>> inputs = {
      std::move(subnormal_ramp),
      {std::numeric_limits<TypeParam>::max(), std::numeric_limits<TypeParam>::min(),
       std::numeric_limits<TypeParam>::lowest(),
       std::numeric_limits<TypeParam>::denorm_min(),
       std::numeric_limits<TypeParam>::epsilon(), -std::numeric_limits<TypeParam>::max(),
       -std::numeric_limits<TypeParam>::min(),
       -std::numeric_limits<TypeParam>::denorm_min(),
       -std::numeric_limits<TypeParam>::epsilon(), static_cast<TypeParam>(0.0)},
      // At the integer bounds and one representable step past each. The encoder
      // must not overflow while deciding, and decode must return the input bits.
      {static_cast<TypeParam>(0), static_cast<TypeParam>(1), static_cast<TypeParam>(-1),
       static_cast<TypeParam>(kExactMax), static_cast<TypeParam>(kExactMin),
       std::nextafter(static_cast<TypeParam>(kExactMax),
                      std::numeric_limits<TypeParam>::infinity()),
       std::nextafter(static_cast<TypeParam>(kExactMin),
                      -std::numeric_limits<TypeParam>::infinity()),
       std::numeric_limits<TypeParam>::max(), std::numeric_limits<TypeParam>::lowest()},
  };

  for (const auto& input : inputs) {
    RoundTripVector(input);
  }
}

// Bit-Width Edge Cases Tests

TYPED_TEST(AlpVectorTest, ZeroBitWidth) {
  // All identical values should result in bit_width=0
  std::vector<TypeParam> input(1024);
  std::fill(input.begin(), input.end(), static_cast<TypeParam>(123.456));

  auto preset = AlpEncodingPreset::MakeDefault();
  auto encoded = AlpCompression<TypeParam>::Compress(input, preset);

  // bit_width should be 0 for constant values
  EXPECT_EQ(encoded.for_info().bit_width(), 0);

  // Verify round-trip
  std::vector<TypeParam> output(input.size());
  DecodeEncodedVector(encoded, &output);
  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

TYPED_TEST(AlpVectorTest, SmallBitWidths) {
  for (uint8_t bit_width = 1; bit_width <= 8; ++bit_width) {
    SCOPED_TRACE("bit_width=" + std::to_string(bit_width));
    std::vector<TypeParam> input(1024);
    for (size_t i = 0; i < input.size(); ++i) {
      input[i] = static_cast<TypeParam>(
          1000 + static_cast<int64_t>(i % (size_t{1} << bit_width)));
    }

    const auto encoded =
        AlpCompression<TypeParam>::Compress(input, AlpEncodingPreset::MakeDefault());

    EXPECT_EQ(encoded.alp_info().num_exceptions(), 0);
    EXPECT_EQ(encoded.for_info().bit_width(), bit_width);

    std::vector<TypeParam> output(input.size());
    DecodeEncodedVector(encoded, &output);
    EXPECT_TRUE(IsBitwiseEqual(output, input));
  }
}

// The widest frame-of-reference range the encoded integer type can hold: the two
// values sit just inside the limits, so they must be encodable, and the range
// between them must need every bit of the type.
TYPED_TEST(AlpVectorTest, FullWidthFor) {
  std::vector<TypeParam> input(2);
  if constexpr (std::is_same_v<TypeParam, float>) {
    // 2^31 - 2^8, exactly representable as float.
    input = {-2147483392.0f, 2147483392.0f};
  } else {
    // 2^63 - 2^32, exactly representable as double.
    input = {-9223372032559808512.0, 9223372032559808512.0};
  }

  const auto encoded =
      AlpCompression<TypeParam>::Compress(input, AlpEncodingPreset::MakeDefault());

  EXPECT_EQ(encoded.alp_info().num_exceptions(), 0);
  EXPECT_EQ(encoded.for_info().bit_width(), static_cast<uint8_t>(sizeof(TypeParam) * 8));

  std::vector<TypeParam> output(input.size());
  DecodeEncodedVector(encoded, &output);
  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

TYPED_TEST(AlpVectorTest, EmptyInput) {
  const std::vector<TypeParam> input;
  const auto preset = AlpEncodingPreset::MakeDefault();
  const auto encoded = AlpCompression<TypeParam>::Compress(input, preset);

  EXPECT_EQ(encoded.num_elements(), 0);
  EXPECT_EQ(encoded.alp_info().num_exceptions(), 0);
  EXPECT_EQ(encoded.packed_values().size(), 0);
  EXPECT_EQ(encoded.exceptions().size(), 0);
  EXPECT_EQ(encoded.exception_positions().size(), 0);

  std::vector<TypeParam> output;
  DecodeEncodedVector(encoded, &output);
  EXPECT_TRUE(output.empty());
}

// A maximum-size vector where every value is an exception has num_exceptions
// == 32768, the largest value the uint16_t count can hold.
TYPED_TEST(AlpVectorTest, AllExceptionsAtMaxVectorSize) {
  constexpr int32_t kMaxVectorSize = 1 << AlpFormatConstants::kMaxLogVectorSize;
  static_assert(kMaxVectorSize == 32768, "expected a 32768-element max vector");
  ASSERT_GT(kMaxVectorSize, std::numeric_limits<int16_t>::max());

  // Every NaN is an exception: NaN != NaN, so decode(encode(v)) never
  // compares equal to the input and the encoder must take the fallback path.
  const std::vector<TypeParam> input(kMaxVectorSize,
                                     std::numeric_limits<TypeParam>::quiet_NaN());

  auto preset = AlpEncodingPreset::MakeDefault();
  auto encoded = AlpCompression<TypeParam>::Compress(input, preset);

  // The count must survive as 32768, not wrap negative.
  EXPECT_EQ(encoded.alp_info().num_exceptions(), kMaxVectorSize);
  EXPECT_EQ(encoded.exception_positions().size(), static_cast<size_t>(kMaxVectorSize));
  EXPECT_GT(encoded.GetStoredSize(), 0);

  std::vector<TypeParam> output(input.size());
  DecodeEncodedVector(encoded, &output);
  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

// An exception at the last index of a maximum-size vector stores position
// 32767, the largest value an exception position can hold.
TYPED_TEST(AlpVectorTest, ExceptionAtMaxPosition) {
  constexpr int32_t kMaxVectorSize = 1 << AlpFormatConstants::kMaxLogVectorSize;
  // Whole numbers so that every value but the last encodes exactly, leaving
  // exactly one exception, at the highest index a position can name.
  std::vector<TypeParam> input(kMaxVectorSize);
  for (int32_t i = 0; i < kMaxVectorSize; ++i) {
    input[i] = static_cast<TypeParam>(i);
  }
  input.back() = std::numeric_limits<TypeParam>::quiet_NaN();

  auto preset = AlpEncodingPreset::MakeDefault();
  auto encoded = AlpCompression<TypeParam>::Compress(input, preset);

  ASSERT_EQ(encoded.alp_info().num_exceptions(), 1);
  EXPECT_EQ(encoded.exception_positions().front(), kMaxVectorSize - 1);

  std::vector<TypeParam> output(input.size());
  DecodeEncodedVector(encoded, &output);
  EXPECT_TRUE(IsBitwiseEqual(output, input));
}

// AlpEncodedVector Tests

template <typename T>
class AlpEncodedVectorTest : public ::testing::Test {};

TYPED_TEST_SUITE(AlpEncodedVectorTest, FloatingTestTypes);

// Store then Load has to hand back the same metadata and the same bytes.
TYPED_TEST(AlpEncodedVectorTest, StoreLoadRoundTrip) {
  // Every eighth value is a NaN, so the exception arrays are non-empty and carry
  // NaN bit patterns.
  std::vector<TypeParam> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = (i % 8 == 0) ? std::numeric_limits<TypeParam>::quiet_NaN()
                            : static_cast<TypeParam>(i) * static_cast<TypeParam>(0.25);
  }

  const auto encoded =
      AlpCompression<TypeParam>::Compress(input, AlpEncodingPreset::MakeDefault());
  ASSERT_GT(encoded.alp_info().num_exceptions(), 0);

  std::vector<uint8_t> buffer(static_cast<size_t>(encoded.GetStoredSize()));
  encoded.Store(buffer);

  ASSERT_OK_AND_ASSIGN(auto view, AlpEncodedVectorView<TypeParam>::Load(
                                      buffer, static_cast<uint16_t>(input.size()),
                                      arrow::default_memory_pool()));
  EXPECT_EQ(view.alp_info(), encoded.alp_info());
  EXPECT_EQ(view.for_info(), encoded.for_info());
  EXPECT_EQ(view.num_elements(), encoded.num_elements());
  EXPECT_EQ(view.GetDataStoredSize(), encoded.GetDataStoredSize());

  // Bit-exact, so NaN exception values have to survive too.
  ASSERT_EQ(view.packed_values().size(), encoded.packed_values().size());
  EXPECT_EQ(std::memcmp(view.packed_values().data(), encoded.packed_values().data(),
                        view.packed_values().size()),
            0);
  ASSERT_EQ(view.exception_positions().size(), encoded.exception_positions().size());
  EXPECT_EQ(
      std::memcmp(
          view.exception_positions().data(), encoded.exception_positions().data(),
          view.exception_positions().size() * sizeof(AlpFormatConstants::PositionType)),
      0);
  ASSERT_EQ(view.exceptions().size(), encoded.exceptions().size());
  EXPECT_EQ(std::memcmp(view.exceptions().data(), encoded.exceptions().data(),
                        view.exceptions().size() * sizeof(TypeParam)),
            0);
}

// The exception arrays follow the packed values, so an odd bit_packed_size or an
// odd buffer start misaligns them. The view copies them into aligned storage.
TYPED_TEST(AlpEncodedVectorTest, ViewLoad) {
  const auto preset = AlpEncodingPreset::MakeDefault();

  // A full vector with NaN and Inf exceptions, and a short one whose packed size
  // can be odd.
  std::vector<TypeParam> long_input(64);
  for (size_t i = 0; i < long_input.size(); ++i) {
    if (i % 10 == 0) {
      long_input[i] = std::numeric_limits<TypeParam>::quiet_NaN();
    } else if (i % 10 == 5) {
      long_input[i] = std::numeric_limits<TypeParam>::infinity();
    } else {
      long_input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
    }
  }
  std::vector<TypeParam> short_input = {static_cast<TypeParam>(1.0),
                                        static_cast<TypeParam>(2.0),
                                        static_cast<TypeParam>(3.0),
                                        std::numeric_limits<TypeParam>::quiet_NaN(),
                                        static_cast<TypeParam>(5.0),
                                        static_cast<TypeParam>(6.0),
                                        std::numeric_limits<TypeParam>::infinity()};

  for (const auto* input : {&long_input, &short_input}) {
    const auto encoded = AlpCompression<TypeParam>::Compress(*input, preset);
    ASSERT_GT(encoded.alp_info().num_exceptions(), 0);

    const auto stored_size = static_cast<size_t>(encoded.GetStoredSize());
    std::vector<uint8_t> padded_buffer(stored_size + 8);
    std::vector<TypeParam> output(input->size());
    std::vector<typename AlpCompression<TypeParam>::EncodedUnsigned> unpacked(
        input->size());

    // Every start offset, to cover all alignments.
    for (size_t offset = 0; offset < 8; ++offset) {
      SCOPED_TRACE("num_elements=" + std::to_string(input->size()) +
                   " offset=" + std::to_string(offset));

      uint8_t* buffer_start = padded_buffer.data() + offset;
      encoded.Store({buffer_start, stored_size});

      ASSERT_OK_AND_ASSIGN(auto view, AlpEncodedVectorView<TypeParam>::Load(
                                          {buffer_start, stored_size},
                                          static_cast<uint16_t>(input->size()),
                                          arrow::default_memory_pool()));

      AlpCompression<TypeParam>::Decompress(view, std::span<TypeParam>(output), unpacked);
      EXPECT_TRUE(IsBitwiseEqual(output, *input));
    }
  }
}

// AlpCodec Tests

template <typename T>
class AlpCodecTest : public ::testing::Test {};

TYPED_TEST_SUITE(AlpCodecTest, FloatingTestTypes);

TYPED_TEST(AlpCodecTest, DecodesVectorsInOrder) {
  constexpr int32_t kVectorSize = 64;
  std::vector<TypeParam> input(3 * kVectorSize);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.25);
  }

  ASSERT_OK_AND_ASSIGN(int64_t max_comp_size,
                       AlpCodec<TypeParam>::GetMaxCompressedSize(
                           static_cast<int64_t>(input.size()), kVectorSize));
  std::vector<uint8_t> buffer(max_comp_size);
  ASSERT_OK_AND_ASSIGN(const int64_t comp_size,
                       AlpCodec<TypeParam>::Encode(input, kVectorSize, buffer));

  ASSERT_OK_AND_ASSIGN(auto reader,
                       AlpVectorReader<TypeParam>::Open(
                           std::span(buffer).first(static_cast<size_t>(comp_size)),
                           arrow::default_memory_pool()));
  ASSERT_EQ(reader.num_vectors(), 3);

  for (int32_t i = 0; i < reader.num_vectors(); ++i) {
    SCOPED_TRACE(i);
    ASSERT_OK_AND_ASSIGN(const int32_t vector_length, reader.VectorLength(i));

    std::vector<TypeParam> output(static_cast<size_t>(vector_length));
    ASSERT_OK(reader.Decode(i, std::span(output)));

    const auto expected = std::vector<TypeParam>(
        input.begin() + i * kVectorSize, input.begin() + i * kVectorSize + vector_length);
    EXPECT_TRUE(IsBitwiseEqual(output, expected));
  }
}

TYPED_TEST(AlpCodecTest, RejectsInvalidVectorSize) {
  std::vector<TypeParam> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }
  std::vector<uint8_t> buffer(4096);
  const int64_t num_elements = static_cast<int64_t>(input.size());

  for (const int32_t vector_size : {0, 1, 2, 3, 4, 1 << 16}) {
    SCOPED_TRACE("vector_size=" + std::to_string(vector_size));
    ASSERT_RAISES(Invalid, AlpCodec<TypeParam>::Encode(input, vector_size, buffer));
    ASSERT_RAISES(Invalid,
                  AlpCodec<TypeParam>::GetMaxCompressedSize(num_elements, vector_size));
  }
}

// Preset/Sampling Tests

template <typename T>
class AlpSamplerTest : public ::testing::Test {};

TYPED_TEST_SUITE(AlpSamplerTest, FloatingTestTypes);

TYPED_TEST(AlpSamplerTest, PresetGenerationDecimalData) {
  AlpSampler<TypeParam> sampler;

  std::vector<TypeParam> data(10000);
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<TypeParam>(100.0 + i * 0.01);
  }
  sampler.AddSample(data);

  constexpr size_t kNumValues = 1024;
  const auto encoded = AlpCompression<TypeParam>::Compress(
      std::span(data).first(kNumValues), sampler.MakePreset());

  std::vector<TypeParam> output(kNumValues);
  DecodeEncodedVector(encoded, &output);

  EXPECT_TRUE(IsBitwiseEqual(
      output, std::vector<TypeParam>(data.begin(), data.begin() + kNumValues)));
}

TYPED_TEST(AlpSamplerTest, NoUsableSamplesFallBackToDefault) {
  const std::vector<AlpExponentAndFactor> identity =
      AlpEncodingPreset::MakeDefault().combinations;

  AlpSampler<TypeParam> empty_sampler;
  EXPECT_EQ(empty_sampler.MakePreset().combinations, identity);

  const std::vector<TypeParam> unusable(8, std::numeric_limits<TypeParam>::quiet_NaN());
  EXPECT_EQ(AlpCompression<TypeParam>::MakePreset({unusable}).combinations, identity);
}

// A sample with too few encodable values cannot produce a candidate, so it must
// not change the preset.
TYPED_TEST(AlpSamplerTest, IgnoresUnusableSamples) {
  const std::vector<TypeParam> usable = {
      static_cast<TypeParam>(1.25), static_cast<TypeParam>(2.5),
      static_cast<TypeParam>(3.75), static_cast<TypeParam>(5.0),
      static_cast<TypeParam>(6.25)};
  const std::vector<TypeParam> unusable(8, std::numeric_limits<TypeParam>::quiet_NaN());

  const AlpEncodingPreset only_usable = AlpCompression<TypeParam>::MakePreset({usable});
  const AlpEncodingPreset with_unusable =
      AlpCompression<TypeParam>::MakePreset({unusable, usable});

  EXPECT_EQ(with_unusable.combinations, only_usable.combinations);
  EXPECT_EQ(with_unusable.estimated_compressed_size_bytes,
            only_usable.estimated_compressed_size_bytes);
}

// Samples that need different decimal scales must all survive the vote, so the
// per-vector search has several candidates to pick from.
TYPED_TEST(AlpSamplerTest, KeepsSeveralCombinations) {
  std::vector<TypeParam> tenths(256), thousandths(256);
  for (size_t i = 0; i < tenths.size(); ++i) {
    tenths[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
    thousandths[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.001);
  }

  const AlpEncodingPreset preset =
      AlpCompression<TypeParam>::MakePreset({tenths, thousandths});

  EXPECT_GT(preset.combinations.size(), 1);

  // The surviving candidates are still usable: the preset stays lossless.
  const auto encoded = AlpCompression<TypeParam>::Compress(tenths, preset);
  std::vector<TypeParam> output(tenths.size());
  DecodeEncodedVector(encoded, &output);
  EXPECT_TRUE(IsBitwiseEqual(output, tenths));
}

// Corrupted Data Handling Tests
//
// Decoding invalid or corrupted data must return a status, never crash.
template <typename T>
class AlpRobustnessTest : public ::testing::Test {};

TYPED_TEST_SUITE(AlpRobustnessTest, FloatingTestTypes);

TEST(AlpRobustnessTest, RejectsTruncatedCompactVectorMetadata) {
  constexpr size_t kMetadataSize = AlpInfo::kStoredSize + AlpForInfo<double>::kStoredSize;
  for (size_t size = 0; size < kMetadataSize; ++size) {
    SCOPED_TRACE(size);
    std::vector<uint8_t> buffer(size);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("too small for metadata"),
        AlpEncodedVectorView<double>::Load({buffer.data(), buffer.size()}, 1,
                                           arrow::default_memory_pool()));
  }

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("must be non-negative"),
      AlpEncodedVectorView<double>::Load({}, -1, arrow::default_memory_pool()));
}

TEST(AlpRobustnessTest, TruncatedData) {
  // Opening a buffer cut short of the compressed size must fail, not crash and
  // not return a partial page.
  std::vector<double> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<double>(i) * 0.123;
  }

  const int64_t num_elements = static_cast<int64_t>(input.size());
  ASSERT_OK_AND_ASSIGN(int64_t max_size,
                       AlpCodec<double>::GetMaxCompressedSize(
                           num_elements, AlpFormatConstants::kDefaultVectorSize));
  std::vector<uint8_t> buffer(max_size);
  ASSERT_OK_AND_ASSIGN(
      const int64_t comp_size,
      AlpCodec<double>::Encode(input, AlpFormatConstants::kDefaultVectorSize, buffer));

  const auto page = std::span(buffer).first(static_cast<size_t>(comp_size));
  ASSERT_OK(AlpVectorReader<double>::Open(page, arrow::default_memory_pool()));

  // Cut at several points, to hit the header, the offset table and the vector
  // body.
  for (int64_t truncated_size :
       {int64_t{0}, int64_t{3}, comp_size / 4, comp_size / 2, comp_size - 1}) {
    if (truncated_size >= comp_size) continue;
    SCOPED_TRACE("truncated_size=" + std::to_string(truncated_size));
    // Which guard fires depends on the section the cut lands in.
    ASSERT_RAISES(Invalid, AlpVectorReader<double>::Open(
                               page.first(static_cast<size_t>(truncated_size)),
                               arrow::default_memory_pool()));
  }
}

TEST(AlpRobustnessTest, HeaderElementCountMismatch) {
  std::vector<double> input(1024);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<double>(i) * 0.123;
  }
  const int64_t num_elements = static_cast<int64_t>(input.size());
  ASSERT_OK_AND_ASSIGN(int64_t max_size,
                       AlpCodec<double>::GetMaxCompressedSize(
                           num_elements, AlpFormatConstants::kDefaultVectorSize));
  std::vector<uint8_t> buffer(max_size);
  ASSERT_OK_AND_ASSIGN(
      const int64_t comp_size,
      AlpCodec<double>::Encode(input, AlpFormatConstants::kDefaultVectorSize, buffer));

  const auto page = std::span(buffer).first(static_cast<size_t>(comp_size));
  ASSERT_OK_AND_ASSIGN(auto reader,
                       AlpVectorReader<double>::Open(page, arrow::default_memory_pool()));
  EXPECT_EQ(reader.num_elements(), 1024);

  // num_elements sits at byte 3 of the header.
  for (int32_t corrupt_count : {int32_t{0}, int32_t{512}, int32_t{2048}}) {
    SCOPED_TRACE("corrupt_count=" + std::to_string(corrupt_count));
    std::vector<uint8_t> corrupted(buffer.begin(), buffer.begin() + comp_size);
    util::SafeStore(corrupted.data() + 3, bit_util::ToLittleEndian(corrupt_count));
    ASSERT_RAISES(Invalid, AlpVectorReader<double>::Open(std::span(corrupted),
                                                         arrow::default_memory_pool()));
  }
}

TEST(AlpRobustnessTest, CorruptedOffsetChain) {
  // The spec fixes every offset, so a duplicate, backward or gapped offset has to
  // be rejected rather than used to read the wrong bytes.
  constexpr int32_t kNumElements = 4 * AlpFormatConstants::kDefaultVectorSize;
  std::vector<double> input(kNumElements);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<double>(i) * 0.123;
  }
  ASSERT_OK_AND_ASSIGN(int64_t max_size,
                       AlpCodec<double>::GetMaxCompressedSize(
                           kNumElements, AlpFormatConstants::kDefaultVectorSize));
  std::vector<uint8_t> buffer(max_size);
  ASSERT_OK_AND_ASSIGN(
      const int64_t comp_size,
      AlpCodec<double>::Encode(input, AlpFormatConstants::kDefaultVectorSize, buffer));
  const auto page = std::span(buffer).first(static_cast<size_t>(comp_size));
  ASSERT_OK(AlpVectorReader<double>::Open(page, arrow::default_memory_pool()));

  // The offsets follow the 7-byte header, one uint32 per vector.
  constexpr int64_t kOffsetsStart = 7;
  using OffsetType = AlpFormatConstants::OffsetType;
  const auto read_offset = [&](int i) {
    return bit_util::FromLittleEndian(util::SafeLoadAs<OffsetType>(
        buffer.data() + kOffsetsStart + i * sizeof(OffsetType)));
  };
  const OffsetType offset0 = read_offset(0);
  const OffsetType offset1 = read_offset(1);
  const OffsetType offset2 = read_offset(2);
  ASSERT_EQ(offset0, 4 * sizeof(OffsetType));
  ASSERT_LT(offset0, offset1);

  // Each corruption keeps every offset inside the buffer, so only the chain rule
  // can reject it.
  const std::vector<std::pair<const char*, OffsetType>> corruptions = {
      {"duplicate", offset0},
      {"backward", static_cast<OffsetType>(offset0 + 1)},
      {"gapped", static_cast<OffsetType>(offset1 + 4)},
      {"skips a vector", offset2},
  };
  for (const auto& [name, corrupt_offset] : corruptions) {
    SCOPED_TRACE(name);
    std::vector<uint8_t> corrupted(buffer.begin(), buffer.begin() + comp_size);
    util::SafeStore(corrupted.data() + kOffsetsStart + sizeof(OffsetType),
                    bit_util::ToLittleEndian(corrupt_offset));
    ASSERT_LT(corrupt_offset, comp_size);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("previous vector ends at"),
        AlpVectorReader<double>::Open(std::span(corrupted),
                                      arrow::default_memory_pool()));
  }
}

TEST(AlpRobustnessTest, ElementCountAboveInt32Max) {
  // The header stores the count as int32, so a larger count has to be refused
  // before it is used to form a span over the input.
  constexpr int64_t kTooMany = int64_t{std::numeric_limits<int32_t>::max()} + 1;
  double one_value = 1.0;
  uint8_t output_byte = 0;

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("exceeds INT32_MAX"),
                                  AlpCodec<double>::GetMaxCompressedSize(
                                      kTooMany, AlpFormatConstants::kDefaultVectorSize));
  // The count is rejected before the input span is read.
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("exceeds INT32_MAX"),
      AlpCodec<double>::Encode({&one_value, static_cast<size_t>(kTooMany)},
                               AlpFormatConstants::kDefaultVectorSize,
                               {&output_byte, 1}));
}

// A page header carrying an out-of-range log_vector_size must be rejected at
// load time rather than trusted.
TYPED_TEST(AlpRobustnessTest, RejectsOutOfRangeLogVectorSizeInHeader) {
  std::vector<TypeParam> input(64);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<TypeParam>(i) * static_cast<TypeParam>(0.1);
  }

  ASSERT_OK_AND_ASSIGN(
      int64_t max_comp_size,
      AlpCodec<TypeParam>::GetMaxCompressedSize(static_cast<int64_t>(input.size()),
                                                AlpFormatConstants::kDefaultVectorSize));
  std::vector<uint8_t> comp_buffer(max_comp_size);
  ASSERT_OK_AND_ASSIGN(const int64_t comp_size,
                       AlpCodec<TypeParam>::Encode(
                           input, AlpFormatConstants::kDefaultVectorSize, comp_buffer));

  for (const uint8_t bad : {uint8_t{0}, uint8_t{1}, uint8_t{2}, uint8_t{16}}) {
    SCOPED_TRACE("log_vector_size=" + std::to_string(bad));
    std::vector<uint8_t> corrupted(comp_buffer.begin(), comp_buffer.begin() + comp_size);
    corrupted[2] = bad;
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("log_vector_size"),
        AlpVectorReader<TypeParam>::Open(
            std::span(corrupted).first(static_cast<size_t>(comp_size)),
            arrow::default_memory_pool()));
  }
}

// A page written by a future ALP variant carries a compression_mode this reader
// does not know.
TYPED_TEST(AlpRobustnessTest, RejectsUnsupportedCompressionModeInHeader) {
  std::vector<TypeParam> input;
  const std::vector<uint8_t> comp_buffer = EncodeSmallPage(&input);

  // compression_mode is byte 0 of the page header.
  for (const uint8_t bad : {uint8_t{1}, uint8_t{2}, uint8_t{255}}) {
    SCOPED_TRACE("compression_mode=" + std::to_string(bad));
    std::vector<uint8_t> corrupted = comp_buffer;
    corrupted[0] = bad;
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("unsupported compression mode"),
        AlpVectorReader<TypeParam>::Open(std::span(corrupted).first(static_cast<size_t>(
                                             static_cast<int64_t>(corrupted.size()))),
                                         arrow::default_memory_pool()));
  }
}

// The same guarantee for the integer encoding field. A later check also rejects
// an unknown value, so this test pins the header-level check specifically.
TYPED_TEST(AlpRobustnessTest, RejectsUnsupportedIntegerEncodingInHeader) {
  std::vector<TypeParam> input;
  const std::vector<uint8_t> comp_buffer = EncodeSmallPage(&input);

  // integer_encoding is byte 1 of the page header.
  for (const uint8_t bad : {uint8_t{1}, uint8_t{2}, uint8_t{255}}) {
    SCOPED_TRACE("integer_encoding=" + std::to_string(bad));
    std::vector<uint8_t> corrupted = comp_buffer;
    corrupted[1] = bad;
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("unsupported integer encoding"),
        AlpVectorReader<TypeParam>::Open(std::span(corrupted).first(static_cast<size_t>(
                                             static_cast<int64_t>(corrupted.size()))),
                                         arrow::default_memory_pool()));
  }
}

// A bit_width wider than the encoded integer type is malformed. A later
// buffer-size check also rejects it, so this test pins the early check.
TYPED_TEST(AlpRobustnessTest, RejectsOutOfRangeForBitWidth) {
  std::vector<TypeParam> input;
  const std::vector<uint8_t> buffer = EncodeSmallVector(&input);

  // bit_width is the last byte of ForInfo.
  const int64_t bit_width_pos =
      AlpInfo::kStoredSize + AlpForInfo<TypeParam>::kStoredSize - 1;

  using EncodedUnsigned = typename AlpForInfo<TypeParam>::FrameType;
  constexpr uint8_t kMaxBitWidth = sizeof(EncodedUnsigned) * 8;
  for (const uint8_t bad : {static_cast<uint8_t>(kMaxBitWidth + 1), uint8_t{255}}) {
    SCOPED_TRACE("bit_width=" + std::to_string(bad));
    std::vector<uint8_t> corrupted = buffer;
    corrupted[bit_width_pos] = bad;
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("bit_width out of range"),
        AlpEncodedVectorView<TypeParam>::Load({corrupted.data(), corrupted.size()},
                                              static_cast<int32_t>(input.size()),
                                              arrow::default_memory_pool()));
  }
}

// exponent and factor index the power-of-ten tables, whose bounds are only
// ARROW_DCHECKed. The range is [0, 10] for FLOAT and [0, 18] for DOUBLE.
TYPED_TEST(AlpRobustnessTest, RejectsOutOfRangeExponent) {
  std::vector<TypeParam> input;
  const std::vector<uint8_t> buffer = EncodeSmallVector(&input);

  // exponent is the first byte of AlpInfo, factor the second.
  constexpr uint8_t kMaxExponent = AlpTypedConstants<TypeParam>::kMaxExponent;
  for (const uint8_t bad : {static_cast<uint8_t>(kMaxExponent + 1), uint8_t{255}}) {
    SCOPED_TRACE("exponent=" + std::to_string(bad));
    std::vector<uint8_t> corrupted = buffer;
    corrupted[0] = bad;
    // Leave factor at 0 so the exponent is the only field out of range.
    corrupted[1] = 0;
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("ALP exponent"),
        AlpEncodedVectorView<TypeParam>::Load({corrupted.data(), corrupted.size()},
                                              static_cast<int32_t>(input.size()),
                                              arrow::default_memory_pool()));
  }
}

// The factor must be in [0, exponent]. A larger factor would look up a negative
// power of ten.
TYPED_TEST(AlpRobustnessTest, RejectsFactorAboveExponent) {
  std::vector<TypeParam> input;
  const std::vector<uint8_t> buffer = EncodeSmallVector(&input);

  const uint8_t exponent = buffer[0];
  ASSERT_LE(exponent, AlpTypedConstants<TypeParam>::kMaxExponent);

  std::vector<uint8_t> corrupted = buffer;
  corrupted[1] = static_cast<uint8_t>(exponent + 1);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("ALP factor"),
      AlpEncodedVectorView<TypeParam>::Load({corrupted.data(), corrupted.size()},
                                            static_cast<int32_t>(input.size()),
                                            arrow::default_memory_pool()));
}

// num_exceptions sizes the patch loop, which writes into an output of
// num_elements slots, so a count above the vector length is malformed.
TYPED_TEST(AlpRobustnessTest, RejectsNumExceptionsAboveVectorLength) {
  std::vector<TypeParam> input;
  std::vector<uint8_t> corrupted = EncodeSmallVector(&input);

  const uint16_t bad = static_cast<uint16_t>(input.size() + 1);
  ASSERT_OK_AND_ASSIGN(AlpInfo alp_info, AlpInfo::Load(corrupted));
  alp_info.SetNumExceptions(bad);
  alp_info.Store(corrupted);

  // Pad the vector so it is not rejected as truncated before the count check; the
  // padding reads back as position 0, which is in range.
  corrupted.resize(corrupted.size() + bad * (sizeof(AlpFormatConstants::PositionType) +
                                             sizeof(TypeParam)),
                   0);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("exceptions but only"),
      AlpEncodedVectorView<TypeParam>::Load({corrupted.data(), corrupted.size()},
                                            static_cast<int32_t>(input.size()),
                                            arrow::default_memory_pool()));
}

// The patch step writes output[position] once per exception, so repeated
// positions have to be rejected.
TYPED_TEST(AlpRobustnessTest, RejectsDuplicateExceptionPositions) {
  constexpr int32_t kNumElements = 64;
  std::vector<TypeParam> input(kNumElements);
  for (int32_t i = 0; i < kNumElements; ++i) {
    input[i] = static_cast<TypeParam>(i);
  }
  // Whole numbers encode exactly, so only these two NaNs are exceptions.
  input[5] = std::numeric_limits<TypeParam>::quiet_NaN();
  input[40] = std::numeric_limits<TypeParam>::quiet_NaN();

  const auto encoded =
      AlpCompression<TypeParam>::Compress(input, AlpEncodingPreset::MakeDefault());
  std::vector<uint8_t> comp_buffer(static_cast<size_t>(encoded.GetStoredSize()));
  encoded.Store(comp_buffer);

  ASSERT_OK_AND_ASSIGN(const AlpInfo alp_info, AlpInfo::Load(comp_buffer));
  ASSERT_GE(alp_info.num_exceptions(), 2);

  constexpr int64_t kForInfoSize = AlpForInfo<TypeParam>::kStoredSize;
  const uint8_t bit_width = comp_buffer[AlpInfo::kStoredSize + kForInfoSize - 1];
  const int64_t position_pos = AlpInfo::kStoredSize + kForInfoSize +
                               bit_util::BytesForBits(int64_t{kNumElements} * bit_width);
  constexpr int64_t kPositionSize =
      static_cast<int64_t>(sizeof(AlpFormatConstants::PositionType));
  ASSERT_LE(position_pos + 2 * kPositionSize, static_cast<int64_t>(comp_buffer.size()));

  // Copy the second position over the first, so both name the same slot.
  std::vector<uint8_t> corrupted = comp_buffer;
  std::memcpy(corrupted.data() + position_pos,
              corrupted.data() + position_pos + kPositionSize, kPositionSize);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("exception positions must increase"),
      AlpEncodedVectorView<TypeParam>::Load({corrupted.data(), corrupted.size()},
                                            kNumElements, arrow::default_memory_pool()));
}

// The patch step writes output[position], so a position at or past the end of
// the vector is an out-of-bounds write into the caller's buffer.
TYPED_TEST(AlpRobustnessTest, RejectsExceptionPositionPastVector) {
  constexpr int32_t kNumElements = 64;
  std::vector<TypeParam> input(kNumElements);
  for (int32_t i = 0; i < kNumElements; ++i) {
    input[i] = static_cast<TypeParam>(i);
  }
  // Whole numbers encode exactly and NaN never does, so this vector carries
  // exactly one exception and its position is the last index.
  input.back() = std::numeric_limits<TypeParam>::quiet_NaN();

  const auto encoded =
      AlpCompression<TypeParam>::Compress(input, AlpEncodingPreset::MakeDefault());
  std::vector<uint8_t> comp_buffer(static_cast<size_t>(encoded.GetStoredSize()));
  encoded.Store(comp_buffer);

  ASSERT_OK_AND_ASSIGN(const AlpInfo alp_info, AlpInfo::Load(comp_buffer));
  ASSERT_EQ(alp_info.num_exceptions(), 1);

  // Positions follow the packed values, whose length comes from bit_width, the
  // last byte of ForInfo.
  constexpr int64_t kForInfoSize = AlpForInfo<TypeParam>::kStoredSize;
  const uint8_t bit_width = comp_buffer[AlpInfo::kStoredSize + kForInfoSize - 1];
  const int64_t position_pos = AlpInfo::kStoredSize + kForInfoSize +
                               bit_util::BytesForBits(int64_t{kNumElements} * bit_width);
  ASSERT_LE(position_pos + static_cast<int64_t>(sizeof(AlpFormatConstants::PositionType)),
            static_cast<int64_t>(comp_buffer.size()));

  for (const uint16_t bad : {static_cast<uint16_t>(kNumElements), uint16_t{65535}}) {
    SCOPED_TRACE("position=" + std::to_string(bad));
    std::vector<uint8_t> corrupted = comp_buffer;
    std::memcpy(corrupted.data() + position_pos, &bad, sizeof(bad));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("exception position"),
                                    AlpEncodedVectorView<TypeParam>::Load(
                                        {corrupted.data(), corrupted.size()},
                                        kNumElements, arrow::default_memory_pool()));
  }
}

}  // namespace arrow::util::alp
