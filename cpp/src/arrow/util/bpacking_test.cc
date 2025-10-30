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

#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/bpacking_scalar_internal.h"
#include "arrow/util/bpacking_simd_internal.h"

#if defined(ARROW_HAVE_RUNTIME_AVX2)
#  include "arrow/util/cpu_info.h"
#endif

namespace arrow::internal {

template <typename Int>
using UnpackFunc = void (*)(const uint8_t*, Int*, int, int, int);

/// Get the number of bytes associate with a packing.
int GetNumBytes(int num_values, int bit_width, int bit_offset) {
  return static_cast<int>(bit_util::BytesForBits(num_values * bit_width + bit_offset));
}

/// Generate random values that can be packed within the given bit width.
template <typename Uint>
std::vector<Uint> GenerateRandomValuesForPacking(int num_values, int bit_width) {
  constexpr uint32_t kSeed = 3214;

  num_values = std::max(1, num_values);  // We need a valid pointer for size 0
  std::vector<Uint> out(num_values);

  if (bit_width == 0) {
    return out;
  }

  if constexpr (std::is_same_v<Uint, bool>) {
    random_is_valid(num_values, 0.5, &out, kSeed);
  } else {
    const uint64_t max = bit_util::LeastSignificantBitMask<uint64_t, true>(bit_width);
    rand_uniform_int(out.size(), kSeed, /* min= */ decltype(max){0}, max, out.data());
  }
  return out;
}

/// Convenience wrapper to unpack into a vector
template <typename Int>
std::vector<Int> UnpackValues(const uint8_t* packed, int32_t num_values,
                              int32_t bit_width, int32_t bit_offset,
                              UnpackFunc<Int> unpack) {
  if constexpr (std::is_same_v<Int, bool>) {
    // Using dynamic array to avoid std::vector<bool>
    auto buffer = std::make_unique<Int[]>(num_values);
    unpack(packed, buffer.get(), num_values, bit_width, bit_offset);
    return std::vector<Int>(buffer.get(), buffer.get() + num_values);
  } else {
    std::vector<Int> out(num_values);
    unpack(packed, out.data(), num_values, bit_width, bit_offset);
    return out;
  }
}

/// Use BitWriter to pack values into a vector.
template <typename Int>
std::vector<uint8_t> PackValues(const std::vector<Int>& values, int num_values,
                                int bit_width, int bit_offset) {
  if (bit_width == 0) {
    return {};
  }

  const auto num_bytes = GetNumBytes(num_values, bit_width, bit_offset);

  std::vector<uint8_t> out(static_cast<std::size_t>(num_bytes));
  bit_util::BitWriter writer(out.data(), num_bytes);

  // Write a first 0 value to make an offset
  const bool written = writer.PutValue(0, bit_offset);
  ARROW_DCHECK(written);
  for (const auto& v : values) {
    const bool written = writer.PutValue(v, bit_width);
    ARROW_DCHECK(written);
  }

  writer.Flush();

  return out;
}

class TestUnpack : public ::testing::TestWithParam<int> {
 protected:
  template <typename Int>
  void TestRoundtripAlignment(UnpackFunc<Int> unpack, int num_values, int bit_width,
                              int bit_offset) {
    const auto original = GenerateRandomValuesForPacking<Int>(num_values, bit_width);
    const auto packed = PackValues(original, num_values, bit_width, bit_offset);
    const auto unpacked =
        UnpackValues(packed.data(), num_values, bit_width, bit_offset, unpack);
    EXPECT_EQ(unpacked.size(), num_values);
    EXPECT_EQ(original, unpacked);
  }

  template <typename Int>
  void TestUnpackZeros(UnpackFunc<Int> unpack, int num_values, int bit_width,
                       int bit_offset) {
    const auto num_bytes = GetNumBytes(num_values, bit_width, bit_offset);

    const std::vector<uint8_t> packed(static_cast<std::size_t>(num_bytes), uint8_t{0});
    const auto unpacked =
        UnpackValues(packed.data(), num_values, bit_width, bit_offset, unpack);

    const std::vector<Int> expected(static_cast<std::size_t>(num_values), Int{0});
    EXPECT_EQ(unpacked, expected);
  }

  template <typename Int>
  void TestUnpackOnes(UnpackFunc<Int> unpack, int num_values, int bit_width,
                      int bit_offset) {
    const auto num_bytes = GetNumBytes(num_values, bit_width, bit_offset);

    const std::vector<uint8_t> packed(static_cast<std::size_t>(num_bytes), uint8_t{0xFF});
    const auto unpacked =
        UnpackValues(packed.data(), num_values, bit_width, bit_offset, unpack);

    // Generate bit_width ones
    Int expected_value = 0;
    if constexpr (std::is_same_v<Int, bool>) {
      expected_value = static_cast<bool>(bit_width);
    } else {
      for (int i = 0; i < bit_width; ++i) {
        expected_value = (expected_value << 1) | 1;
      }
    }
    const std::vector<Int> expected(static_cast<std::size_t>(num_values), expected_value);
    EXPECT_EQ(unpacked, expected);
  }

  template <typename Int>
  void TestUnpackAlternating(UnpackFunc<Int> unpack, int num_values, int bit_width,
                             int bit_offset) {
    const auto num_bytes = GetNumBytes(num_values, bit_width, bit_offset);

    // Pick between two different bit patterns so that we always unpack starting with 1
    const uint8_t byte = bit_offset % 2 == 0 ? 0b10101010 : 0b01010101;
    const std::vector<uint8_t> packed(static_cast<std::size_t>(num_bytes), byte);
    const auto unpacked =
        UnpackValues(packed.data(), num_values, bit_width, bit_offset, unpack);

    // Generate alternative bit sequence starting with either 0 or 1
    Int one_zero_value = 0;
    Int zero_one_value = 0;
    for (int i = 0; i < bit_width; ++i) {
      zero_one_value = (zero_one_value << 1) | (i % 2);
      one_zero_value = (one_zero_value << 1) | ((i + 1) % 2);
    }

    std::vector<Int> expected;
    if (bit_width % 2 == 0) {
      // For even bit_width, the same pattern repeats every time
      expected.resize(static_cast<std::size_t>(num_values), one_zero_value);
    } else {
      // For odd bit_width, we alternate a pattern leading with 0 and 1
      for (int i = 0; i < num_values; ++i) {
        expected.push_back(i % 2 == 0 ? zero_one_value : one_zero_value);
      }
    }
    EXPECT_EQ(unpacked, expected);
  }

  template <typename Int>
  void TestAll(UnpackFunc<Int> unpack) {
    const int num_values_base = GetParam();

    constexpr int kMaxBitWidth = std::is_same_v<Int, bool> ? 1 : 8 * sizeof(Int);

    // Given how many edge cases there are in unpacking integers, it is best to test all
    // sizes
    for (int bit_width = 0; bit_width <= kMaxBitWidth; ++bit_width) {
      SCOPED_TRACE(::testing::Message() << "Testing bit_width=" << bit_width);

      // We test all bit offset within a byte / misalignments to change how the
      // prolog.
      for (int bit_offset = 0; bit_offset < 8; ++bit_offset) {
        SCOPED_TRACE(::testing::Message() << "Testing bit_offset=" << bit_offset);

        // Known values
        TestUnpackZeros(unpack, num_values_base, bit_width, bit_offset);
        TestUnpackOnes(unpack, num_values_base, bit_width, bit_offset);
        TestUnpackAlternating(unpack, num_values_base, bit_width, bit_offset);

        // Roundtrips
        TestRoundtripAlignment(unpack, num_values_base, bit_width, bit_offset);

        if (testing::Test::HasFailure()) return;
      }

      // Similarly, we test all epilogue sizes. That is extra values that could make it
      // fall outside of an SIMD register
      for (int epilogue_size = 0; epilogue_size <= kMaxBitWidth; ++epilogue_size) {
        SCOPED_TRACE(::testing::Message() << "Testing epilogue_size=" << epilogue_size);

        const int num_values = num_values_base + epilogue_size;

        // Known values
        TestUnpackZeros(unpack, num_values, bit_width, /* bit_offset= */ 0);
        TestUnpackOnes(unpack, num_values, bit_width, /* bit_offset= */ 0);
        TestUnpackAlternating(unpack, num_values, bit_width, /* bit_offset= */ 0);

        // Roundtrips
        TestRoundtripAlignment(unpack, num_values, bit_width, /* bit_offset= */ 0);

        if (testing::Test::HasFailure()) return;
      }
    }
  }
};

// There are actually many differences across the different sizes.
// It is best to test them all.
INSTANTIATE_TEST_SUITE_P(UnpackMultiplesOf64Values, TestUnpack,
                         ::testing::Values(64, 128, 2048),
                         [](const ::testing::TestParamInfo<TestUnpack::ParamType>& info) {
                           return "Length" + std::to_string(info.param);
                         });

TEST_P(TestUnpack, UnpackBoolScalar) { this->TestAll(&unpack_scalar<bool>); }
TEST_P(TestUnpack, Unpack8Scalar) { this->TestAll(&unpack_scalar<uint8_t>); }
TEST_P(TestUnpack, Unpack16Scalar) { this->TestAll(&unpack_scalar<uint16_t>); }
TEST_P(TestUnpack, Unpack32Scalar) { this->TestAll(&unpack_scalar<uint32_t>); }
TEST_P(TestUnpack, Unpack64Scalar) { this->TestAll(&unpack_scalar<uint64_t>); }

#if defined(ARROW_HAVE_SSE4_2)
TEST_P(TestUnpack, UnpackBoolSse4_2) { this->TestAll(&unpack_sse4_2<bool>); }
TEST_P(TestUnpack, Unpack8Sse4_2) { this->TestAll(&unpack_sse4_2<uint8_t>); }
TEST_P(TestUnpack, Unpack16Sse4_2) { this->TestAll(&unpack_sse4_2<uint16_t>); }
TEST_P(TestUnpack, Unpack32Sse4_2) { this->TestAll(&unpack_sse4_2<uint32_t>); }
TEST_P(TestUnpack, Unpack64Sse4_2) { this->TestAll(&unpack_sse4_2<uint64_t>); }
#endif

#if defined(ARROW_HAVE_RUNTIME_AVX2)
TEST_P(TestUnpack, UnpackBoolAvx2) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX2)) {
    GTEST_SKIP() << "Test requires AVX2";
  }
  this->TestAll(&unpack_avx2<bool>);
}
TEST_P(TestUnpack, Unpack8Avx2) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX2)) {
    GTEST_SKIP() << "Test requires AVX2";
  }
  this->TestAll(&unpack_avx2<uint8_t>);
}
TEST_P(TestUnpack, Unpack16Avx2) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX2)) {
    GTEST_SKIP() << "Test requires AVX2";
  }
  this->TestAll(&unpack_avx2<uint16_t>);
}
TEST_P(TestUnpack, Unpack32Avx2) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX2)) {
    GTEST_SKIP() << "Test requires AVX2";
  }
  this->TestAll(&unpack_avx2<uint32_t>);
}
TEST_P(TestUnpack, Unpack64Avx2) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX2)) {
    GTEST_SKIP() << "Test requires AVX2";
  }
  this->TestAll(&unpack_avx2<uint64_t>);
}
#endif

#if defined(ARROW_HAVE_RUNTIME_AVX512)
TEST_P(TestUnpack, UnpackBoolAvx512) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX512)) {
    GTEST_SKIP() << "Test requires AVX512";
  }
  this->TestAll(&unpack_avx512<bool>);
}
TEST_P(TestUnpack, Unpack8Avx512) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX512)) {
    GTEST_SKIP() << "Test requires AVX512";
  }
  this->TestAll(&unpack_avx512<uint8_t>);
}
TEST_P(TestUnpack, Unpack16Avx512) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX512)) {
    GTEST_SKIP() << "Test requires AVX512";
  }
  this->TestAll(&unpack_avx512<uint16_t>);
}
TEST_P(TestUnpack, Unpack32Avx512) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX512)) {
    GTEST_SKIP() << "Test requires AVX512";
  }
  this->TestAll(&unpack_avx512<uint32_t>);
}
TEST_P(TestUnpack, Unpack64Avx512) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX512)) {
    GTEST_SKIP() << "Test requires AVX512";
  }
  this->TestAll(&unpack_avx512<uint64_t>);
}
#endif

#if defined(ARROW_HAVE_NEON)
TEST_P(TestUnpack, UnpackBoolNeon) { this->TestAll(&unpack_neon<bool>); }
TEST_P(TestUnpack, Unpack8Neon) { this->TestAll(&unpack_neon<uint8_t>); }
TEST_P(TestUnpack, Unpack16Neon) { this->TestAll(&unpack_neon<uint16_t>); }
TEST_P(TestUnpack, Unpack32Neon) { this->TestAll(&unpack_neon<uint32_t>); }
TEST_P(TestUnpack, Unpack64Neon) { this->TestAll(&unpack_neon<uint64_t>); }
#endif

TEST_P(TestUnpack, UnpackBool) { this->TestAll(&unpack<bool>); }
TEST_P(TestUnpack, Unpack8) { this->TestAll(&unpack<uint8_t>); }
TEST_P(TestUnpack, Unpack16) { this->TestAll(&unpack<uint16_t>); }
TEST_P(TestUnpack, Unpack32) { this->TestAll(&unpack<uint32_t>); }
TEST_P(TestUnpack, Unpack64) { this->TestAll(&unpack<uint64_t>); }

}  // namespace arrow::internal
