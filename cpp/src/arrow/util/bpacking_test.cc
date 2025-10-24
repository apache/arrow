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

#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/bpacking_scalar_internal.h"
#include "arrow/util/bpacking_simd_internal.h"
#include "arrow/util/logging.h"

#if defined(ARROW_HAVE_RUNTIME_AVX2)
#  include "arrow/util/cpu_info.h"
#endif

namespace arrow::internal {

template <typename Int>
using UnpackFunc = int (*)(const uint8_t*, Int*, int, int);

/// Get the number of bytes associate with a packing.
Result<int32_t> GetNumBytes(int32_t num_values, int32_t bit_width) {
  const auto num_bits = num_values * bit_width;
  if (num_bits % 8 != 0) {
    return Status::NotImplemented(
        "The unpack functions only work on a multiple of 8 bits.");
  }
  return num_bits / 8;
}

/// Generate random bytes as packed integers.
std::vector<uint8_t> GenerateRandomPackedValues(int32_t num_values, int32_t bit_width) {
  constexpr uint32_t kSeed = 3214;
  EXPECT_OK_AND_ASSIGN(const auto num_bytes, GetNumBytes(num_values, bit_width));

  std::vector<uint8_t> out(std::max(1, num_bytes));  // We need a valid pointer for size 0
  random_bytes(num_bytes, kSeed, out.data());

  return out;
}

/// Convenience wrapper to unpack into a vector
template <typename Int>
std::vector<Int> UnpackValues(const uint8_t* packed, int32_t num_values,
                              int32_t bit_width, UnpackFunc<Int> unpack) {
  // Using dynamic array to avoid std::vector<bool>
  auto buffer = std::make_unique<Int[]>(num_values);
  int values_read = unpack(packed, buffer.get(), num_values, bit_width);
  ARROW_DCHECK_GE(values_read, 0);
  EXPECT_LE(values_read, num_values);

  return std::vector<Int>(buffer.get(), buffer.get() + values_read);
}

/// Use BitWriter to pack values into a vector.
template <typename Int>
std::vector<uint8_t> PackValues(const std::vector<Int>& values, int32_t num_values,
                                int32_t bit_width) {
  EXPECT_OK_AND_ASSIGN(const auto num_bytes, GetNumBytes(num_values, bit_width));

  std::vector<uint8_t> out(static_cast<std::size_t>(num_bytes));
  bit_util::BitWriter writer(out.data(), num_bytes);
  for (const auto& v : values) {
    bool written = writer.PutValue(v, bit_width);
    if (!written) {
      throw std::runtime_error("Cannot write move values");
    }
  }

  return out;
}

template <typename Int>
void CheckUnpackPackRoundtrip(const uint8_t* packed, int32_t num_values,
                              int32_t bit_width, UnpackFunc<Int> unpack) {
  EXPECT_OK_AND_ASSIGN(const auto num_bytes, GetNumBytes(num_values, bit_width));

  const auto unpacked = UnpackValues(packed, num_values, bit_width, unpack);
  EXPECT_EQ(unpacked.size(), num_values);
  const auto roundtrip = PackValues(unpacked, num_values, bit_width);
  EXPECT_EQ(num_bytes, roundtrip.size());
  for (int i = 0; i < num_bytes; ++i) {
    EXPECT_EQ(packed[i], roundtrip[i]) << "differ in position " << i;
  }
}

const uint8_t* GetNextAlignedByte(const uint8_t* ptr, std::size_t alignment) {
  auto addr = reinterpret_cast<std::uintptr_t>(ptr);

  if (addr % alignment == 0) {
    return ptr;
  }

  auto remainder = addr % alignment;
  auto bytes_to_add = alignment - remainder;

  return ptr + bytes_to_add;
}

class TestUnpack : public ::testing::TestWithParam<int> {
 protected:
  template <typename Int>
  void TestRoundtripAlignment(UnpackFunc<Int> unpack, int bit_width,
                              std::size_t alignment_offset) {
    int num_values = GetParam();

    // Assume std::vector allocation is likely be aligned for greater than a byte.
    // So we allocate more values than necessary and skip to the next byte with the
    // desired (non) alignment to test the proper condition.
    constexpr int32_t kExtraValues = sizeof(Int) * 8;
    const auto packed = GenerateRandomPackedValues(num_values + kExtraValues, bit_width);
    const uint8_t* packed_unaligned =
        GetNextAlignedByte(packed.data(), sizeof(Int)) + alignment_offset;

    CheckUnpackPackRoundtrip(packed_unaligned, num_values, bit_width, unpack);
  }

  template <typename Int>
  void TestUnpackZeros(UnpackFunc<Int> unpack, int bit_width) {
    int num_values = GetParam();
    EXPECT_OK_AND_ASSIGN(const auto num_bytes, GetNumBytes(num_values, bit_width));

    const std::vector<uint8_t> packed(static_cast<std::size_t>(num_bytes), uint8_t{0});
    const auto unpacked = UnpackValues(packed.data(), num_values, bit_width, unpack);

    const std::vector<Int> expected(static_cast<std::size_t>(num_values), Int{0});
    EXPECT_EQ(unpacked, expected);
  }

  template <typename Int>
  void TestUnpackOnes(UnpackFunc<Int> unpack, int bit_width) {
    int num_values = GetParam();
    EXPECT_OK_AND_ASSIGN(const auto num_bytes, GetNumBytes(num_values, bit_width));

    const std::vector<uint8_t> packed(static_cast<std::size_t>(num_bytes), uint8_t{0xFF});
    const auto unpacked = UnpackValues(packed.data(), num_values, bit_width, unpack);

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
  void TestUnpackAlternating(UnpackFunc<Int> unpack, int bit_width) {
    int num_values = GetParam();
    EXPECT_OK_AND_ASSIGN(const auto num_bytes, GetNumBytes(num_values, bit_width));

    const std::vector<uint8_t> packed(static_cast<std::size_t>(num_bytes), uint8_t{0xAA});
    const auto unpacked = UnpackValues(packed.data(), num_values, bit_width, unpack);

    // Generate alternative bit sequence sratring with either 0 or 1
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
    constexpr int kMaxBitWidth = std::is_same_v<Int, bool> ? 1 : 8 * sizeof(Int);
    // Given how many edge cases there are in unpacking integers, it is best to test all
    // sizes
    for (int bit_width = 0; bit_width <= kMaxBitWidth; ++bit_width) {
      SCOPED_TRACE(::testing::Message() << "Testing bit_width=" << bit_width);

      // Known values
      TestUnpackZeros(unpack, bit_width);
      TestUnpackOnes(unpack, bit_width);
      TestUnpackAlternating(unpack, bit_width);

      // Roundtrips
      TestRoundtripAlignment(unpack, bit_width, /* alignment_offset= */ 0);
      TestRoundtripAlignment(unpack, bit_width, /* alignment_offset= */ 1);
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
