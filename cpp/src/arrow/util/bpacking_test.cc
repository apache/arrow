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

#if defined(ARROW_HAVE_RUNTIME_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX512) || \
    defined(ARROW_HAVE_RUNTIME_SVE128) || defined(ARROW_HAVE_RUNTIME_SVE256)
#  include "arrow/util/cpu_info.h"
#endif

namespace arrow::internal {

template <typename Int>
using UnpackFunc = void (*)(const uint8_t*, Int*, const UnpackOptions&);

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
std::vector<Int> UnpackValues(const uint8_t* packed, const UnpackOptions& opts,
                              UnpackFunc<Int> unpack) {
  if constexpr (std::is_same_v<Int, bool>) {
    // Using dynamic array to avoid std::vector<bool>
    auto buffer = std::make_unique<Int[]>(opts.batch_size);
    unpack(packed, buffer.get(), opts);
    return std::vector<Int>(buffer.get(), buffer.get() + opts.batch_size);
  } else {
    std::vector<Int> out(opts.batch_size);
    unpack(packed, out.data(), opts);
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

template <typename Int>
class TestUnpack : public ::testing::Test {
 protected:
  void TestRoundtripAlignment(UnpackFunc<Int> unpack, const UnpackOptions& opts) {
    const auto original =
        GenerateRandomValuesForPacking<Int>(opts.batch_size, opts.bit_width);
    const auto packed =
        PackValues(original, opts.batch_size, opts.bit_width, opts.bit_offset);
    const auto unpacked = UnpackValues(packed.data(), opts, unpack);

    ASSERT_EQ(unpacked.size(), opts.batch_size);
    const auto [iter_original, iter_unpacked] =
        std::mismatch(original.cbegin(), original.cend(), unpacked.cbegin());
    Int val_original = 0;
    Int val_unpacked = 0;
    const auto mismatch_idx = static_cast<std::size_t>(iter_original - original.cbegin());
    if (mismatch_idx < unpacked.size()) {
      val_original = *iter_original;
      val_unpacked = *iter_unpacked;
    }
    EXPECT_EQ(original, unpacked) << "At position " << mismatch_idx << "/"
                                  << unpacked.size() << ", expected original value "
                                  << val_original << " but unpacked " << val_unpacked;
  }

  void TestUnpackZeros(UnpackFunc<Int> unpack, const UnpackOptions& opts) {
    const auto num_bytes = GetNumBytes(opts.batch_size, opts.bit_width, opts.bit_offset);

    const std::vector<uint8_t> packed(static_cast<std::size_t>(num_bytes), uint8_t{0});
    const auto unpacked = UnpackValues(packed.data(), opts, unpack);

    const std::vector<Int> expected(static_cast<std::size_t>(opts.batch_size), Int{0});
    EXPECT_EQ(unpacked, expected);
  }

  void TestUnpackOnes(UnpackFunc<Int> unpack, const UnpackOptions& opts) {
    const auto num_bytes = GetNumBytes(opts.batch_size, opts.bit_width, opts.bit_offset);

    const std::vector<uint8_t> packed(static_cast<std::size_t>(num_bytes), uint8_t{0xFF});
    const auto unpacked = UnpackValues(packed.data(), opts, unpack);

    // Generate bit_width ones
    Int expected_value = 0;
    if constexpr (std::is_same_v<Int, bool>) {
      expected_value = static_cast<bool>(opts.bit_width);
    } else {
      for (int i = 0; i < opts.bit_width; ++i) {
        expected_value = (expected_value << 1) | 1;
      }
    }
    const std::vector<Int> expected(static_cast<std::size_t>(opts.batch_size),
                                    expected_value);
    EXPECT_EQ(unpacked, expected);
  }

  void TestUnpackAlternating(UnpackFunc<Int> unpack, const UnpackOptions& opts) {
    const auto num_bytes = GetNumBytes(opts.batch_size, opts.bit_width, opts.bit_offset);

    // Pick between two different bit patterns so that we always unpack starting with 1
    const uint8_t byte = opts.bit_offset % 2 == 0 ? 0b10101010 : 0b01010101;
    const std::vector<uint8_t> packed(static_cast<std::size_t>(num_bytes), byte);
    const auto unpacked = UnpackValues(packed.data(), opts, unpack);

    // Generate alternative bit sequence starting with either 0 or 1
    Int one_zero_value = 0;
    Int zero_one_value = 0;
    for (int i = 0; i < opts.bit_width; ++i) {
      zero_one_value = (zero_one_value << 1) | (i % 2);
      one_zero_value = (one_zero_value << 1) | ((i + 1) % 2);
    }

    std::vector<Int> expected;
    if (opts.bit_width % 2 == 0) {
      // For even bit_width, the same pattern repeats every time
      expected.resize(static_cast<std::size_t>(opts.batch_size), one_zero_value);
    } else {
      // For odd bit_width, we alternate a pattern leading with 0 and 1
      for (int i = 0; i < opts.batch_size; ++i) {
        expected.push_back(i % 2 == 0 ? zero_one_value : one_zero_value);
      }
    }
    EXPECT_EQ(unpacked, expected);
  }

  void TestAll(UnpackFunc<Int> unpack) {
    // There are actually many differences across the different sizes.
    // It is best to test them all.
    for (int num_values_base : {64, 128, 2048}) {
      SCOPED_TRACE(::testing::Message() << "Testing num_values=" << num_values_base);

      constexpr int kMaxBitWidth = std::is_same_v<Int, bool> ? 1 : 8 * sizeof(Int);

      // Given how many edge cases there are in unpacking integers, it is best to test all
      // sizes
      for (int bit_width = 0; bit_width <= kMaxBitWidth; ++bit_width) {
        SCOPED_TRACE(::testing::Message() << "Testing bit_width=" << bit_width);

        // We test all bit offset within a byte / misalignments to change how the
        // prolog.
        for (int bit_offset = 0; bit_offset < 8; ++bit_offset) {
          SCOPED_TRACE(::testing::Message() << "Testing bit_offset=" << bit_offset);

          const UnpackOptions opts{
              .batch_size = num_values_base,
              .bit_width = bit_width,
              .bit_offset = bit_offset,
              .max_read_bytes = -1,  // No over-reading in testing (strict ASAN)
          };

          // Known values
          TestUnpackZeros(unpack, opts);
          TestUnpackOnes(unpack, opts);
          TestUnpackAlternating(unpack, opts);

          // Roundtrips
          TestRoundtripAlignment(unpack, opts);

          if (testing::Test::HasFailure()) return;
        }

        // Similarly, we test all epilog sizes. That is extra values that could make it
        // fall outside of an SIMD register
        for (int epilogue_size = 0; epilogue_size <= kMaxBitWidth; ++epilogue_size) {
          SCOPED_TRACE(::testing::Message() << "Testing epilog_size=" << epilogue_size);

          const int num_values = num_values_base + epilogue_size;

          const UnpackOptions opts{
              .batch_size = num_values,
              .bit_width = bit_width,
              .bit_offset = 0,
              .max_read_bytes = -1,  // No over-reading in testing (strict ASAN)
          };

          // Known values
          TestUnpackZeros(unpack, opts);
          TestUnpackOnes(unpack, opts);
          TestUnpackAlternating(unpack, opts);

          // Roundtrips
          TestRoundtripAlignment(unpack, opts);

          if (testing::Test::HasFailure()) return;
        }
      }
    }
  }
};

using UnpackTypes = ::testing::Types<bool, uint8_t, uint16_t, uint32_t, uint64_t>;

struct UnpackTypeNames {
  template <typename T>
  static std::string GetName(int) {
    if constexpr (std::is_same_v<T, bool>) return "bool";
    if constexpr (std::is_same_v<T, uint8_t>) return "uint8_t";
    if constexpr (std::is_same_v<T, uint16_t>) return "uint16_t";
    if constexpr (std::is_same_v<T, uint32_t>) return "uint32_t";
    if constexpr (std::is_same_v<T, uint64_t>) return "uint64_t";
  }
};

TYPED_TEST_SUITE(TestUnpack, UnpackTypes, UnpackTypeNames);

TYPED_TEST(TestUnpack, UnpackScalar) {
  this->TestAll(&bpacking::unpack_scalar<TypeParam>);
}

#if defined(ARROW_HAVE_SSE4_2)
TYPED_TEST(TestUnpack, UnpackSse4_2) {
  this->TestAll(&bpacking::unpack_sse4_2<TypeParam>);
}
#endif

#if defined(ARROW_HAVE_RUNTIME_AVX2)
TYPED_TEST(TestUnpack, UnpackAvx2) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX2)) {
    GTEST_SKIP() << "Test requires AVX2";
  }
  this->TestAll(&bpacking::unpack_avx2<TypeParam>);
}
#endif

#if defined(ARROW_HAVE_RUNTIME_AVX512)
TYPED_TEST(TestUnpack, UnpackAvx512) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX512)) {
    GTEST_SKIP() << "Test requires AVX512";
  }
  this->TestAll(&bpacking::unpack_avx512<TypeParam>);
}
#endif

#if defined(ARROW_HAVE_NEON)
TYPED_TEST(TestUnpack, UnpackNeon) { this->TestAll(&bpacking::unpack_neon<TypeParam>); }
#endif

#if defined(ARROW_HAVE_RUNTIME_SVE128)
TYPED_TEST(TestUnpack, UnpackSve128) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::SVE128)) {
    GTEST_SKIP() << "Test requires SVE128";
  }
  this->TestAll(&bpacking::unpack_sve128<TypeParam>);
}
#endif

#if defined(ARROW_HAVE_RUNTIME_SVE256)
TYPED_TEST(TestUnpack, UnpackSve256) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::SVE256)) {
    GTEST_SKIP() << "Test requires SVE256";
  }
  this->TestAll(&bpacking::unpack_sve256<TypeParam>);
}
#endif

TYPED_TEST(TestUnpack, Unpack) { this->TestAll(&unpack<TypeParam>); }

}  // namespace arrow::internal
