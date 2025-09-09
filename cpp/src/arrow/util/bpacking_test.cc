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

#include <stdexcept>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/util.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/logging.h"

#if defined(ARROW_HAVE_RUNTIME_AVX2)
#  include "arrow/util/bpacking_avx2_internal.h"
#  include "arrow/util/cpu_info.h"
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
#  include "arrow/util/bpacking_avx512_internal.h"
#endif
#if defined(ARROW_HAVE_NEON)
#  include "arrow/util/bpacking_neon_internal.h"
#endif

namespace arrow::internal {

template <typename Int>
using UnpackFunc = int (*)(const uint8_t*, Int*, int, int);

/// Get the number of bytes associate with a packing.
constexpr int32_t GetNumBytes(int32_t num_values, int32_t bit_width) {
  auto const num_bits = num_values * bit_width;
  if (num_bits % 8 != 0) {
    throw std::invalid_argument("Must pack a multiple of 8 bits.");
  }
  return num_bits / 8;
}

/// Generate random bytes as packed integers.
std::vector<uint8_t> GenerateRandomPackedValues(int32_t num_values, int32_t bit_width) {
  constexpr uint32_t kSeed = 3214;
  auto const num_bytes = GetNumBytes(num_values, bit_width);

  std::vector<uint8_t> out(num_bytes);
  random_bytes(num_bytes, kSeed, out.data());

  return out;
}

/// Convenience wrapper to unpack into a vector
template <typename Int>
std::vector<Int> UnpackValues(const uint8_t* packed, int32_t num_values,
                              int32_t bit_width, UnpackFunc<Int> unpack) {
  std::vector<Int> out(num_values);
  int values_read = unpack(packed, out.data(), num_values, bit_width);
  ARROW_DCHECK_GE(values_read, 0);
  out.resize(values_read);
  return out;
}

/// Use BitWriter to pack values into a vector.
template <typename Int>
std::vector<uint8_t> PackValues(std::vector<Int> const& values, int32_t num_values,
                                int32_t bit_width) {
  auto const num_bytes = GetNumBytes(num_values, bit_width);

  std::vector<uint8_t> out(static_cast<std::size_t>(num_bytes));
  bit_util::BitWriter writer(out.data(), num_bytes);
  for (auto const& v : values) {
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
  auto const num_bytes = GetNumBytes(num_values, bit_width);

  auto const unpacked = UnpackValues(packed, num_values, bit_width, unpack);
  EXPECT_EQ(unpacked.size(), num_values);
  auto const roundtrip = PackValues(unpacked, num_values, bit_width);
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

struct UnpackingData {
  int32_t num_values;
  int32_t bit_width;
};

class UnpackingRandomRoundTrip : public ::testing::TestWithParam<UnpackingData> {
 protected:
  template <typename Int>
  void TestRoundtripAlignment(UnpackFunc<Int> unpack, std::size_t alignment_offset) {
    auto [num_values, bit_width] = GetParam();

    // Assume std::vector allocation is likely be aligned for greater than a byte.
    // So we allocate more values than necessary and skip to the next byte with the
    // desired (non) alignment to test the proper condition.
    constexpr int32_t kExtraValues = sizeof(Int) * 8;
    auto const packed = GenerateRandomPackedValues(num_values + kExtraValues, bit_width);
    const uint8_t* packed_unaligned =
        GetNextAlignedByte(packed.data(), sizeof(Int)) + alignment_offset;

    CheckUnpackPackRoundtrip(packed_unaligned, num_values, bit_width, unpack);
  }

  template <typename Int>
  void TestRoundtrip(UnpackFunc<Int> unpack) {
    // Aligned test
    TestRoundtripAlignment(unpack, 0);
    // Unaligned test
    TestRoundtripAlignment(unpack, 1);
  }
};

INSTANTIATE_TEST_SUITE_P(
    MutpliesOf64Values, UnpackingRandomRoundTrip,
    ::testing::Values(UnpackingData{64, 1}, UnpackingData{128, 1}, UnpackingData{2048, 1},
                      UnpackingData{64, 31}, UnpackingData{128, 31},
                      UnpackingData{2048, 31}, UnpackingData{64000, 7},
                      UnpackingData{64000, 8}, UnpackingData{64000, 13},
                      UnpackingData{64000, 16}, UnpackingData{64000, 31},
                      UnpackingData{64000, 32}));

TEST_P(UnpackingRandomRoundTrip, unpack32Default) {
  this->TestRoundtrip(&unpack32_default);
}
TEST_P(UnpackingRandomRoundTrip, unpack64Default) {
  this->TestRoundtrip(&unpack64_default);
}

#if defined(ARROW_HAVE_RUNTIME_AVX2)
TEST_P(UnpackingRandomRoundTrip, unpack32Avx2) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX2)) {
    GTEST_SKIP() << "Test requires AVX2";
  }
  this->testRoundtrip(&unpack32_avx2);
}
#endif

#if defined(ARROW_HAVE_RUNTIME_AVX512)
TEST_P(UnpackingRandomRoundTrip, unpack32Avx512) {
  if (!CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX512)) {
    GTEST_SKIP() << "Test requires AVX512";
  }
  this->testRoundtrip(&unpack32_avx512);
}
#endif

#if defined(ARROW_HAVE_NEON)
TEST_P(UnpackingRandomRoundTrip, unpack32Neon) { this->TestRoundtrip(&unpack32_neon); }
#endif

TEST_P(UnpackingRandomRoundTrip, unpack32) { this->TestRoundtrip(&unpack32); }
TEST_P(UnpackingRandomRoundTrip, unpack64) { this->TestRoundtrip(&unpack64); }

}  // namespace arrow::internal
