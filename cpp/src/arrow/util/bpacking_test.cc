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
#include "arrow/util/bpacking_neon_internal.h"
#include "arrow/util/logging.h"

namespace arrow::internal {

template <typename Int>
using UnpackFunc = int (*)(const uint8_t*, Int*, int, int);

/// Generate random bytes as packed integers.
std::vector<uint8_t> generateRandomPackedValues(int32_t num_values, int32_t bit_width) {
  constexpr uint32_t kSeed = 3214;

  auto const num_bits = num_values * bit_width;
  if (num_bits % 8 != 0) {
    throw std::invalid_argument("Must generate a multiple of 8 bits.");
  }
  auto const num_bytes = num_bits / 8;

  std::vector<uint8_t> out(num_bytes);
  random_bytes(num_bytes, kSeed, out.data());

  return out;
}

/// Convenience wrapper to unpack into a vector
template <typename Int>
std::vector<Int> unpackValues(std::vector<uint8_t> const& packed, int32_t num_values,
                              int32_t bit_width, UnpackFunc<Int> unpack) {
  std::vector<Int> out(num_values);
  int values_read = unpack(packed.data(), out.data(), num_values, bit_width);
  ARROW_DCHECK_GE(values_read, 0);
  out.resize(values_read);
  return out;
}

/// Use BitWriter to pack values into a vector.
template <typename Int>
std::vector<uint8_t> packValues(std::vector<Int> const& values, int32_t num_values,
                                int32_t bit_width) {
  auto const num_bits = num_values * bit_width;
  if (num_bits % 8 != 0) {
    throw std::invalid_argument("Must generate a multiple of 8 bits.");
  }
  auto const num_bytes = num_bits / 8;

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
void checkUnpackPackRoundtrip(std::vector<uint8_t> const& packed, int32_t num_values,
                              int32_t bit_width, UnpackFunc<Int> unpack) {
  auto const unpacked = unpackValues(packed, num_values, bit_width, unpack);
  EXPECT_EQ(unpacked.size(), num_values);
  auto const roundtrip = packValues(unpacked, num_values, bit_width);
  EXPECT_EQ(packed.size(), roundtrip.size());
  EXPECT_EQ(packed, roundtrip);
}

struct UnpackingData {
  int32_t num_values;
  int32_t bit_width;
};

class UnpackingRandomRoundTrip : public ::testing::TestWithParam<UnpackingData> {
 protected:
  template <typename Int>
  void testRoundtrip(UnpackFunc<Int> unpack) {
    auto [num_values, bit_width] = GetParam();
    auto const original = generateRandomPackedValues(num_values, bit_width);
    checkUnpackPackRoundtrip(original, num_values, bit_width, unpack);
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
  this->testRoundtrip(&unpack32_default);
}
TEST_P(UnpackingRandomRoundTrip, unpack64Default) {
  this->testRoundtrip(&unpack64_default);
}

#if defined(ARROW_HAVE_RUNTIME_AVX2)
TEST_P(UnpackingRandomRoundTrip, unpack32Avx2) { this->testRoundtrip(&unpack32_avx2); }
#endif

#if defined(ARROW_HAVE_RUNTIME_AVX512)
TEST_P(UnpackingRandomRoundTrip, unpack32Avx512) {
  this->testRoundtrip(&unpack32_avx512);
}
#endif

#if defined(ARROW_HAVE_NEON)
TEST_P(UnpackingRandomRoundTrip, unpack32Neon) { this->testRoundtrip(&unpack32_neon); }
#endif

TEST_P(UnpackingRandomRoundTrip, unpack32) { this->testRoundtrip(&unpack32); }
TEST_P(UnpackingRandomRoundTrip, unpack64) { this->testRoundtrip(&unpack64); }

}  // namespace arrow::internal
