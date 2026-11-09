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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/spaced_internal.h"
#include "arrow/util/spaced_simd_internal.h"

#if defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_RUNTIME_SSE4_2)
#  include "arrow/util/cpu_info.h"
#endif

namespace arrow::util::internal {

namespace {

struct Pattern {
  enum Kind { kBernoulli, kRuns, kTopNulls };

  std::string name;
  Kind kind;
  // kBernoulli: probability of a valid slot; kTopNulls: number of nulls
  double p = 0;
  // kRuns: mean lengths of the runs of valid and null slots
  int mean_valid = 0;
  int mean_null = 0;
};

std::vector<Pattern> AllPatterns() {
  std::vector<Pattern> patterns;
  for (double p : {0.0, 0.02, 0.1, 0.5, 0.9, 0.95, 0.97, 0.99, 0.995, 0.999, 1.0}) {
    patterns.push_back({"p" + std::to_string(p), Pattern::kBernoulli, p});
  }
  for (auto [valid, null] : std::vector<std::pair<int, int>>{{2, 2},
                                                             {8, 8},
                                                             {32, 32},
                                                             {256, 256},
                                                             {64, 2},
                                                             {128, 1},
                                                             {128, 8},
                                                             {1000, 1}}) {
    patterns.push_back({"runs_v" + std::to_string(valid) + "_n" + std::to_string(null),
                        Pattern::kRuns, 0, valid, null});
  }
  // All valid except a few nulls in the last word: exercises the chunk
  // kernel next to the end of the buffer.
  for (int k : {1, 2, 3, 5, 7, 9, 12}) {
    patterns.push_back({"top" + std::to_string(k), Pattern::kTopNulls, double(k)});
  }
  return patterns;
}

// Random bits outside of [offset, offset + n) so that the offset matters.
std::vector<uint8_t> MakeBitmap(const Pattern& pattern, int n, int64_t offset,
                                std::mt19937_64& rng) {
  std::vector<uint8_t> bitmap(std::max<int64_t>(1, bit_util::BytesForBits(offset + n)));
  for (auto& byte : bitmap) byte = static_cast<uint8_t>(rng());
  auto set = [&](int i, bool valid) {
    bit_util::SetBitTo(bitmap.data(), offset + i, valid);
  };
  switch (pattern.kind) {
    case Pattern::kBernoulli: {
      std::bernoulli_distribution valid(pattern.p);
      for (int i = 0; i < n; ++i) set(i, valid(rng));
      break;
    }
    case Pattern::kRuns: {
      std::geometric_distribution<int> valid_len(1.0 / pattern.mean_valid);
      std::geometric_distribution<int> null_len(1.0 / pattern.mean_null);
      bool valid = true;
      for (int i = 0; i < n;) {
        const int len = 1 + (valid ? valid_len(rng) : null_len(rng));
        for (int end = std::min(n, i + len); i < end; ++i) set(i, valid);
        valid = !valid;
      }
      break;
    }
    case Pattern::kTopNulls: {
      for (int i = 0; i < n; ++i) set(i, true);
      const int top = std::min(n, 64);
      for (int k = 0; k < static_cast<int>(pattern.p) && top > 0; ++k) {
        set(n - 1 - static_cast<int>(rng() % top), false);
      }
      break;
    }
  }
  return bitmap;
}

template <typename T>
T MakeValue(int k) {
  // Never 0, so that a value landing in a null slot is caught.
  return static_cast<T>(k % 30011 + 1);
}

template <typename T>
bool BitwiseEqual(T a, T b) {
  return std::memcmp(&a, &b, sizeof(T)) == 0;
}

// Check `expand` against a straightforward implementation. Null slots must
// be zero if `zeros_nulls`, and are not checked otherwise.
template <typename T, typename Expand>
void CheckExpand(Expand&& expand, bool zeros_nulls) {
  constexpr int kGuard = 16;
  std::mt19937_64 rng(42);
  for (int n : {0, 1, 7, 63, 64, 65, 127, 128, 129, 900, 2048}) {
    for (int64_t offset : {0, 1, 5, 63}) {
      for (const auto& pattern : AllPatterns()) {
        ARROW_SCOPED_TRACE("n = ", n, ", offset = ", offset,
                           ", pattern = ", pattern.name);
        const auto bitmap = MakeBitmap(pattern, n, offset, rng);

        std::vector<T> expected(n);
        int num_valid = 0;
        for (int i = 0; i < n; ++i) {
          if (bit_util::GetBit(bitmap.data(), offset + i)) {
            expected[i] = MakeValue<T>(num_valid++);
          }
        }
        const int null_count = n - num_valid;

        // Leftover bytes in the null slots and after the buffer
        std::vector<T> buffer(n + kGuard);
        std::memset(static_cast<void*>(buffer.data()), 0xA5, sizeof(T) * buffer.size());
        const std::vector<T> guard(buffer.end() - kGuard, buffer.end());
        for (int k = 0; k < num_valid; ++k) buffer[k] = MakeValue<T>(k);

        expand(buffer.data(), n, null_count, bitmap.data(), offset);

        for (int i = 0; i < n; ++i) {
          const bool valid = bit_util::GetBit(bitmap.data(), offset + i);
          if (valid || zeros_nulls) {
            ASSERT_TRUE(BitwiseEqual(buffer[i], expected[i])) << "at index " << i;
          }
        }
        for (int i = 0; i < kGuard; ++i) {
          ASSERT_TRUE(BitwiseEqual(buffer[n + i], guard[i])) << "write past the end";
        }
      }
    }
  }
}

template <typename Uint>
class TestSpacedExpandRightward : public ::testing::Test {};

using UintTypes = ::testing::Types<uint32_t, uint64_t>;
TYPED_TEST_SUITE(TestSpacedExpandRightward, UintTypes);

TYPED_TEST(TestSpacedExpandRightward, Scalar) {
  CheckExpand<TypeParam>(&SpacedExpandRightwardScalar<TypeParam>, /*zeros_nulls=*/false);
}

#if defined(ARROW_HAVE_NEON)
TYPED_TEST(TestSpacedExpandRightward, Neon) {
  CheckExpand<TypeParam>(&SpacedExpandRightwardNeon<TypeParam>, /*zeros_nulls=*/true);
}
#endif

#if defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_RUNTIME_SSE4_2)
TYPED_TEST(TestSpacedExpandRightward, Sse4_2) {
  if (!::arrow::internal::CpuInfo::GetInstance()->IsSupported(
          ::arrow::internal::CpuInfo::SSE4_2)) {
    GTEST_SKIP() << "Test requires SSE4.2";
  }
  CheckExpand<TypeParam>(&SpacedExpandRightwardSse4_2<TypeParam>, /*zeros_nulls=*/true);
}
#endif

// Through the public template: 4 and 8 byte types are dispatched (possibly
// to the scalar implementation), other widths use the scalar implementation.
template <typename T>
class TestSpacedExpandRightwardTyped : public ::testing::Test {};

using ValueTypes = ::testing::Types<int32_t, int64_t, float, double, int16_t>;
TYPED_TEST_SUITE(TestSpacedExpandRightwardTyped, ValueTypes);

TYPED_TEST(TestSpacedExpandRightwardTyped, Dispatch) {
  CheckExpand<TypeParam>(&SpacedExpandRightward<TypeParam>, /*zeros_nulls=*/false);
}

}  // namespace

}  // namespace arrow::util::internal
