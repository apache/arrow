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

#include "arrow/acero/time_series_util.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

namespace arrow {
namespace acero {
namespace {

// Sweep every value of T in increasing order and assert that NormalizeTime is
// strictly increasing and injective over the whole domain. Only usable for
// types narrow enough to enumerate.
template <typename T>
void AssertOrderPreservedExhaustively() {
  using Wide = int64_t;
  constexpr Wide kMin = static_cast<Wide>(std::numeric_limits<T>::min());
  constexpr Wide kMax = static_cast<Wide>(std::numeric_limits<T>::max());

  std::unordered_set<uint64_t> seen;
  uint64_t previous = 0;
  bool first = true;
  for (Wide value = kMin; value <= kMax; ++value) {
    const T t = static_cast<T>(value);
    const uint64_t normalized = NormalizeTime(t);
    if (!first) {
      ASSERT_GT(normalized, previous)
          << "not strictly increasing at " << value << " (width " << 8 * sizeof(T)
          << ", signed " << std::is_signed<T>::value << ")";
    }
    ASSERT_TRUE(seen.insert(normalized).second)
        << "collision at " << value << " -> " << normalized;
    previous = normalized;
    first = false;
  }
  ASSERT_EQ(seen.size(), static_cast<size_t>(kMax - kMin + 1));
}

// Values around the extremes and around zero, sorted and deduplicated. Zero
// is the interesting one: it is where the sign bit changes, and where the
// previous implementation folded the negative half of the domain back onto the
// non-negative half. Candidates outside T's range are skipped rather than
// wrapped, so the sweep is strictly increasing by construction for every T.
template <typename T>
std::vector<T> BoundarySweep() {
  constexpr T kMin = std::numeric_limits<T>::min();
  constexpr T kMax = std::numeric_limits<T>::max();

  std::vector<T> values = {kMin,
                           static_cast<T>(kMin + 1),
                           static_cast<T>(kMin / 2),
                           static_cast<T>(0),
                           static_cast<T>(1),
                           static_cast<T>(2),
                           static_cast<T>(kMax / 2),
                           static_cast<T>(kMax - 1),
                           kMax};
  if (std::is_signed<T>::value) {
    values.push_back(static_cast<T>(-1));
    values.push_back(static_cast<T>(-2));
  }
  // Only representable if T is wide enough; int8_t would wrap.
  if (static_cast<int64_t>(kMax) >= 1000) {
    values.push_back(static_cast<T>(1000));
    if (std::is_signed<T>::value) values.push_back(static_cast<T>(-1000));
  }

  std::sort(values.begin(), values.end());
  values.erase(std::unique(values.begin(), values.end()), values.end());
  return values;
}

template <typename T>
void AssertOrderPreservedAtBoundaries() {
  const std::vector<T> values = BoundarySweep<T>();
  ASSERT_GE(values.size(), 2u);
  for (size_t i = 1; i < values.size(); ++i) {
    ASSERT_LT(values[i - 1], values[i]) << "test data is not sorted";
    ASSERT_LT(NormalizeTime(values[i - 1]), NormalizeTime(values[i]))
        << "order not preserved between " << static_cast<int64_t>(values[i - 1])
        << " and " << static_cast<int64_t>(values[i]);
  }
}

}  // namespace

TEST(NormalizeTime, OrderPreservedExhaustively) {
  AssertOrderPreservedExhaustively<int8_t>();
  AssertOrderPreservedExhaustively<uint8_t>();
  AssertOrderPreservedExhaustively<int16_t>();
  AssertOrderPreservedExhaustively<uint16_t>();
}

TEST(NormalizeTime, OrderPreservedAtBoundaries) {
  AssertOrderPreservedAtBoundaries<int8_t>();
  AssertOrderPreservedAtBoundaries<uint8_t>();
  AssertOrderPreservedAtBoundaries<int16_t>();
  AssertOrderPreservedAtBoundaries<uint16_t>();
  AssertOrderPreservedAtBoundaries<int32_t>();
  AssertOrderPreservedAtBoundaries<uint32_t>();
  AssertOrderPreservedAtBoundaries<int64_t>();
  AssertOrderPreservedAtBoundaries<uint64_t>();
}

// The most negative value must land at zero and the most positive at the top
// of the range, so that the full width of uint64_t is used for 64-bit inputs
// and TolType's saturation at kMinValue/kMaxValue keeps working.
TEST(NormalizeTime, EndpointsMapToEndpoints) {
  ASSERT_EQ(NormalizeTime(std::numeric_limits<int64_t>::min()), 0u);
  ASSERT_EQ(NormalizeTime(std::numeric_limits<int64_t>::max()),
            std::numeric_limits<uint64_t>::max());
  ASSERT_EQ(NormalizeTime(std::numeric_limits<uint64_t>::min()), 0u);
  ASSERT_EQ(NormalizeTime(std::numeric_limits<uint64_t>::max()),
            std::numeric_limits<uint64_t>::max());
}

// TolType::Accepts compares differences of normalized values, so a difference
// that spans zero has to come out exact. Under the previous implementation the
// negative and non-negative halves were both mapped into [0, 2^(W-1)), which
// made differences across zero wrong by about 2^(W-1).
TEST(NormalizeTime, DifferencesAcrossZeroAreExact) {
  for (int64_t delta : {int64_t{1}, int64_t{2}, int64_t{1000}, int64_t{1} << 32}) {
    const int64_t left = -delta / 2;
    const int64_t right = left + delta;
    ASSERT_EQ(NormalizeTime(right) - NormalizeTime(left), static_cast<uint64_t>(delta))
        << "difference across zero is wrong for delta " << delta;
  }
  // Same property for a narrower type, where the fold was easier to hit.
  ASSERT_EQ(NormalizeTime(int32_t{500}) - NormalizeTime(int32_t{-500}), 1000u);
}

}  // namespace acero
}  // namespace arrow
