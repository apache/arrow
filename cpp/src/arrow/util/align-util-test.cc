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
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/util/align-util.h"

namespace arrow {
namespace internal {

template <int64_t NBYTES>
void CheckBitmapWordAlign(const uint8_t* data, int64_t bit_offset, int64_t length,
                          BitmapWordAlignParams expected) {
  auto p = BitmapWordAlign<static_cast<uint64_t>(NBYTES)>(data, bit_offset, length);

  ASSERT_EQ(p.leading_bits, expected.leading_bits);
  ASSERT_EQ(p.trailing_bits, expected.trailing_bits);
  if (p.trailing_bits > 0) {
    // Only relevant if trailing_bits > 0
    ASSERT_EQ(p.trailing_bit_offset, expected.trailing_bit_offset);
  }
  ASSERT_EQ(p.aligned_bits, expected.aligned_bits);
  ASSERT_EQ(p.aligned_words, expected.aligned_words);
  if (p.aligned_bits > 0) {
    // Only relevant if aligned_bits > 0
    ASSERT_EQ(p.aligned_start, expected.aligned_start);
  }

  // Invariants
  ASSERT_LT(p.leading_bits, NBYTES * 8);
  ASSERT_LT(p.trailing_bits, NBYTES * 8);
  ASSERT_EQ(p.leading_bits + p.aligned_bits + p.trailing_bits, length);
  ASSERT_EQ(p.aligned_bits, NBYTES * 8 * p.aligned_words);
  if (p.aligned_bits > 0) {
    ASSERT_EQ(reinterpret_cast<size_t>(p.aligned_start) & (NBYTES - 1), 0);
  }
  if (p.trailing_bits > 0) {
    ASSERT_EQ(p.trailing_bit_offset, bit_offset + p.leading_bits + p.aligned_bits);
    ASSERT_EQ(p.trailing_bit_offset + p.trailing_bits, bit_offset + length);
  }
}

TEST(BitmapWordAlign, AlignedDataStart) {
  alignas(8) char buf[1];

  // A 8-byte aligned pointer
  const uint8_t* P = reinterpret_cast<const uint8_t*>(buf);
  const uint8_t* A = P;

  // {leading_bits, trailing_bits, trailing_bit_offset,
  //  aligned_start, aligned_bits, aligned_words}
  CheckBitmapWordAlign<8>(P, 0, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 13, {0, 13, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 63, {0, 63, 0, A, 0, 0});

  CheckBitmapWordAlign<8>(P, 0, 64, {0, 0, 0, A, 64, 1});
  CheckBitmapWordAlign<8>(P, 0, 73, {0, 9, 64, A, 64, 1});
  CheckBitmapWordAlign<8>(P, 0, 191, {0, 63, 128, A, 128, 2});

  CheckBitmapWordAlign<8>(P, 5, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 13, {13, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 59, {59, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 60, {59, 1, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 64, {59, 5, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 122, {59, 63, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 123, {59, 0, 64, A + 8, 64, 1});
  CheckBitmapWordAlign<8>(P, 5, 314, {59, 63, 256, A + 8, 192, 3});
  CheckBitmapWordAlign<8>(P, 5, 315, {59, 0, 320, A + 8, 256, 4});

  CheckBitmapWordAlign<8>(P, 63, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 2, {1, 1, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 64, {1, 63, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 65, {1, 0, 128, A + 8, 64, 1});
  CheckBitmapWordAlign<8>(P, 63, 128, {1, 63, 128, A + 8, 64, 1});
  CheckBitmapWordAlign<8>(P, 63, 129, {1, 0, 192, A + 8, 128, 2});

  CheckBitmapWordAlign<8>(P, 1024, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1024, 130, {0, 2, 1152, A + 128, 128, 2});

  CheckBitmapWordAlign<8>(P, 1025, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1025, 63, {63, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1025, 64, {63, 1, 1088, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1025, 128, {63, 1, 1152, A + 136, 64, 1});
}

TEST(BitmapWordAlign, UnalignedDataStart) {
  alignas(8) char buf[1];

  const uint8_t* P = reinterpret_cast<const uint8_t*>(buf) + 1;
  const uint8_t* A = P + 7;

  // {leading_bits, trailing_bits, trailing_bit_offset,
  //  aligned_start, aligned_bits, aligned_words}
  CheckBitmapWordAlign<8>(P, 0, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 13, {13, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 56, {56, 0, 0, A, 0, 0});

  CheckBitmapWordAlign<8>(P, 0, 57, {56, 1, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 119, {56, 63, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 120, {56, 0, 120, A, 64, 1});
  CheckBitmapWordAlign<8>(P, 0, 184, {56, 0, 184, A, 128, 2});
  CheckBitmapWordAlign<8>(P, 0, 185, {56, 1, 184, A, 128, 2});

  CheckBitmapWordAlign<8>(P, 55, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 55, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 55, 2, {1, 1, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 55, 66, {1, 1, 120, A, 64, 1});

  // (P + 56 bits) is 64-bit aligned
  CheckBitmapWordAlign<8>(P, 56, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 56, 1, {0, 1, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 56, 63, {0, 63, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 56, 191, {0, 63, 184, A, 128, 2});

  // (P + 1016 bits) is 64-bit aligned
  CheckBitmapWordAlign<8>(P, 1016, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1016, 5, {0, 5, 1016, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1016, 63, {0, 63, 1016, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1016, 64, {0, 0, 1080, A + 120, 64, 1});
  CheckBitmapWordAlign<8>(P, 1016, 129, {0, 1, 1144, A + 120, 128, 2});

  CheckBitmapWordAlign<8>(P, 1017, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 63, {63, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 64, {63, 1, 1080, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 128, {63, 1, 1144, A + 128, 64, 1});
}

}  // namespace internal
}  // namespace arrow
