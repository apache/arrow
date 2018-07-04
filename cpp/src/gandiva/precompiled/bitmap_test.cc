// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include "precompiled/types.h"

namespace gandiva {

TEST(TestBitMap, TestSimple) {
  static const int kNumBytes = 16;
  uint8_t bit_map[kNumBytes];
  memset(bit_map, 0, kNumBytes);

  EXPECT_EQ(bitMapGetBit(bit_map, 100), false);

  // set 100th bit and verify
  bitMapSetBit(bit_map, 100, true);
  EXPECT_EQ(bitMapGetBit(bit_map, 100), true);

  // clear 100th bit and verify
  bitMapSetBit(bit_map, 100, false);
  EXPECT_EQ(bitMapGetBit(bit_map, 100), false);
}

TEST(TestBitMap, TestClearIfFalse) {
  static const int kNumBytes = 32;
  uint8_t bit_map[kNumBytes];
  memset(bit_map, 0, kNumBytes);

  bitMapSetBit(bit_map, 24, true);

  // bit should remain unchanged.
  bitMapClearBitIfFalse(bit_map, 24, true);
  EXPECT_EQ(bitMapGetBit(bit_map, 24), true);

  // bit should be cleared.
  bitMapClearBitIfFalse(bit_map, 24, false);
  EXPECT_EQ(bitMapGetBit(bit_map, 24), false);

  // this function should have no impact if the bit is already clear.
  bitMapClearBitIfFalse(bit_map, 24, true);
  EXPECT_EQ(bitMapGetBit(bit_map, 24), false);

  bitMapClearBitIfFalse(bit_map, 24, false);
  EXPECT_EQ(bitMapGetBit(bit_map, 24), false);
}

}  // namespace gandiva
