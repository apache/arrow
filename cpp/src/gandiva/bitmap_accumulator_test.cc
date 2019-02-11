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

#include "gandiva/bitmap_accumulator.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

#include "gandiva/dex.h"

namespace gandiva {

class TestBitMapAccumulator : public ::testing::Test {
 protected:
  void FillBitMap(uint8_t* bmap, uint32_t seed, int nrecords);
  void ByteWiseIntersectBitMaps(uint8_t* dst, const std::vector<uint8_t*>& srcs,
                                int nrecords);
};

void TestBitMapAccumulator::FillBitMap(uint8_t* bmap, uint32_t seed, int nbytes) {
  ::arrow::random_bytes(nbytes, seed, bmap);
}

void TestBitMapAccumulator::ByteWiseIntersectBitMaps(uint8_t* dst,
                                                     const std::vector<uint8_t*>& srcs,
                                                     int nrecords) {
  int nbytes = nrecords / 8;
  for (int i = 0; i < nbytes; ++i) {
    dst[i] = 0xff;
    for (uint32_t j = 0; j < srcs.size(); ++j) {
      dst[i] = dst[i] & srcs[j][i];
    }
  }
}

TEST_F(TestBitMapAccumulator, TestIntersectBitMaps) {
  const int length = 128;
  const int nrecords = length * 8;
  uint8_t src_bitmaps[4][length];
  uint8_t dst_bitmap[length];
  uint8_t expected_bitmap[length];

  for (int i = 0; i < 4; i++) {
    FillBitMap(src_bitmaps[i], i, length);
  }

  for (int i = 0; i < 4; i++) {
    std::vector<uint8_t*> src_bitmap_ptrs;
    for (int j = 0; j < i; ++j) {
      src_bitmap_ptrs.push_back(src_bitmaps[j]);
    }

    BitMapAccumulator::IntersectBitMaps(dst_bitmap, src_bitmap_ptrs, nrecords);
    ByteWiseIntersectBitMaps(expected_bitmap, src_bitmap_ptrs, nrecords);
    EXPECT_EQ(memcmp(dst_bitmap, expected_bitmap, length), 0);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace gandiva
