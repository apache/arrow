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
                                const std::vector<int64_t>& srcOffsets, int nrecords);
};

void TestBitMapAccumulator::FillBitMap(uint8_t* bmap, uint32_t seed, int nbytes) {
  ::arrow::random_bytes(nbytes, seed, bmap);
}

void TestBitMapAccumulator::ByteWiseIntersectBitMaps(
    uint8_t* dst, const std::vector<uint8_t*>& srcs,
    const std::vector<int64_t>& srcOffsets, int nrecords) {
  if (srcs.empty()) {
    arrow::BitUtil::SetBitsTo(dst, 0, nrecords, true);
    return;
  }

  arrow::internal::CopyBitmap(srcs[0], srcOffsets[0], nrecords, dst, 0);
  for (uint32_t j = 1; j < srcs.size(); ++j) {
    arrow::internal::BitmapAnd(dst, 0, srcs[j], srcOffsets[j], nrecords, 0, dst);
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
    std::vector<int64_t> src_bitmap_offsets(i, 0);
    for (int j = 0; j < i; ++j) {
      src_bitmap_ptrs.push_back(src_bitmaps[j]);
    }

    BitMapAccumulator::IntersectBitMaps(dst_bitmap, src_bitmap_ptrs, src_bitmap_offsets,
                                        nrecords);
    ByteWiseIntersectBitMaps(expected_bitmap, src_bitmap_ptrs, src_bitmap_offsets,
                             nrecords);
    EXPECT_EQ(memcmp(dst_bitmap, expected_bitmap, length), 0);
  }
}

TEST_F(TestBitMapAccumulator, TestIntersectBitMapsWithOffset) {
  const int length = 128;
  uint8_t src_bitmaps[4][length];
  uint8_t dst_bitmap[length];
  uint8_t expected_bitmap[length];

  for (int i = 0; i < 4; i++) {
    FillBitMap(src_bitmaps[i], i, length);
  }

  for (int i = 0; i < 4; i++) {
    std::vector<uint8_t*> src_bitmap_ptrs;
    std::vector<int64_t> src_bitmap_offsets;
    for (int j = 0; j < i; ++j) {
      src_bitmap_ptrs.push_back(src_bitmaps[j]);
      src_bitmap_offsets.push_back(j);  // offset j
    }
    const int nrecords = (i == 0) ? length * 8 : length * 8 - i + 1;

    BitMapAccumulator::IntersectBitMaps(dst_bitmap, src_bitmap_ptrs, src_bitmap_offsets,
                                        nrecords);
    ByteWiseIntersectBitMaps(expected_bitmap, src_bitmap_ptrs, src_bitmap_offsets,
                             nrecords);
    EXPECT_TRUE(
        arrow::internal::BitmapEquals(dst_bitmap, 0, expected_bitmap, 0, nrecords));
  }
}

}  // namespace gandiva
