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

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow::internal {

TEST(BitmapOpsTest, PartialLeadingByteSafety) {
  const int64_t num_bytes = 16;
  const std::vector<uint8_t> left(num_bytes, 0), right(num_bytes, 0xFF);
  std::vector<uint8_t> out = right;
  int64_t num_bits = num_bytes * 8;

  auto left_data = left.data();
  auto right_data = right.data();
  auto out_data = out.data();

  for (int64_t out_offset = num_bits - 1; out_offset >= 0; --out_offset) {
    ARROW_SCOPED_TRACE("out offset = " + std::to_string(out_offset));
    for (int64_t left_offset : {out_offset, out_offset + 1}) {
      ARROW_SCOPED_TRACE("left offset = " + std::to_string(left_offset));
      for (int64_t right_offset : {out_offset, out_offset + 2}) {
        ARROW_SCOPED_TRACE("right offset = " + std::to_string(right_offset));
        for (int64_t length = 1;
             length <= num_bits - out_offset && length <= num_bits - left_offset &&
             length <= num_bits - right_offset;
             ++length) {
          ARROW_SCOPED_TRACE("length = " + std::to_string(length));
          BitmapAnd(left_data, out_offset, right_data, right_offset, length, out_offset,
                    out_data);
          // Bytes before out_offset.
          for (int64_t i = 0; i < out_offset / 8; ++i) {
            EXPECT_EQ(out_data[i], 0xFF);
          }
          // The byte holding the out_offset bit.
          EXPECT_EQ(out_data[out_offset / 8], (1 << (out_offset % 8)) - 1);
          // Bytes after the last bit.
          for (int64_t i = (out_offset + length + 7) / 8; i < num_bytes; ++i) {
            EXPECT_EQ(out_data[i], 0);
          }
        }
      }
    }
  }
}

}  // namespace arrow::internal
