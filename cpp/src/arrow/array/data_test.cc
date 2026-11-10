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

#include <type_traits>

#include <gtest/gtest.h>

#include "arrow/array/array_base.h"
#include "arrow/array/data.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

TEST(ArraySpan, SetSlice) {
  auto arr = ArrayFromJSON(int32(), "[0, 1, 2, 3, 4, 5, 6, null, 7, 8, 9]");
  ArraySpan span(*arr->data());
  ASSERT_EQ(span.length, arr->length());
  ASSERT_EQ(span.null_count, 1);
  ASSERT_EQ(span.offset, 0);

  span.SetSlice(0, 7);
  ASSERT_EQ(span.length, 7);
  ASSERT_EQ(span.null_count, kUnknownNullCount);
  ASSERT_EQ(span.offset, 0);
  ASSERT_EQ(span.GetNullCount(), 0);

  span.SetSlice(7, 4);
  ASSERT_EQ(span.length, 4);
  ASSERT_EQ(span.null_count, kUnknownNullCount);
  ASSERT_EQ(span.offset, 7);
  ASSERT_EQ(span.GetNullCount(), 1);
}

TEST(ArrayData, GetSpanRespectsOffset) {
  std::vector<uint16_t> values = {1, 2, 3, 4, 5};

  auto buffer = Buffer::FromVector(std::move(values));
  auto data = ArrayData::Make(uint16(), /*length=*/3, {nullptr, buffer}, /*null_count=*/0,
                              /*offset=*/1);
  auto span = data->GetSpan<uint16_t>(1, 3);

  const bool is_const_pointer = std::is_same_v<decltype(span)::pointer, const uint16_t*>;
  ASSERT_TRUE(is_const_pointer);

  EXPECT_EQ(span.size(), 3);
  EXPECT_EQ(span[0], 2);
  EXPECT_EQ(span[1], 3);
  EXPECT_EQ(span[2], 4);
}

TEST(ArrayData, GetMutableSpanRespectsOffset) {
  std::vector<uint16_t> values = {10, 20, 30, 40, 50};

  auto buffer = std::make_shared<MutableBuffer>(reinterpret_cast<uint8_t*>(values.data()),
                                                values.size() * sizeof(uint16_t));
  std::vector<std::shared_ptr<Buffer>> buffers = {nullptr, buffer};

  auto data =
      ArrayData::Make(uint16(), /*length=*/3, buffers, /*null_count=*/0, /*offset=*/1);
  auto span = data->GetMutableSpan<uint16_t>(1, 3);

  const bool is_mut_pointer = std::is_same_v<decltype(span)::pointer, uint16_t*>;
  ASSERT_TRUE(is_mut_pointer);

  EXPECT_EQ(span.size(), 3);
  EXPECT_EQ(span[0], 20);
  EXPECT_EQ(span[1], 30);
  EXPECT_EQ(span[2], 40);

  span[0] = 200;
  span[1] = 300;
  span[2] = 400;

  auto raw = reinterpret_cast<uint16_t*>(buffer->mutable_data());

  EXPECT_EQ(raw[1], 200);
  EXPECT_EQ(raw[2], 300);
  EXPECT_EQ(raw[3], 400);
}

TEST(ArrayData, GetSpanNullBuffer) {
  auto data = ArrayData::Make(uint16(), /*length=*/0, /*buffers=*/{nullptr, nullptr},
                              /*null_count=*/0, /*offset=*/0);
  auto span = data->GetSpan<uint16_t>(1, 0);
  EXPECT_EQ(span.size(), 0);
}

TEST(ArrayData, GetMutableSpanNullBuffer) {
  auto data = ArrayData::Make(uint16(), /*length=*/0, /*buffers=*/{nullptr, nullptr},
                              /*null_count=*/0, /*offset=*/0);
  auto span = data->GetMutableSpan<uint16_t>(1, 0);
  EXPECT_EQ(span.size(), 0);
}

}  // namespace arrow
