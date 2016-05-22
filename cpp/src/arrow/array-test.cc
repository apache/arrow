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

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/types/primitive.h"
#include "arrow/util/buffer.h"
#include "arrow/util/memory-pool.h"

namespace arrow {

#define default_pool_buffer std::make_shared<PoolBuffer>(pool_)

class TestArray : public ::testing::Test {
 public:
  void SetUp() { pool_ = default_memory_pool(); }

 protected:
  MemoryPool* pool_;
};

TEST_F(TestArray, TestNullCount) {
  auto data = default_pool_buffer;
  auto null_bitmap = default_pool_buffer;

  std::unique_ptr<Int32Array> arr(new Int32Array(100, data, 10, null_bitmap));
  ASSERT_EQ(10, arr->null_count());

  std::unique_ptr<Int32Array> arr_no_nulls(new Int32Array(100, data));
  ASSERT_EQ(0, arr_no_nulls->null_count());
}

TEST_F(TestArray, TestLength) {
  auto data = default_pool_buffer;
  std::unique_ptr<Int32Array> arr(new Int32Array(100, data));
  ASSERT_EQ(arr->length(), 100);
}

TEST_F(TestArray, TestSlice) {
  auto mutable_data = default_pool_buffer;
  mutable_data->Reserve(100);
  std::unique_ptr<Int32Array> mutable_arr(new Int32Array(25, mutable_data));

  // test slice data buffer
  auto raw_data =
      std::dynamic_pointer_cast<MutableBuffer>(mutable_arr->data())->mutable_data();
  for (int i = 0; i < 10; i++) {
    raw_data[i] = i;
  }

  auto sliced_arr_1 = std::dynamic_pointer_cast<Int32Array>(mutable_arr->Slice(1, 9));
  auto sliced_data_1 = sliced_arr_1->data()->data();
  ASSERT_EQ(9, sliced_arr_1->length());
  ASSERT_EQ(
      0, memcmp(raw_data + sizeof(uint32_t), sliced_data_1, sliced_arr_1->length()));

  auto sliced_arr_2 = std::dynamic_pointer_cast<Int32Array>(mutable_arr->Slice(0));
  auto sliced_data_2 = sliced_arr_2->data()->data();
  ASSERT_EQ(sliced_arr_2->length(), mutable_arr->length());
  ASSERT_EQ(0, memcmp(raw_data, sliced_data_2, sliced_arr_2->length()));

  // test slice null_bitmap
  // clang-format off
  std::vector<uint8_t> null_bitmap = {1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 0, 1};
  // clang-format on
  int32_t null_count = 0;
  for (uint8_t x : null_bitmap) {
    if (x == 0) ++null_count;
  }
  std::shared_ptr<Buffer> null_buf = test::bytes_to_null_buffer(null_bitmap);
  std::unique_ptr<Array> bitmap_arr;
  bitmap_arr.reset(new Int32Array(25, mutable_data, null_count, null_buf));
  ASSERT_EQ(null_count, bitmap_arr->null_count());
  ASSERT_EQ(5, null_buf->size());
  ASSERT_EQ(25, bitmap_arr->length());

  for (size_t i = 0; i < null_bitmap.size(); ++i) {
    EXPECT_EQ(null_bitmap[i], !bitmap_arr->IsNull(i)) << i;
  }

  // test 1
  auto sliced_arr_3 = std::dynamic_pointer_cast<Int32Array>(bitmap_arr->Slice(1, 9));
  ASSERT_NE(nullptr, sliced_arr_3->null_bitmap());
  ASSERT_EQ(2, sliced_arr_3->null_bitmap()->size());
  ASSERT_EQ(5, sliced_arr_3->null_count());
  // clang-format off
  std::vector<uint8_t> sliced_null_bitmap = {0, 1, 1, 0, 1, 0, 0, 1,
                                             0};
  // clang-format on
  for (size_t i = 0; i < sliced_null_bitmap.size(); ++i) {
    EXPECT_EQ(sliced_null_bitmap[i], !sliced_arr_3->IsNull(i)) << i;
  }

  // test 2
  auto sliced_arr_4 = std::dynamic_pointer_cast<Int32Array>(bitmap_arr->Slice(1));
  ASSERT_NE(nullptr, sliced_arr_4->null_bitmap());
  ASSERT_EQ(3, sliced_arr_4->null_bitmap()->size());
  ASSERT_EQ(12, sliced_arr_4->null_count());
  // clang-format off
  std::vector<uint8_t> sliced_null_bitmap_2 = {0, 1, 1, 0, 1, 0, 0, 1,
                                               0, 1, 1, 0, 1, 0, 0, 1,
                                               0, 1, 1, 0, 1, 0, 0, 1};
  // clang-format on
  for (size_t i = 0; i < sliced_null_bitmap_2.size(); ++i) {
    EXPECT_EQ(sliced_null_bitmap_2[i], !sliced_arr_4->IsNull(i)) << i;
  }
}

TEST_F(TestArray, TestIsNull) {
  // clang-format off
  std::vector<uint8_t> null_bitmap = {1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 0, 1};
  // clang-format on
  int32_t null_count = 0;
  for (uint8_t x : null_bitmap) {
    if (x == 0) { ++null_count; }
  }

  std::shared_ptr<Buffer> null_buf = test::bytes_to_null_buffer(null_bitmap);
  std::unique_ptr<Array> arr;
  arr.reset(new Int32Array(null_bitmap.size(), nullptr, null_count, null_buf));

  ASSERT_EQ(null_count, arr->null_count());
  ASSERT_EQ(5, null_buf->size());

  ASSERT_TRUE(arr->null_bitmap()->Equals(*null_buf.get()));

  for (size_t i = 0; i < null_bitmap.size(); ++i) {
    EXPECT_EQ(null_bitmap[i], !arr->IsNull(i)) << i;
  }
}

TEST_F(TestArray, TestCopy) {}

}  // namespace arrow
