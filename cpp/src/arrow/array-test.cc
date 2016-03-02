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

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/types/integer.h"
#include "arrow/types/primitive.h"
#include "arrow/util/buffer.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

using std::string;
using std::vector;

namespace arrow {

static TypePtr int32 = TypePtr(new Int32Type());
static TypePtr int32_nn = TypePtr(new Int32Type(false));


class TestArray : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = GetDefaultMemoryPool();

    auto data = std::make_shared<PoolBuffer>(pool_);
    auto nulls = std::make_shared<PoolBuffer>(pool_);

    ASSERT_OK(data->Resize(400));
    ASSERT_OK(nulls->Resize(128));

    arr_.reset(new Int32Array(100, data, nulls));
  }

 protected:
  MemoryPool* pool_;
  std::unique_ptr<Int32Array> arr_;
};


TEST_F(TestArray, TestNullable) {
  std::shared_ptr<Buffer> tmp = arr_->data();
  std::unique_ptr<Int32Array> arr_nn(new Int32Array(100, tmp));

  ASSERT_TRUE(arr_->nullable());
  ASSERT_FALSE(arr_nn->nullable());
}


TEST_F(TestArray, TestLength) {
  ASSERT_EQ(arr_->length(), 100);
}

TEST_F(TestArray, TestIsNull) {
  vector<uint8_t> nulls = {1, 0, 1, 1, 0, 1, 0, 0,
                           1, 0, 1, 1, 0, 1, 0, 0,
                           1, 0, 1, 1, 0, 1, 0, 0,
                           1, 0, 1, 1, 0, 1, 0, 0,
                           1, 0, 0, 1};

  std::shared_ptr<Buffer> null_buf = bytes_to_null_buffer(nulls.data(), nulls.size());
  std::unique_ptr<Array> arr;
  arr.reset(new Array(int32, nulls.size(), null_buf));

  ASSERT_EQ(null_buf->size(), 5);
  for (size_t i = 0; i < nulls.size(); ++i) {
    ASSERT_EQ(static_cast<bool>(nulls[i]), arr->IsNull(i));
  }
}


TEST_F(TestArray, TestCopy) {
}

} // namespace arrow
