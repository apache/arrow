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
#include <memory>
#include <string>
#include <vector>

#include "arrow/table/column.h"
#include "arrow/table/schema.h"
#include "arrow/table/table.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/memory-pool.h"

namespace arrow {

class TestBase : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = GetDefaultMemoryPool();
  }

  template <typename ArrayType>
  std::shared_ptr<Array> MakePrimitive(int32_t length, int32_t null_count = 0) {
    auto data = std::make_shared<PoolBuffer>(pool_);
    auto nulls = std::make_shared<PoolBuffer>(pool_);
    EXPECT_OK(data->Resize(length * sizeof(typename ArrayType::value_type)));
    EXPECT_OK(nulls->Resize(util::bytes_for_bits(length)));
    return std::make_shared<ArrayType>(length, data, 10, nulls);
  }

 protected:
  MemoryPool* pool_;
};

} // namespace arrow
