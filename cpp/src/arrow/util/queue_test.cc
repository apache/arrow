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
#include "arrow/util/queue.h"

namespace arrow {
namespace util {

TEST(TestQueue, TestMoveOnly) {
  SpscQueue<MoveOnlyDataType> queue(2);
  MoveOnlyDataType in(42);
  queue.write(std::move(in));
  MoveOnlyDataType out = std::move(*queue.frontPtr());
  ASSERT_EQ(42, *out.data);
}

// Just make something rediculously large enough that allocation will fail
struct LargeDataType {
  uint64_t a, b, c, d, e, f, g, h;
};

// A warning for future users of SpscQueue, it can throw if allocation fails
TEST(TestQueue, TestCreateLargeThrows) {
  ASSERT_THROW(SpscQueue<LargeDataType> queue(std::numeric_limits<uint32_t>::max()),
               std::bad_alloc);
}

}  // namespace util
}  // namespace arrow
