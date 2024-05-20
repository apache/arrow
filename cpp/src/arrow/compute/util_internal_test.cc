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

#include "arrow/buffer.h"
#include "arrow/compute/util_internal.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace util {

class TempVectorStackTest : public ::testing::Test {
 protected:
  static const uint8_t* BufferData(const TempVectorStack& stack) {
    return stack.buffer_->data();
  }

  static int64_t BufferCapacity(const TempVectorStack& stack) {
    return stack.buffer_->capacity();
  }
};

// GH-41738: Test the underlying buffer capacity is sufficient to hold the requested
// vector.
TEST_F(TempVectorStackTest, BufferCapacitySufficiency) {
  for (uint32_t stack_size : {1, 7, 8, 63, 64, 65535, 65536}) {
    ARROW_SCOPED_TRACE("stack_size = ", stack_size);
    TempVectorStack stack;
    ASSERT_OK(stack.Init(default_memory_pool(), stack_size));

    TempVectorHolder<uint8_t> v(&stack, stack_size);
    ASSERT_LE(v.mutable_data() + stack_size, BufferData(stack) + BufferCapacity(stack));
  }
}

}  // namespace util
}  // namespace arrow
