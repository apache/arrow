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
  static int64_t EstimatedAllocationSize(int64_t size) {
    return TempVectorStack::EstimatedAllocationSize(size);
  }
};

TEST_F(TempVectorStackTest, OverflowCheck) {
  int64_t stack_size = 64;  // exact 64b of actual buffer size.
  auto stack_allocation_size = EstimatedAllocationSize(
      stack_size);  // padded 144b = 64b(buffer) + 64b(padding) + 16b(two guards).
  std::cout << "stack_allocation_size: " << stack_allocation_size << std::endl;
  TempVectorStack stack;
  ASSERT_OK(stack.Init(default_memory_pool(), stack_size));

  uint32_t v1_size = 64;
  auto v1_allocation_size = EstimatedAllocationSize(v1_size);
  std::cout << "v1_allocation_size: " << v1_allocation_size << std::endl;
  TempVectorHolder<uint8_t> v1(&stack, v1_size);  // vector allocation is OK.
  auto v1_data =
      v1.mutable_data();  // data addr will be on buffer addr + 8b(heading guard).
  for (uint32_t i = 0; i < v1_size; ++i) {
    // The last 8b access will exceed buffer's boundary and can be caught by ASAN.
    v1_data[i] = i;
  }
}

}  // namespace util
}  // namespace arrow
