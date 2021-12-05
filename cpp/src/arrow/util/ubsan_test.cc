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

#include "arrow/util/ubsan.h"

#include <gtest/gtest.h>

namespace arrow {
namespace internal {

TEST(SafeLoad, TargetOfSameLength) {
  uint64_t source = ~0ULL;
  uint8_t* source_pointer = (uint8_t*)(&source);
  uint64_t target = util::SafeLoadAs<uint64_t>(source_pointer);
  ASSERT_EQ(target, source);

  target = util::SafeLoad<uint64_t>(&source);
  ASSERT_EQ(target, source);
}

TEST(SafeLoad, TargetOfSmallerLength) {
  uint64_t source = ~0ULL;
  uint8_t* source_pointer = (uint8_t*)(&source);
  int32_t target = util::SafeLoadAs<int32_t>(source_pointer);
  ASSERT_EQ(target, ~0UL);

  target = util::SafeLoad<uint8_t>(source_pointer);
  ASSERT_EQ(target, 0xff);
}

TEST(SafeLoadWithLength, Basics) {
  uint64_t source = ~0ULL;
  uint8_t* source_pointer = (uint8_t*)(&source);
  uint64_t target = util::SafeLoadAs<uint64_t>(source_pointer, 1);
  uint8_t* target_pointer = (uint8_t*)(&target);
  ASSERT_EQ(*target_pointer, 0xff);
  ASSERT_EQ(*(target_pointer + 1), 0);

  target = util::SafeLoadAs<uint64_t>(source_pointer, 8);
  ASSERT_EQ(target, source);

  target = util::SafeLoadAs<uint64_t>(source_pointer, 2);
  ASSERT_EQ(*target_pointer, 0xff);
  ASSERT_EQ(*(target_pointer + 1), 0xff);
  ASSERT_EQ(*(target_pointer + 2), 0);

  target = util::SafeLoad<uint64_t>(&source, 8);
  ASSERT_EQ(target, source);
}

}  // namespace internal
}  // namespace arrow
