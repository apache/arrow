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

TEST(TestSpscQueue, TestMoveOnly) {
  SpscQueue<MoveOnlyDataType> queue(3);
  ASSERT_TRUE(queue.IsEmpty());
  ASSERT_FALSE(queue.IsFull());
  ASSERT_EQ(queue.SizeGuess(), 0);

  MoveOnlyDataType in(42);
  queue.Write(std::move(in));
  ASSERT_FALSE(queue.IsEmpty());
  ASSERT_FALSE(queue.IsFull());
  ASSERT_EQ(queue.SizeGuess(), 1);

  queue.Write(43);
  ASSERT_FALSE(queue.IsEmpty());
  ASSERT_TRUE(queue.IsFull());
  ASSERT_EQ(queue.SizeGuess(), 2);

  MoveOnlyDataType out = std::move(*queue.FrontPtr());
  ASSERT_EQ(42, *out.data);
  queue.PopFront();
  ASSERT_TRUE(queue.Read(out));
  ASSERT_EQ(43, *out.data);

  ASSERT_TRUE(queue.IsEmpty());
  ASSERT_FALSE(queue.IsFull());
  ASSERT_EQ(queue.SizeGuess(), 0);
}

}  // namespace util
}  // namespace arrow
