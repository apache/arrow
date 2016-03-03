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
#include <limits>
#include <string>

#include "arrow/test-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

using std::string;

namespace arrow {

class TestBuffer : public ::testing::Test {
};

TEST_F(TestBuffer, Resize) {
  PoolBuffer buf;

  ASSERT_EQ(0, buf.size());
  ASSERT_OK(buf.Resize(100));
  ASSERT_EQ(100, buf.size());
  ASSERT_OK(buf.Resize(200));
  ASSERT_EQ(200, buf.size());

  // Make it smaller, too
  ASSERT_OK(buf.Resize(50));
  ASSERT_EQ(50, buf.size());
}

TEST_F(TestBuffer, ResizeOOM) {
  // realloc fails, even though there may be no explicit limit
  PoolBuffer buf;
  ASSERT_OK(buf.Resize(100));
  int64_t to_alloc = std::numeric_limits<int64_t>::max();
  ASSERT_RAISES(OutOfMemory, buf.Resize(to_alloc));
}

} // namespace arrow
