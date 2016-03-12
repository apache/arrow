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

#include "arrow/util/bit-util.h"

#include "gtest/gtest.h"

namespace arrow {

TEST(UtilTests, TestNextPower2) {
  using util::next_power2;

  ASSERT_EQ(8, next_power2(6));
  ASSERT_EQ(8, next_power2(8));

  ASSERT_EQ(1, next_power2(1));
  ASSERT_EQ(256, next_power2(131));

  ASSERT_EQ(1024, next_power2(1000));

  ASSERT_EQ(4096, next_power2(4000));

  ASSERT_EQ(65536, next_power2(64000));

  ASSERT_EQ(1LL << 32, next_power2((1LL << 32) - 1));
  ASSERT_EQ(1LL << 31, next_power2((1LL << 31) - 1));
  ASSERT_EQ(1LL << 62, next_power2((1LL << 62) - 1));
}

} // namespace arrow
