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

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/rle_util.h"

namespace arrow {
namespace rle_util {

TEST(TestRleUtil, FindPhysicalOffsetTest) {
  ASSERT_EQ(FindPhysicalOffset((const int64_t[]){1}, 1, 0), 0);
  ASSERT_EQ(FindPhysicalOffset((const int64_t[]){1, 2, 3}, 3, 0), 0);
  ASSERT_EQ(FindPhysicalOffset((const int64_t[]){1, 2, 3}, 3, 1), 1);
  ASSERT_EQ(FindPhysicalOffset((const int64_t[]){1, 2, 3}, 3, 2), 2);
  ASSERT_EQ(FindPhysicalOffset((const int64_t[]){1, 2, 3}, 3, 3), 3);
  ASSERT_EQ(FindPhysicalOffset((const int64_t[]){2, 3, 4}, 3, 0), 0);
  ASSERT_EQ(FindPhysicalOffset((const int64_t[]){2, 3, 4}, 3, 1), 0);
  ASSERT_EQ(FindPhysicalOffset((const int64_t[]){2, 3, 4}, 3, 2), 1);
  ASSERT_EQ(FindPhysicalOffset((const int64_t[]){2, 3, 4}, 3, 3), 2);
  ASSERT_EQ(FindPhysicalOffset((const int64_t[]){2, 4, 6}, 3, 3), 1);
}

}  // namespace rle_util
}  // namespace arrow
