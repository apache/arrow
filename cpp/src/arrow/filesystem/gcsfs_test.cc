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

#include "arrow/filesystem/gcsfs.h"

#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

#include <string>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace fs {
namespace {

using ::testing::IsEmpty;
using ::testing::Not;
using ::testing::NotNull;

TEST(GCSFileSystem, Compare) {
  auto a = internal::MakeGCSFileSystemForTest(GCSOptions{});
  EXPECT_THAT(a.get(), NotNull());
  EXPECT_EQ(a, a);

  auto b = internal::MakeGCSFileSystemForTest(GCSOptions{});
  EXPECT_THAT(b.get(), NotNull());
  EXPECT_EQ(b, b);

  EXPECT_NE(a, b);
}

}  // namespace
}  // namespace fs
}  // namespace arrow
