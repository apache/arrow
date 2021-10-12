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

TEST(GcsFileSystem, OptionsCompare) {
  GcsOptions a;
  GcsOptions b;
  b.endpoint_override = "localhost:1234";
  EXPECT_TRUE(a.Equals(a));
  EXPECT_TRUE(b.Equals(b));
  auto c = b;
  c.scheme = "http";
  EXPECT_FALSE(b.Equals(c));
}

TEST(GcsFileSystem, FileSystemCompare) {
  auto a = internal::MakeGcsFileSystemForTest(GcsOptions{});
  EXPECT_THAT(a, NotNull());
  EXPECT_TRUE(a->Equals(*a));

  GcsOptions options;
  options.endpoint_override = "localhost:1234";
  auto b = internal::MakeGcsFileSystemForTest(options);
  EXPECT_THAT(b, NotNull());
  EXPECT_TRUE(b->Equals(*b));

  EXPECT_FALSE(a->Equals(*b));
}

}  // namespace
}  // namespace fs
}  // namespace arrow
