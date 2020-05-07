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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/versioning.h"

namespace arrow {
namespace internal {

TEST(SemVer, TestParseBasic) {
  auto v = SemVer(0, 0, 0);

  ASSERT_EQ(v.major, 0);
  ASSERT_EQ(v.minor, 0);
  ASSERT_EQ(v.patch, 0);
  ASSERT_EQ(v.pre_release, "");
  ASSERT_EQ(v.build_info, "");
  ASSERT_EQ(v.ToString(), "0.0.0");

  auto versions = {
    "1.0.0",
    "1.0.",
    "1.0",
    "1.",
    "1"
  };
  for (auto const& version : versions) {
    ASSERT_OK_AND_ASSIGN(v, SemVer::Parse(version));
    ASSERT_EQ(v.major, 1);
    ASSERT_EQ(v.minor, 0);
    ASSERT_EQ(v.patch, 0);
    ASSERT_EQ(v.pre_release, "");
    ASSERT_EQ(v.build_info, "");
    ASSERT_EQ(v.ToString(), "1.0.0");
  }

  ASSERT_OK_AND_ASSIGN(v, SemVer::Parse("0.10.2"));
  ASSERT_EQ(v.major, 0);
  ASSERT_EQ(v.minor, 10);
  ASSERT_EQ(v.patch, 2);
  ASSERT_EQ(v.pre_release, "");
  ASSERT_EQ(v.build_info, "");
  ASSERT_EQ(v.ToString(), "0.10.2");
}

TEST(SemVer, TestParseMetadata) {
  ASSERT_OK_AND_ASSIGN(auto v1, SemVer::Parse("2.3.4-alpha.1+123"));
  ASSERT_EQ(v1.major, 2);
  ASSERT_EQ(v1.minor, 3);
  ASSERT_EQ(v1.patch, 4);
  ASSERT_EQ(v1.pre_release, "alpha.1");
  ASSERT_EQ(v1.build_info, "123");
  ASSERT_EQ(v1.ToString(), "2.3.4-alpha.1+123");

  ASSERT_OK_AND_ASSIGN(auto v2, SemVer::Parse("1.2.12-beta.alpha.1.2"));
  ASSERT_EQ(v2.major, 1);
  ASSERT_EQ(v2.minor, 2);
  ASSERT_EQ(v2.patch, 12);
  ASSERT_EQ(v2.pre_release, "beta.alpha.1.2");
  ASSERT_EQ(v2.build_info, "");
  ASSERT_EQ(v2.ToString(), "1.2.12-beta.alpha.1.2");

  ASSERT_OK_AND_ASSIGN(auto v3, SemVer::Parse("0.12+20200101"));
  ASSERT_EQ(v3.major, 0);
  ASSERT_EQ(v3.minor, 12);
  ASSERT_EQ(v3.patch, 0);
  ASSERT_EQ(v3.pre_release, "");
  ASSERT_EQ(v3.build_info, "20200101");
  ASSERT_EQ(v3.ToString(), "0.12.0+20200101");
}

TEST(SemVer, TestCompare) {
  ASSERT_OK_AND_ASSIGN(auto v0, SemVer::Parse("0"));
  ASSERT_OK_AND_ASSIGN(auto v1, SemVer::Parse("1"));
  ASSERT_OK_AND_ASSIGN(auto v107, SemVer::Parse("1.0.7"));
  ASSERT_OK_AND_ASSIGN(auto v111, SemVer::Parse("1.1.1"));
  ASSERT_OK_AND_ASSIGN(auto v13, SemVer::Parse("1.3"));
  ASSERT_OK_AND_ASSIGN(auto v15, SemVer::Parse("1.5"));
  ASSERT_OK_AND_ASSIGN(auto v22, SemVer::Parse("2.2"));
  ASSERT_OK_AND_ASSIGN(auto v3, SemVer::Parse("3"));

  ASSERT_TRUE(v0 == v0);
  ASSERT_TRUE(v107 == v107);
  ASSERT_TRUE(v0 != v1);
  ASSERT_TRUE(v0 < v1);
  ASSERT_TRUE(v13 <= v15);
  ASSERT_TRUE(v107 <= v22);
  ASSERT_TRUE(v3 > v111);
  ASSERT_TRUE(v3 >= v111);
}

// TODO(kszucs): test errors

}
}
