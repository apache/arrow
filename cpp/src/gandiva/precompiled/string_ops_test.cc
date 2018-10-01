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
#include "gandiva/execution_context.h"
#include "gandiva/precompiled/types.h"
namespace gandiva {

TEST(TestStringOps, TestCompare) {
  const char* left = "abcd789";
  const char* right = "abcd123";

  // 0 for equal
  EXPECT_EQ(mem_compare(left, 4, right, 4), 0);

  // compare lengths if the prefixes match
  EXPECT_GT(mem_compare(left, 5, right, 4), 0);
  EXPECT_LT(mem_compare(left, 4, right, 5), 0);

  // compare bytes if the prefixes don't match
  EXPECT_GT(mem_compare(left, 5, right, 5), 0);
  EXPECT_GT(mem_compare(left, 5, right, 7), 0);
  EXPECT_GT(mem_compare(left, 7, right, 5), 0);
}

TEST(TestStringOps, TestBeginsEnds) {
  // starts_with
  EXPECT_TRUE(starts_with_utf8_utf8("hello sir", 9, "hello", 5));
  EXPECT_TRUE(starts_with_utf8_utf8("hellos", 6, "hello", 5));
  EXPECT_TRUE(starts_with_utf8_utf8("hello", 5, "hello", 5));
  EXPECT_FALSE(starts_with_utf8_utf8("hell", 4, "hello", 5));
  EXPECT_FALSE(starts_with_utf8_utf8("world hello", 11, "hello", 5));

  // ends_with
  EXPECT_TRUE(ends_with_utf8_utf8("hello sir", 9, "sir", 3));
  EXPECT_TRUE(ends_with_utf8_utf8("ssir", 4, "sir", 3));
  EXPECT_TRUE(ends_with_utf8_utf8("sir", 3, "sir", 3));
  EXPECT_FALSE(ends_with_utf8_utf8("ir", 2, "sir", 3));
  EXPECT_FALSE(ends_with_utf8_utf8("hello", 5, "sir", 3));
}

TEST(TestStringOps, TestCharLength) {
  bool valid;

  EXPECT_EQ(utf8_length("hello sir", 9, true, 0, &valid), 9);
  EXPECT_TRUE(valid);

  std::string a("âpple");
  EXPECT_EQ(utf8_length(a.data(), a.length(), true, 0, &valid), 5);
  EXPECT_TRUE(valid);

  std::string b("मदन");
  EXPECT_EQ(utf8_length(b.data(), b.length(), true, 0, &valid), 3);
  EXPECT_TRUE(valid);

  // invalid utf8
  gandiva::helpers::ExecutionContext ctx;
  std::string c("\xf8\x28");
  EXPECT_EQ(utf8_length(c.data(), c.length(), true, (int64)&ctx, &valid), 0);
  EXPECT_TRUE(ctx.get_error().find(
                  "unexpected byte \\f8 encountered while decoding utf8 string") !=
              std::string::npos)
      << ctx.get_error();
  EXPECT_FALSE(valid);
}

}  // namespace gandiva
