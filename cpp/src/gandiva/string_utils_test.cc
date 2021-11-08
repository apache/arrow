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

#include "gandiva/string_utils.h"

#include <gtest/gtest.h>

namespace gandiva {

TEST(TestStringUtils, TestStringMatches) {
  std::string string = "hello world";
  std::string substring = "world";
  auto substring_len = static_cast<int32_t>(substring.size());

  EXPECT_FALSE(string_matches(string.c_str(), substring.c_str(), 0, substring_len));
  EXPECT_TRUE(string_matches(string.c_str(), substring.c_str(), 6, substring_len));

  string = "hi johny cage";
  substring = "johny";
  substring_len = static_cast<int32_t>(substring.size());

  EXPECT_FALSE(string_matches(string.c_str(), substring.c_str(), 0, substring_len));
  EXPECT_TRUE(string_matches(string.c_str(), substring.c_str(), 3, substring_len));

  string = "$tr1nG matched!";
  substring = "$tr1nG";
  substring_len = static_cast<int32_t>(substring.size());

  EXPECT_TRUE(string_matches(string.c_str(), substring.c_str(), 0, substring_len));
  EXPECT_FALSE(string_matches(string.c_str(), substring.c_str(), 2, substring_len));
}

}
