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

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/util/string.h"

namespace arrow {
namespace internal {

TEST(Trim, Basics) {
  std::vector<std::pair<std::string, std::string>> test_cases = {
      {"", ""},         {" ", ""},     {"  ", ""},       {"\t ", ""},
      {" \ta\t ", "a"}, {" \ta", "a"}, {"ab   \t", "ab"}};
  for (auto case_ : test_cases) {
    EXPECT_EQ(case_.second, TrimString(case_.first));
  }
}

}  // namespace internal
}  // namespace arrow
