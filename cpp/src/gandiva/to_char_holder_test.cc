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

#include "gandiva/to_char_holder.h"
#include "arrow/testing/gtest_util.h"

#include <gtest/gtest.h>

namespace gandiva {
class ToCharHolderTest : public ::testing::Test {
 public:
  static FunctionNode BuildToChar(std::string pattern,
                                  std::shared_ptr<arrow::DataType> in_type) {
    auto field = std::make_shared<FieldNode>(arrow::field("toChar", in_type));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    return FunctionNode("to_char", {field, pattern_node}, arrow::utf8());
  }
};

TEST(ToCharHolderTest, testSimpleMake) {
  std::shared_ptr<ToCharHolder> to_char_holder;
  ASSERT_OK(ToCharHolder::Make("$###,###.##", &to_char_holder));

  ASSERT_STREQ("$5.50", to_char_holder->Format(5.5f));
  ASSERT_STREQ("$5.50", to_char_holder->Format(5.5));
  ASSERT_STREQ("$999,999.99", to_char_holder->Format((int32_t)99999999));
  ASSERT_STREQ("$9999,999.99", to_char_holder->Format((int64_t)999999999));
}

TEST(ToCharHolderTest, testFunctionHolderMake) {
  std::shared_ptr<ToCharHolder> to_char_holder;

  ASSERT_OK(ToCharHolder::Make(ToCharHolderTest::BuildToChar("$###,###.##", arrow::float32()), &to_char_holder));

  ASSERT_STREQ("$5.50", to_char_holder->Format(5.5f));
  ASSERT_STREQ("$5.50", to_char_holder->Format(5.5));
  ASSERT_STREQ("$999,999.99", to_char_holder->Format((int32_t)99999999));
  ASSERT_STREQ("$9999,999.99", to_char_holder->Format((int64_t)999999999));
}
}  // namespace gandiva
