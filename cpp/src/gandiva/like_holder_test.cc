// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "codegen/like_holder.h"
#include "codegen/regex_util.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace gandiva {

class TestLikeHolder : public ::testing::Test {};

TEST_F(TestLikeHolder, TestMatchAny) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("ab%", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto &like = *like_holder;
  EXPECT_TRUE(like("ab"));
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("abcd"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("cab"));
}

TEST_F(TestLikeHolder, TestMatchOne) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("ab_", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto &like = *like_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("abd"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("abcd"));
  EXPECT_FALSE(like("dabc"));
}

TEST_F(TestLikeHolder, TestPcreSpecial) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make(".*ab_", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto &like = *like_holder;
  EXPECT_TRUE(like(".*abc"));  // . and * aren't special in sql regex
  EXPECT_FALSE(like("xxabc"));
}

TEST_F(TestLikeHolder, TestRegexEscape) {
  std::string res;
  auto status = RegexUtil::SqlLikePatternToPcre("#%hello#_abc_def##", '#', res);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_EQ(res, "%hello_abc.def#");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace gandiva
