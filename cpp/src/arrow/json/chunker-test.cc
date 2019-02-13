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

#include <cstdint>
#include <memory>
#include <numeric>
#include <string>

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/reader.h>

#include "arrow/json/chunker.h"
#include "arrow/json/options.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/util.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace json {

// Use no nested objects and no string literals containing braces in this test.
// This way the positions of '{' and '}' can be used as simple proxies
// for object begin/end.

using util::string_view;

template <typename Lines>
std::string join(Lines&& lines, std::string delimiter) {
  std::string joined;
  for (const auto& line : lines) {
    joined += line + delimiter;
  }
  return joined;
}

std::string PrettyPrint(std::string one_line) {
  rapidjson::Document document;
  document.ParseInsitu(const_cast<char*>(one_line.data()));
  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
  document.Accept(writer);
  return sb.GetString();
}

bool WhitespaceOnly(string_view s) {
  return s.find_first_not_of(" \t\r\n") == string_view::npos;
}

std::size_t ConsumeWholeObject(string_view* str) {
  auto fail = [str] {
    *str = string_view();
    return string_view::npos;
  };
  if (WhitespaceOnly(*str)) return fail();
  auto open_brace = str->find_first_not_of(" \t\r\n");
  if (str->at(open_brace) != '{') return fail();
  auto close_brace = str->find_first_of("}");
  if (close_brace == string_view::npos) return fail();
  if (str->at(close_brace) != '}') return fail();
  auto length = close_brace + 1;
  *str = str->substr(length);
  return length;
}

void AssertWholeObjects(Chunker& chunker, string_view block, int expected_count) {
  string_view whole;
  ASSERT_OK(chunker.Process(block, &whole));
  int count = 0;
  while (!WhitespaceOnly(whole)) {
    if (ConsumeWholeObject(&whole) == string_view::npos) FAIL();
    ++count;
  }
  ASSERT_EQ(count, expected_count);
}

void AssertChunking(Chunker& chunker, std::string str, int total_count) {
  // First chunkize whole JSON block
  AssertWholeObjects(chunker, str, total_count);

  // Then chunkize incomplete substrings of the block
  for (int i = 0; i != total_count; ++i) {
    // ensure shearing the closing brace off the last object causes it to be chunked out
    string_view str_view(str);
    auto last_brace = str_view.find_last_of('}');
    AssertWholeObjects(chunker, str.substr(0, last_brace), total_count - i - 1);

    // ensure skipping one object reduces the count by one
    ASSERT_NE(ConsumeWholeObject(&str_view), string_view::npos);
    str = str_view.to_string();
    AssertWholeObjects(chunker, str, total_count - i - 1);
  }
}

void AssertStraddledChunking(Chunker& chunker, string_view str) {
  auto first_half = str.substr(0, str.size() / 2).to_string();
  auto second_half = str.substr(str.size() / 2);
  AssertChunking(chunker, first_half, 1);
  string_view first_whole;
  ASSERT_OK(chunker.Process(first_half, &first_whole));
  ASSERT_TRUE(string_view(first_half).starts_with(first_whole));
  auto partial = string_view(first_half).substr(first_whole.size());
  string_view completion;
  ASSERT_OK(chunker.Process(partial, second_half, &completion));
  ASSERT_TRUE(second_half.starts_with(completion));
  auto straddling = partial.to_string() + completion.to_string();
  string_view straddling_view(straddling);
  auto length = ConsumeWholeObject(&straddling_view);
  ASSERT_NE(length, string_view::npos);
  ASSERT_NE(length, 0);
  auto final_whole = second_half.substr(completion.size());
  length = ConsumeWholeObject(&final_whole);
  ASSERT_NE(length, string_view::npos);
  ASSERT_NE(length, 0);
}

std::unique_ptr<Chunker> MakeChunker(bool newlines_in_values) {
  auto options = ParseOptions::Defaults();
  options.newlines_in_values = newlines_in_values;
  return Chunker::Make(options);
}

class BaseChunkerTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override { chunker_ = MakeChunker(GetParam()); }

  std::unique_ptr<Chunker> chunker_;
};

INSTANTIATE_TEST_CASE_P(ChunkerTest, BaseChunkerTest, ::testing::Values(true));

INSTANTIATE_TEST_CASE_P(NoNewlineChunkerTest, BaseChunkerTest, ::testing::Values(false));

constexpr auto object_count = 3;
const std::vector<std::string>& lines() {
  static const std::vector<std::string> l = {R"({"0":"ab","1":"c","2":""})",
                                             R"({"0":"def","1":"","2":"gh"})",
                                             R"({"0":"","1":"ij","2":"kl"})"};
  return l;
}

TEST_P(BaseChunkerTest, Basics) {
  AssertChunking(*chunker_, join(lines(), "\n"), object_count);
}

TEST_P(BaseChunkerTest, Empty) {
  AssertChunking(*chunker_, "\n", 0);
  AssertChunking(*chunker_, "\n\n", 0);
}

TEST(ChunkerTest, PrettyPrinted) {
  std::string pretty[object_count];
  std::transform(std::begin(lines()), std::end(lines()), std::begin(pretty), PrettyPrint);
  auto chunker = MakeChunker(true);
  AssertChunking(*chunker, join(pretty, "\n"), object_count);
}

TEST(ChunkerTest, SingleLine) {
  auto chunker = MakeChunker(true);
  AssertChunking(*chunker, join(lines(), ""), object_count);
}

TEST_P(BaseChunkerTest, Straddling) {
  AssertStraddledChunking(*chunker_, join(lines(), "\n"));
}

TEST(ChunkerTest, StraddlingPrettyPrinted) {
  std::string pretty[object_count];
  std::transform(std::begin(lines()), std::end(lines()), std::begin(pretty), PrettyPrint);
  auto chunker = MakeChunker(true);
  AssertStraddledChunking(*chunker, join(pretty, "\n"));
}

TEST(ChunkerTest, StraddlingSingleLine) {
  auto chunker = MakeChunker(true);
  AssertStraddledChunking(*chunker, join(lines(), ""));
}

TEST_P(BaseChunkerTest, StraddlingEmpty) {
  auto joined = join(lines(), "\n");
  auto first = string_view(joined).substr(0, lines()[0].size() + 1);
  auto rest = string_view(joined).substr(first.size());
  string_view first_whole;
  ASSERT_OK(chunker_->Process(first, &first_whole));
  auto partial = first.substr(first_whole.size());
  string_view completion;
  ASSERT_OK(chunker_->Process(partial, rest, &completion));
  ASSERT_EQ(completion.size(), 0);
}

}  // namespace json
}  // namespace arrow
