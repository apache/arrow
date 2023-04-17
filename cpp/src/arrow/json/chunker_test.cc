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

#include <algorithm>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/json/chunker.h"
#include "arrow/json/test_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"

namespace arrow {

using internal::StartsWith;

namespace json {

// Use no nested objects and no string literals containing braces in this test.
// This way the positions of '{' and '}' can be used as simple proxies
// for object begin/end.

using std::string_view;

template <typename Lines>
static std::shared_ptr<Buffer> join(Lines&& lines, std::string delimiter,
                                    bool delimiter_at_end = true) {
  std::shared_ptr<Buffer> joined;
  BufferVector line_buffers;
  auto delimiter_buffer = std::make_shared<Buffer>(delimiter);
  for (const auto& line : lines) {
    line_buffers.push_back(std::make_shared<Buffer>(line));
    line_buffers.push_back(delimiter_buffer);
  }
  if (!delimiter_at_end) {
    line_buffers.pop_back();
  }
  return *ConcatenateBuffers(line_buffers);
}

static bool WhitespaceOnly(string_view s) {
  return s.find_first_not_of(" \t\r\n") == string_view::npos;
}

static bool WhitespaceOnly(const std::shared_ptr<Buffer>& b) {
  return WhitespaceOnly(string_view(*b));
}

static std::size_t ConsumeWholeObject(std::shared_ptr<Buffer>* buf) {
  auto str = string_view(**buf);
  auto fail = [buf] {
    *buf = nullptr;
    return string_view::npos;
  };
  if (WhitespaceOnly(str)) return fail();
  auto open_brace = str.find_first_not_of(" \t\r\n");
  if (str.at(open_brace) != '{') return fail();
  auto close_brace = str.find_first_of("}");
  if (close_brace == string_view::npos) return fail();
  auto length = close_brace + 1;
  *buf = SliceBuffer(*buf, length);
  return length;
}

void AssertOnlyWholeObjects(Chunker& chunker, std::shared_ptr<Buffer> whole, int* count) {
  *count = 0;
  while (whole && !WhitespaceOnly(whole)) {
    auto buf = whole;
    if (ConsumeWholeObject(&whole) == string_view::npos) {
      FAIL() << "Not a whole JSON object: '" << buf->ToString() << "'";
    }
    ++*count;
  }
}

void AssertWholeObjects(Chunker& chunker, const std::shared_ptr<Buffer>& block,
                        int expected_count) {
  std::shared_ptr<Buffer> whole, partial;
  ASSERT_OK(chunker.Process(block, &whole, &partial));
  int count;
  AssertOnlyWholeObjects(chunker, whole, &count);
  ASSERT_EQ(count, expected_count);
}

void AssertChunking(Chunker& chunker, std::shared_ptr<Buffer> buf, int total_count) {
  // First chunkize whole JSON block
  AssertWholeObjects(chunker, buf, total_count);

  // Then chunkize incomplete substrings of the block
  for (int i = 0; i < total_count; ++i) {
    // ensure shearing the closing brace off the last object causes it to be chunked out
    auto last_brace = string_view(*buf).find_last_of('}');
    AssertWholeObjects(chunker, SliceBuffer(buf, 0, last_brace), total_count - i - 1);

    // ensure skipping one object reduces the count by one
    ASSERT_NE(ConsumeWholeObject(&buf), string_view::npos);
    AssertWholeObjects(chunker, buf, total_count - i - 1);
  }
}

void AssertChunkingBlockSize(Chunker& chunker, std::shared_ptr<Buffer> buf,
                             int64_t block_size, int expected_count) {
  std::shared_ptr<Buffer> partial = Buffer::FromString({});
  int64_t pos = 0;
  int total_count = 0;
  while (pos < buf->size()) {
    int count;
    auto block = SliceBuffer(buf, pos, std::min(block_size, buf->size() - pos));
    pos += block->size();
    std::shared_ptr<Buffer> completion, whole, next_partial;

    if (pos == buf->size()) {
      // Last block
      ASSERT_OK(chunker.ProcessFinal(partial, block, &completion, &whole));
    } else {
      std::shared_ptr<Buffer> starts_with_whole;
      ASSERT_OK(
          chunker.ProcessWithPartial(partial, block, &completion, &starts_with_whole));
      ASSERT_OK(chunker.Process(starts_with_whole, &whole, &next_partial));
    }
    // partial + completion should be a valid JSON block
    ASSERT_OK_AND_ASSIGN(partial, ConcatenateBuffers({partial, completion}));
    AssertOnlyWholeObjects(chunker, partial, &count);
    total_count += count;
    // whole should be a valid JSON block
    AssertOnlyWholeObjects(chunker, whole, &count);
    total_count += count;
    partial = next_partial;
  }
  ASSERT_EQ(pos, buf->size());
  ASSERT_EQ(total_count, expected_count);
}

void AssertStraddledChunking(Chunker& chunker, const std::shared_ptr<Buffer>& buf) {
  auto first_half = SliceBuffer(buf, 0, buf->size() / 2);
  auto second_half = SliceBuffer(buf, buf->size() / 2);
  AssertChunking(chunker, first_half, 1);
  std::shared_ptr<Buffer> first_whole, partial;
  ASSERT_OK(chunker.Process(first_half, &first_whole, &partial));
  ASSERT_TRUE(StartsWith(std::string_view(*first_half), std::string_view(*first_whole)));
  std::shared_ptr<Buffer> completion, rest;
  ASSERT_OK(chunker.ProcessWithPartial(partial, second_half, &completion, &rest));
  ASSERT_TRUE(StartsWith(std::string_view(*second_half), std::string_view(*completion)));
  std::shared_ptr<Buffer> straddling;
  ASSERT_OK_AND_ASSIGN(straddling, ConcatenateBuffers({partial, completion}));
  auto length = ConsumeWholeObject(&straddling);
  ASSERT_NE(length, string_view::npos);
  ASSERT_NE(length, 0);
  auto final_whole = SliceBuffer(second_half, completion->size());
  ASSERT_EQ(string_view(*final_whole), string_view(*rest));
  length = ConsumeWholeObject(&final_whole);
  ASSERT_NE(length, string_view::npos);
  ASSERT_NE(length, 0);
}

std::unique_ptr<Chunker> MakeChunker(bool newlines_in_values) {
  auto options = ParseOptions::Defaults();
  options.newlines_in_values = newlines_in_values;
  return MakeChunker(options);
}

class BaseChunkerTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override { chunker_ = MakeChunker(GetParam()); }

  std::unique_ptr<Chunker> chunker_;
};

INSTANTIATE_TEST_SUITE_P(NoNewlineChunkerTest, BaseChunkerTest, ::testing::Values(false));

INSTANTIATE_TEST_SUITE_P(ChunkerTest, BaseChunkerTest, ::testing::Values(true));

constexpr int object_count = 4;
constexpr int min_block_size = 28;

static const std::vector<std::string>& lines() {
  // clang-format off
  static const std::vector<std::string> l = {
    R"({"0":"ab","1":"c","2":""})",
    R"({"0":"def","1":"","2":"gh"})",
    R"({"0":null})",
    R"({"0":"","1":"ij","2":"kl"})"
  };
  // clang-format on
  return l;
}

TEST_P(BaseChunkerTest, Basics) {
  AssertChunking(*chunker_, join(lines(), "\n"), object_count);
}

TEST_P(BaseChunkerTest, BlockSizes) {
  auto check_block_sizes = [&](std::shared_ptr<Buffer> data) {
    for (int64_t block_size = min_block_size; block_size < min_block_size + 30;
         ++block_size) {
      AssertChunkingBlockSize(*chunker_, data, block_size, object_count);
    }
  };

  check_block_sizes(join(lines(), "\n"));
  check_block_sizes(join(lines(), "\r\n"));
  // Without ending newline
  check_block_sizes(join(lines(), "\n", false));
  check_block_sizes(join(lines(), "\r\n", false));
}

TEST_P(BaseChunkerTest, Empty) {
  auto empty = std::make_shared<Buffer>("\n");
  AssertChunking(*chunker_, empty, 0);
  empty = std::make_shared<Buffer>("\n\n");
  AssertChunking(*chunker_, empty, 0);
}

TEST(ChunkerTest, PrettyPrinted) {
  std::string pretty[object_count];
  std::transform(std::begin(lines()), std::end(lines()), std::begin(pretty), PrettyPrint);
  auto chunker = MakeChunker(true);
  AssertChunking(*chunker, join(pretty, "\n"), object_count);
}

TEST(ChunkerTest, SingleLine) {
  auto chunker = MakeChunker(true);
  auto single_line = join(lines(), "");
  AssertChunking(*chunker, single_line, object_count);
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

TEST(ChunkerTest, Errors) {
  std::string parts[] = {R"({"a":0})", "}", R"({"a":1})"};
  auto chunker = MakeChunker(true);
  std::shared_ptr<Buffer> whole, rest, completion;
  ASSERT_OK(chunker->Process(Buffer::FromString(parts[0] + parts[1]), &whole, &rest));
  ASSERT_EQ(std::string_view(*whole), parts[0]);
  ASSERT_EQ(std::string_view(*rest), parts[1]);
  auto status =
      chunker->ProcessWithPartial(rest, Buffer::FromString(parts[2]), &completion, &rest);
  ASSERT_RAISES(Invalid, status);
  EXPECT_THAT(status.message(),
              ::testing::StartsWith("JSON chunk error: invalid data at end of document"));
}

TEST_P(BaseChunkerTest, StraddlingEmpty) {
  auto all = join(lines(), "\n");

  auto first = SliceBuffer(all, 0, lines()[0].size() + 1);
  std::shared_ptr<Buffer> first_whole, partial;
  ASSERT_OK(chunker_->Process(first, &first_whole, &partial));
  ASSERT_TRUE(WhitespaceOnly(partial));

  auto others = SliceBuffer(all, first->size());
  std::shared_ptr<Buffer> completion, rest;
  ASSERT_OK(chunker_->ProcessWithPartial(partial, others, &completion, &rest));
  ASSERT_EQ(completion->size(), 0);
  ASSERT_TRUE(rest->Equals(*others));
}

}  // namespace json
}  // namespace arrow
