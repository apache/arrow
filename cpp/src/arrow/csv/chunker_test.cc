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

#include "arrow/buffer.h"
#include "arrow/csv/chunker.h"
#include "arrow/csv/options.h"
#include "arrow/csv/test_common.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace csv {

void AssertChunkSize(Chunker& chunker, const std::string& str, uint32_t chunk_size) {
  std::shared_ptr<Buffer> block, whole, partial;
  block = std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(str.data()),
                                   static_cast<int64_t>(str.size()));
  ASSERT_OK(chunker.Process(block, &whole, &partial));
  ASSERT_EQ(block->size(), whole->size() + partial->size());
  auto actual_chunk_size = static_cast<uint32_t>(whole->size());
  ASSERT_EQ(actual_chunk_size, chunk_size);
}

template <typename IntContainer>
void AssertChunking(Chunker& chunker, const std::string& str,
                    const IntContainer& expected_lengths) {
  uint32_t expected_chunk_size;

  // First chunkize whole CSV block
  expected_chunk_size = static_cast<uint32_t>(
      std::accumulate(expected_lengths.begin(), expected_lengths.end(), 0ULL));
  AssertChunkSize(chunker, str, expected_chunk_size);

  // Then chunkize incomplete substrings of the block
  expected_chunk_size = 0;
  for (const auto length : expected_lengths) {
    AssertChunkSize(chunker, str.substr(0, expected_chunk_size + length - 1),
                    expected_chunk_size);

    expected_chunk_size += static_cast<uint32_t>(length);
    AssertChunkSize(chunker, str.substr(0, expected_chunk_size), expected_chunk_size);
  }
}

class BaseChunkerTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    options_ = ParseOptions::Defaults();
    options_.newlines_in_values = GetParam();
  }

  void MakeChunker() { chunker_ = ::arrow::csv::MakeChunker(options_); }

  void AssertSkip(const std::string& str, int64_t count, int64_t rem_count,
                  int64_t rest_size) {
    MakeChunker();
    {
      auto test_count = count;
      auto partial = std::make_shared<Buffer>("");
      auto block = std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(str.data()),
                                            static_cast<int64_t>(str.size()));
      std::shared_ptr<Buffer> rest;
      ASSERT_OK(chunker_->ProcessSkip(partial, block, true, &test_count, &rest));
      ASSERT_EQ(rem_count, test_count);
      ASSERT_EQ(rest_size, rest->size());
      AssertBufferEqual(*SliceBuffer(block, block->size() - rest_size), *rest);
    }
    {
      auto test_count = count;
      auto split = static_cast<int64_t>(str.find_first_of('\n'));
      auto partial =
          std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(str.data()), split);
      auto block =
          std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(str.data() + split),
                                   static_cast<int64_t>(str.size()) - split);
      std::shared_ptr<Buffer> rest;
      ASSERT_OK(chunker_->ProcessSkip(partial, block, true, &test_count, &rest));
      ASSERT_EQ(rem_count, test_count);
      ASSERT_EQ(rest_size, rest->size());
      AssertBufferEqual(*SliceBuffer(block, block->size() - rest_size), *rest);
    }
  }

  ParseOptions options_;
  std::unique_ptr<Chunker> chunker_;
};

INSTANTIATE_TEST_SUITE_P(ChunkerTest, BaseChunkerTest, ::testing::Values(true));

INSTANTIATE_TEST_SUITE_P(NoNewlineChunkerTest, BaseChunkerTest, ::testing::Values(false));

TEST_P(BaseChunkerTest, Basics) {
  auto csv = MakeCSVData({"ab,c,\n", "def,,gh\n", ",ij,kl\n"});
  auto lengths = {6, 8, 7};

  MakeChunker();
  AssertChunking(*chunker_, csv, lengths);
}

TEST_P(BaseChunkerTest, Empty) {
  MakeChunker();
  {
    auto csv = MakeCSVData({"\n"});
    auto lengths = {1};
    AssertChunking(*chunker_, csv, lengths);
  }
  {
    auto csv = MakeCSVData({"\n\n"});
    auto lengths = {1, 1};
    AssertChunking(*chunker_, csv, lengths);
  }
  {
    auto csv = MakeCSVData({",\n"});
    auto lengths = {2};
    AssertChunking(*chunker_, csv, lengths);
  }
  {
    auto csv = MakeCSVData({",\n,\n"});
    auto lengths = {2, 2};
    AssertChunking(*chunker_, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, Newlines) {
  auto check_csv = [&](const std::string& csv) {
    AssertChunkSize(*chunker_, csv, static_cast<uint32_t>(csv.size()));
    // Trailing \n after \r is optional
    AssertChunkSize(*chunker_, csv.substr(0, csv.size() - 1),
                    static_cast<uint32_t>(csv.size() - 1));
  };

  MakeChunker();
  {
    ARROW_SCOPED_TRACE("short values");
    check_csv(MakeCSVData({"a\n", "b\r", "c,d\r\n"}));
    ARROW_SCOPED_TRACE("long values");
    check_csv(MakeCSVData(
        {"aaaaaaaaaaaaaaa\n", "bbbbbbbbbbbbb\r", "cccccccccccccc,ddddddddddd\r\n"}));
  }
}

TEST_P(BaseChunkerTest, QuotingSimple) {
  auto check_csv = [&](const std::string& csv) {
    auto lengths = {csv.size()};
    AssertChunking(*chunker_, csv, lengths);
  };

  auto csv_short_values = MakeCSVData({"1,\",3,\",5\n"});
  auto csv_long_values = MakeCSVData({"111111111111,\",3333333333333,\",55555555555\n"});

  for (auto quoting : {true, false}) {
    options_.quoting = quoting;
    MakeChunker();
    ARROW_SCOPED_TRACE("short values");
    check_csv(csv_short_values);
    ARROW_SCOPED_TRACE("long values");
    check_csv(csv_long_values);
  }
}

TEST_P(BaseChunkerTest, QuotingNewline) {
  auto csv = MakeCSVData({"a,\"c \n d\",e\n"});
  if (options_.newlines_in_values) {
    MakeChunker();
    auto lengths = {12};
    AssertChunking(*chunker_, csv, lengths);
  }
  {
    options_.quoting = false;
    MakeChunker();
    auto lengths = {6, 6};
    AssertChunking(*chunker_, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, QuotingUnbalanced) {
  // Quote introduces a quoted field that doesn't end
  auto csv = MakeCSVData({"a,b\n", "1,\",3,,5\n", "c,d\n"});
  if (options_.newlines_in_values) {
    MakeChunker();
    auto lengths = {4};
    AssertChunking(*chunker_, csv, lengths);
  }
  {
    options_.quoting = false;
    MakeChunker();
    auto lengths = {4, 9, 4};
    AssertChunking(*chunker_, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, QuotingEmpty) {
  MakeChunker();
  {
    auto csv = MakeCSVData({"\"\"\n", "a\n"});
    auto lengths = {3, 2};
    AssertChunking(*chunker_, csv, lengths);
  }
  {
    auto csv = MakeCSVData({",\"\"\n", "a\n"});
    auto lengths = {4, 2};
    AssertChunking(*chunker_, csv, lengths);
  }
  {
    auto csv = MakeCSVData({"\"\",\n", "a\n"});
    auto lengths = {4, 2};
    AssertChunking(*chunker_, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, QuotingDouble) {
  {
    MakeChunker();
    // 4 quotes is a quoted quote
    auto csv = MakeCSVData({"\"\"\"\"\n", "a\n"});
    auto lengths = {5, 2};
    AssertChunking(*chunker_, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, QuotesSpecial) {
  // Some non-trivial cases
  {
    MakeChunker();
    auto csv = MakeCSVData({"a,b\"c,d\n", "e\n"});
    auto lengths = {8, 2};
    AssertChunking(*chunker_, csv, lengths);
  }
  {
    MakeChunker();
    auto csv = MakeCSVData({"a,\"b\" \"c\",d\n", "e\n"});
    auto lengths = {12, 2};
    AssertChunking(*chunker_, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, Escaping) {
  {
    auto csv = MakeCSVData({"a\\b,c\n", "d\n"});
    auto lengths = {6, 2};
    {
      options_.escaping = false;
      MakeChunker();
      AssertChunking(*chunker_, csv, lengths);
    }
    {
      options_.escaping = true;
      MakeChunker();
      AssertChunking(*chunker_, csv, lengths);
    }
  }
  {
    auto csv = MakeCSVData({"a\\,b,c\n", "d\n"});
    auto lengths = {7, 2};
    {
      options_.escaping = false;
      MakeChunker();
      AssertChunking(*chunker_, csv, lengths);
    }
    {
      options_.escaping = true;
      MakeChunker();
      AssertChunking(*chunker_, csv, lengths);
    }
  }
}

TEST_P(BaseChunkerTest, EscapingNewline) {
  if (options_.newlines_in_values) {
    auto csv = MakeCSVData({"a\\\nb\n", "c\n"});
    {
      auto lengths = {3, 2, 2};
      MakeChunker();
      AssertChunking(*chunker_, csv, lengths);
    }
    options_.escaping = true;
    {
      auto lengths = {5, 2};
      MakeChunker();
      AssertChunking(*chunker_, csv, lengths);
    }
  }
}

TEST_P(BaseChunkerTest, EscapingAndQuoting) {
  if (options_.newlines_in_values) {
    {
      auto csv = MakeCSVData({"\"a\\\"\n", "\"b\\\"\n"});
      {
        options_.quoting = true;
        options_.escaping = true;
        auto lengths = {10};
        MakeChunker();
        AssertChunking(*chunker_, csv, lengths);
      }
      {
        options_.quoting = true;
        options_.escaping = false;
        auto lengths = {5, 5};
        MakeChunker();
        AssertChunking(*chunker_, csv, lengths);
      }
    }
    {
      auto csv = MakeCSVData({"\"a\\\n\"\n"});
      {
        options_.quoting = false;
        options_.escaping = true;
        auto lengths = {6};
        MakeChunker();
        AssertChunking(*chunker_, csv, lengths);
      }
      {
        options_.quoting = false;
        options_.escaping = false;
        auto lengths = {4, 2};
        MakeChunker();
        AssertChunking(*chunker_, csv, lengths);
      }
    }
  }
}

TEST_P(BaseChunkerTest, ParseSkip) {
  {
    auto csv = MakeCSVData({"ab,c,\n", "def,,gh\n", ",ij,kl\n"});
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 1, 0, 15));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 2, 0, 7));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 3, 0, 0));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 4, 1, 0));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 6, 3, 0));
  }

  // Test with no trailing new line
  {
    auto csv = MakeCSVData({"ab,c,\n", "def,,gh\n", ",ij,kl"});
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 2, 0, 6));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 3, 0, 0));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 4, 1, 0));
  }

  // Test skip with new lines in values
  {
    auto csv = MakeCSVData({"ab,\"c\n\",\n", "\"d\nef\",,gh\n", ",ij,\"nkl\"\n"});
    options_.newlines_in_values = true;
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 1, 0, 21));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 2, 0, 10));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 3, 0, 0));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 4, 1, 0));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 6, 3, 0));
  }

  // Test with no trailing new line and new lines in values
  {
    auto csv = MakeCSVData({"ab,\"c\n\",\n", "\"d\nef\",,gh\n", ",ij,\"nkl\""});
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 2, 0, 9));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 3, 0, 0));
    ASSERT_NO_FATAL_FAILURE(AssertSkip(csv, 4, 1, 0));
  }
}

}  // namespace csv
}  // namespace arrow
