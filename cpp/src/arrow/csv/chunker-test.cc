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
#include <numeric>
#include <string>

#include <gtest/gtest.h>

#include "arrow/csv/chunker.h"
#include "arrow/csv/options.h"
#include "arrow/csv/test-common.h"
#include "arrow/test-util.h"

namespace arrow {
namespace csv {

void AssertChunkSize(Chunker& chunker, const std::string& str, uint32_t chunk_size) {
  uint32_t actual_chunk_size;
  ASSERT_OK(
      chunker.Process(str.data(), static_cast<uint32_t>(str.size()), &actual_chunk_size));
  ASSERT_EQ(actual_chunk_size, chunk_size);
}

template <typename IntContainer>
void AssertChunking(Chunker& chunker, const std::string& str,
                    const IntContainer& lengths) {
  uint32_t expected_chunk_size;

  // First chunkize whole CSV block
  expected_chunk_size =
      static_cast<uint32_t>(std::accumulate(lengths.begin(), lengths.end(), 0ULL));
  AssertChunkSize(chunker, str, expected_chunk_size);

  // Then chunkize incomplete substrings of the block
  expected_chunk_size = 0;
  for (const auto length : lengths) {
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

  ParseOptions options_;
};

INSTANTIATE_TEST_CASE_P(ChunkerTest, BaseChunkerTest, ::testing::Values(true));

INSTANTIATE_TEST_CASE_P(NoNewlineChunkerTest, BaseChunkerTest, ::testing::Values(false));

TEST_P(BaseChunkerTest, Basics) {
  auto csv = MakeCSVData({"ab,c,\n", "def,,gh\n", ",ij,kl\n"});
  auto lengths = {6, 8, 7};
  Chunker chunker(options_);

  AssertChunking(chunker, csv, lengths);
}

TEST_P(BaseChunkerTest, Empty) {
  Chunker chunker(options_);
  {
    auto csv = MakeCSVData({"\n"});
    auto lengths = {1};
    AssertChunking(chunker, csv, lengths);
  }
  {
    auto csv = MakeCSVData({"\n\n"});
    auto lengths = {1, 1};
    AssertChunking(chunker, csv, lengths);
  }
  {
    auto csv = MakeCSVData({",\n"});
    auto lengths = {2};
    AssertChunking(chunker, csv, lengths);
  }
  {
    auto csv = MakeCSVData({",\n,\n"});
    auto lengths = {2, 2};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, Newlines) {
  Chunker chunker(options_);
  {
    auto csv = MakeCSVData({"a\n", "b\r", "c,d\r\n"});
    AssertChunkSize(chunker, csv, static_cast<uint32_t>(csv.size()));
    // Trailing \n after \r is optional
    AssertChunkSize(chunker, csv.substr(0, csv.size() - 1),
                    static_cast<uint32_t>(csv.size() - 1));
  }
}

TEST_P(BaseChunkerTest, QuotingSimple) {
  auto csv = MakeCSVData({"1,\",3,\",5\n"});
  {
    Chunker chunker(options_);
    auto lengths = {csv.size()};
    AssertChunking(chunker, csv, lengths);
  }
  {
    options_.quoting = false;
    Chunker chunker(options_);
    auto lengths = {csv.size()};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, QuotingNewline) {
  auto csv = MakeCSVData({"a,\"c \n d\",e\n"});
  if (options_.newlines_in_values) {
    Chunker chunker(options_);
    auto lengths = {12};
    AssertChunking(chunker, csv, lengths);
  }
  {
    options_.quoting = false;
    Chunker chunker(options_);
    auto lengths = {6, 6};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, QuotingUnbalanced) {
  // Quote introduces a quoted field that doesn't end
  auto csv = MakeCSVData({"a,b\n", "1,\",3,,5\n", "c,d\n"});
  if (options_.newlines_in_values) {
    Chunker chunker(options_);
    auto lengths = {4};
    AssertChunking(chunker, csv, lengths);
  }
  {
    options_.quoting = false;
    Chunker chunker(options_);
    auto lengths = {4, 9, 4};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, QuotingEmpty) {
  Chunker chunker(options_);
  {
    auto csv = MakeCSVData({"\"\"\n", "a\n"});
    auto lengths = {3, 2};
    AssertChunking(chunker, csv, lengths);
  }
  {
    auto csv = MakeCSVData({",\"\"\n", "a\n"});
    auto lengths = {4, 2};
    AssertChunking(chunker, csv, lengths);
  }
  {
    auto csv = MakeCSVData({"\"\",\n", "a\n"});
    auto lengths = {4, 2};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, QuotingDouble) {
  {
    Chunker chunker(options_);
    // 4 quotes is a quoted quote
    auto csv = MakeCSVData({"\"\"\"\"\n", "a\n"});
    auto lengths = {5, 2};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, QuotesSpecial) {
  // Some non-trivial cases
  {
    Chunker chunker(options_);
    auto csv = MakeCSVData({"a,b\"c,d\n", "e\n"});
    auto lengths = {8, 2};
    AssertChunking(chunker, csv, lengths);
  }
  {
    Chunker chunker(options_);
    auto csv = MakeCSVData({"a,\"b\" \"c\",d\n", "e\n"});
    auto lengths = {12, 2};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST_P(BaseChunkerTest, Escaping) {
  {
    auto csv = MakeCSVData({"a\\b,c\n", "d\n"});
    auto lengths = {6, 2};
    {
      options_.escaping = false;
      Chunker chunker(options_);
      AssertChunking(chunker, csv, lengths);
    }
    {
      options_.escaping = true;
      Chunker chunker(options_);
      AssertChunking(chunker, csv, lengths);
    }
  }
  {
    auto csv = MakeCSVData({"a\\,b,c\n", "d\n"});
    auto lengths = {7, 2};
    {
      options_.escaping = false;
      Chunker chunker(options_);
      AssertChunking(chunker, csv, lengths);
    }
    {
      options_.escaping = true;
      Chunker chunker(options_);
      AssertChunking(chunker, csv, lengths);
    }
  }
}

TEST_P(BaseChunkerTest, EscapingNewline) {
  if (options_.newlines_in_values) {
    auto csv = MakeCSVData({"a\\\nb\n", "c\n"});
    {
      auto lengths = {3, 2, 2};
      Chunker chunker(options_);
      AssertChunking(chunker, csv, lengths);
    }
    options_.escaping = true;
    {
      auto lengths = {5, 2};
      Chunker chunker(options_);
      AssertChunking(chunker, csv, lengths);
    }
  }
}

}  // namespace csv
}  // namespace arrow
