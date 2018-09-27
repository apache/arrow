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

#include <algorithm>
#include <numeric>
#include <string>
#include <vector>

#include "arrow/csv/chunker.h"
#include "arrow/csv/test-common.h"
#include "arrow/status.h"
#include "arrow/test-util.h"

namespace arrow {
namespace csv {

void AssertChunkSize(Chunker& chunker, const std::string& str, uint32_t chunk_size,
                     uint32_t num_rows) {
  uint32_t actual_chunk_size;
  ASSERT_OK(
      chunker.Process(str.data(), static_cast<uint32_t>(str.size()), &actual_chunk_size));
  ASSERT_EQ(actual_chunk_size, chunk_size);
  ASSERT_EQ(chunker.num_rows(), num_rows);
}

template <typename IntContainer>
void AssertChunking(Chunker& chunker, const std::string& str,
                    const IntContainer& lengths) {
  uint32_t expected_chunk_size;
  uint32_t num_rows =
      static_cast<uint32_t>(std::distance(lengths.begin(), lengths.end()));

  // First chunkize whole CSV block
  expected_chunk_size =
      static_cast<uint32_t>(std::accumulate(lengths.begin(), lengths.end(), 0ULL));
  AssertChunkSize(chunker, str, expected_chunk_size, num_rows);

  // Then chunkize incomplete substrings of the block
  expected_chunk_size = 0;
  num_rows = 0;
  for (const auto length : lengths) {
    AssertChunkSize(chunker, str.substr(0, expected_chunk_size + length - 1),
                    expected_chunk_size, num_rows);

    ++num_rows;
    expected_chunk_size += static_cast<uint32_t>(length);
    AssertChunkSize(chunker, str.substr(0, expected_chunk_size), expected_chunk_size,
                    num_rows);
  }
}

TEST(Chunker, Basics) {
  auto csv = MakeCSVData({"ab,c,\n", "def,,gh\n", ",ij,kl\n"});
  auto lengths = {6, 8, 7};
  Chunker chunker(ParseOptions::Defaults());

  AssertChunking(chunker, csv, lengths);
}

TEST(Chunker, Empty) {
  Chunker chunker(ParseOptions::Defaults());
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

TEST(Chunker, Newlines) {
  Chunker chunker(ParseOptions::Defaults());
  {
    auto csv = MakeCSVData({"a\n", "b\r", "c,d\r\n"});
    AssertChunkSize(chunker, csv, static_cast<uint32_t>(csv.size()), 3);
    // Trailing \n after \r is optional
    AssertChunkSize(chunker, csv.substr(0, csv.size() - 1),
                    static_cast<uint32_t>(csv.size() - 1), 3);
  }
}

TEST(Chunker, MaxNumRows) {
  Chunker chunker(ParseOptions::Defaults(), 3 /* max_num_rows */);
  auto csv = MakeCSVData({"ab\n", "c\n", "def\n", "g\n"});
  auto lengths = {3, 2, 4};
  AssertChunking(chunker, csv, lengths);
}

TEST(Chunker, QuotingSimple) {
  auto csv = MakeCSVData({"1,\",3,\",5\n"});
  {
    Chunker chunker(ParseOptions::Defaults());
    auto lengths = {csv.size()};
    AssertChunking(chunker, csv, lengths);
  }
  {
    auto options = ParseOptions::Defaults();
    options.quoting = false;
    Chunker chunker(options);
    auto lengths = {csv.size()};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST(Chunker, QuotingNewline) {
  auto csv = MakeCSVData({"a,\"c \n d\",e\n"});
  {
    Chunker chunker(ParseOptions::Defaults());
    auto lengths = {12};
    AssertChunking(chunker, csv, lengths);
  }
  {
    auto options = ParseOptions::Defaults();
    options.quoting = false;
    Chunker chunker(options);
    auto lengths = {6, 6};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST(Chunker, QuotingUnbalanced) {
  // Quote introduces a quoted field that doesn't end
  auto csv = MakeCSVData({"a,b\n", "1,\",3,,5\n", "c,d\n"});
  {
    Chunker chunker(ParseOptions::Defaults());
    auto lengths = {4};
    AssertChunking(chunker, csv, lengths);
  }
  {
    auto options = ParseOptions::Defaults();
    options.quoting = false;
    Chunker chunker(options);
    auto lengths = {4, 9, 4};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST(Chunker, QuotingEmpty) {
  Chunker chunker(ParseOptions::Defaults());
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

TEST(Chunker, QuotingDouble) {
  {
    Chunker chunker(ParseOptions::Defaults());
    // 4 quotes is a quoted quote
    auto csv = MakeCSVData({"\"\"\"\"\n", "a\n"});
    auto lengths = {5, 2};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST(Chunker, QuotesSpecial) {
  // Some non-trivial cases
  {
    Chunker chunker(ParseOptions::Defaults());
    auto csv = MakeCSVData({"a,b\"c,d\n", "e\n"});
    auto lengths = {8, 2};
    AssertChunking(chunker, csv, lengths);
  }
  {
    Chunker chunker(ParseOptions::Defaults());
    auto csv = MakeCSVData({"a,\"b\" \"c\",d\n", "e\n"});
    auto lengths = {12, 2};
    AssertChunking(chunker, csv, lengths);
  }
}

TEST(Chunker, Escaping) {
  auto options = ParseOptions::Defaults();
  options.escaping = true;
  {
    auto csv = MakeCSVData({"a\\b,c\n", "d\n"});
    auto lengths = {6, 2};
    {
      Chunker chunker(ParseOptions::Defaults());
      AssertChunking(chunker, csv, lengths);
    }
    {
      Chunker chunker(options);
      AssertChunking(chunker, csv, lengths);
    }
  }
  {
    auto csv = MakeCSVData({"a\\,b,c\n", "d\n"});
    auto lengths = {7, 2};
    {
      Chunker chunker(ParseOptions::Defaults());
      AssertChunking(chunker, csv, lengths);
    }
    {
      Chunker chunker(options);
      AssertChunking(chunker, csv, lengths);
    }
  }
  {
    // Escaped newline
    auto csv = MakeCSVData({"a\\\nb\n", "c\n"});
    {
      auto lengths = {3, 2, 2};
      Chunker chunker(ParseOptions::Defaults());
      AssertChunking(chunker, csv, lengths);
    }
    {
      auto lengths = {5, 2};
      Chunker chunker(options);
      AssertChunking(chunker, csv, lengths);
    }
  }
}

}  // namespace csv
}  // namespace arrow
