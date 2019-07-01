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
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/csv/test-common.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace csv {

// Read the column with the given index out of the BlockParser.
void GetColumn(const BlockParser& parser, int32_t col_index,
               std::vector<std::string>* out, std::vector<bool>* out_quoted = nullptr) {
  std::vector<std::string> values;
  std::vector<bool> quoted_values;
  auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
    values.push_back(std::string(reinterpret_cast<const char*>(data), size));
    if (out_quoted) {
      quoted_values.push_back(quoted);
    }
    return Status::OK();
  };
  ASSERT_OK(parser.VisitColumn(col_index, visit));
  *out = std::move(values);
  if (out_quoted) {
    *out_quoted = std::move(quoted_values);
  }
}

Status Parse(BlockParser& parser, const std::string& str, uint32_t* out_size) {
  const char* data = str.data();
  uint32_t size = static_cast<uint32_t>(str.length());
  return parser.Parse(data, size, out_size);
}

Status ParseFinal(BlockParser& parser, const std::string& str, uint32_t* out_size) {
  const char* data = str.data();
  uint32_t size = static_cast<uint32_t>(str.length());
  return parser.ParseFinal(data, size, out_size);
}

void AssertParseOk(BlockParser& parser, const std::string& str) {
  uint32_t parsed_size = static_cast<uint32_t>(-1);
  ASSERT_OK(Parse(parser, str, &parsed_size));
  ASSERT_EQ(parsed_size, str.size());
}

void AssertParseFinal(BlockParser& parser, const std::string& str) {
  uint32_t parsed_size = static_cast<uint32_t>(-1);
  ASSERT_OK(ParseFinal(parser, str, &parsed_size));
  ASSERT_EQ(parsed_size, str.size());
}

void AssertParsePartial(BlockParser& parser, const std::string& str,
                        uint32_t expected_size) {
  uint32_t parsed_size = static_cast<uint32_t>(-1);
  ASSERT_OK(Parse(parser, str, &parsed_size));
  ASSERT_EQ(parsed_size, expected_size);
}

void AssertColumnEq(const BlockParser& parser, int32_t col_index,
                    const std::vector<std::string> expected) {
  std::vector<std::string> values;
  GetColumn(parser, col_index, &values);
  ASSERT_EQ(parser.num_rows(), expected.size());
  ASSERT_EQ(values, expected);
}

void AssertColumnEq(const BlockParser& parser, int32_t col_index,
                    const std::vector<std::string> expected,
                    const std::vector<bool> expected_quoted) {
  std::vector<std::string> values;
  std::vector<bool> quoted;
  GetColumn(parser, col_index, &values, &quoted);
  ASSERT_EQ(parser.num_rows(), expected.size());
  ASSERT_EQ(values, expected);
  ASSERT_EQ(quoted, expected_quoted);
}

void AssertColumnsEq(const BlockParser& parser,
                     const std::vector<std::vector<std::string>> expected) {
  ASSERT_EQ(parser.num_cols(), expected.size());
  for (int32_t col_index = 0; col_index < parser.num_cols(); ++col_index) {
    AssertColumnEq(parser, col_index, expected[col_index]);
  }
}

void AssertColumnsEq(const BlockParser& parser,
                     const std::vector<std::vector<std::string>> expected,
                     const std::vector<std::vector<bool>> quoted) {
  ASSERT_EQ(parser.num_cols(), expected.size());
  for (int32_t col_index = 0; col_index < parser.num_cols(); ++col_index) {
    AssertColumnEq(parser, col_index, expected[col_index], quoted[col_index]);
  }
  uint32_t total_bytes = 0;
  for (const auto& col : expected) {
    for (const auto& field : col) {
      total_bytes += static_cast<uint32_t>(field.size());
    }
  }
  ASSERT_EQ(total_bytes, parser.num_bytes());
}

TEST(BlockParser, Basics) {
  auto csv = MakeCSVData({"ab,cd,\n", "ef,,gh\n", ",ij,kl\n"});
  BlockParser parser(ParseOptions::Defaults());
  AssertParseOk(parser, csv);
  AssertColumnsEq(parser, {{"ab", "ef", ""}, {"cd", "", "ij"}, {"", "gh", "kl"}});
}

TEST(BlockParser, EmptyHeader) {
  // Cannot infer number of columns
  uint32_t out_size;
  {
    auto csv = MakeCSVData({""});
    BlockParser parser(ParseOptions::Defaults());
    ASSERT_RAISES(Invalid, ParseFinal(parser, csv, &out_size));
  }
  {
    auto csv = MakeCSVData({"\n"});
    BlockParser parser(ParseOptions::Defaults());
    ASSERT_RAISES(Invalid, ParseFinal(parser, csv, &out_size));
  }
}

TEST(BlockParser, Empty) {
  {
    auto csv = MakeCSVData({",\n"});
    BlockParser parser(ParseOptions::Defaults());
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{""}, {""}});
  }
  {
    auto csv = MakeCSVData({",\n,\n"});
    BlockParser parser(ParseOptions::Defaults());
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"", ""}, {"", ""}});
  }
}

TEST(BlockParser, Whitespace) {
  // Non-newline whitespace is preserved
  auto csv = MakeCSVData({"a b, cd, \n", " ef, \t,gh\n"});
  BlockParser parser(ParseOptions::Defaults());
  AssertParseOk(parser, csv);
  AssertColumnsEq(parser, {{"a b", " ef"}, {" cd", " \t"}, {" ", "gh"}});
}

TEST(BlockParser, Newlines) {
  auto csv = MakeCSVData({"a,b\n", "c,d\r\n", "e,f\r", "g,h\r"});
  BlockParser parser(ParseOptions::Defaults());

  AssertParseOk(parser, csv);
  AssertColumnsEq(parser, {{"a", "c", "e", "g"}, {"b", "d", "f", "h"}});
}

TEST(BlockParser, MaxNumRows) {
  auto csv = MakeCSVData({"a\n", "b\n", "c\n", "d\n"});
  BlockParser parser(ParseOptions::Defaults(), -1, 3 /* max_num_rows */);

  AssertParsePartial(parser, csv, 6);
  AssertColumnsEq(parser, {{"a", "b", "c"}});

  AssertParseOk(parser, csv.substr(6));
  AssertColumnsEq(parser, {{"d"}});

  AssertParseOk(parser, csv.substr(8));
  AssertColumnsEq(parser, {{}});
}

TEST(BlockParser, EmptyLinesWithOneColumn) {
  auto csv = MakeCSVData({"a\n", "\n", "b\r", "\r", "c\r\n", "\r\n", "d\n"});
  {
    BlockParser parser(ParseOptions::Defaults());
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a", "b", "c", "d"}});
  }
  {
    auto options = ParseOptions::Defaults();
    options.ignore_empty_lines = false;
    BlockParser parser(options);
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a", "", "b", "", "c", "", "d"}});
  }
}

TEST(BlockParser, EmptyLinesWithSeveralColumns) {
  uint32_t out_size;
  auto csv = MakeCSVData({"a,b\n", "\n", "c,d\r", "\r", "e,f\r\n", "\r\n", "g,h\n"});
  {
    BlockParser parser(ParseOptions::Defaults());
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a", "c", "e", "g"}, {"b", "d", "f", "h"}});
  }
  {
    // A non-ignored empty line is a single value, but two columns are expected
    auto options = ParseOptions::Defaults();
    options.ignore_empty_lines = false;
    BlockParser parser(options);
    Status st = Parse(parser, csv, &out_size);
    ASSERT_RAISES(Invalid, st);
  }
}

TEST(BlockParser, TruncatedData) {
  BlockParser parser(ParseOptions::Defaults());
  auto csv = MakeCSVData({"a,b\n", "c,d\n"});
  for (auto trim : {1, 2, 3}) {
    AssertParsePartial(parser, csv.substr(0, csv.length() - trim), 4);
    AssertColumnsEq(parser, {{"a"}, {"b"}});
  }
}

TEST(BlockParser, Final) {
  // Tests for ParseFinal()
  BlockParser parser(ParseOptions::Defaults());
  auto csv = MakeCSVData({"ab,cd\n", "ef,gh\n"});
  AssertParseFinal(parser, csv);
  AssertColumnsEq(parser, {{"ab", "ef"}, {"cd", "gh"}});

  // Same without newline
  csv = MakeCSVData({"ab,cd\n", "ef,gh"});
  AssertParseFinal(parser, csv);
  AssertColumnsEq(parser, {{"ab", "ef"}, {"cd", "gh"}});

  // Same with empty last item
  csv = MakeCSVData({"ab,cd\n", "ef,"});
  AssertParseFinal(parser, csv);
  AssertColumnsEq(parser, {{"ab", "ef"}, {"cd", ""}});

  // Same with single line
  csv = MakeCSVData({"ab,cd"});
  AssertParseFinal(parser, csv);
  AssertColumnsEq(parser, {{"ab"}, {"cd"}});
}

TEST(BlockParser, FinalTruncatedData) {
  // Test ParseFinal() with truncated data
  uint32_t out_size;
  BlockParser parser(ParseOptions::Defaults());
  auto csv = MakeCSVData({"ab,cd\n", "ef"});
  Status st = ParseFinal(parser, csv, &out_size);
  ASSERT_RAISES(Invalid, st);
}

TEST(BlockParser, QuotingSimple) {
  auto csv = MakeCSVData({"1,\",3,\",5\n"});

  {
    BlockParser parser(ParseOptions::Defaults());
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"1"}, {",3,"}, {"5"}},
                    {{false}, {true}, {false}} /* quoted */);
  }
  {
    auto options = ParseOptions::Defaults();
    options.quoting = false;
    BlockParser parser(options);
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"1"}, {"\""}, {"3"}, {"\""}, {"5"}},
                    {{false}, {false}, {false}, {false}, {false}} /* quoted */);
  }
  {
    auto options = ParseOptions::Defaults();
    options.quote_char = 'Z';
    BlockParser parser(options);
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"1"}, {"\""}, {"3"}, {"\""}, {"5"}},
                    {{false}, {false}, {false}, {false}, {false}} /* quoted */);
  }
}

TEST(BlockParser, QuotingNewline) {
  auto csv = MakeCSVData({"a,\"c \n d\",e\n"});
  BlockParser parser(ParseOptions::Defaults());
  AssertParseOk(parser, csv);
  AssertColumnsEq(parser, {{"a"}, {"c \n d"}, {"e"}},
                  {{false}, {true}, {false}} /* quoted */);
}

TEST(BlockParser, QuotingUnbalanced) {
  // Quote introduces a quoted field that doesn't end
  auto csv = MakeCSVData({"a,b\n", "1,\",3,,5\n"});
  BlockParser parser(ParseOptions::Defaults());
  AssertParsePartial(parser, csv, 4);
  AssertColumnsEq(parser, {{"a"}, {"b"}}, {{false}, {false}} /* quoted */);
}

TEST(BlockParser, QuotingEmpty) {
  {
    BlockParser parser(ParseOptions::Defaults());
    auto csv = MakeCSVData({"\"\"\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{""}}, {{true}} /* quoted */);
  }
  {
    BlockParser parser(ParseOptions::Defaults());
    auto csv = MakeCSVData({",\"\"\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{""}, {""}}, {{false}, {true}} /* quoted */);
  }
  {
    BlockParser parser(ParseOptions::Defaults());
    auto csv = MakeCSVData({"\"\",\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{""}, {""}}, {{true}, {false}} /* quoted */);
  }
}

TEST(BlockParser, QuotingDouble) {
  {
    BlockParser parser(ParseOptions::Defaults());
    // 4 quotes is a quoted quote
    auto csv = MakeCSVData({"\"\"\"\"\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"\""}}, {{true}} /* quoted */);
  }
  {
    BlockParser parser(ParseOptions::Defaults());
    // 4 quotes is a quoted quote
    auto csv = MakeCSVData({"a,\"\"\"\",b\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a"}, {"\""}, {"b"}},
                    {{false}, {true}, {false}} /* quoted */);
  }
  {
    BlockParser parser(ParseOptions::Defaults());
    // 6 quotes is two quoted quotes
    auto csv = MakeCSVData({"\"\"\"\"\"\"\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"\"\""}}, {{true}} /* quoted */);
  }
  {
    BlockParser parser(ParseOptions::Defaults());
    // 6 quotes is two quoted quotes
    auto csv = MakeCSVData({"a,\"\"\"\"\"\",b\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a"}, {"\"\""}, {"b"}},
                    {{false}, {true}, {false}} /* quoted */);
  }
}

TEST(BlockParser, QuotesAndMore) {
  // There may be trailing data after the quoted part of a field
  {
    BlockParser parser(ParseOptions::Defaults());
    auto csv = MakeCSVData({"a,\"b\"c,d\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a"}, {"bc"}, {"d"}},
                    {{false}, {true}, {false}} /* quoted */);
  }
}

TEST(BlockParser, QuotesSpecial) {
  // Some non-trivial cases
  {
    BlockParser parser(ParseOptions::Defaults());
    auto csv = MakeCSVData({"a,b\"c,d\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a"}, {"b\"c"}, {"d"}},
                    {{false}, {false}, {false}} /* quoted */);
  }
  {
    BlockParser parser(ParseOptions::Defaults());
    auto csv = MakeCSVData({"a,\"b\" \"c\",d\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a"}, {"b \"c\""}, {"d"}},
                    {{false}, {true}, {false}} /* quoted */);
  }
}

TEST(BlockParser, MismatchingNumColumns) {
  uint32_t out_size;
  {
    BlockParser parser(ParseOptions::Defaults());
    auto csv = MakeCSVData({"a,b\nc\n"});
    Status st = Parse(parser, csv, &out_size);
    ASSERT_RAISES(Invalid, st);
  }
  {
    BlockParser parser(ParseOptions::Defaults(), 2 /* num_cols */);
    auto csv = MakeCSVData({"a\n"});
    Status st = Parse(parser, csv, &out_size);
    ASSERT_RAISES(Invalid, st);
  }
  {
    BlockParser parser(ParseOptions::Defaults(), 2 /* num_cols */);
    auto csv = MakeCSVData({"a,b,c\n"});
    Status st = Parse(parser, csv, &out_size);
    ASSERT_RAISES(Invalid, st);
  }
}

TEST(BlockParser, Escaping) {
  auto options = ParseOptions::Defaults();
  options.escaping = true;

  {
    auto csv = MakeCSVData({"a\\b,c\n"});
    {
      BlockParser parser(ParseOptions::Defaults());
      AssertParseOk(parser, csv);
      AssertColumnsEq(parser, {{"a\\b"}, {"c"}});
    }
    {
      BlockParser parser(options);
      AssertParseOk(parser, csv);
      AssertColumnsEq(parser, {{"ab"}, {"c"}});
    }
  }
  {
    auto csv = MakeCSVData({"a\\,b,c\n"});
    BlockParser parser(options);
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a,b"}, {"c"}});
  }
}

// Generate test data with the given number of columns.
std::string MakeLotsOfCsvColumns(int32_t num_columns) {
  std::string values, header;
  header.reserve(num_columns * 10);
  values.reserve(num_columns * 10);
  for (int x = 0; x < num_columns; x++) {
    if (x != 0) {
      header += ",";
      values += ",";
    }
    header += "c" + std::to_string(x);
    values += std::to_string(x);
  }

  header += "\n";
  values += "\n";
  return MakeCSVData({header, values});
}

TEST(BlockParser, LotsOfColumns) {
  auto options = ParseOptions::Defaults();
  BlockParser parser(options);
  AssertParseOk(parser, MakeLotsOfCsvColumns(1024 * 100));
}

TEST(BlockParser, QuotedEscape) {
  auto options = ParseOptions::Defaults();
  options.escaping = true;

  {
    auto csv = MakeCSVData({"\"a\\,b\",c\n"});
    BlockParser parser(options);
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a,b"}, {"c"}}, {{true}, {false}} /* quoted */);
  }
  {
    auto csv = MakeCSVData({"\"a\\\"b\",c\n"});
    BlockParser parser(options);
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a\"b"}, {"c"}}, {{true}, {false}} /* quoted */);
  }
}

}  // namespace csv
}  // namespace arrow
