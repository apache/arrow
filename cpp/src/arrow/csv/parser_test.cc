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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/csv/test_common.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace csv {

void CheckSkipRows(const std::string& rows, int32_t num_rows,
                   int32_t expected_skipped_rows, int32_t expected_skipped_bytes) {
  const uint8_t* start = reinterpret_cast<const uint8_t*>(rows.data());
  const uint8_t* data;
  int32_t skipped_rows =
      SkipRows(start, static_cast<int32_t>(rows.size()), num_rows, &data);
  ASSERT_EQ(skipped_rows, expected_skipped_rows);
  ASSERT_EQ(data - start, expected_skipped_bytes);
}

TEST(SkipRows, Basics) {
  CheckSkipRows("", 0, 0, 0);
  CheckSkipRows("", 15, 0, 0);

  CheckSkipRows("a\nb\nc\nd", 1, 1, 2);
  CheckSkipRows("a\nb\nc\nd", 2, 2, 4);
  CheckSkipRows("a\nb\nc\nd", 3, 3, 6);
  CheckSkipRows("a\nb\nc\nd", 4, 3, 6);

  CheckSkipRows("a\nb\nc\nd\n", 3, 3, 6);
  CheckSkipRows("a\nb\nc\nd\n", 4, 4, 8);
  CheckSkipRows("a\nb\nc\nd\n", 5, 4, 8);

  CheckSkipRows("\t\n\t\n\t\n\t", 1, 1, 2);
  CheckSkipRows("\t\n\t\n\t\n\t", 3, 3, 6);
  CheckSkipRows("\t\n\t\n\t\n\t", 4, 3, 6);

  CheckSkipRows("a\r\nb\nc\rd\r\n", 1, 1, 3);
  CheckSkipRows("a\r\nb\nc\rd\r\n", 2, 2, 5);
  CheckSkipRows("a\r\nb\nc\rd\r\n", 3, 3, 7);
  CheckSkipRows("a\r\nb\nc\rd\r\n", 4, 4, 10);
  CheckSkipRows("a\r\nb\nc\rd\r\n", 5, 4, 10);

  CheckSkipRows("a\r\nb\nc\rd\r", 4, 4, 9);
  CheckSkipRows("a\r\nb\nc\rd\r", 5, 4, 9);
  CheckSkipRows("a\r\nb\nc\rd\re", 4, 4, 9);
  CheckSkipRows("a\r\nb\nc\rd\re", 5, 4, 9);

  CheckSkipRows("\n\r\n\r\r\n\n\r\n\r", 1, 1, 1);
  CheckSkipRows("\n\r\n\r\r\n\n\r\n\r", 2, 2, 3);
  CheckSkipRows("\n\r\n\r\r\n\n\r\n\r", 3, 3, 4);
  CheckSkipRows("\n\r\n\r\r\n\n\r\n\r", 4, 4, 6);
  CheckSkipRows("\n\r\n\r\r\n\n\r\n\r", 5, 5, 7);
  CheckSkipRows("\n\r\n\r\r\n\n\r\n\r", 6, 6, 9);
  CheckSkipRows("\n\r\n\r\r\n\n\r\n\r", 7, 7, 10);
  CheckSkipRows("\n\r\n\r\r\n\n\r\n\r", 8, 7, 10);
}

////////////////////////////////////////////////////////////////////////////
// BlockParser tests

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

void GetLastRow(const BlockParser& parser, std::vector<std::string>* out,
                std::vector<bool>* out_quoted = nullptr) {
  std::vector<std::string> values;
  std::vector<bool> quoted_values;
  auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
    values.push_back(std::string(reinterpret_cast<const char*>(data), size));
    if (out_quoted) {
      quoted_values.push_back(quoted);
    }
    return Status::OK();
  };
  ASSERT_OK(parser.VisitLastRow(visit));
  *out = std::move(values);
  if (out_quoted) {
    *out_quoted = std::move(quoted_values);
  }
}

size_t TotalViewLength(const std::vector<util::string_view>& views) {
  size_t total_view_length = 0;
  for (const auto& view : views) {
    total_view_length += view.length();
  }
  return total_view_length;
}

Status Parse(BlockParser& parser, const std::string& str, uint32_t* out_size) {
  return parser.Parse(util::string_view(str), out_size);
}

Status ParseFinal(BlockParser& parser, const std::string& str, uint32_t* out_size) {
  return parser.ParseFinal(util::string_view(str), out_size);
}

void AssertParseOk(BlockParser& parser, const std::string& str) {
  uint32_t parsed_size = static_cast<uint32_t>(-1);
  ASSERT_OK(Parse(parser, str, &parsed_size));
  ASSERT_EQ(parsed_size, str.size());
}

void AssertParseOk(BlockParser& parser, const std::vector<util::string_view>& data) {
  uint32_t parsed_size = static_cast<uint32_t>(-1);
  ASSERT_OK(parser.Parse(data, &parsed_size));
  ASSERT_EQ(parsed_size, TotalViewLength(data));
}

void AssertParseFinal(BlockParser& parser, const std::string& str) {
  uint32_t parsed_size = static_cast<uint32_t>(-1);
  ASSERT_OK(ParseFinal(parser, str, &parsed_size));
  ASSERT_EQ(parsed_size, str.size());
}

void AssertParseFinal(BlockParser& parser, const std::vector<util::string_view>& data) {
  uint32_t parsed_size = static_cast<uint32_t>(-1);
  ASSERT_OK(parser.ParseFinal(data, &parsed_size));
  ASSERT_EQ(parsed_size, TotalViewLength(data));
}

void AssertParsePartial(BlockParser& parser, const std::string& str,
                        uint32_t expected_size) {
  uint32_t parsed_size = static_cast<uint32_t>(-1);
  ASSERT_OK(Parse(parser, str, &parsed_size));
  ASSERT_EQ(parsed_size, expected_size);
}

void AssertLastRowEq(const BlockParser& parser, const std::vector<std::string> expected) {
  std::vector<std::string> values;
  GetLastRow(parser, &values);
  ASSERT_EQ(parser.num_rows(), expected.size());
  ASSERT_EQ(values, expected);
}

void AssertLastRowEq(const BlockParser& parser, const std::vector<std::string> expected,
                     const std::vector<bool> expected_quoted) {
  std::vector<std::string> values;
  std::vector<bool> quoted;
  GetLastRow(parser, &values, &quoted);
  ASSERT_EQ(parser.num_cols(), expected.size());
  ASSERT_EQ(values, expected);
  ASSERT_EQ(quoted, expected_quoted);
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
  {
    auto csv = MakeCSVData({"ab,cd,\n", "ef,,gh\n", ",ij,kl\n"});
    BlockParser parser(ParseOptions::Defaults());
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"ab", "ef", ""}, {"cd", "", "ij"}, {"", "gh", "kl"}});
    AssertLastRowEq(parser, {"", "ij", "kl"}, {false, false, false});
  }
  {
    auto csv1 = MakeCSVData({"ab,cd,\n", "ef,,gh\n"});
    auto csv2 = MakeCSVData({",ij,kl\n"});
    std::vector<util::string_view> csvs = {csv1, csv2};
    BlockParser parser(ParseOptions::Defaults());
    AssertParseOk(parser, {{csv1}, {csv2}});
    AssertColumnsEq(parser, {{"ab", "ef", ""}, {"cd", "", "ij"}, {"", "gh", "kl"}});
    AssertLastRowEq(parser, {"", "ij", "kl"}, {false, false, false});
  }
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
    AssertLastRowEq(parser, {"", ""}, {false, false});
  }
  {
    auto csv = MakeCSVData({",\n,\n"});
    BlockParser parser(ParseOptions::Defaults());
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"", ""}, {"", ""}});
    AssertLastRowEq(parser, {"", ""}, {false, false});
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
  BlockParser parser(ParseOptions::Defaults(), -1, 0, 3 /* max_num_rows */);

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
  auto csv = MakeCSVData({"a,b\n", "\n", "c,d\r", "\r", "e,f\r\n", "\r\n", "g,h\n"});
  {
    BlockParser parser(ParseOptions::Defaults());
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a", "c", "e", "g"}, {"b", "d", "f", "h"}});
  }
  {
    // Non-ignored empty lines get turned into empty values
    auto options = ParseOptions::Defaults();
    options.ignore_empty_lines = false;
    BlockParser parser(options);
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser,
                    {{"a", "", "c", "", "e", "", "g"}, {"b", "", "d", "", "f", "", "h"}});
  }
}

TEST(BlockParser, EmptyLineFirst) {
  auto csv = MakeCSVData({"\n", "\n", "a\n", "b\n"});
  {
    BlockParser parser(ParseOptions::Defaults());
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"a", "b"}});
  }
  {
    auto options = ParseOptions::Defaults();
    options.ignore_empty_lines = false;
    BlockParser parser(options);
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{"", "", "a", "b"}});
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

  // Two blocks
  auto csv1 = MakeCSVData({"ab,cd\n"});
  auto csv2 = MakeCSVData({"ef,"});
  AssertParseFinal(parser, {{csv1}, {csv2}});
  AssertColumnsEq(parser, {{"ab", "ef"}, {"cd", ""}});
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
    AssertLastRowEq(parser, {""}, {true});
  }
  {
    BlockParser parser(ParseOptions::Defaults());
    auto csv = MakeCSVData({",\"\"\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{""}, {""}}, {{false}, {true}} /* quoted */);
    AssertLastRowEq(parser, {"", ""}, {false, true});
  }
  {
    BlockParser parser(ParseOptions::Defaults());
    auto csv = MakeCSVData({"\"\",\n"});
    AssertParseOk(parser, csv);
    AssertColumnsEq(parser, {{""}, {""}}, {{true}, {false}} /* quoted */);
    AssertLastRowEq(parser, {"", ""}, {true, false});
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
    BlockParser parser(ParseOptions::Defaults(), -1, 0 /* first_row */);
    auto csv = MakeCSVData({"a,b\nc\n"});
    Status st = Parse(parser, csv, &out_size);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        testing::HasSubstr("CSV parse error: Row #1: Expected 2 columns, got 1: c"), st);
  }
  {
    BlockParser parser(ParseOptions::Defaults(), 2 /* num_cols */, 0 /* first_row */);
    auto csv = MakeCSVData({"a\n"});
    Status st = Parse(parser, csv, &out_size);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        testing::HasSubstr("CSV parse error: Row #0: Expected 2 columns, got 1: a"), st);
  }
  {
    BlockParser parser(ParseOptions::Defaults(), 2 /* num_cols */, 50 /* first_row */);
    auto csv = MakeCSVData({"a,b,c\n"});
    Status st = Parse(parser, csv, &out_size);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        testing::HasSubstr("CSV parse error: Row #50: Expected 2 columns, got 3: a,b,c"),
        st);
  }
  // No row number
  {
    BlockParser parser(ParseOptions::Defaults(), 2 /* num_cols */, -1);
    auto csv = MakeCSVData({"a\n"});
    Status st = Parse(parser, csv, &out_size);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, testing::HasSubstr("CSV parse error: Expected 2 columns, got 1: a"), st);
  }
}

TEST(BlockParser, MismatchingNumColumnsHandler) {
  struct CustomHandler {
    operator InvalidRowHandler() {
      return [this](const InvalidRow& row) {
        // Copy the row to a string since the array behind the string_view can go away
        rows.emplace_back(row, row.text.to_string());
        return InvalidRowResult::Skip;
      };
    }

    std::vector<std::pair<InvalidRow, std::string>> rows;
  };

  {
    ParseOptions opts = ParseOptions::Defaults();
    CustomHandler handler;
    opts.invalid_row_handler = handler;
    BlockParser parser(opts);
    ASSERT_NO_FATAL_FAILURE(AssertParseOk(parser, "a,b\nc\nd,e\n"));
    ASSERT_EQ(2, parser.num_rows());
    ASSERT_EQ(3, parser.total_num_rows());
    ASSERT_EQ(1, handler.rows.size());
    ASSERT_EQ(2, handler.rows[0].first.expected_columns);
    ASSERT_EQ(1, handler.rows[0].first.actual_columns);
    ASSERT_EQ("c", handler.rows[0].second);
    ASSERT_NO_FATAL_FAILURE(AssertLastRowEq(parser, {"d", "e"}, {false, false}));
  }
  {
    ParseOptions opts = ParseOptions::Defaults();
    CustomHandler handler;
    opts.invalid_row_handler = handler;
    BlockParser parser(opts, 2 /* num_cols */);
    ASSERT_NO_FATAL_FAILURE(AssertParseOk(parser, "a\nb,c\n"));
    ASSERT_EQ(1, parser.num_rows());
    ASSERT_EQ(2, parser.total_num_rows());
    ASSERT_EQ(1, handler.rows.size());
    ASSERT_EQ(2, handler.rows[0].first.expected_columns);
    ASSERT_EQ(1, handler.rows[0].first.actual_columns);
    ASSERT_EQ("a", handler.rows[0].second);
    ASSERT_NO_FATAL_FAILURE(AssertLastRowEq(parser, {"b", "c"}, {false, false}));
  }
  {
    ParseOptions opts = ParseOptions::Defaults();
    CustomHandler handler;
    opts.invalid_row_handler = handler;
    BlockParser parser(opts, 2 /* num_cols */);
    ASSERT_NO_FATAL_FAILURE(AssertParseOk(parser, "a,b,c\nd,e\n"));
    ASSERT_EQ(1, parser.num_rows());
    ASSERT_EQ(2, parser.total_num_rows());
    ASSERT_EQ(1, handler.rows.size());
    ASSERT_EQ(2, handler.rows[0].first.expected_columns);
    ASSERT_EQ(3, handler.rows[0].first.actual_columns);
    ASSERT_EQ("a,b,c", handler.rows[0].second);
    ASSERT_NO_FATAL_FAILURE(AssertLastRowEq(parser, {"d", "e"}, {false, false}));
  }

  // Skip multiple bad lines are skipped
  {
    ParseOptions opts = ParseOptions::Defaults();
    CustomHandler handler;
    opts.invalid_row_handler = handler;
    BlockParser parser(opts, /*num_col=*/2, /*first_row=*/1);
    ASSERT_NO_FATAL_FAILURE(AssertParseOk(parser, "a,b,c\nd,e\nf,g\nh\ni\nj,k\nl\n"));
    ASSERT_EQ(3, parser.num_rows());
    ASSERT_EQ(7, parser.total_num_rows());
    ASSERT_EQ(4, handler.rows.size());

    {
      auto row = handler.rows[0];
      ASSERT_EQ(2, row.first.expected_columns);
      ASSERT_EQ(3, row.first.actual_columns);
      ASSERT_EQ(1, row.first.number);
      ASSERT_EQ("a,b,c", row.second);
    }

    {
      auto row = handler.rows[1];
      ASSERT_EQ(2, row.first.expected_columns);
      ASSERT_EQ(1, row.first.actual_columns);
      ASSERT_EQ(4, row.first.number);
      ASSERT_EQ("h", row.second);
    }

    {
      auto row = handler.rows[2];
      ASSERT_EQ(2, row.first.expected_columns);
      ASSERT_EQ(1, row.first.actual_columns);
      ASSERT_EQ(5, row.first.number);
      ASSERT_EQ("i", row.second);
    }

    {
      auto row = handler.rows[3];
      ASSERT_EQ(2, row.first.expected_columns);
      ASSERT_EQ(1, row.first.actual_columns);
      ASSERT_EQ(7, row.first.number);
      ASSERT_EQ("l", row.second);
    }

    ASSERT_NO_FATAL_FAILURE(AssertLastRowEq(parser, {"j", "k"}, {false, false}));
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

TEST(BlockParser, RowNumberAppendedToError) {
  auto options = ParseOptions::Defaults();
  auto csv = "a,b,c\nd,e,f\ng,h,i\n";
  {
    BlockParser parser(options, -1, 0);
    ASSERT_NO_FATAL_FAILURE(AssertParseOk(parser, csv));
    int row = 0;
    auto status = parser.VisitColumn(
        0, [row](const uint8_t* data, uint32_t size, bool quoted) mutable -> Status {
          return ++row == 2 ? Status::Invalid("Bad value") : Status::OK();
        });
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Row #1: Bad value"),
                                    status);
  }

  {
    BlockParser parser(options, -1, 100);
    ASSERT_NO_FATAL_FAILURE(AssertParseOk(parser, csv));
    int row = 0;
    auto status = parser.VisitColumn(
        0, [row](const uint8_t* data, uint32_t size, bool quoted) mutable -> Status {
          return ++row == 3 ? Status::Invalid("Bad value") : Status::OK();
        });
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Row #102: Bad value"),
                                    status);
  }

  // No first row specified should not append row information
  {
    BlockParser parser(options, -1, -1);
    ASSERT_NO_FATAL_FAILURE(AssertParseOk(parser, csv));
    int row = 0;
    auto status = parser.VisitColumn(
        0, [row](const uint8_t* data, uint32_t size, bool quoted) mutable -> Status {
          return ++row == 3 ? Status::Invalid("Bad value") : Status::OK();
        });
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::Not(testing::HasSubstr("Row")),
                                    status);
  }

  // Error message is correct even with skipped parsed rows
  {
    ParseOptions opts = ParseOptions::Defaults();
    opts.invalid_row_handler = [](const InvalidRow& row) {
      return InvalidRowResult::Skip;
    };
    BlockParser parser(opts, /*num_cols=*/2, /*first_row=*/1);
    ASSERT_NO_FATAL_FAILURE(AssertParseOk(parser, "a,b,c\nd,e\nf,g\nh\ni\nj,k\nl\n"));
    int row = 0;
    auto status = parser.VisitColumn(
        0, [row](const uint8_t* data, uint32_t size, bool quoted) mutable -> Status {
          return ++row == 3 ? Status::Invalid("Bad value") : Status::OK();
        });

    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Row #6: Bad value"),
                                    status);
  }
}

}  // namespace csv
}  // namespace arrow
