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

#include "arrow/io/interfaces.h"
#include "arrow/json/options.h"
#include "arrow/json/reader.h"
#include "arrow/json/test-common.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace json {

using util::string_view;

using internal::checked_cast;

static std::string scalars_only_src() {
  return R"(
    { "hello": 3.5, "world": false, "yo": "thing" }
    { "hello": 3.25, "world": null }
    { "hello": 3.125, "world": null, "yo": "\u5fcd" }
    { "hello": 0.0, "world": true, "yo": null }
  )";
}

static std::string nested_src() {
  return R"(
    { "hello": 3.5, "world": false, "yo": "thing", "arr": [1, 2, 3], "nuf": {} }
    { "hello": 3.25, "world": null, "arr": [2], "nuf": null }
    { "hello": 3.125, "world": null, "yo": "\u5fcd", "arr": [], "nuf": { "ps": 78 } }
    { "hello": 0.0, "world": true, "yo": null, "arr": null, "nuf": { "ps": 90 } }
  )";
}

class ReaderTest : public ::testing::TestWithParam<bool> {
 public:
  void SetUpReader() {
    read_options_.use_threads = GetParam();
    ASSERT_OK(TableReader::Make(default_memory_pool(), input_, read_options_,
                                parse_options_, &reader_));
  }

  void SetUpReader(util::string_view input) {
    ASSERT_OK(MakeStream(input, &input_));
    SetUpReader();
  }

  std::shared_ptr<Column> ColumnFromJSON(const std::shared_ptr<Field>& field,
                                         const std::string& data) {
    return std::make_shared<Column>(field, ArrayFromJSON(field->type(), data));
  }

  std::shared_ptr<Column> ColumnFromJSON(const std::shared_ptr<Field>& field,
                                         const std::vector<std::string>& data) {
    ArrayVector chunks(data.size());
    for (size_t i = 0; i < chunks.size(); ++i) {
      chunks[i] = ArrayFromJSON(field->type(), data[i]);
    }
    return std::make_shared<Column>(field, std::move(chunks));
  }

  ParseOptions parse_options_ = ParseOptions::Defaults();
  ReadOptions read_options_ = ReadOptions::Defaults();
  std::shared_ptr<io::InputStream> input_;
  std::shared_ptr<TableReader> reader_;
  std::shared_ptr<Table> table_;
};

INSTANTIATE_TEST_CASE_P(ReaderTest, ReaderTest, ::testing::Values(false, true));

TEST_P(ReaderTest, Empty) {
  SetUpReader("{}\n{}\n");
  ASSERT_OK(reader_->Read(&table_));

  auto expected_table = Table::Make(schema({}), ArrayVector(), 2);
  AssertTablesEqual(*table_, *expected_table);
}

TEST_P(ReaderTest, Basics) {
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  auto src = scalars_only_src();
  SetUpReader(src);
  ASSERT_OK(reader_->Read(&table_));

  auto expected_table = Table::Make({
      ColumnFromJSON(field("hello", float64()), "[3.5, 3.25, 3.125, 0.0]"),
      ColumnFromJSON(field("world", boolean()), "[false, null, null, true]"),
      ColumnFromJSON(field("yo", utf8()), "[\"thing\", null, \"\xe5\xbf\x8d\", null]"),
  });
  AssertTablesEqual(*table_, *expected_table);
}

TEST_P(ReaderTest, Nested) {
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  auto src = nested_src();
  SetUpReader(src);
  ASSERT_OK(reader_->Read(&table_));

  auto expected_table = Table::Make({
      ColumnFromJSON(field("hello", float64()), "[3.5, 3.25, 3.125, 0.0]"),
      ColumnFromJSON(field("world", boolean()), "[false, null, null, true]"),
      ColumnFromJSON(field("yo", utf8()), "[\"thing\", null, \"\xe5\xbf\x8d\", null]"),
      ColumnFromJSON(field("arr", list(int64())), R"([[1, 2, 3], [2], [], null])"),
      ColumnFromJSON(field("nuf", struct_({field("ps", int64())})),
                     R"([{"ps":null}, null, {"ps":78}, {"ps":90}])"),
  });
  AssertTablesEqual(*table_, *expected_table);
}

TEST_P(ReaderTest, PartialSchema) {
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  parse_options_.explicit_schema =
      schema({field("nuf", struct_({field("absent", date32())})),
              field("arr", list(float32()))});
  auto src = nested_src();
  SetUpReader(src);
  ASSERT_OK(reader_->Read(&table_));

  auto expected_table = Table::Make({
      // NB: explicitly declared fields will appear first
      ColumnFromJSON(
          field("nuf", struct_({field("absent", date32()), field("ps", int64())})),
          R"([{"absent":null,"ps":null}, null, {"absent":null,"ps":78}, {"absent":null,"ps":90}])"),
      ColumnFromJSON(field("arr", list(float32())), R"([[1, 2, 3], [2], [], null])"),
      // ...followed by undeclared fields
      ColumnFromJSON(field("hello", float64()), "[3.5, 3.25, 3.125, 0.0]"),
      ColumnFromJSON(field("world", boolean()), "[false, null, null, true]"),
      ColumnFromJSON(field("yo", utf8()), "[\"thing\", null, \"\xe5\xbf\x8d\", null]"),
  });
  AssertTablesEqual(*table_, *expected_table);
}

TEST_P(ReaderTest, TypeInference) {
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  SetUpReader(R"(
    {"ts":null, "f": null}
    {"ts":"1970-01-01", "f": 3}
    {"ts":"2018-11-13 17:11:10", "f":3.125}
    )");
  ASSERT_OK(reader_->Read(&table_));

  auto expected_table =
      Table::Make({ColumnFromJSON(field("ts", timestamp(TimeUnit::SECOND)),
                                  R"([null, "1970-01-01", "2018-11-13 17:11:10"])"),
                   ColumnFromJSON(field("f", float64()), R"([null, 3, 3.125])")});
  AssertTablesEqual(*table_, *expected_table);
}

}  // namespace json
}  // namespace arrow
