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

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/io/interfaces.h"
#include "arrow/json/options.h"
#include "arrow/json/reader.h"
#include "arrow/json/test_common.h"
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

  std::shared_ptr<ChunkedArray> ChunkedFromJSON(const std::shared_ptr<Field>& field,
                                                const std::vector<std::string>& data) {
    ArrayVector chunks(data.size());
    for (size_t i = 0; i < chunks.size(); ++i) {
      chunks[i] = ArrayFromJSON(field->type(), data[i]);
    }
    return std::make_shared<ChunkedArray>(std::move(chunks));
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
  AssertTablesEqual(*expected_table, *table_);
}

TEST_P(ReaderTest, Basics) {
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  auto src = scalars_only_src();
  SetUpReader(src);
  ASSERT_OK(reader_->Read(&table_));

  auto schema = ::arrow::schema(
      {field("hello", float64()), field("world", boolean()), field("yo", utf8())});

  auto expected_table = Table::Make(
      schema, {
                  ArrayFromJSON(schema->field(0)->type(), "[3.5, 3.25, 3.125, 0.0]"),
                  ArrayFromJSON(schema->field(1)->type(), "[false, null, null, true]"),
                  ArrayFromJSON(schema->field(2)->type(),
                                "[\"thing\", null, \"\xe5\xbf\x8d\", null]"),
              });
  AssertTablesEqual(*expected_table, *table_);
}

TEST_P(ReaderTest, Nested) {
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  auto src = nested_src();
  SetUpReader(src);
  ASSERT_OK(reader_->Read(&table_));

  auto schema = ::arrow::schema({field("hello", float64()), field("world", boolean()),
                                 field("yo", utf8()), field("arr", list(int64())),
                                 field("nuf", struct_({field("ps", int64())}))});

  auto a0 = ArrayFromJSON(schema->field(0)->type(), "[3.5, 3.25, 3.125, 0.0]");
  auto a1 = ArrayFromJSON(schema->field(1)->type(), "[false, null, null, true]");
  auto a2 = ArrayFromJSON(schema->field(2)->type(),
                          "[\"thing\", null, \"\xe5\xbf\x8d\", null]");
  auto a3 = ArrayFromJSON(schema->field(3)->type(), "[[1, 2, 3], [2], [], null]");
  auto a4 = ArrayFromJSON(schema->field(4)->type(),
                          R"([{"ps":null}, null, {"ps":78}, {"ps":90}])");
  auto expected_table = Table::Make(schema, {a0, a1, a2, a3, a4});
  AssertTablesEqual(*expected_table, *table_);
}

TEST_P(ReaderTest, PartialSchema) {
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  parse_options_.explicit_schema =
      schema({field("nuf", struct_({field("absent", date32())})),
              field("arr", list(float32()))});
  auto src = nested_src();
  SetUpReader(src);
  ASSERT_OK(reader_->Read(&table_));

  auto schema = ::arrow::schema(
      {field("nuf", struct_({field("absent", date32()), field("ps", int64())})),
       field("arr", list(float32())), field("hello", float64()),
       field("world", boolean()), field("yo", utf8())});

  auto expected_table = Table::Make(
      schema,
      {
          // NB: explicitly declared fields will appear first
          ArrayFromJSON(
              schema->field(0)->type(),
              R"([{"absent":null,"ps":null}, null, {"absent":null,"ps":78}, {"absent":null,"ps":90}])"),
          ArrayFromJSON(schema->field(1)->type(), R"([[1, 2, 3], [2], [], null])"),
          // ...followed by undeclared fields
          ArrayFromJSON(schema->field(2)->type(), "[3.5, 3.25, 3.125, 0.0]"),
          ArrayFromJSON(schema->field(3)->type(), "[false, null, null, true]"),
          ArrayFromJSON(schema->field(4)->type(),
                        "[\"thing\", null, \"\xe5\xbf\x8d\", null]"),
      });
  AssertTablesEqual(*expected_table, *table_);
}

TEST_P(ReaderTest, TypeInference) {
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  SetUpReader(R"(
    {"ts":null, "f": null}
    {"ts":"1970-01-01", "f": 3}
    {"ts":"2018-11-13 17:11:10", "f":3.125}
    )");
  ASSERT_OK(reader_->Read(&table_));

  auto schema =
      ::arrow::schema({field("ts", timestamp(TimeUnit::SECOND)), field("f", float64())});
  auto expected_table = Table::Make(
      schema, {ArrayFromJSON(schema->field(0)->type(),
                             R"([null, "1970-01-01", "2018-11-13 17:11:10"])"),
               ArrayFromJSON(schema->field(1)->type(), R"([null, 3, 3.125])")});
  AssertTablesEqual(*expected_table, *table_);
}

TEST_P(ReaderTest, MultipleChunks) {
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;

  auto src = scalars_only_src();
  read_options_.block_size = static_cast<int>(src.length() / 3);

  SetUpReader(src);
  ASSERT_OK(reader_->Read(&table_));

  auto schema = ::arrow::schema(
      {field("hello", float64()), field("world", boolean()), field("yo", utf8())});

  // there is an empty chunk because the last block of the file is "  "
  auto expected_table = Table::Make(
      schema,
      {
          ChunkedFromJSON(schema->field(0), {"[3.5]", "[3.25]", "[3.125, 0.0]", "[]"}),
          ChunkedFromJSON(schema->field(1), {"[false]", "[null]", "[null, true]", "[]"}),
          ChunkedFromJSON(schema->field(2),
                          {"[\"thing\"]", "[null]", "[\"\xe5\xbf\x8d\", null]", "[]"}),
      });
  AssertTablesEqual(*expected_table, *table_);
}

template <typename T>
std::string RowsOfOneColumn(string_view name, std::initializer_list<T> values,
                            decltype(std::to_string(*values.begin()))* = nullptr) {
  std::stringstream ss;
  for (auto value : values) {
    ss << R"({")" << name << R"(":)" << std::to_string(value) << "}\n";
  }
  return ss.str();
}

TEST(ReaderTest, MultipleChunksParallel) {
  int64_t count = 1 << 10;

  ParseOptions parse_options;
  parse_options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  ReadOptions read_options;
  read_options.block_size =
      static_cast<int>(count / 2);  // there will be about two dozen blocks

  std::string json;
  for (int i = 0; i < count; ++i) {
    json += "{\"a\":" + std::to_string(i) + "}\n";
  }
  std::shared_ptr<io::InputStream> input;
  std::shared_ptr<Table> serial, threaded;
  std::shared_ptr<TableReader> reader;

  read_options.use_threads = true;
  ASSERT_OK(MakeStream(json, &input));
  ASSERT_OK(TableReader::Make(default_memory_pool(), input, read_options, parse_options,
                              &reader));
  ASSERT_OK(reader->Read(&threaded));

  read_options.use_threads = false;
  ASSERT_OK(MakeStream(json, &input));
  ASSERT_OK(TableReader::Make(default_memory_pool(), input, read_options, parse_options,
                              &reader));
  ASSERT_OK(reader->Read(&serial));

  ASSERT_EQ(serial->column(0)->type()->id(), Type::INT64);
  int expected = 0;
  for (auto chunk : serial->column(0)->chunks()) {
    for (int64_t i = 0; i < chunk->length(); ++i) {
      ASSERT_EQ(checked_cast<const Int64Array*>(chunk.get())->GetView(i), expected)
          << " at index " << i;
      ++expected;
    }
  }

  AssertTablesEqual(*serial, *threaded);
}

}  // namespace json
}  // namespace arrow
