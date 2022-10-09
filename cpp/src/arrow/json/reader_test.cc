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
#include "arrow/type_fwd.h"

namespace arrow {
namespace json {

using std::string_view;

using internal::checked_cast;

class ReaderTest : public ::testing::TestWithParam<bool> {
 public:
  void SetUpReader() {
    read_options_.use_threads = GetParam();
    ASSERT_OK_AND_ASSIGN(reader_, TableReader::Make(default_memory_pool(), input_,
                                                    read_options_, parse_options_));
  }

  void SetUpReader(std::string_view input) {
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

INSTANTIATE_TEST_SUITE_P(ReaderTest, ReaderTest, ::testing::Values(false, true));

TEST_P(ReaderTest, Empty) {
  SetUpReader("{}\n{}\n");
  ASSERT_OK_AND_ASSIGN(table_, reader_->Read());

  auto expected_table = Table::Make(schema({}), ArrayVector(), 2);
  AssertTablesEqual(*expected_table, *table_);
}

TEST_P(ReaderTest, EmptyNoNewlineAtEnd) {
  SetUpReader("{}\n{}");
  ASSERT_OK_AND_ASSIGN(table_, reader_->Read());

  auto expected_table = Table::Make(schema({}), ArrayVector(), 2);
  AssertTablesEqual(*expected_table, *table_);
}

TEST_P(ReaderTest, EmptyManyNewlines) {
  SetUpReader("{}\n\r\n{}\n\r\n");
  ASSERT_OK_AND_ASSIGN(table_, reader_->Read());

  auto expected_table = Table::Make(schema({}), ArrayVector(), 2);
  AssertTablesEqual(*expected_table, *table_);
}

TEST_P(ReaderTest, Basics) {
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  auto src = scalars_only_src();
  SetUpReader(src);
  ASSERT_OK_AND_ASSIGN(table_, reader_->Read());

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
  ASSERT_OK_AND_ASSIGN(table_, reader_->Read());

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
  ASSERT_OK_AND_ASSIGN(table_, reader_->Read());

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
  ASSERT_OK_AND_ASSIGN(table_, reader_->Read());

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
  ASSERT_OK_AND_ASSIGN(table_, reader_->Read());

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

TEST_P(ReaderTest, UnquotedDecimal) {
  auto schema =
      ::arrow::schema({field("price", decimal(9, 2)), field("cost", decimal(9, 3))});
  parse_options_.explicit_schema = schema;
  auto src = unquoted_decimal_src();
  SetUpReader(src);
  ASSERT_OK_AND_ASSIGN(table_, reader_->Read());

  auto expected_table = TableFromJSON(schema, {R"([
    { "price": "30.04", "cost":"30.001" },
    { "price": "1.23", "cost":"1.229" }
  ])"});
  AssertTablesEqual(*expected_table, *table_);
}

TEST_P(ReaderTest, MixedDecimal) {
  auto schema =
      ::arrow::schema({field("price", decimal(9, 2)), field("cost", decimal(9, 3))});
  parse_options_.explicit_schema = schema;
  auto src = mixed_decimal_src();
  SetUpReader(src);
  ASSERT_OK_AND_ASSIGN(table_, reader_->Read());

  auto expected_table = TableFromJSON(schema, {R"([
    { "price": "30.04", "cost":"30.001" },
    { "price": "1.23", "cost":"1.229" }
  ])"});
  AssertTablesEqual(*expected_table, *table_);
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
  std::shared_ptr<TableReader> reader;

  read_options.use_threads = true;
  ASSERT_OK(MakeStream(json, &input));
  ASSERT_OK_AND_ASSIGN(reader, TableReader::Make(default_memory_pool(), input,
                                                 read_options, parse_options));
  ASSERT_OK_AND_ASSIGN(auto threaded, reader->Read());

  read_options.use_threads = false;
  ASSERT_OK(MakeStream(json, &input));
  ASSERT_OK_AND_ASSIGN(reader, TableReader::Make(default_memory_pool(), input,
                                                 read_options, parse_options));
  ASSERT_OK_AND_ASSIGN(auto serial, reader->Read());

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

TEST(ReaderTest, ListArrayWithFewValues) {
  // ARROW-7647
  ParseOptions parse_options;
  parse_options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  ReadOptions read_options;

  auto expected_batch = RecordBatchFromJSON(
      schema({field("a", list(int64())),
              field("b", struct_({field("c", boolean()),
                                  field("d", timestamp(TimeUnit::SECOND))}))}),
      R"([
        {"a": [1], "b": {"c": true, "d": "1991-02-03"}},
        {"a": [], "b": {"c": false, "d": "2019-04-01"}}
      ])");
  ASSERT_OK_AND_ASSIGN(auto expected_table, Table::FromRecordBatches({expected_batch}));

  std::string json = R"({"a": [1], "b": {"c": true, "d": "1991-02-03"}}
{"a": [], "b": {"c": false, "d": "2019-04-01"}}
)";
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK(MakeStream(json, &input));

  read_options.use_threads = false;
  ASSERT_OK_AND_ASSIGN(auto reader, TableReader::Make(default_memory_pool(), input,
                                                      read_options, parse_options));

  ASSERT_OK_AND_ASSIGN(auto actual_table, reader->Read());
  AssertTablesEqual(*actual_table, *expected_table);
}

TEST(ReaderTest, FailOnInvalidEOF) {
  auto read_options = ReadOptions::Defaults();
  auto parse_options = ParseOptions::Defaults();
  read_options.use_threads = false;
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK(MakeStream("}", &input));

  for (auto newlines_in_values : {false, true}) {
    parse_options.newlines_in_values = newlines_in_values;
    ASSERT_OK_AND_ASSIGN(auto reader, TableReader::Make(default_memory_pool(), input,
                                                        read_options, parse_options));
    ASSERT_RAISES(Invalid, reader->Read());
  }
}

class StreamingReaderTest : public ::testing::TestWithParam<bool> {
 public:
  ParseOptions parse_options_ = ParseOptions::Defaults();
  ReadOptions read_options_ = DefaultReadOptions();
  io::IOContext io_context_ = io::default_io_context();
  std::shared_ptr<io::InputStream> input_;
  std::shared_ptr<StreamingReader> reader_;

 private:
  [[nodiscard]] ReadOptions DefaultReadOptions() const {
    auto read_options = ReadOptions::Defaults();
    read_options.use_threads = GetParam();
    return read_options;
  }
};

INSTANTIATE_TEST_SUITE_P(StreamingReaderTest, StreamingReaderTest,
                         ::testing::Values(false, true));

TEST_P(StreamingReaderTest, FailOnEmptyInput) {
  ASSERT_OK(MakeStream("", &input_));
  ASSERT_RAISES(
      Invalid, StreamingReader::Make(io_context_, input_, read_options_, parse_options_));
}

TEST_P(StreamingReaderTest, FailOnParseError) {
  std::string json = R"(
{"n": 10000}
{"n": "foo"})";

  read_options_.block_size = 16;
  ASSERT_OK(MakeStream(json, &input_));
  ASSERT_OK_AND_ASSIGN(
      reader_, StreamingReader::Make(io_context_, input_, read_options_, parse_options_));
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(reader_->ReadNext(&batch));
  ASSERT_EQ(14, reader_->bytes_read());
  ASSERT_RAISES(Invalid, reader_->ReadNext(&batch));
}

TEST_P(StreamingReaderTest, IgnoreLeadingEmptyBlocks) {
  std::string json;
  json.insert(json.end(), 32, '\n');
  json += R"({"b": true, "s": "foo"})";
  auto json_len = static_cast<int64_t>(json.length());

  parse_options_.explicit_schema = schema({field("b", boolean()), field("s", utf8())});
  read_options_.block_size = 24;
  ASSERT_OK(MakeStream(json, &input_));
  ASSERT_OK_AND_ASSIGN(
      reader_, StreamingReader::Make(io_context_, input_, read_options_, parse_options_));

  auto expected_schema = parse_options_.explicit_schema;
  auto expected_batch =
      RecordBatchFromJSON(expected_schema, R"([{"b": true, "s": "foo"}])");
  std::shared_ptr<RecordBatch> actual_batch;

  ASSERT_EQ(*reader_->schema(), *expected_schema);
  ASSERT_OK(reader_->ReadNext(&actual_batch));
  ASSERT_TRUE(actual_batch);
  ASSERT_EQ(json_len, reader_->bytes_read());
  ASSERT_BATCHES_EQUAL(*actual_batch, *expected_batch);

  ASSERT_OK(reader_->ReadNext(&actual_batch));
  ASSERT_FALSE(actual_batch);
}

TEST_P(StreamingReaderTest, ExplicitSchema) {
  std::string json = R"({"s": "foo", "t": "2022-01-01", "b": true})";
  auto json_len = static_cast<int64_t>(json.length());

  parse_options_.explicit_schema = schema({field("s", utf8()), field("t", utf8())});
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::Ignore;

  ASSERT_OK(MakeStream(json, &input_));
  ASSERT_OK_AND_ASSIGN(
      reader_, StreamingReader::Make(io_context_, input_, read_options_, parse_options_));

  auto expected_schema = parse_options_.explicit_schema;
  auto expected_batch =
      RecordBatchFromJSON(expected_schema, R"([{"s": "foo", "t": "2022-01-01"}])");
  std::shared_ptr<RecordBatch> actual_batch;

  ASSERT_EQ(*reader_->schema(), *expected_schema);
  ASSERT_OK(reader_->ReadNext(&actual_batch));
  ASSERT_TRUE(actual_batch);
  ASSERT_EQ(json_len, reader_->bytes_read());
  ASSERT_BATCHES_EQUAL(*actual_batch, *expected_batch);

  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  ASSERT_OK(MakeStream(json, &input_));
  ASSERT_RAISES(
      Invalid, StreamingReader::Make(io_context_, input_, read_options_, parse_options_));
}

TEST_P(StreamingReaderTest, InferredSchema) {
  std::string json = R"(
{"a": 0, "b": "foo"       }
{"a": 1, "c": true        }
{"a": 2, "d": "2022-01-01"}
)";

  std::shared_ptr<Schema> expected_schema;
  std::shared_ptr<RecordBatch> expected_batch;
  std::shared_ptr<RecordBatch> actual_batch;

  FieldVector fields = {field("a", int64()), field("b", utf8())};

  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;

  // Schema derived from the first line
  read_options_.block_size = 32;
  ASSERT_OK(MakeStream(json, &input_));
  ASSERT_OK_AND_ASSIGN(
      reader_, StreamingReader::Make(io_context_, input_, read_options_, parse_options_));

  expected_schema = schema(fields);
  ASSERT_EQ(*reader_->schema(), *expected_schema);

  expected_batch = RecordBatchFromJSON(expected_schema, R"([{"a": 0, "b": "foo"}])");
  ASSERT_OK(reader_->ReadNext(&actual_batch));
  ASSERT_TRUE(actual_batch);
  ASSERT_EQ(29, reader_->bytes_read());
  ASSERT_BATCHES_EQUAL(*actual_batch, *expected_batch);

  expected_batch = RecordBatchFromJSON(expected_schema, R"([{"a": 1, "b": null}])");
  ASSERT_OK(reader_->ReadNext(&actual_batch));
  ASSERT_TRUE(actual_batch);
  ASSERT_EQ(57, reader_->bytes_read());
  ASSERT_BATCHES_EQUAL(*actual_batch, *expected_batch);

  expected_batch = RecordBatchFromJSON(expected_schema, R"([{"a": 2, "b": null}])");
  ASSERT_OK(reader_->ReadNext(&actual_batch));
  ASSERT_TRUE(actual_batch);
  ASSERT_EQ(85, reader_->bytes_read());
  ASSERT_BATCHES_EQUAL(*actual_batch, *expected_batch);

  // Schema derived from the first 2 lines
  read_options_.block_size = 64;
  ASSERT_OK(MakeStream(json, &input_));
  ASSERT_OK_AND_ASSIGN(
      reader_, StreamingReader::Make(io_context_, input_, read_options_, parse_options_));

  fields.push_back(field("c", boolean()));
  expected_schema = schema(fields);
  ASSERT_EQ(*reader_->schema(), *expected_schema);

  expected_batch = RecordBatchFromJSON(expected_schema, R"([
    {"a": 0, "b": "foo", "c": null},
    {"a": 1, "b":  null, "c": true}
  ])");
  ASSERT_OK(reader_->ReadNext(&actual_batch));
  ASSERT_TRUE(actual_batch);
  ASSERT_EQ(57, reader_->bytes_read());
  ASSERT_BATCHES_EQUAL(*actual_batch, *expected_batch);

  expected_batch = RecordBatchFromJSON(expected_schema, R"([
    {"a": 2, "b": null, "c": null}
  ])");
  ASSERT_OK(reader_->ReadNext(&actual_batch));
  ASSERT_TRUE(actual_batch);
  ASSERT_EQ(85, reader_->bytes_read());
  ASSERT_BATCHES_EQUAL(*actual_batch, *expected_batch);

  // Schema derived from all 3 lines
  read_options_.block_size = 96;
  ASSERT_OK(MakeStream(json, &input_));
  ASSERT_OK_AND_ASSIGN(
      reader_, StreamingReader::Make(io_context_, input_, read_options_, parse_options_));

  fields.push_back(field("d", timestamp(TimeUnit::SECOND)));
  expected_schema = schema(fields);
  ASSERT_EQ(*reader_->schema(), *expected_schema);

  expected_batch = RecordBatchFromJSON(expected_schema, R"([
    {"a": 0, "b": "foo", "c": null, "d":  null},
    {"a": 1, "b":  null, "c": true, "d":  null},
    {"a": 2, "b":  null, "c": null, "d":  "2022-01-01"}
  ])");
  ASSERT_OK(reader_->ReadNext(&actual_batch));
  ASSERT_TRUE(actual_batch);
  ASSERT_EQ(85, reader_->bytes_read());
  ASSERT_BATCHES_EQUAL(*actual_batch, *expected_batch);

  ASSERT_OK(reader_->ReadNext(&actual_batch));
  ASSERT_FALSE(actual_batch);
}

}  // namespace json
}  // namespace arrow
