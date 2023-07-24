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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/io/interfaces.h"
#include "arrow/io/slow.h"
#include "arrow/json/options.h"
#include "arrow/json/reader.h"
#include "arrow/json/test_common.h"
#include "arrow/table.h"
#include "arrow/testing/async_test_util.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/vector.h"

namespace arrow {
namespace json {

using std::string_view;

using internal::checked_cast;

static Result<std::shared_ptr<Table>> ReadToTable(std::string json,
                                                  const ReadOptions& read_options,
                                                  const ParseOptions& parse_options) {
  std::shared_ptr<io::InputStream> input;
  RETURN_NOT_OK(MakeStream(json, &input));
  ARROW_ASSIGN_OR_RAISE(auto reader, TableReader::Make(default_memory_pool(), input,
                                                       read_options, parse_options));
  return reader->Read();
}

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

// ARROW-18106
TEST(ReaderTest, FailOnTimeUnitMismatch) {
  std::string json = R"({"t":"2022-09-05T08:08:46.000"})";

  auto read_options = ReadOptions::Defaults();
  read_options.use_threads = false;
  auto parse_options = ParseOptions::Defaults();
  parse_options.explicit_schema = schema({field("t", timestamp(TimeUnit::SECOND))});

  std::shared_ptr<io::InputStream> input;
  std::shared_ptr<TableReader> reader;
  for (auto behavior : {UnexpectedFieldBehavior::Error, UnexpectedFieldBehavior::Ignore,
                        UnexpectedFieldBehavior::InferType}) {
    parse_options.unexpected_field_behavior = behavior;
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::StartsWith("Invalid: Failed to convert JSON to timestamp[s]"),
        ReadToTable(json, read_options, parse_options));
  }
}

TEST(ReaderTest, InferNestedFieldsWithSchema) {
  std::string json = R"({}
    {"a": {"c": null}}
    {"a": {"c": {}}}
    {"a": {"c": {"d": null}}}
    {"a": {"c": {"d": []}}}
    {"a": {"c": {"d": [null]}}}
    {"a": {"c": {"d": [{}]}}}
    {"a": {"c": {"d": [{"e": null}]}}}
    {"a": {"c": {"d": [{"e": true}]}}}
  )";

  auto read_options = ReadOptions::Defaults();
  read_options.use_threads = false;
  auto parse_options = ParseOptions::Defaults();
  parse_options.explicit_schema =
      schema({field("a", struct_({field("b", timestamp(TimeUnit::SECOND))}))});
  parse_options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;

  auto expected_schema = schema({field(
      "a", struct_({field("b", timestamp(TimeUnit::SECOND)),
                    field("c", struct_({field(
                                   "d", list(struct_({field("e", boolean())})))}))}))});
  auto expected_batch = RecordBatchFromJSON(expected_schema, R"([
    {"a": null},
    {"a": {"b": null, "c": null}},
    {"a": {"b": null, "c": {"d": null}}},
    {"a": {"b": null, "c": {"d": null}}},
    {"a": {"b": null, "c": {"d": []}}},
    {"a": {"b": null, "c": {"d": [null]}}},
    {"a": {"b": null, "c": {"d": [{"e": null}]}}},
    {"a": {"b": null, "c": {"d": [{"e": null}]}}},
    {"a": {"b": null, "c": {"d": [{"e": true}]}}}
  ])");
  ASSERT_OK_AND_ASSIGN(auto expected_table, Table::FromRecordBatches({expected_batch}));

  ASSERT_OK_AND_ASSIGN(auto table, ReadToTable(json, read_options, parse_options));
  AssertTablesEqual(*expected_table, *table);

  json += std::string(R"({"a": {"b": "2022-09-05T08:08:46.000"}})") + "\n";
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::StartsWith("Invalid: Failed to convert JSON to timestamp[s]"),
      ReadToTable(json, read_options, parse_options));
}

TEST(ReaderTest, InferNestedFieldsInListWithSchema) {
  std::string json = R"({}
    {"a": [{"b": "2022-09-05T08:08:00"}]}
    {"a": [{"b": "2022-09-05T08:08:01", "c": null}]}
    {"a": [{"b": "2022-09-05T08:08:02", "c": {"d": true}}]}
  )";

  auto read_options = ReadOptions::Defaults();
  read_options.use_threads = false;
  auto parse_options = ParseOptions::Defaults();
  parse_options.explicit_schema =
      schema({field("a", list(struct_({field("b", timestamp(TimeUnit::SECOND))})))});
  parse_options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;

  auto expected_schema =
      schema({field("a", list(struct_({field("b", timestamp(TimeUnit::SECOND)),
                                       field("c", struct_({field("d", boolean())}))})))});
  auto expected_batch = RecordBatchFromJSON(expected_schema, R"([
    {"a": null},
    {"a": [{"b": "2022-09-05T08:08:00", "c": null}]},
    {"a": [{"b": "2022-09-05T08:08:01", "c": null}]},
    {"a": [{"b": "2022-09-05T08:08:02", "c": {"d": true}}]}
  ])");
  ASSERT_OK_AND_ASSIGN(auto expected_table, Table::FromRecordBatches({expected_batch}));

  ASSERT_OK_AND_ASSIGN(auto table, ReadToTable(json, read_options, parse_options));
  AssertTablesEqual(*expected_table, *table);

  json += std::string(R"({"a": [{"b": "2022-09-05T08:08:03.000", "c": {}}]})") + "\n";
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::StartsWith("Invalid: Failed to convert JSON to timestamp[s]"),
      ReadToTable(json, read_options, parse_options));
}

class StreamingReaderTestBase {
 public:
  virtual ~StreamingReaderTestBase() = default;

 protected:
  static std::shared_ptr<io::InputStream> MakeTestStream(std::string str) {
    auto buffer = Buffer::FromString(std::move(str));
    return std::make_shared<io::BufferReader>(std::move(buffer));
  }
  // Stream with simulated latency
  static std::shared_ptr<io::InputStream> MakeTestStream(std::string str,
                                                         double latency) {
    return std::make_shared<io::SlowInputStream>(MakeTestStream(std::move(str)), latency);
  }

  Result<std::shared_ptr<StreamingReader>> MakeReader(
      std::shared_ptr<io::InputStream> stream) {
    return StreamingReader::Make(std::move(stream), read_options_, parse_options_,
                                 io_context_, executor_);
  }
  template <typename... Args>
  Result<std::shared_ptr<StreamingReader>> MakeReader(Args&&... args) {
    return MakeReader(MakeTestStream(std::forward<Args>(args)...));
  }

  AsyncGenerator<std::shared_ptr<RecordBatch>> MakeGenerator(
      std::shared_ptr<StreamingReader> reader) {
    return [reader = std::move(reader)] { return reader->ReadNextAsync(); };
  }
  template <typename... Args>
  Result<AsyncGenerator<std::shared_ptr<RecordBatch>>> MakeGenerator(Args&&... args) {
    ARROW_ASSIGN_OR_RAISE(auto reader, MakeReader(std::forward<Args>(args)...));
    return MakeGenerator(std::move(reader));
  }

  static void AssertReadNext(const std::shared_ptr<StreamingReader>& reader,
                             std::shared_ptr<RecordBatch>* out) {
    ASSERT_OK(reader->ReadNext(out));
    ASSERT_FALSE(IsIterationEnd(*out));
    ASSERT_OK((**out).ValidateFull());
  }
  static void AssertReadEnd(const std::shared_ptr<StreamingReader>& reader) {
    std::shared_ptr<RecordBatch> out;
    ASSERT_OK(reader->ReadNext(&out));
    ASSERT_TRUE(IsIterationEnd(out));
  }

  static void AssertBatchSequenceEquals(const RecordBatchVector& expected_batches,
                                        const RecordBatchVector& sequence) {
    ASSERT_OK_AND_ASSIGN(auto expected_table, Table::FromRecordBatches(expected_batches));
    ASSERT_OK(expected_table->ValidateFull());

    auto first_null = std::find(sequence.cbegin(), sequence.cend(), nullptr);
    for (auto it = first_null; it != sequence.cend(); ++it) {
      ASSERT_EQ(*it, nullptr);
    }

    RecordBatchVector batches(sequence.cbegin(), first_null);
    EXPECT_EQ(batches.size(), expected_batches.size());
    ASSERT_OK_AND_ASSIGN(auto table, Table::FromRecordBatches(batches));
    ASSERT_OK(table->ValidateFull());
    ASSERT_TABLES_EQUAL(*expected_table, *table);
  }

  struct TestCase {
    std::string json;
    int json_size;
    int block_size;
    int num_rows;
    int num_batches;
    std::shared_ptr<Schema> schema;
    RecordBatchVector batches;
  };

  // Creates a test case from valid JSON objects with a human-readable index field and a
  // struct field of random data. `block_size_multiplier` is applied to the largest
  // generated row length to determine the target block_size. i.e - higher multiplier
  // means fewer batches
  static TestCase GenerateTestCase(int num_rows, double block_size_multiplier = 3.0) {
    FieldVector data_fields = {field("s", utf8()), field("f", float64()),
                               field("b", boolean())};
    FieldVector fields = {field("i", int64()), field("d", struct_({data_fields}))};
    TestCase out;
    out.schema = schema(fields);
    out.num_rows = num_rows;

    constexpr int kSeed = 0x432432;
    std::default_random_engine engine(kSeed);
    std::vector<std::string> rows(num_rows);
    size_t max_row_size = 1;

    auto options = GenerateOptions::Defaults();
    options.null_probability = 0;
    for (int i = 0; i < num_rows; ++i) {
      StringBuffer string_buffer;
      Writer writer(string_buffer);
      ABORT_NOT_OK(Generate(data_fields, engine, &writer, options));
      std::string json = string_buffer.GetString();
      rows[i] = Join({"{\"i\":", std::to_string(i), ",\"d\":", json, "}\n"});
      max_row_size = std::max(max_row_size, rows[i].size());
    }

    auto block_size = static_cast<size_t>(max_row_size * block_size_multiplier);
    // Deduce the expected record batches from the target block size.
    std::vector<std::string> batch_rows;
    size_t pos = 0;
    for (const auto& row : rows) {
      pos += row.size();
      if (pos > block_size) {
        out.batches.push_back(
            RecordBatchFromJSON(out.schema, Join({"[", Join(batch_rows, ","), "]"})));
        batch_rows.clear();
        pos -= block_size;
      }
      batch_rows.push_back(row);
      out.json += row;
    }
    if (!batch_rows.empty()) {
      out.batches.push_back(
          RecordBatchFromJSON(out.schema, Join({"[", Join(batch_rows, ","), "]"})));
    }

    out.json_size = static_cast<int>(out.json.size());
    out.block_size = static_cast<int>(block_size);
    out.num_batches = static_cast<int>(out.batches.size());

    return out;
  }

  static std::string Join(const std::vector<std::string>& strings,
                          const std::string& delim = "", bool trailing_delim = false) {
    std::string out;
    for (size_t i = 0; i < strings.size();) {
      out += strings[i++];
      if (i != strings.size() || trailing_delim) {
        out += delim;
      }
    }
    return out;
  }

  internal::Executor* executor_ = nullptr;
  ParseOptions parse_options_ = ParseOptions::Defaults();
  ReadOptions read_options_ = ReadOptions::Defaults();
  io::IOContext io_context_ = io::default_io_context();
};

class AsyncStreamingReaderTest : public StreamingReaderTestBase, public ::testing::Test {
 protected:
  void SetUp() override { read_options_.use_threads = true; }
};

class StreamingReaderTest : public StreamingReaderTestBase,
                            public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override { read_options_.use_threads = GetParam(); }
};

INSTANTIATE_TEST_SUITE_P(StreamingReaderTest, StreamingReaderTest,
                         ::testing::Values(false, true));

TEST_P(StreamingReaderTest, ErrorOnEmptyStream) {
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::StartsWith("Invalid: Empty JSON stream"), MakeReader(""));
  std::string data(100, '\n');
  for (auto block_size : {25, 49, 50, 100, 200}) {
    read_options_.block_size = block_size;
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::StartsWith("Invalid: Empty JSON stream"), MakeReader(data));
  }
}

TEST_P(StreamingReaderTest, PropagateChunkingErrors) {
  constexpr double kIoLatency = 1e-3;

  auto test_schema = schema({field("i", int64())});
  // Object straddles multiple blocks
  auto bad_first_chunk = Join(
      {
          R"({"i": 0            })",
          R"({"i": 1})",
      },
      "\n");
  auto bad_middle_chunk = Join(
      {
          R"({"i": 0})",
          R"({"i":    1})",
          R"({"i": 2})",
      },
      "\n");

  read_options_.block_size = 10;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::StartsWith("Invalid: straddling object straddles two block boundaries"),
      MakeReader(bad_first_chunk));

  ASSERT_OK_AND_ASSIGN(auto reader, MakeReader(bad_middle_chunk, kIoLatency));

  std::shared_ptr<RecordBatch> batch;
  AssertReadNext(reader, &batch);
  EXPECT_EQ(reader->bytes_processed(), 9);
  ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(test_schema, "[{\"i\":0}]"), *batch);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::StartsWith("Invalid: straddling object straddles two block boundaries"),
      reader->ReadNext(&batch));
  EXPECT_EQ(reader->bytes_processed(), 9);
  AssertReadEnd(reader);
  AssertReadEnd(reader);
  EXPECT_EQ(reader->bytes_processed(), 9);
}

TEST_P(StreamingReaderTest, PropagateParsingErrors) {
  auto test_schema = schema({field("n", int64())});
  auto bad_first_block = Join(
      {
          R"({"n": })",
          R"({"n": 10000})",
      },
      "\n");
  auto bad_first_block_after_empty = Join(
      {
          R"(            )",
          R"({"n": })",
          R"({"n": 10000})",
      },
      "\n");
  auto bad_middle_block = Join(
      {
          R"({"n": 10000})",
          R"({"n": 200 0})",
          R"({"n": 30000})",
      },
      "\n");

  read_options_.block_size = 16;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::StartsWith("Invalid: JSON parse error: Invalid value"),
      MakeReader(bad_first_block));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::StartsWith("Invalid: JSON parse error: Invalid value"),
      MakeReader(bad_first_block_after_empty));

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK_AND_ASSIGN(auto reader, MakeReader(bad_middle_block));
  EXPECT_EQ(reader->bytes_processed(), 0);
  AssertSchemaEqual(reader->schema(), test_schema);

  AssertReadNext(reader, &batch);
  EXPECT_EQ(reader->bytes_processed(), 13);
  ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(test_schema, R"([{"n":10000}])"), *batch);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::StartsWith(
          "Invalid: JSON parse error: Missing a comma or '}' after an object member"),
      reader->ReadNext(&batch));
  EXPECT_EQ(reader->bytes_processed(), 13);
  AssertReadEnd(reader);
  EXPECT_EQ(reader->bytes_processed(), 13);
}

TEST_P(StreamingReaderTest, PropagateErrorsNonLinewiseChunker) {
  auto test_schema = schema({field("i", int64())});
  auto bad_first_block = Join(
      {
          R"({"i":0}{1})",
          R"({"i":2})",
      },
      "\n");
  auto bad_middle_blocks = Join(
      {
          R"({"i": 0})",
          R"({"i":    1})",
          R"({}"i":2})",
          R"({"i": 3})",
      },
      "\n");

  std::shared_ptr<RecordBatch> batch;
  std::shared_ptr<StreamingReader> reader;
  Status status;
  read_options_.block_size = 10;
  parse_options_.newlines_in_values = true;

  ASSERT_OK_AND_ASSIGN(reader, MakeReader(bad_first_block));
  AssertReadNext(reader, &batch);
  EXPECT_EQ(reader->bytes_processed(), 7);
  ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(test_schema, "[{\"i\":0}]"), *batch);

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::StartsWith("Invalid: JSON parse error"),
                                  reader->ReadNext(&batch));
  EXPECT_EQ(reader->bytes_processed(), 7);
  AssertReadEnd(reader);

  ASSERT_OK_AND_ASSIGN(reader, MakeReader(bad_middle_blocks));
  AssertReadNext(reader, &batch);
  EXPECT_EQ(reader->bytes_processed(), 9);
  ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(test_schema, "[{\"i\":0}]"), *batch);
  // Chunker doesn't require newline delimiters, so this should be valid
  AssertReadNext(reader, &batch);
  EXPECT_EQ(reader->bytes_processed(), 20);
  ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(test_schema, "[{\"i\":1}]"), *batch);

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::StartsWith("Invalid: JSON parse error"),
                                  reader->ReadNext(&batch));
  EXPECT_EQ(reader->bytes_processed(), 20);
  // Incoming chunker error from ":2}" shouldn't leak through after the first failure,
  // which is a possibility if async tasks are still outstanding due to readahead.
  AssertReadEnd(reader);
  AssertReadEnd(reader);
  EXPECT_EQ(reader->bytes_processed(), 20);
}

TEST_P(StreamingReaderTest, IgnoreLeadingEmptyBlocks) {
  std::string test_json(32, '\n');
  test_json += R"({"b": true, "s": "foo"})";
  ASSERT_EQ(test_json.length(), 55);

  parse_options_.explicit_schema = schema({field("b", boolean()), field("s", utf8())});
  read_options_.block_size = 24;
  ASSERT_OK_AND_ASSIGN(auto reader, MakeReader(test_json));
  EXPECT_EQ(reader->bytes_processed(), 0);

  auto expected_schema = parse_options_.explicit_schema;
  auto expected_batch = RecordBatchFromJSON(expected_schema, R"([{"b":true,"s":"foo"}])");

  AssertSchemaEqual(reader->schema(), expected_schema);

  std::shared_ptr<RecordBatch> actual_batch;
  AssertReadNext(reader, &actual_batch);
  EXPECT_EQ(reader->bytes_processed(), 55);
  ASSERT_BATCHES_EQUAL(*expected_batch, *actual_batch);

  AssertReadEnd(reader);
}

TEST_P(StreamingReaderTest, ExplicitSchemaErrorOnUnexpectedFields) {
  std::string test_json =
      Join({R"({"s": "foo", "t": "2022-01-01"})", R"({"s": "bar", "t": "2022-01-02"})",
            R"({"s": "baz", "t": "2022-01-03", "b": true})"},
           "\n");

  FieldVector expected_fields = {field("s", utf8())};
  std::shared_ptr<Schema> expected_schema = schema(expected_fields);

  parse_options_.explicit_schema = expected_schema;
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  read_options_.block_size = 48;

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::StartsWith("Invalid: JSON parse error: unexpected field"),
      MakeReader(test_json));

  expected_fields.push_back(field("t", utf8()));
  expected_schema = schema(expected_fields);

  parse_options_.explicit_schema = expected_schema;
  ASSERT_OK_AND_ASSIGN(auto reader, MakeReader(test_json));
  AssertSchemaEqual(reader->schema(), expected_schema);

  std::shared_ptr<RecordBatch> batch;
  AssertReadNext(reader, &batch);
  ASSERT_BATCHES_EQUAL(
      *RecordBatchFromJSON(expected_schema, R"([{"s":"foo","t":"2022-01-01"}])"), *batch);
  EXPECT_EQ(reader->bytes_processed(), 32);

  AssertReadNext(reader, &batch);
  ASSERT_BATCHES_EQUAL(
      *RecordBatchFromJSON(expected_schema, R"([{"s":"bar","t":"2022-01-02"}])"), *batch);
  EXPECT_EQ(reader->bytes_processed(), 64);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::StartsWith("Invalid: JSON parse error: unexpected field"),
      reader->ReadNext(&batch));
  EXPECT_EQ(reader->bytes_processed(), 64);
  AssertReadEnd(reader);
}

TEST_P(StreamingReaderTest, ExplicitSchemaIgnoreUnexpectedFields) {
  std::string test_json =
      Join({R"({"s": "foo", "u": "2022-01-01"})", R"({"s": "bar", "t": "2022-01-02"})",
            R"({"s": "baz", "t": "2022-01-03", "b": true})"},
           "\n");

  FieldVector expected_fields = {field("s", utf8()), field("t", utf8())};
  std::shared_ptr<Schema> expected_schema = schema(expected_fields);

  parse_options_.explicit_schema = expected_schema;
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::Ignore;
  read_options_.block_size = 48;

  ASSERT_OK_AND_ASSIGN(auto reader, MakeReader(test_json));
  AssertSchemaEqual(reader->schema(), expected_schema);

  std::shared_ptr<RecordBatch> batch;
  AssertReadNext(reader, &batch);
  ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(expected_schema, R"([{"s":"foo","t":null}])"),
                       *batch);
  EXPECT_EQ(reader->bytes_processed(), 32);

  AssertReadNext(reader, &batch);
  ASSERT_BATCHES_EQUAL(
      *RecordBatchFromJSON(expected_schema, R"([{"s":"bar","t":"2022-01-02"}])"), *batch);
  EXPECT_EQ(reader->bytes_processed(), 64);

  AssertReadNext(reader, &batch);
  ASSERT_BATCHES_EQUAL(
      *RecordBatchFromJSON(expected_schema, R"([{"s":"baz","t":"2022-01-03"}])"), *batch);
  EXPECT_EQ(reader->bytes_processed(), 106);
  AssertReadEnd(reader);
}

TEST_P(StreamingReaderTest, InferredSchema) {
  auto test_json = Join(
      {
          R"({"a": 0, "b": "foo"       })",
          R"({"a": 1, "c": true        })",
          R"({"a": 2, "d": "2022-01-01"})",
      },
      "\n", true);

  std::shared_ptr<StreamingReader> reader;
  std::shared_ptr<Schema> expected_schema;
  std::shared_ptr<RecordBatch> expected_batch;
  std::shared_ptr<RecordBatch> actual_batch;

  FieldVector fields = {field("a", int64()), field("b", utf8())};
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  parse_options_.explicit_schema = nullptr;

  // Schema derived from the first line
  expected_schema = schema(fields);

  read_options_.block_size = 32;
  ASSERT_OK_AND_ASSIGN(reader, MakeReader(test_json));
  AssertSchemaEqual(reader->schema(), expected_schema);

  expected_batch = RecordBatchFromJSON(expected_schema, R"([{"a": 0, "b": "foo"}])");
  AssertReadNext(reader, &actual_batch);
  EXPECT_EQ(reader->bytes_processed(), 28);
  ASSERT_BATCHES_EQUAL(*expected_batch, *actual_batch);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::StartsWith("Invalid: JSON parse error: unexpected field"),
      reader->ReadNext(&actual_batch));

  // Schema derived from the first 2 lines
  fields.push_back(field("c", boolean()));
  expected_schema = schema(fields);

  read_options_.block_size = 64;
  ASSERT_OK_AND_ASSIGN(reader, MakeReader(test_json));
  AssertSchemaEqual(reader->schema(), expected_schema);

  expected_batch = RecordBatchFromJSON(expected_schema, R"([
    {"a": 0, "b": "foo", "c": null},
    {"a": 1, "b":  null, "c": true}
  ])");
  AssertReadNext(reader, &actual_batch);
  EXPECT_EQ(reader->bytes_processed(), 56);
  ASSERT_BATCHES_EQUAL(*expected_batch, *actual_batch);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::StartsWith("Invalid: JSON parse error: unexpected field"),
      reader->ReadNext(&actual_batch));

  // Schema derived from all 3 lines
  fields.push_back(field("d", timestamp(TimeUnit::SECOND)));
  expected_schema = schema(fields);

  read_options_.block_size = 96;
  ASSERT_OK_AND_ASSIGN(reader, MakeReader(test_json));
  AssertSchemaEqual(reader->schema(), expected_schema);

  expected_batch = RecordBatchFromJSON(expected_schema, R"([
    {"a": 0, "b": "foo", "c": null, "d":  null},
    {"a": 1, "b":  null, "c": true, "d":  null},
    {"a": 2, "b":  null, "c": null, "d":  "2022-01-01"}
  ])");
  AssertReadNext(reader, &actual_batch);
  EXPECT_EQ(reader->bytes_processed(), 84);
  ASSERT_BATCHES_EQUAL(*expected_batch, *actual_batch);

  AssertReadEnd(reader);
}

TEST_F(AsyncStreamingReaderTest, AsyncReentrancy) {
  constexpr int kNumRows = 16;
  constexpr double kIoLatency = 1e-2;

  auto expected = GenerateTestCase(kNumRows);
  parse_options_.explicit_schema = expected.schema;
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  read_options_.block_size = expected.block_size;

  std::vector<Future<std::shared_ptr<RecordBatch>>> futures(expected.num_batches + 2);
  ASSERT_OK_AND_ASSIGN(auto reader, MakeReader(expected.json, kIoLatency));
  EXPECT_EQ(reader->bytes_processed(), 0);
  for (auto& future : futures) {
    future = reader->ReadNextAsync();
  }

  ASSERT_FINISHES_OK_AND_ASSIGN(auto results, All(std::move(futures)));
  EXPECT_EQ(reader->bytes_processed(), expected.json_size);
  ASSERT_OK_AND_ASSIGN(auto batches, internal::UnwrapOrRaise(std::move(results)));
  AssertBatchSequenceEquals(expected.batches, batches);
}

TEST_F(AsyncStreamingReaderTest, FuturesOutliveReader) {
  constexpr int kNumRows = 16;
  constexpr double kIoLatency = 1e-2;

  auto expected = GenerateTestCase(kNumRows);
  parse_options_.explicit_schema = expected.schema;
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  read_options_.block_size = expected.block_size;

  auto stream = MakeTestStream(expected.json, kIoLatency);
  std::vector<Future<std::shared_ptr<RecordBatch>>> futures(expected.num_batches + 2);
  {
    ASSERT_OK_AND_ASSIGN(auto reader, MakeReader(stream));
    EXPECT_EQ(reader->bytes_processed(), 0);
    for (auto& future : futures) {
      future = reader->ReadNextAsync();
    }
  }

  ASSERT_FINISHES_OK_AND_ASSIGN(auto results, All(std::move(futures)));
  ASSERT_OK_AND_ASSIGN(auto batches, internal::UnwrapOrRaise(std::move(results)));
  AssertBatchSequenceEquals(expected.batches, batches);
}

TEST_F(AsyncStreamingReaderTest, StressBufferedReads) {
  constexpr int kNumRows = 500;

  auto expected = GenerateTestCase(kNumRows);
  parse_options_.explicit_schema = expected.schema;
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  read_options_.block_size = expected.block_size;

  std::vector<Future<std::shared_ptr<RecordBatch>>> futures(expected.num_batches + 2);
  ASSERT_OK_AND_ASSIGN(auto reader, MakeReader(expected.json));
  EXPECT_EQ(reader->bytes_processed(), 0);
  for (auto& future : futures) {
    future = reader->ReadNextAsync();
  }

  ASSERT_FINISHES_OK_AND_ASSIGN(auto results, All(std::move(futures)));
  ASSERT_OK_AND_ASSIGN(auto batches, internal::UnwrapOrRaise(results));
  AssertBatchSequenceEquals(expected.batches, batches);
}

TEST_F(AsyncStreamingReaderTest, StressSharedIoAndCpuExecutor) {
  constexpr int kNumRows = 500;
  constexpr double kIoLatency = 1e-4;

  auto expected = GenerateTestCase(kNumRows);
  parse_options_.explicit_schema = expected.schema;
  parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::Error;
  read_options_.block_size = expected.block_size;

  // Force the serial -> parallel pipeline to contend for a single thread
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
  io_context_ = io::IOContext(thread_pool.get());
  executor_ = thread_pool.get();

  ASSERT_OK_AND_ASSIGN(auto generator, MakeGenerator(expected.json, kIoLatency));
  ASSERT_FINISHES_OK_AND_ASSIGN(auto batches, CollectAsyncGenerator(generator));
  AssertBatchSequenceEquals(expected.batches, batches);
}

}  // namespace json
}  // namespace arrow
