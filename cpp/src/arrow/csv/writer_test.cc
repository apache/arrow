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

#include "gtest/gtest.h"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/csv/writer.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/result_internal.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace csv {

struct WriterTestParams {
  WriterTestParams(std::shared_ptr<Schema> schema, std::string batch_data,
                   WriteOptions options, std::optional<std::string> expected_output,
                   Status expected_status = Status::OK())
      : schema(std::move(schema)),
        batch_data(std::move(batch_data)),
        options(std::move(options)),
        expected_output(std::move(expected_output)),
        expected_status(std::move(expected_status)) {}
  std::shared_ptr<Schema> schema;
  std::string batch_data;
  WriteOptions options;
  std::optional<std::string> expected_output;
  Status expected_status;
};

// Avoid Valgrind failures with GTest trying to represent a WriterTestParams
void PrintTo(const WriterTestParams& p, std::ostream* os) {
  *os << "WriterTestParams(" << reinterpret_cast<const void*>(&p) << ")";
}

WriteOptions DefaultTestOptions(bool include_header = false,
                                const std::string& null_string = "",
                                QuotingStyle quoting_style = QuotingStyle::Needed,
                                const std::string& eol = "\n", char delimiter = ',',
                                int batch_size = 5) {
  WriteOptions options;
  options.batch_size = batch_size;
  options.include_header = include_header;
  options.null_string = null_string;
  options.eol = eol;
  options.quoting_style = quoting_style;
  options.delimiter = delimiter;
  return options;
}

std::string UtilGetExpectedWithEOL(const std::string& eol) {
  return std::string("1,,-1,,,,") + eol +        // line 1
         R"(1,"abc""efg",2324,,,,)" + eol +      // line 2
         R"(,"abcd",5467,,,,)" + eol +           // line 3
         R"(,,,,,,)" + eol +                     // line 4
         R"(546,"",517,,,,)" + eol +             // line 5
         R"(124,"a""""b""",,,,,)" + eol +        // line 6
         R"(,,,1970-01-01,,,)" + eol +           // line 7
         R"(,,,,1970-01-02,,)" + eol +           // line 8
         R"(,,,,,2004-02-29 01:02:03,)" + eol +  // line 9
         R"(,,,,,,3600)" + eol +                 // line 10
         R"(,"NA",,,,,)" + eol;                  // line 11
}

std::vector<WriterTestParams> GenerateTestCases() {
  // Dummy schema and data for testing invalid options.
  auto dummy_schema = schema({field("a", uint8())});
  std::string dummy_batch_data = R"([{"a": null}])";

  // Schema to test various types.
  auto abc_schema = schema({
      field("a", uint64()),
      field("b\"", utf8()),
      field("c ", int32()),
      field("d", date32()),
      field("e", date64()),
      field("f", timestamp(TimeUnit::SECOND)),
      field("g", duration(TimeUnit::SECOND)),
  });
  auto populated_batch = R"([{"a": 1, "c ": -1},
                             { "a": 1, "b\"": "abc\"efg", "c ": 2324},
                             { "b\"": "abcd", "c ": 5467},
                             { },
                             { "a": 546, "b\"": "", "c ": 517 },
                             { "a": 124, "b\"": "a\"\"b\"" },
                             { "d": 0 },
                             { "e": 86400000 },
                             { "f": 1078016523 },
                             { "g": 3600 },
                             { "b\"": "NA" }])";

  std::string expected_header = std::string(R"("a","b""","c ","d","e","f","g")") + "\n";

  // Expected output without header when using default QuotingStyle::Needed.
  std::string expected_without_header = UtilGetExpectedWithEOL("\n");

  std::string expected_with_custom_eol = UtilGetExpectedWithEOL("_EOL_");

  // Expected output without header when using QuotingStyle::AllValid.
  std::string expected_quoting_style_all_valid =
      std::string(R"("1",,"-1",,,,)") + "\n" +   // line 1
      R"("1","abc""efg","2324",,,,)" + "\n" +    // line 2
      R"(,"abcd","5467",,,,)" + "\n" +           // line 3
      R"(,,,,,,)" + "\n" +                       // line 4
      R"("546","","517",,,,)" + "\n" +           // line 5
      R"("124","a""""b""",,,,,)" + "\n" +        // line 6
      R"(,,,"1970-01-01",,,)" + "\n" +           // line 7
      R"(,,,,"1970-01-02",,)" + "\n" +           // line 8
      R"(,,,,,"2004-02-29 01:02:03",)" + "\n" +  // line 9
      R"(,,,,,,"3600")" + "\n" +                 // line 10
      R"(,"NA",,,,,)" + "\n";                    // line 11

  // Batch when testing QuotingStyle::None. The values may not contain any quotes for this
  // style according to RFC4180.
  auto populated_batch_quoting_style_none = R"([{"a": 1, "c ": -1},
                             { "a": 1, "b\"": "abcefg", "c ": 2324},
                             { "b\"": "abcd", "c ": 5467},
                             { },
                             { "a": 546, "b\"": "", "c ": 517 },
                             { "a": 124, "b\"": "ab" },
                             { "d": 0 },
                             { "e": 86400000 },
                             { "f": 1078016523 },
                             { "g": 3600 }])";
  // Expected output for QuotingStyle::None.
  std::string expected_quoting_style_none = std::string("1,,-1,,,,") + "\n" +  // line 1
                                            R"(1,abcefg,2324,,,,)" + "\n" +    // line 2
                                            R"(,abcd,5467,,,,)" + "\n" +       // line 3
                                            R"(,,,,,,)" + "\n" +               // line 4
                                            R"(546,,517,,,,)" + "\n" +         // line 5
                                            R"(124,ab,,,,,)" + "\n" +          // line 6
                                            R"(,,,1970-01-01,,,)" + "\n" +     // line 7
                                            R"(,,,,1970-01-02,,)" + "\n" +     // line 8
                                            R"(,,,,,2004-02-29 01:02:03,)" +
                                            "\n" +                   // line 9
                                            R"(,,,,,,3600)" + "\n";  // line 10

  // Schema and data to test custom null value string.
  auto schema_custom_na = schema({field("g", uint64()), field("h", utf8())});
  auto populated_batch_custom_na = R"([{"g": 42, "h": "NA"},
                                                  { },
                                                  {"g": 1337, "h": "\"NA\""}])";
  std::string expected_custom_na = std::string(R"(42,"NA")") + "\n" +  // line 1
                                   R"(NA,NA)" + "\n" +                 // line 2
                                   R"(1337,"""NA""")" + "\n";          // line 3
  auto expected_status_invalid_null_string =
      Status::Invalid("Null string cannot contain quotes.");

  // Schema/expected message and test params generation for rejecting structural
  // characters when quoting style is None
  auto schema_custom_reject_structural = schema({field("a", utf8())});
  auto expected_status_no_quotes_with_structural = [](const char* value) {
    return Status::Invalid(
        "CSV values may not contain structural characters if quoting "
        "style is \"None\". See RFC4180. Invalid value: ",
        value);
  };

  auto reject_structural_params = [&](std::vector<const char*> rows,
                                      const char* error_val) -> WriterTestParams {
    std::string json_rows = "[";
    for (size_t i = 0; i < rows.size(); ++i) {
      if (rows[i]) {
        json_rows += std::string(R"({"a": ")") + rows[i] + R"("})";
      } else {
        json_rows += std::string(R"({"a": null})");
      }
      if (i != rows.size() - 1) json_rows += ',';
    }
    json_rows += ']';
    return {schema_custom_reject_structural, json_rows,
            DefaultTestOptions(/*include_header=*/false,
                               /*null_string=*/"", QuotingStyle::None),
            /*expected_output*/ "", expected_status_no_quotes_with_structural(error_val)};
  };

  // Schema/expected message for delimiter test
  auto schema_custom_delimiter = schema({field("a", int64()), field("b", int64())});
  auto batch_custom_delimiter = R"([{"a": 42, "b": -12}])";
  auto expected_output_delimiter_tabs = "42\t-12\n";
  auto expected_status_illegal_delimiter = [](const char value) {
    return Status::Invalid(
        "WriteOptions: delimiter cannot be \\r or \\n or \" or EOL. Invalid value: ",
        value);
  };

  return std::vector<WriterTestParams>{
      {abc_schema, "[]", DefaultTestOptions(), ""},
      {abc_schema, "[]", DefaultTestOptions(/*include_header=*/true), expected_header},
      {abc_schema, populated_batch, DefaultTestOptions(), expected_without_header},
      {abc_schema, populated_batch,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::Needed, "_EOL_"),
       expected_with_custom_eol},
      {abc_schema, populated_batch, DefaultTestOptions(/*include_header=*/true),
       expected_header + expected_without_header},
      {schema_custom_na, populated_batch_custom_na,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"NA"),
       expected_custom_na},
      {dummy_schema, dummy_batch_data,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/R"("NA")"),
       /*expected_output*/ "", expected_status_invalid_null_string},
      {dummy_schema, dummy_batch_data,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/R"(")",
                          QuotingStyle::Needed),
       /*expected_output*/ "", expected_status_invalid_null_string},
      {abc_schema, populated_batch,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::AllValid),
       expected_quoting_style_all_valid},
      {abc_schema, populated_batch_quoting_style_none,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::None),
       expected_quoting_style_none},
      {abc_schema, populated_batch,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::None),
       /*expected_output*/ "", expected_status_no_quotes_with_structural("abc\"efg")},
      reject_structural_params({"hi\\nbye"}, "hi\nbye"),
      reject_structural_params({",xyz"}, ",xyz"),
      reject_structural_params({"a\\\"sdf"}, "a\"sdf"),
      reject_structural_params({"foo\\r"}, "foo\r"),
      reject_structural_params({nullptr, "a", nullptr, "c,d", nullptr, ",e", "f"}, "c,d"),
      reject_structural_params({"a", "b", nullptr, "c,d", nullptr, nullptr}, "c,d"),
      // exercise simd code with strings total length >= 16
      {abc_schema, populated_batch,
       DefaultTestOptions(/*include_header=*/false, "", QuotingStyle::AllValid, "\n", ',',
                          /*batch_size=*/100),
       expected_quoting_style_all_valid},
      reject_structural_params({nullptr, "0123456789\\nabcdef"}, "0123456789\nabcdef"),
      reject_structural_params({"0123456\\r789", nullptr, "abcdef"}, "0123456\r789"),
      reject_structural_params({"0123456789", nullptr, "abcde,", nullptr}, "abcde,"),
      reject_structural_params({"0123456789", nullptr, "abcdef,", nullptr}, "abcdef,"),
      reject_structural_params({nullptr, nullptr, ",0123456789", "abcde"}, ",0123456789"),
      reject_structural_params({"0123456", nullptr, "7\\\"89", ",abcdef"}, "7\"89"),
      // exercise custom delimiter
      {schema_custom_delimiter, batch_custom_delimiter,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::Needed, "\n", /*delimiter=*/'\t'),
       expected_output_delimiter_tabs},
      {schema_custom_delimiter, batch_custom_delimiter,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::Needed, /*eol=*/"\n", /*delimiter=*/'\r'),
       /*expected_output*/ "", expected_status_illegal_delimiter('\r')},
      {schema_custom_delimiter, batch_custom_delimiter,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::Needed, /*eol=*/"\n", /*delimiter=*/'\n'),
       /*expected_output*/ "", expected_status_illegal_delimiter('\n')},
      {schema_custom_delimiter, batch_custom_delimiter,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::Needed, /*eol=*/"\n", /*delimiter=*/'"'),
       /*expected_output*/ "", expected_status_illegal_delimiter('"')},
      {schema_custom_delimiter, batch_custom_delimiter,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::Needed, /*eol=*/";", /*delimiter=*/';'),
       /*expected_output*/ "", expected_status_illegal_delimiter(';')}};
}

class TestWriteCSV : public ::testing::TestWithParam<WriterTestParams> {
 protected:
  template <typename Data>
  Result<std::string> ToCsvString(const Data& data, const WriteOptions& options) {
    std::shared_ptr<io::BufferOutputStream> out;
    ASSIGN_OR_RAISE(out, io::BufferOutputStream::Create());

    RETURN_NOT_OK(WriteCSV(data, options, out.get()));
    ASSIGN_OR_RAISE(std::shared_ptr<Buffer> buffer, out->Finish());
    return std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size());
  }

  Result<std::string> ToCsvStringUsingWriter(const Table& data,
                                             const WriteOptions& options) {
    std::shared_ptr<io::BufferOutputStream> out;
    ASSIGN_OR_RAISE(out, io::BufferOutputStream::Create());
    // Write row-by-row
    ASSIGN_OR_RAISE(auto writer, MakeCSVWriter(out, data.schema(), options));
    TableBatchReader reader(data);
    reader.set_chunksize(1);
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(reader.ReadNext(&batch));
    while (batch != nullptr) {
      RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
      RETURN_NOT_OK(reader.ReadNext(&batch));
    }
    RETURN_NOT_OK(writer->Close());
    EXPECT_EQ(data.num_rows(), writer->stats().num_record_batches);
    ASSIGN_OR_RAISE(std::shared_ptr<Buffer> buffer, out->Finish());
    return std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size());
  }
};

TEST_P(TestWriteCSV, TestWrite) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<io::BufferOutputStream> out,
                       io::BufferOutputStream::Create());
  WriteOptions options = GetParam().options;
  std::string csv;
  auto record_batch = RecordBatchFromJSON(GetParam().schema, GetParam().batch_data);
  if (!GetParam().expected_status.ok()) {
    // If an error status is expected, check if the expected status code and message
    // matches.
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr(GetParam().expected_status.message()),
        ToCsvString(*record_batch, options));
  } else {
    ASSERT_OK_AND_ASSIGN(csv, ToCsvString(*record_batch, options));
    EXPECT_EQ(csv, GetParam().expected_output);

    // Batch size shouldn't matter.
    options.batch_size /= 2;
    ASSERT_OK_AND_ASSIGN(csv, ToCsvString(*record_batch, options));
    EXPECT_EQ(csv, GetParam().expected_output);

    // Table and Record batch should work identically.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         Table::FromRecordBatches({record_batch}));
    ASSERT_OK_AND_ASSIGN(csv, ToCsvString(*table, options));
    EXPECT_EQ(csv, GetParam().expected_output);

    // RecordBatchReader should work identically.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RecordBatchReader> reader,
                         RecordBatchReader::Make({record_batch}));
    ASSERT_OK_AND_ASSIGN(csv, ToCsvString(reader, options));
    EXPECT_EQ(csv, GetParam().expected_output);

    // The writer should work identically.
    ASSERT_OK_AND_ASSIGN(csv, ToCsvStringUsingWriter(*table, options));
    EXPECT_EQ(csv, GetParam().expected_output);
  }
}

INSTANTIATE_TEST_SUITE_P(MultiColumnWriteCSVTest, TestWriteCSV,
                         ::testing::ValuesIn(GenerateTestCases()));

INSTANTIATE_TEST_SUITE_P(SingleColumnWriteCSVTest, TestWriteCSV,
                         ::testing::Values(WriterTestParams(
                             schema({field("int64", int64())}),
                             R"([{ "int64": 9999}, {}, { "int64": -15}])", WriteOptions(),
                             R"("int64")"
                             "\n9999\n\n-15\n",
                             Status::OK())));

#ifndef _WIN32
// TODO(ARROW-13168):
INSTANTIATE_TEST_SUITE_P(
    TimestampWithTimezoneWriteCSVTest, TestWriteCSV,
    ::testing::Values(
        WriterTestParams(schema({
                             field("tz", timestamp(TimeUnit::SECOND, "America/Phoenix")),
                             field("utc", timestamp(TimeUnit::SECOND, "UTC")),
                         }),
                         R"([{ "tz": 1456767743, "utc": 1456767743 }])", WriteOptions(),
                         R"("tz","utc")"
                         "\n2016-02-29 10:42:23-0700,2016-02-29 17:42:23Z\n")));
#endif

}  // namespace csv
}  // namespace arrow
