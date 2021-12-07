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
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/csv/writer.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/result_internal.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/optional.h"

namespace arrow {
namespace csv {

struct WriterTestParams {
  WriterTestParams(std::shared_ptr<Schema> schema, std::string batch_data,
                   WriteOptions options, util::optional<std::string> expected_output,
                   Status expected_status = Status::OK())
      : schema(std::move(schema)),
        batch_data(std::move(batch_data)),
        options(std::move(options)),
        expected_output(std::move(expected_output)),
        expected_status(std::move(expected_status)) {}
  std::shared_ptr<Schema> schema;
  std::string batch_data;
  WriteOptions options;
  util::optional<std::string> expected_output;
  Status expected_status;
};

// Avoid Valgrind failures with GTest trying to represent a WriterTestParams
void PrintTo(const WriterTestParams& p, std::ostream* os) {
  *os << "WriterTestParams(" << reinterpret_cast<const void*>(&p) << ")";
}

WriteOptions DefaultTestOptions(bool include_header = false,
                                const std::string& null_string = "",
                                QuotingStyle quoting_style = QuotingStyle::Needed) {
  WriteOptions options;
  options.batch_size = 5;
  options.include_header = include_header;
  options.null_string = null_string;
  options.quoting_style = quoting_style;
  return options;
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
                             { "b\"": "NA" }])";
  std::string expected_header = std::string(R"("a","b""","c ","d","e","f")") + "\n";
  // Expected output without header when using default QuotingStyle::Needed.
  std::string expected_without_header = std::string("1,,-1,,,") + "\n" +        // line 1
                                        R"(1,"abc""efg",2324,,,)" + "\n" +      // line 2
                                        R"(,"abcd",5467,,,)" + "\n" +           // line 3
                                        R"(,,,,,)" + "\n" +                     // line 4
                                        R"(546,"",517,,,)" + "\n" +             // line 5
                                        R"(124,"a""""b""",,,,)" + "\n" +        // line 6
                                        R"(,,,1970-01-01,,)" + "\n" +           // line 7
                                        R"(,,,,1970-01-02,)" + "\n" +           // line 8
                                        R"(,,,,,2004-02-29 01:02:03)" + "\n" +  // line 9
                                        R"(,"NA",,,,)" + "\n";                  // line 10
  // Expected output without header when using QuotingStyle::AllValid.
  std::string expected_quoting_style_all_valid =
      std::string(R"("1",,"-1",,,)") + "\n" +   // line 1
      R"("1","abc""efg","2324",,,)" + "\n" +    // line 2
      R"(,"abcd","5467",,,)" + "\n" +           // line 3
      R"(,,,,,)" + "\n" +                       // line 4
      R"("546","","517",,,)" + "\n" +           // line 5
      R"("124","a""""b""",,,,)" + "\n" +        // line 6
      R"(,,,"1970-01-01",,)" + "\n" +           // line 7
      R"(,,,,"1970-01-02",)" + "\n" +           // line 8
      R"(,,,,,"2004-02-29 01:02:03")" + "\n" +  // line 9
      R"(,"NA",,,,)" + "\n";                    // line 10

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
                             { "f": 1078016523 }])";
  // Expected output for QuotingStyle::None.
  std::string expected_quoting_style_none = std::string("1,,-1,,,") + "\n" +  // line 1
                                            R"(1,abcefg,2324,,,)" + "\n" +    // line 2
                                            R"(,abcd,5467,,,)" + "\n" +       // line 3
                                            R"(,,,,,)" + "\n" +               // line 4
                                            R"(546,,517,,,)" + "\n" +         // line 5
                                            R"(124,ab,,,,)" + "\n" +          // line 6
                                            R"(,,,1970-01-01,,)" + "\n" +     // line 7
                                            R"(,,,,1970-01-02,)" + "\n" +     // line 8
                                            R"(,,,,,2004-02-29 01:02:03)" +
                                            "\n";  // line 9

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
  auto reject_structural_params = [&](const char* json_val,
                                      const char* error_val) -> WriterTestParams {
    return {schema_custom_reject_structural,
            std::string(R"([{"a": ")") + json_val + R"("}])",
            DefaultTestOptions(/*include_header=*/false,
                               /*null_string=*/"", QuotingStyle::None),
            /*expected_output*/ "", expected_status_no_quotes_with_structural(error_val)};
  };

  return std::vector<WriterTestParams>{
      {abc_schema, "[]", DefaultTestOptions(), ""},
      {abc_schema, "[]", DefaultTestOptions(/*include_header=*/true), expected_header},
      {abc_schema, populated_batch, DefaultTestOptions(), expected_without_header},
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
      reject_structural_params("hi\\nbye", "hi\nbye"),
      reject_structural_params(",xyz", ",xyz"),
      reject_structural_params("a\\\"sdf", "a\"sdf"),
      reject_structural_params("foo\\r", "foo\r")};
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
  if (GetParam().expected_status != Status::OK()) {
    // If an error status is expected, check if the expected status matches.
    EXPECT_EQ(ToCsvString(*record_batch, options), GetParam().expected_status);
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
