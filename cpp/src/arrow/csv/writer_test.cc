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
  std::shared_ptr<Schema> schema;
  std::string batch_data;
  WriteOptions options;
  // When set, expect the test to fail just from the supplied WriteOptions alone.
  bool expect_invalid_options;
  std::string expected_output;
  // When set, expect the test to fail with this string.
  util::optional<std::string> expect_invalid_string;
};

// Avoid Valgrind failures with GTest trying to represent a WriterTestParams
void PrintTo(const WriterTestParams& p, std::ostream* os) {
  *os << "WriterTestParams(" << reinterpret_cast<const void*>(&p) << ")";
}

WriteOptions DefaultTestOptions(bool include_header, const std::string& null_string,
                                QuotingStyle quote_style) {
  WriteOptions options;
  options.batch_size = 5;
  options.include_header = include_header;
  options.null_string = null_string;
  options.quoting_style = quote_style;
  return options;
}

std::vector<WriterTestParams> GenerateTestCases() {
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
  // Expected output without header when using default QuoteStyle::Needed.
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
  // Expected output without header when using QuoteStyle::AllValid.
  std::string expected_quote_style_all_valid =
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

  // Batch when testing QuoteStyle::None. The values may not contain any quotes for this
  // style according to RFC4180.
  auto populated_batch_quote_style_none = R"([{"a": 1, "c ": -1},
                             { "a": 1, "b\"": "abcefg", "c ": 2324},
                             { "b\"": "abcd", "c ": 5467},
                             { },
                             { "a": 546, "b\"": "", "c ": 517 },
                             { "a": 124, "b\"": "ab" },
                             { "d": 0 },
                             { "e": 86400000 },
                             { "f": 1078016523 }])";
  // Expected output for QuoteStyle::None
  std::string expected_quote_style_none = std::string("1,,-1,,,") + "\n" +       // line 1
                                          R"(1,abcefg,2324,,,)" + "\n" +         // line 2
                                          R"(,abcd,5467,,,)" + "\n" +            // line 3
                                          R"(,,,,,)" + "\n" +                    // line 4
                                          R"(546,,517,,,)" + "\n" +              // line 5
                                          R"(124,ab,,,,)" + "\n" +               // line 6
                                          R"(,,,1970-01-01,,)" + "\n" +          // line 7
                                          R"(,,,,1970-01-02,)" + "\n" +          // line 8
                                          R"(,,,,,2004-02-29 01:02:03)" + "\n";  // line 9

  // Schema and data to test custom null value string.
  auto schema_custom_na = schema({field("g", uint64()), field("h", utf8())});
  auto populated_batch_custom_na = R"([{"g": 42, "h": "NA"},
                                                  { },
                                                  {"g": 1337, "h": "\"NA\""}])";
  std::string expected_custom_na = std::string(R"(42,"NA")") + "\n" +  // line 1
                                   R"(NA,NA)" + "\n" +                 // line 2
                                   R"(1337,"""NA""")" + "\n";          // line 3

  // Dummy schema and data for testing invalid options.
  auto dummy_schema = schema({field("a", uint8())});
  std::string dummy_batch_data = R"([{"a": null}])";

  return std::vector<WriterTestParams>{
      {abc_schema, "[]",
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::Needed),
       /*expect_invalid_options*/ false, "", util::nullopt},
      {abc_schema, "[]",
       DefaultTestOptions(/*include_header=*/true, /*null_string=*/"",
                          QuotingStyle::Needed),
       /*expect_invalid_options*/ false, expected_header, util::nullopt},
      {abc_schema, populated_batch,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::Needed),
       /*expect_invalid_options*/ false, expected_without_header, util::nullopt},
      {abc_schema, populated_batch,
       DefaultTestOptions(/*include_header=*/true, /*null_string=*/"",
                          QuotingStyle::Needed),
       /*expect_invalid_options*/ false, expected_header + expected_without_header,
       util::nullopt},
      {schema_custom_na, populated_batch_custom_na,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"NA",
                          QuotingStyle::Needed),
       /*expect_invalid_options*/ false, expected_custom_na, util::nullopt},
      {dummy_schema, dummy_batch_data,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/R"("NA")",
                          QuotingStyle::Needed),
       /*expect_invalid_options*/ true, /*expected_output*/ "", util::nullopt},
      {dummy_schema, dummy_batch_data,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/R"(")",
                          QuotingStyle::Needed),
       /*expect_invalid_options*/ true, /*expected_output*/ "", util::nullopt},
      {abc_schema, populated_batch,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::AllValid),
       /*expect_invalid_options*/ false, expected_quote_style_all_valid, util::nullopt},
      {abc_schema, populated_batch_quote_style_none,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::None),
       /*expect_invalid_options*/ false, expected_quote_style_none, util::nullopt},
      {abc_schema, populated_batch,
       DefaultTestOptions(/*include_header=*/false, /*null_string=*/"",
                          QuotingStyle::None),
       /*expect_invalid_options*/ false,
       /*expected_output*/ "",
       /*expect_invalid_string*/
       "Invalid: CSV values may not contain quotes if quote style is \"None\". See "
       "RFC4180. Invalid value: abc\"efg"}};
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
  if (GetParam().expect_invalid_options) {
    // If invalid options are expected, they should be rejected.
    ASSERT_RAISES(Invalid, ToCsvString(*record_batch, options));
  } else if (GetParam().expect_invalid_string) {
    // If the invalid string options is set, expect the function to fail with invalid
    // status and check if the expected message matches.
    ASSERT_RAISES_WITH_MESSAGE(Invalid, *GetParam().expect_invalid_string,
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

    // The writer should work identically.
    ASSERT_OK_AND_ASSIGN(csv, ToCsvStringUsingWriter(*table, options));
    EXPECT_EQ(csv, GetParam().expected_output);
  }
}

INSTANTIATE_TEST_SUITE_P(MultiColumnWriteCSVTest, TestWriteCSV,
                         ::testing::ValuesIn(GenerateTestCases()));

INSTANTIATE_TEST_SUITE_P(SingleColumnWriteCSVTest, TestWriteCSV,
                         ::testing::Values(WriterTestParams{
                             schema({field("int64", int64())}),
                             R"([{ "int64": 9999}, {}, { "int64": -15}])", WriteOptions(),
                             false,
                             R"("int64")"
                             "\n9999\n\n-15\n",
                             util::nullopt}));

#ifndef _WIN32
// TODO(ARROW-13168):
INSTANTIATE_TEST_SUITE_P(
    TimestampWithTimezoneWriteCSVTest, TestWriteCSV,
    ::testing::Values(WriterTestParams{
        schema({
            field("tz", timestamp(TimeUnit::SECOND, "America/Phoenix")),
            field("utc", timestamp(TimeUnit::SECOND, "UTC")),
        }),
        R"([{ "tz": 1456767743, "utc": 1456767743 }])", WriteOptions(), false,
        R"("tz","utc")"
        "\n2016-02-29 10:42:23-0700,2016-02-29 17:42:23Z\n",
        util::nullopt}));
#endif

}  // namespace csv
}  // namespace arrow
