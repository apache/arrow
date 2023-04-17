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

#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"

#include "parquet/api/reader.h"
#include "parquet/api/writer.h"

#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_writer.h"
#include "parquet/test_util.h"

using arrow::ArrayFromJSON;
using arrow::Buffer;
using arrow::default_memory_pool;
using arrow::ResizableBuffer;
using arrow::Table;

using arrow::io::BufferReader;

namespace parquet {
namespace arrow {

struct StatisticsTestParam {
  std::shared_ptr<::arrow::Table> table;
  int expected_null_count;
  // This is the non-null count and not the num_values in the page headers.
  int expected_value_count;
  std::string expected_min;
  std::string expected_max;
};

// Define a custom print since the default Googletest print trips Valgrind
void PrintTo(const StatisticsTestParam& param, std::ostream* os) {
  (*os) << "StatisticsTestParam{"
        << "table.schema=" << param.table->schema()->ToString()
        << ", expected_null_count=" << param.expected_null_count
        << ", expected_value_count=" << param.expected_value_count
        << ", expected_min=" << param.expected_min
        << ", expected_max=" << param.expected_max << "}";
}

class ParameterizedStatisticsTest : public ::testing::TestWithParam<StatisticsTestParam> {
};

std::string GetManyEmptyLists() {
  std::string many_empty_lists = "[";
  for (int i = 0; i < 2000; ++i) {
    many_empty_lists += "[],";
  }
  many_empty_lists += "[1,2,3,4,5,6,7,8,null]]";
  return many_empty_lists;
}

// PARQUET-2067: Tests that nulls from parent fields are included in null statistics.
TEST_P(ParameterizedStatisticsTest, NoNullCountWrittenForRepeatedFields) {
  std::shared_ptr<::arrow::ResizableBuffer> serialized_data = AllocateBuffer();
  auto out_stream = std::make_shared<::arrow::io::BufferOutputStream>(serialized_data);
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<FileWriter> writer,
      FileWriter::Open(*GetParam().table->schema(), default_memory_pool(), out_stream,
                       default_writer_properties(), default_arrow_writer_properties()));
  ASSERT_OK(writer->WriteTable(*GetParam().table, std::numeric_limits<int64_t>::max()));
  ASSERT_OK(writer->Close());
  ASSERT_OK(out_stream->Close());

  auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(serialized_data);
  auto parquet_reader = ParquetFileReader::Open(std::move(buffer_reader));
  std::shared_ptr<FileMetaData> metadata = parquet_reader->metadata();
  std::shared_ptr<Statistics> stats = metadata->RowGroup(0)->ColumnChunk(0)->statistics();
  EXPECT_EQ(stats->null_count(), GetParam().expected_null_count);
  EXPECT_EQ(stats->num_values(), GetParam().expected_value_count);
  ASSERT_TRUE(stats->HasMinMax());
  EXPECT_EQ(stats->EncodeMin(), GetParam().expected_min);
  EXPECT_EQ(stats->EncodeMax(), GetParam().expected_max);
}

INSTANTIATE_TEST_SUITE_P(
    StatsTests, ParameterizedStatisticsTest,
    ::testing::Values(
        StatisticsTestParam{
            /*table=*/Table::Make(::arrow::schema({::arrow::field("a", ::arrow::utf8())}),
                                  {ArrayFromJSON(::arrow::utf8(),
                                                 R"(["1", null, "3"])")}),
            /*expected_null_count=*/1, /* empty list counts as null as well */
            /*expected_value_count=*/2,
            /*expected_min=*/"1",
            /*expected_max=*/"3"},
        StatisticsTestParam{
            /*table=*/Table::Make(
                ::arrow::schema({::arrow::field("a", list(::arrow::utf8()))}),
                {ArrayFromJSON(list(::arrow::utf8()),
                               R"([["1"], [], null, ["1", null, "3"]])")}),
            /*expected_null_count=*/3, /* empty list counts as null as well */
            /*expected_value_count=*/3,
            /*expected_min=*/"1",
            /*expected_max=*/"3"},
        StatisticsTestParam{
            /*table=*/Table::Make(
                ::arrow::schema({::arrow::field("a", ::arrow::int64())}),
                {ArrayFromJSON(::arrow::int64(), R"([1, null, 3, null])")}),
            /*expected_null_count=*/2, /* empty list counts as null as well */
            /*expected_value_count=*/2,
            /*expected_min=*/std::string("\x1\0\0\0\0\0\0\0", 8),
            /*expected_max=*/std::string("\x3\0\0\0\0\0\0\0", 8)},
        StatisticsTestParam{
            /*table=*/Table::Make(
                ::arrow::schema({::arrow::field("a", list(::arrow::utf8()))}),
                {ArrayFromJSON(list(::arrow::utf8()), R"([["1"], [], ["1", "3"]])")}),
            /*expected_null_count=*/1, /* empty list counts as null as well */
            /*expected_value_count=*/3,
            /*expected_min=*/"1",
            /*expected_max=*/"3"},
        StatisticsTestParam{
            /*table=*/Table::Make(
                ::arrow::schema({::arrow::field("a", list(::arrow::int64()))}),
                {ArrayFromJSON(list(::arrow::int64()),
                               R"([[1], [], null, [1, null, 3]])")}),
            /*expected_null_count=*/3, /* empty list counts as null as well */
            /*expected_value_count=*/3,
            /*expected_min=*/std::string("\x1\0\0\0\0\0\0\0", 8),
            /*expected_max=*/std::string("\x3\0\0\0\0\0\0\0", 8)},
        StatisticsTestParam{
            /*table=*/Table::Make(
                ::arrow::schema({::arrow::field("a", list(::arrow::int64()), false)}),
                {ArrayFromJSON(list(::arrow::int64()), GetManyEmptyLists())}),
            /*expected_null_count=*/2001, /* empty list counts as null as well */
            /*expected_value_count=*/8,
            /*expected_min=*/std::string("\x1\0\0\0\0\0\0\0", 8),
            /*expected_max=*/std::string("\x8\0\0\0\0\0\0\0", 8)},
        StatisticsTestParam{
            /*table=*/Table::Make(
                ::arrow::schema({::arrow::field("a", list(dictionary(::arrow::int32(),
                                                                     ::arrow::utf8())))}),
                {ArrayFromJSON(list(dictionary(::arrow::int32(), ::arrow::utf8())),
                               R"([null, ["z", null, "z"], null, null, null])")}),
            /*expected_null_count=*/5,
            /*expected_value_count=*/2,
            /*expected_min=*/"z",
            /*expected_max=*/"z"}));

}  // namespace arrow
}  // namespace parquet
