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

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"

#include "parquet/api/reader.h"
#include "parquet/api/writer.h"

#include "parquet/arrow/reader.h"
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

namespace parquet::arrow {

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

TEST(StatisticsTest, TruncateOnlyHalfMinMax) {
  // GH-43382: Tests when we only have min or max, the `HasMinMax` should be false.
  std::shared_ptr<::arrow::ResizableBuffer> serialized_data = AllocateBuffer();
  auto out_stream = std::make_shared<::arrow::io::BufferOutputStream>(serialized_data);
  auto schema = ::arrow::schema({::arrow::field("a", ::arrow::utf8())});
  ::parquet::WriterProperties::Builder properties_builder;
  properties_builder.max_statistics_size(2);
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<FileWriter> writer,
      FileWriter::Open(*schema, default_memory_pool(), out_stream,
                       properties_builder.build(), default_arrow_writer_properties()));
  auto table = Table::Make(schema, {ArrayFromJSON(::arrow::utf8(), R"(["a", "abc"])")});
  ASSERT_OK(writer->WriteTable(*table, std::numeric_limits<int64_t>::max()));
  ASSERT_OK(writer->Close());
  ASSERT_OK(out_stream->Close());

  auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(serialized_data);
  auto parquet_reader = ParquetFileReader::Open(std::move(buffer_reader));
  std::shared_ptr<FileMetaData> metadata = parquet_reader->metadata();
  std::shared_ptr<Statistics> stats = metadata->RowGroup(0)->ColumnChunk(0)->statistics();
  ASSERT_FALSE(stats->HasMinMax());
}

namespace {
::arrow::Result<std::shared_ptr<::arrow::Array>> StatisticsReadArray(
    std::shared_ptr<::arrow::DataType> data_type, std::shared_ptr<::arrow::Array> array,
    std::shared_ptr<WriterProperties> writer_properties = default_writer_properties(),
    const ArrowReaderProperties& reader_properties = default_arrow_reader_properties()) {
  auto schema = ::arrow::schema({::arrow::field("column", data_type)});
  auto record_batch = ::arrow::RecordBatch::Make(schema, array->length(), {array});
  ARROW_ASSIGN_OR_RAISE(auto sink, ::arrow::io::BufferOutputStream::Create());
  const auto arrow_writer_properties =
      parquet::ArrowWriterProperties::Builder().store_schema()->build();
  ARROW_ASSIGN_OR_RAISE(auto writer,
                        FileWriter::Open(*schema, ::arrow::default_memory_pool(), sink,
                                         writer_properties, arrow_writer_properties));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*record_batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  ARROW_ASSIGN_OR_RAISE(auto buffer, sink->Finish());

  auto reader =
      ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
  std::unique_ptr<FileReader> file_reader;
  ARROW_RETURN_NOT_OK(FileReader::Make(::arrow::default_memory_pool(), std::move(reader),
                                       reader_properties, &file_reader));
  std::shared_ptr<::arrow::ChunkedArray> chunked_array;
  ARROW_RETURN_NOT_OK(file_reader->ReadColumn(0, &chunked_array));
  return chunked_array->chunk(0);
}

template <typename ArrowType, typename MinMaxType>
void TestStatisticsReadArray(std::shared_ptr<::arrow::DataType> arrow_type) {
  using ArrowArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;
  using ArrowArrayBuilder = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  using ArrowCType = typename ArrowType::c_type;
  constexpr auto min = std::numeric_limits<ArrowCType>::lowest();
  constexpr auto max = std::numeric_limits<ArrowCType>::max();

  std::unique_ptr<ArrowArrayBuilder> builder;
  if constexpr (::arrow::TypeTraits<ArrowType>::is_parameter_free) {
    builder = std::make_unique<ArrowArrayBuilder>(::arrow::default_memory_pool());
  } else {
    builder =
        std::make_unique<ArrowArrayBuilder>(arrow_type, ::arrow::default_memory_pool());
  }
  ASSERT_OK(builder->Append(max));
  ASSERT_OK(builder->AppendNull());
  ASSERT_OK(builder->Append(min));
  ASSERT_OK(builder->Append(max));
  ASSERT_OK_AND_ASSIGN(auto built_array, builder->Finish());
  ASSERT_OK_AND_ASSIGN(auto read_array,
                       StatisticsReadArray(arrow_type, std::move(built_array)));
  auto typed_read_array = std::static_pointer_cast<ArrowArrayType>(read_array);
  auto statistics = typed_read_array->statistics();
  ASSERT_NE(nullptr, statistics);
  ASSERT_EQ(true, statistics->null_count.has_value());
  ASSERT_EQ(1, statistics->null_count.value());
  ASSERT_EQ(false, statistics->distinct_count.has_value());
  ASSERT_EQ(true, statistics->min.has_value());
  ASSERT_EQ(true, std::holds_alternative<MinMaxType>(*statistics->min));
  ASSERT_EQ(min, std::get<MinMaxType>(*statistics->min));
  ASSERT_EQ(true, statistics->is_min_exact);
  ASSERT_EQ(true, statistics->max.has_value());
  ASSERT_EQ(true, std::holds_alternative<MinMaxType>(*statistics->max));
  ASSERT_EQ(max, std::get<MinMaxType>(*statistics->max));
  ASSERT_EQ(true, statistics->is_min_exact);
}
}  // namespace

TEST(TestStatisticsRead, Boolean) {
  TestStatisticsReadArray<::arrow::BooleanType, bool>(::arrow::boolean());
}

TEST(TestStatisticsRead, Int8) {
  TestStatisticsReadArray<::arrow::Int8Type, int64_t>(::arrow::int8());
}

TEST(TestStatisticsRead, UInt8) {
  TestStatisticsReadArray<::arrow::UInt8Type, uint64_t>(::arrow::uint8());
}

TEST(TestStatisticsRead, Int16) {
  TestStatisticsReadArray<::arrow::Int16Type, int64_t>(::arrow::int16());
}

TEST(TestStatisticsRead, UInt16) {
  TestStatisticsReadArray<::arrow::UInt16Type, uint64_t>(::arrow::uint16());
}

TEST(TestStatisticsRead, Int32) {
  TestStatisticsReadArray<::arrow::Int32Type, int64_t>(::arrow::int32());
}

TEST(TestStatisticsRead, UInt32) {
  TestStatisticsReadArray<::arrow::UInt32Type, uint64_t>(::arrow::uint32());
}

TEST(TestStatisticsRead, Int64) {
  TestStatisticsReadArray<::arrow::Int64Type, int64_t>(::arrow::int64());
}

TEST(TestStatisticsRead, UInt64) {
  TestStatisticsReadArray<::arrow::UInt64Type, uint64_t>(::arrow::uint64());
}

TEST(TestStatisticsRead, Float) {
  TestStatisticsReadArray<::arrow::FloatType, double>(::arrow::float32());
}

TEST(TestStatisticsRead, Double) {
  TestStatisticsReadArray<::arrow::DoubleType, double>(::arrow::float64());
}

TEST(TestStatisticsRead, Date32) {
  TestStatisticsReadArray<::arrow::Date32Type, int64_t>(::arrow::date32());
}

TEST(TestStatisticsRead, Time32) {
  TestStatisticsReadArray<::arrow::Time32Type, int64_t>(
      ::arrow::time32(::arrow::TimeUnit::MILLI));
}

TEST(TestStatisticsRead, Time64) {
  TestStatisticsReadArray<::arrow::Time64Type, int64_t>(
      ::arrow::time64(::arrow::TimeUnit::MICRO));
}

TEST(TestStatisticsRead, TimestampMilli) {
  TestStatisticsReadArray<::arrow::TimestampType, int64_t>(
      ::arrow::timestamp(::arrow::TimeUnit::MILLI));
}

TEST(TestStatisticsRead, TimestampMicro) {
  TestStatisticsReadArray<::arrow::TimestampType, int64_t>(
      ::arrow::timestamp(::arrow::TimeUnit::MICRO));
}

TEST(TestStatisticsRead, TimestampNano) {
  TestStatisticsReadArray<::arrow::TimestampType, int64_t>(
      ::arrow::timestamp(::arrow::TimeUnit::NANO));
}

TEST(TestStatisticsRead, Duration) {
  TestStatisticsReadArray<::arrow::DurationType, int64_t>(
      ::arrow::duration(::arrow::TimeUnit::NANO));
}

TEST(TestStatisticsRead, MultipleRowGroupsDefault) {
  auto arrow_type = ::arrow::int32();
  auto built_array = ArrayFromJSON(arrow_type, R"([1, null, -1])");
  auto writer_properties = WriterProperties::Builder().max_row_group_length(2)->build();
  ASSERT_OK_AND_ASSIGN(
      auto read_array,
      StatisticsReadArray(arrow_type, std::move(built_array), writer_properties));
  auto typed_read_array = std::static_pointer_cast<::arrow::Int32Array>(read_array);
  auto statistics = typed_read_array->statistics();
  ASSERT_EQ(nullptr, statistics);
}

TEST(TestStatisticsRead, MultipleRowGroupsShouldLoadStatistics) {
  auto arrow_type = ::arrow::int32();
  auto built_array = ArrayFromJSON(arrow_type, R"([1, null, -1])");
  auto writer_properties = WriterProperties::Builder().max_row_group_length(2)->build();
  ArrowReaderProperties reader_properties;
  reader_properties.set_should_load_statistics(true);
  ASSERT_OK_AND_ASSIGN(auto read_array,
                       StatisticsReadArray(arrow_type, std::move(built_array),
                                           writer_properties, reader_properties));
  // If we use should_load_statistics, reader doesn't load multiple
  // row groups at once. So the first array in the read chunked array
  // has only 2 elements.
  ASSERT_EQ(2, read_array->length());
  auto typed_read_array = std::static_pointer_cast<::arrow::Int32Array>(read_array);
  auto statistics = typed_read_array->statistics();
  ASSERT_NE(nullptr, statistics);
  ASSERT_EQ(true, statistics->null_count.has_value());
  ASSERT_EQ(1, statistics->null_count.value());
  ASSERT_EQ(false, statistics->distinct_count.has_value());
  ASSERT_EQ(true, statistics->min.has_value());
  // This is not -1 because this array has only the first 2 elements.
  ASSERT_EQ(1, std::get<int64_t>(*statistics->min));
  ASSERT_EQ(true, statistics->is_min_exact);
  ASSERT_EQ(true, statistics->max.has_value());
  ASSERT_EQ(1, std::get<int64_t>(*statistics->max));
  ASSERT_EQ(true, statistics->is_max_exact);
}

}  // namespace parquet::arrow
