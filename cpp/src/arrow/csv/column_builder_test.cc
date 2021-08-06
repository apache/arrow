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

#include "arrow/csv/column_builder.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "arrow/csv/options.h"
#include "arrow/csv/test_common.h"
#include "arrow/memory_pool.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace csv {

class BlockParser;

using internal::checked_cast;
using internal::GetCpuThreadPool;
using internal::TaskGroup;

using ChunkData = std::vector<std::vector<std::string>>;

class ColumnBuilderTest : public ::testing::Test {
 public:
  void AssertBuilding(const std::shared_ptr<ColumnBuilder>& builder,
                      const ChunkData& chunks, bool validate_full,
                      std::shared_ptr<ChunkedArray>* out) {
    for (const auto& chunk : chunks) {
      std::shared_ptr<BlockParser> parser;
      MakeColumnParser(chunk, &parser);
      builder->Append(parser);
    }
    ASSERT_OK(builder->task_group()->Finish());
    ASSERT_OK_AND_ASSIGN(*out, builder->Finish());
    if (validate_full) {
      ASSERT_OK((*out)->ValidateFull());
    } else {
      ASSERT_OK((*out)->Validate());
    }
  }

  void AssertBuilding(const std::shared_ptr<ColumnBuilder>& builder,
                      const ChunkData& chunks, std::shared_ptr<ChunkedArray>* out) {
    AssertBuilding(builder, chunks, /*validate_full=*/true, out);
  }

  void CheckInferred(const std::shared_ptr<TaskGroup>& tg, const ChunkData& csv_data,
                     const ConvertOptions& options,
                     std::shared_ptr<ChunkedArray> expected, bool validate_full = true) {
    std::shared_ptr<ColumnBuilder> builder;
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK_AND_ASSIGN(builder,
                         ColumnBuilder::Make(default_memory_pool(), 0, options, tg));
    AssertBuilding(builder, csv_data, validate_full, &actual);
    AssertChunkedEqual(*actual, *expected);
  }

  void CheckInferred(const std::shared_ptr<TaskGroup>& tg, const ChunkData& csv_data,
                     const ConvertOptions& options,
                     std::vector<std::shared_ptr<Array>> expected_chunks,
                     bool validate_full = true) {
    CheckInferred(tg, csv_data, options, std::make_shared<ChunkedArray>(expected_chunks),
                  validate_full);
  }

  void CheckFixedType(const std::shared_ptr<TaskGroup>& tg,
                      const std::shared_ptr<DataType>& type, const ChunkData& csv_data,
                      const ConvertOptions& options,
                      std::shared_ptr<ChunkedArray> expected) {
    std::shared_ptr<ColumnBuilder> builder;
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK_AND_ASSIGN(
        builder, ColumnBuilder::Make(default_memory_pool(), type, 0, options, tg));
    AssertBuilding(builder, csv_data, &actual);
    AssertChunkedEqual(*actual, *expected);
  }

  void CheckFixedType(const std::shared_ptr<TaskGroup>& tg,
                      const std::shared_ptr<DataType>& type, const ChunkData& csv_data,
                      const ConvertOptions& options,
                      std::vector<std::shared_ptr<Array>> expected_chunks) {
    CheckFixedType(tg, type, csv_data, options,
                   std::make_shared<ChunkedArray>(expected_chunks));
  }

 protected:
  ConvertOptions default_options = ConvertOptions::Defaults();
};

//////////////////////////////////////////////////////////////////////////
// Tests for null column builder

class NullColumnBuilderTest : public ColumnBuilderTest {};

TEST_F(NullColumnBuilderTest, Empty) {
  std::shared_ptr<DataType> type = null();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK_AND_ASSIGN(builder, ColumnBuilder::MakeNull(default_memory_pool(), type, tg));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {}, &actual);

  ChunkedArray expected({}, type);
  AssertChunkedEqual(*actual, expected);
}

TEST_F(NullColumnBuilderTest, InsertNull) {
  // Building a column of nulls with type null()
  std::shared_ptr<DataType> type = null();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK_AND_ASSIGN(builder, ColumnBuilder::MakeNull(default_memory_pool(), type, tg));

  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<ChunkedArray> actual, expected;
  // Those values are indifferent, only the number of rows is used
  MakeColumnParser({"456", "789"}, &parser);
  builder->Insert(1, parser);
  MakeColumnParser({"123"}, &parser);
  builder->Insert(0, parser);
  ASSERT_OK(builder->task_group()->Finish());
  ASSERT_OK_AND_ASSIGN(actual, builder->Finish());
  ASSERT_OK(actual->ValidateFull());

  auto chunks =
      ArrayVector{std::make_shared<NullArray>(1), std::make_shared<NullArray>(2)};
  expected = std::make_shared<ChunkedArray>(chunks);
  AssertChunkedEqual(*actual, *expected);
}

TEST_F(NullColumnBuilderTest, InsertTyped) {
  // Building a column of nulls with another type
  std::shared_ptr<DataType> type = int16();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK_AND_ASSIGN(builder, ColumnBuilder::MakeNull(default_memory_pool(), type, tg));

  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<ChunkedArray> actual, expected;
  // Those values are indifferent, only the number of rows is used
  MakeColumnParser({"abc", "def", "ghi"}, &parser);
  builder->Insert(1, parser);
  MakeColumnParser({"jkl"}, &parser);
  builder->Insert(0, parser);
  ASSERT_OK(builder->task_group()->Finish());
  ASSERT_OK_AND_ASSIGN(actual, builder->Finish());
  ASSERT_OK(actual->ValidateFull());

  auto chunks = ArrayVector{ArrayFromJSON(type, "[null]"),
                            ArrayFromJSON(type, "[null, null, null]")};
  expected = std::make_shared<ChunkedArray>(chunks);
  AssertChunkedEqual(*actual, *expected);
}

TEST_F(NullColumnBuilderTest, EmptyChunks) {
  std::shared_ptr<DataType> type = int16();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK_AND_ASSIGN(builder, ColumnBuilder::MakeNull(default_memory_pool(), type, tg));

  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<ChunkedArray> actual, expected;
  // Those values are indifferent, only the number of rows is used
  MakeColumnParser({}, &parser);
  builder->Insert(0, parser);
  MakeColumnParser({"abc", "def"}, &parser);
  builder->Insert(1, parser);
  MakeColumnParser({}, &parser);
  builder->Insert(2, parser);
  ASSERT_OK(builder->task_group()->Finish());
  ASSERT_OK_AND_ASSIGN(actual, builder->Finish());
  ASSERT_OK(actual->ValidateFull());

  auto chunks =
      ArrayVector{ArrayFromJSON(type, "[]"), ArrayFromJSON(type, "[null, null]"),
                  ArrayFromJSON(type, "[]")};
  expected = std::make_shared<ChunkedArray>(chunks);
  AssertChunkedEqual(*actual, *expected);
}

//////////////////////////////////////////////////////////////////////////
// Tests for fixed-type column builder

class TypedColumnBuilderTest : public ColumnBuilderTest {};

TEST_F(TypedColumnBuilderTest, Empty) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK_AND_ASSIGN(
      builder, ColumnBuilder::Make(default_memory_pool(), int32(), 0, options, tg));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {}, &actual);

  ChunkedArray expected({}, int32());
  AssertChunkedEqual(*actual, expected);
}

TEST_F(TypedColumnBuilderTest, Basics) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckFixedType(tg, int32(), {{"123", "-456"}}, options,
                 {ArrayFromJSON(int32(), "[123, -456]")});
}

TEST_F(TypedColumnBuilderTest, Insert) {
  // Test ColumnBuilder::Insert()
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK_AND_ASSIGN(
      builder, ColumnBuilder::Make(default_memory_pool(), int32(), 0, options, tg));

  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<ChunkedArray> actual, expected;
  MakeColumnParser({"456"}, &parser);
  builder->Insert(1, parser);
  MakeColumnParser({"123"}, &parser);
  builder->Insert(0, parser);
  ASSERT_OK(builder->task_group()->Finish());
  ASSERT_OK_AND_ASSIGN(actual, builder->Finish());
  ASSERT_OK(actual->ValidateFull());

  ChunkedArrayFromVector<Int32Type>({{123}, {456}}, &expected);
  AssertChunkedEqual(*actual, *expected);
}

TEST_F(TypedColumnBuilderTest, MultipleChunks) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckFixedType(tg, int16(), {{"1", "2", "3"}, {"4", "5"}}, options,
                 {ArrayFromJSON(int16(), "[1, 2, 3]"), ArrayFromJSON(int16(), "[4, 5]")});
}

TEST_F(TypedColumnBuilderTest, MultipleChunksParallel) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeThreaded(GetCpuThreadPool());

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int32Type>({{1, 2}, {3}, {4, 5}, {6, 7}}, &expected);
  CheckFixedType(tg, int32(), {{"1", "2"}, {"3"}, {"4", "5"}, {"6", "7"}}, options,
                 expected);
}

TEST_F(TypedColumnBuilderTest, EmptyChunks) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckFixedType(tg, int16(), {{}, {"1", "2"}, {}}, options,
                 {ArrayFromJSON(int16(), "[]"), ArrayFromJSON(int16(), "[1, 2]"),
                  ArrayFromJSON(int16(), "[]")});
}

//////////////////////////////////////////////////////////////////////////
// Tests for type-inferring column builder

class InferringColumnBuilderTest : public ColumnBuilderTest {
 public:
  void CheckAutoDictEncoded(const std::shared_ptr<TaskGroup>& tg,
                            const ChunkData& csv_data, const ConvertOptions& options,
                            std::vector<std::shared_ptr<Array>> expected_indices,
                            std::vector<std::shared_ptr<Array>> expected_dictionaries,
                            bool validate_full = true) {
    std::shared_ptr<ColumnBuilder> builder;
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK_AND_ASSIGN(builder,
                         ColumnBuilder::Make(default_memory_pool(), 0, options, tg));
    AssertBuilding(builder, csv_data, validate_full, &actual);
    ASSERT_EQ(actual->num_chunks(), static_cast<int>(csv_data.size()));
    for (int i = 0; i < actual->num_chunks(); ++i) {
      ASSERT_EQ(actual->chunk(i)->type_id(), Type::DICTIONARY);
      const auto& dict_array = checked_cast<const DictionaryArray&>(*actual->chunk(i));
      AssertArraysEqual(*dict_array.dictionary(), *expected_dictionaries[i]);
      AssertArraysEqual(*dict_array.indices(), *expected_indices[i]);
    }
  }
};

TEST_F(InferringColumnBuilderTest, Empty) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {}, options, std::make_shared<ChunkedArray>(ArrayVector(), null()));
}

TEST_F(InferringColumnBuilderTest, SingleChunkNull) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "NA"}}, options, {std::make_shared<NullArray>(2)});
}

TEST_F(InferringColumnBuilderTest, MultipleChunkNull) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "NA"}, {""}, {"NaN"}}, options,
                {std::make_shared<NullArray>(2), std::make_shared<NullArray>(1),
                 std::make_shared<NullArray>(1)});
}

TEST_F(InferringColumnBuilderTest, SingleChunkInteger) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "123", "456"}}, options,
                {ArrayFromJSON(int64(), "[null, 123, 456]")});
}

TEST_F(InferringColumnBuilderTest, MultipleChunkInteger) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(
      tg, {{""}, {"NA", "123", "456"}}, options,
      {ArrayFromJSON(int64(), "[null]"), ArrayFromJSON(int64(), "[null, 123, 456]")});
}

TEST_F(InferringColumnBuilderTest, SingleChunkBoolean) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "0", "FALSE", "TRUE"}}, options,
                {ArrayFromJSON(boolean(), "[null, false, false, true]")});
}

TEST_F(InferringColumnBuilderTest, MultipleChunkBoolean) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{""}, {"1", "True", "0"}}, options,
                {ArrayFromJSON(boolean(), "[null]"),
                 ArrayFromJSON(boolean(), "[true, true, false]")});
}

TEST_F(InferringColumnBuilderTest, SingleChunkReal) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "0.0", "12.5"}}, options,
                {ArrayFromJSON(float64(), "[null, 0.0, 12.5]")});
}

TEST_F(InferringColumnBuilderTest, MultipleChunkReal) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{""}, {"008"}, {"NaN", "12.5"}}, options,
                {ArrayFromJSON(float64(), "[null]"), ArrayFromJSON(float64(), "[8.0]"),
                 ArrayFromJSON(float64(), "[null, 12.5]")});
}

TEST_F(InferringColumnBuilderTest, SingleChunkDate) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "1970-01-04", "NA"}}, options,
                {ArrayFromJSON(date32(), "[null, 3, null]")});
}

TEST_F(InferringColumnBuilderTest, MultipleChunkDate) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{""}, {"1970-01-04"}, {"NA"}}, options,
                {ArrayFromJSON(date32(), "[null]"), ArrayFromJSON(date32(), "[3]"),
                 ArrayFromJSON(date32(), "[null]")});
}

TEST_F(InferringColumnBuilderTest, SingleChunkTime) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "01:23:45", "NA"}}, options,
                {ArrayFromJSON(time32(TimeUnit::SECOND), "[null, 5025, null]")});
}

TEST_F(InferringColumnBuilderTest, MultipleChunkTime) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();
  auto type = time32(TimeUnit::SECOND);

  CheckInferred(tg, {{""}, {"01:23:45"}, {"NA"}}, options,
                {ArrayFromJSON(type, "[null]"), ArrayFromJSON(type, "[5025]"),
                 ArrayFromJSON(type, "[null]")});
}

TEST_F(InferringColumnBuilderTest, SingleChunkTimestamp) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<TimestampType>(timestamp(TimeUnit::SECOND),
                                        {{false, true, true}}, {{0, 0, 1542129070}},
                                        &expected);
  CheckInferred(tg, {{"", "1970-01-01", "2018-11-13 17:11:10"}}, options, expected);
}

TEST_F(InferringColumnBuilderTest, MultipleChunkTimestamp) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<TimestampType>(timestamp(TimeUnit::SECOND),
                                        {{false}, {true}, {true}},
                                        {{0}, {0}, {1542129070}}, &expected);
  CheckInferred(tg, {{""}, {"1970-01-01"}, {"2018-11-13 17:11:10"}}, options, expected);
}

TEST_F(InferringColumnBuilderTest, SingleChunkTimestampNS) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<TimestampType>(
      timestamp(TimeUnit::NANO), {{false, true, true, true, true}},
      {{0, 0, 1542129070123000000, 1542129070123456000, 1542129070123456789}}, &expected);
  CheckInferred(tg,
                {{"", "1970-01-01", "2018-11-13 17:11:10.123",
                  "2018-11-13 17:11:10.123456", "2018-11-13 17:11:10.123456789"}},
                options, expected);
}

TEST_F(InferringColumnBuilderTest, MultipleChunkTimestampNS) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<TimestampType>(
      timestamp(TimeUnit::NANO), {{false}, {true}, {true, true, true}},
      {{0}, {0}, {1542129070123000000, 1542129070123456000, 1542129070123456789}},
      &expected);
  CheckInferred(tg,
                {{""},
                 {"1970-01-01"},
                 {"2018-11-13 17:11:10.123", "2018-11-13 17:11:10.123456",
                  "2018-11-13 17:11:10.123456789"}},
                options, expected);
}

TEST_F(InferringColumnBuilderTest, SingleChunkIntegerAndTime) {
  // Fallback to utf-8
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "99", "01:23:45", "NA"}}, options,
                {ArrayFromJSON(utf8(), R"(["", "99", "01:23:45", "NA"])")});
}

TEST_F(InferringColumnBuilderTest, MultipleChunkIntegerAndTime) {
  // Fallback to utf-8
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();
  auto type = utf8();

  CheckInferred(tg, {{""}, {"99"}, {"01:23:45", "NA"}}, options,
                {ArrayFromJSON(type, R"([""])"), ArrayFromJSON(type, R"(["99"])"),
                 ArrayFromJSON(type, R"(["01:23:45", "NA"])")});
}

TEST_F(InferringColumnBuilderTest, SingleChunkDateAndTime) {
  // Fallback to utf-8
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "01:23:45", "1998-04-05"}}, options,
                {ArrayFromJSON(utf8(), R"(["", "01:23:45", "1998-04-05"])")});
}

TEST_F(InferringColumnBuilderTest, MultipleChunkDateAndTime) {
  // Fallback to utf-8
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();
  auto type = utf8();

  CheckInferred(tg, {{""}, {"01:23:45"}, {"1998-04-05"}}, options,
                {ArrayFromJSON(type, R"([""])"), ArrayFromJSON(type, R"(["01:23:45"])"),
                 ArrayFromJSON(type, R"(["1998-04-05"])")});
}

TEST_F(InferringColumnBuilderTest, SingleChunkString) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ChunkedArray> expected;

  // With valid UTF8
  CheckInferred(tg, {{"", "foo", "baré"}}, options,
                {ArrayFromJSON(utf8(), R"(["", "foo", "baré"])")});

  // With invalid UTF8, non-checking
  options.check_utf8 = false;
  tg = TaskGroup::MakeSerial();
  ChunkedArrayFromVector<StringType, std::string>({{true, true, true}},
                                                  {{"", "foo\xff", "baré"}}, &expected);
  CheckInferred(tg, {{"", "foo\xff", "baré"}}, options, expected,
                /*validate_full=*/false);
}

TEST_F(InferringColumnBuilderTest, SingleChunkBinary) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ChunkedArray> expected;

  // With invalid UTF8, checking
  tg = TaskGroup::MakeSerial();
  ChunkedArrayFromVector<BinaryType, std::string>({{true, true, true}},
                                                  {{"", "foo\xff", "baré"}}, &expected);
  CheckInferred(tg, {{"", "foo\xff", "baré"}}, options, expected);
}

TEST_F(InferringColumnBuilderTest, MultipleChunkString) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<StringType, std::string>(
      {{true}, {true}, {true, true}}, {{""}, {"008"}, {"NaN", "baré"}}, &expected);

  CheckInferred(tg, {{""}, {"008"}, {"NaN", "baré"}}, options, expected);
}

TEST_F(InferringColumnBuilderTest, MultipleChunkBinary) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<BinaryType, std::string>(
      {{true}, {true}, {true, true}}, {{""}, {"008"}, {"NaN", "baré\xff"}}, &expected);

  CheckInferred(tg, {{""}, {"008"}, {"NaN", "baré\xff"}}, options, expected);
}

// Parallel parsing is tested more comprehensively on the Python side
// (see python/pyarrow/tests/test_csv.py)

TEST_F(InferringColumnBuilderTest, MultipleChunkIntegerParallel) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeThreaded(GetCpuThreadPool());

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int64Type>({{1, 2}, {3}, {4, 5}, {6, 7}}, &expected);
  CheckInferred(tg, {{"1", "2"}, {"3"}, {"4", "5"}, {"6", "7"}}, options, expected);
}

TEST_F(InferringColumnBuilderTest, SingleChunkBinaryAutoDict) {
  auto options = ConvertOptions::Defaults();
  options.auto_dict_encode = true;
  options.auto_dict_max_cardinality = 3;

  // With valid UTF8
  auto expected_indices = ArrayFromJSON(int32(), "[0, 1, 0]");
  auto expected_dictionary = ArrayFromJSON(utf8(), R"(["abé", "cd"])");
  ChunkData csv_data = {{"abé", "cd", "abé"}};

  CheckAutoDictEncoded(TaskGroup::MakeSerial(), csv_data, options, {expected_indices},
                       {expected_dictionary});

  // With invalid UTF8, non-checking
  csv_data = {{"ab", "cd\xff", "ab"}};
  options.check_utf8 = false;
  ArrayFromVector<StringType, std::string>({"ab", "cd\xff"}, &expected_dictionary);

  CheckAutoDictEncoded(TaskGroup::MakeSerial(), csv_data, options, {expected_indices},
                       {expected_dictionary}, /*validate_full=*/false);

  // With invalid UTF8, checking
  options.check_utf8 = true;
  ArrayFromVector<BinaryType, std::string>({"ab", "cd\xff"}, &expected_dictionary);

  CheckAutoDictEncoded(TaskGroup::MakeSerial(), csv_data, options, {expected_indices},
                       {expected_dictionary});
}

}  // namespace csv
}  // namespace arrow
