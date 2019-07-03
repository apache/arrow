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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/csv/column-builder.h"
#include "arrow/csv/options.h"
#include "arrow/csv/test-common.h"
#include "arrow/table.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/task-group.h"
#include "arrow/util/thread-pool.h"

namespace arrow {
namespace csv {

class BlockParser;

using internal::GetCpuThreadPool;
using internal::TaskGroup;

void AssertBuilding(const std::shared_ptr<ColumnBuilder>& builder,
                    const std::vector<std::vector<std::string>>& chunks,
                    std::shared_ptr<ChunkedArray>* out) {
  for (const auto& chunk : chunks) {
    std::shared_ptr<BlockParser> parser;
    MakeColumnParser(chunk, &parser);
    builder->Append(parser);
  }
  ASSERT_OK(builder->task_group()->Finish());
  ASSERT_OK(builder->Finish(out));
}

//////////////////////////////////////////////////////////////////////////
// Tests for fixed-type column builder

TEST(ColumnBuilder, Empty) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(int32(), 0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {}, &actual);

  ChunkedArray expected({}, int32());
  AssertChunkedEqual(*actual, expected);
}

TEST(ColumnBuilder, Basics) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(int32(), 0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{"123", "-456"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int32Type>({{123, -456}}, &expected);
  AssertChunkedEqual(*actual, *expected);
}

TEST(ColumnBuilder, Insert) {
  // Test ColumnBuilder::Insert()
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(int32(), 0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<ChunkedArray> actual, expected;
  MakeColumnParser({"456"}, &parser);
  builder->Insert(1, parser);
  MakeColumnParser({"123"}, &parser);
  builder->Insert(0, parser);
  ASSERT_OK(builder->task_group()->Finish());
  ASSERT_OK(builder->Finish(&actual));

  ChunkedArrayFromVector<Int32Type>({{123}, {456}}, &expected);
  AssertChunkedEqual(*actual, *expected);
}

TEST(ColumnBuilder, MultipleChunks) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(int32(), 0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{"1", "2", "3"}, {"4", "5"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int32Type>({{1, 2, 3}, {4, 5}}, &expected);
  AssertChunkedEqual(*actual, *expected);
}

TEST(ColumnBuilder, MultipleChunksParallel) {
  auto tg = TaskGroup::MakeThreaded(GetCpuThreadPool());
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(int32(), 0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{"1", "2"}, {"3"}, {"4", "5"}, {"6", "7"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int32Type>({{1, 2}, {3}, {4, 5}, {6, 7}}, &expected);
  AssertChunkedEqual(*actual, *expected);
}

//////////////////////////////////////////////////////////////////////////
// Tests for type-inferring column builder

TEST(InferringColumnBuilder, Empty) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {}, &actual);

  ASSERT_EQ(actual->type()->id(), Type::NA);
  ASSERT_EQ(actual->num_chunks(), 0);
}

TEST(InferringColumnBuilder, SingleChunkNull) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{"", "NA"}}, &actual);

  ASSERT_EQ(actual->type()->id(), Type::NA);
  ASSERT_EQ(actual->length(), 2);
}

TEST(InferringColumnBuilder, MultipleChunkNull) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{"", "NA"}, {""}, {"NaN"}}, &actual);

  ASSERT_EQ(actual->type()->id(), Type::NA);
  ASSERT_EQ(actual->length(), 4);
}

TEST(InferringColumnBuilder, SingleChunkInteger) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{"", "123", "456"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int64Type>({{false, true, true}}, {{0, 123, 456}}, &expected);
  AssertChunkedEqual(*expected, *actual);
}

TEST(InferringColumnBuilder, MultipleChunkInteger) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{""}, {"NA", "123", "456"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int64Type>({{false}, {false, true, true}}, {{0}, {0, 123, 456}},
                                    &expected);
  AssertChunkedEqual(*expected, *actual);
}

TEST(InferringColumnBuilder, SingleChunkBoolean) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{"", "0", "FALSE"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<BooleanType, bool>({{false, true, true}},
                                            {{false, false, false}}, &expected);
  AssertChunkedEqual(*expected, *actual);
}

TEST(InferringColumnBuilder, MultipleChunkBoolean) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{""}, {"1", "True", "0"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<BooleanType, bool>({{false}, {true, true, true}},
                                            {{false}, {true, true, false}}, &expected);
  AssertChunkedEqual(*expected, *actual);
}

TEST(InferringColumnBuilder, SingleChunkReal) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{"", "0.0", "12.5"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<DoubleType>({{false, true, true}}, {{0.0, 0.0, 12.5}},
                                     &expected);
  AssertChunkedEqual(*expected, *actual);
}

TEST(InferringColumnBuilder, MultipleChunkReal) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{""}, {"008"}, {"NaN", "12.5"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<DoubleType>({{false}, {true}, {false, true}},
                                     {{0.0}, {8.0}, {0.0, 12.5}}, &expected);
  AssertChunkedEqual(*expected, *actual);
}

TEST(InferringColumnBuilder, SingleChunkTimestamp) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{"", "1970-01-01", "2018-11-13 17:11:10"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<TimestampType>(timestamp(TimeUnit::SECOND),
                                        {{false, true, true}}, {{0, 0, 1542129070}},
                                        &expected);
  AssertChunkedEqual(*expected, *actual);
}

TEST(InferringColumnBuilder, MultipleChunkTimestamp) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{""}, {"1970-01-01"}, {"2018-11-13 17:11:10"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<TimestampType>(timestamp(TimeUnit::SECOND),
                                        {{false}, {true}, {true}},
                                        {{0}, {0}, {1542129070}}, &expected);
  AssertChunkedEqual(*expected, *actual);
}

TEST(InferringColumnBuilder, SingleChunkString) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  std::shared_ptr<ChunkedArray> actual;
  std::shared_ptr<ChunkedArray> expected;

  // With valid UTF8
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));
  AssertBuilding(builder, {{"", "foo", "baré"}}, &actual);

  ChunkedArrayFromVector<StringType, std::string>({{true, true, true}},
                                                  {{"", "foo", "baré"}}, &expected);
  AssertChunkedEqual(*expected, *actual);

  // With invalid UTF8, non-checking
  auto options = ConvertOptions::Defaults();
  options.check_utf8 = false;
  tg = TaskGroup::MakeSerial();
  ASSERT_OK(ColumnBuilder::Make(0, options, tg, &builder));
  AssertBuilding(builder, {{"", "foo\xff", "baré"}}, &actual);

  ChunkedArrayFromVector<StringType, std::string>({{true, true, true}},
                                                  {{"", "foo\xff", "baré"}}, &expected);
  AssertChunkedEqual(*expected, *actual);
}

TEST(InferringColumnBuilder, SingleChunkBinary) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  std::shared_ptr<ChunkedArray> actual;
  std::shared_ptr<ChunkedArray> expected;

  // With invalid UTF8, checking
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));
  AssertBuilding(builder, {{"", "foo\xff", "baré"}}, &actual);

  ChunkedArrayFromVector<BinaryType, std::string>({{true, true, true}},
                                                  {{"", "foo\xff", "baré"}}, &expected);
  AssertChunkedEqual(*expected, *actual);
}

TEST(InferringColumnBuilder, MultipleChunkString) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{""}, {"008"}, {"NaN", "baré"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<StringType, std::string>(
      {{true}, {true}, {true, true}}, {{""}, {"008"}, {"NaN", "baré"}}, &expected);
  AssertChunkedEqual(*expected, *actual);
}

TEST(InferringColumnBuilder, MultipleChunkBinary) {
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{""}, {"008"}, {"NaN", "baré\xff"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<BinaryType, std::string>(
      {{true}, {true}, {true, true}}, {{""}, {"008"}, {"NaN", "baré\xff"}}, &expected);
  AssertChunkedEqual(*expected, *actual);
}

// Parallel parsing is tested more comprehensively on the Python side
// (see python/pyarrow/tests/test_csv.py)

TEST(InferringColumnBuilder, MultipleChunkIntegerParallel) {
  auto tg = TaskGroup::MakeThreaded(GetCpuThreadPool());
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::Make(0, ConvertOptions::Defaults(), tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {{"1", "2"}, {"3"}, {"4", "5"}, {"6", "7"}}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int64Type>({{1, 2}, {3}, {4, 5}, {6, 7}}, &expected);
  AssertChunkedEqual(*actual, *expected);
}

}  // namespace csv
}  // namespace arrow
