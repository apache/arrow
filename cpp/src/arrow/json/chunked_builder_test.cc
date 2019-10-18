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
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/json/chunked_builder.h"
#include "arrow/json/converter.h"
#include "arrow/json/options.h"
#include "arrow/json/test_common.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace json {

using util::string_view;

using internal::checked_cast;
using internal::GetCpuThreadPool;
using internal::TaskGroup;

void AssertBuilding(const std::unique_ptr<ChunkedArrayBuilder>& builder,
                    const std::vector<std::string>& chunks,
                    std::shared_ptr<ChunkedArray>* out) {
  ArrayVector unconverted;

  auto options = ParseOptions::Defaults();
  for (const auto& chunk : chunks) {
    std::shared_ptr<Array> parsed;
    ASSERT_OK(ParseFromString(options, chunk, &parsed));
    unconverted.push_back(parsed);
  }

  int64_t i = 0;
  for (const auto& parsed : unconverted) {
    builder->Insert(i, field("", parsed->type()), parsed);
    ++i;
  }
  ASSERT_OK(builder->Finish(out));
}

std::shared_ptr<ChunkedArray> ExtractField(const std::string& name,
                                           const ChunkedArray& columns) {
  auto chunks = columns.chunks();
  for (auto& chunk : chunks) {
    chunk = checked_cast<const StructArray&>(*chunk).GetFieldByName(name);
  }
  auto struct_type = static_cast<const StructType*>(columns.type().get());
  return std::make_shared<ChunkedArray>(chunks,
                                        struct_type->GetFieldByName(name)->type());
}

void AssertFieldEqual(const std::vector<std::string>& path,
                      const std::shared_ptr<ChunkedArray>& columns,
                      const ChunkedArray& expected) {
  ASSERT_EQ(expected.num_chunks(), columns->num_chunks()) << "# chunks unequal";
  std::shared_ptr<ChunkedArray> actual = columns;
  for (const auto& name : path) {
    actual = ExtractField(name, *actual);
  }
  AssertChunkedEqual(expected, *actual);
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

std::string RowsOfOneColumn(string_view name, std::initializer_list<std::string> values) {
  std::stringstream ss;
  for (auto value : values) {
    ss << R"({")" << name << R"(":)" << value << "}\n";
  }
  return ss.str();
}

TEST(ChunkedArrayBuilder, Empty) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), nullptr,
                                    struct_({field("a", int32())}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {}, &actual);

  ChunkedArray expected({}, int32());
  AssertFieldEqual({"a"}, actual, expected);
}

TEST(ChunkedArrayBuilder, Basics) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), nullptr,
                                    struct_({field("a", int32())}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {RowsOfOneColumn("a", {123, -456})}, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int32Type>({{123, -456}}, &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

TEST(ChunkedArrayBuilder, Insert) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), nullptr,
                                    struct_({field("a", int32())}), &builder));

  auto options = ParseOptions::Defaults();
  std::shared_ptr<ChunkedArray> actual, expected;

  std::shared_ptr<Array> parsed;
  ASSERT_OK(ParseFromString(options, RowsOfOneColumn("a", {-456}), &parsed));
  builder->Insert(1, field("", parsed->type()), parsed);
  ASSERT_OK(ParseFromString(options, RowsOfOneColumn("a", {123}), &parsed));
  builder->Insert(0, field("", parsed->type()), parsed);

  ASSERT_OK(builder->Finish(&actual));

  ChunkedArrayFromVector<Int32Type>({{123}, {-456}}, &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

TEST(ChunkedArrayBuilder, MultipleChunks) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), nullptr,
                                    struct_({field("a", int32())}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder,
                 {
                     RowsOfOneColumn("a", {1, 2, 3}),
                     RowsOfOneColumn("a", {4, 5}),
                 },
                 &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int32Type>({{1, 2, 3}, {4, 5}}, &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

TEST(ChunkedArrayBuilder, MultipleChunksParallel) {
  auto tg = TaskGroup::MakeThreaded(GetCpuThreadPool());
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), nullptr,
                                    struct_({field("a", int32())}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder,
                 {
                     RowsOfOneColumn("a", {1, 2}),
                     RowsOfOneColumn("a", {3}),
                     RowsOfOneColumn("a", {4, 5}),
                     RowsOfOneColumn("a", {6, 7}),
                 },
                 &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int32Type>({{1, 2}, {3}, {4, 5}, {6, 7}}, &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

//////////////////////////////////////////////////////////////////////////
// Tests for type-inferring chunked array builders

TEST(InferringChunkedArrayBuilder, Empty) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {}, &actual);

  ASSERT_TRUE(actual->type()->Equals(*struct_({})));
  ASSERT_EQ(actual->num_chunks(), 0);
}

TEST(InferringChunkedArrayBuilder, SingleChunkNull) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder,
                 {
                     "{}\n" + RowsOfOneColumn("a", {"null", "null"}),
                 },
                 &actual);

  ASSERT_TRUE(actual->type()->Equals(*struct_({field("a", null())})));
  ASSERT_EQ(actual->length(), 3);
}

TEST(InferringChunkedArrayBuilder, MultipleChunkNull) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder,
                 {
                     "{}\n{}\n",
                     "{}\n" + RowsOfOneColumn("a", {"null", "null"}),
                     RowsOfOneColumn("a", {"null"}),
                     RowsOfOneColumn("a", {"null", "null"}) + "{}\n",
                 },
                 &actual);

  ASSERT_TRUE(actual->type()->Equals(*struct_({field("a", null())})));
  ASSERT_EQ(actual->length(), 9);
}

TEST(InferringChunkedArrayBuilder, SingleChunkInteger) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(
      builder,
      {
          "{}\n" + RowsOfOneColumn("a", {123, 456}) + RowsOfOneColumn("a", {"null"}),
      },
      &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int64Type>({{false, true, true, false}}, {{0, 123, 456, 0}},
                                    &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

TEST(InferringChunkedArrayBuilder, MultipleChunkInteger) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder,
                 {
                     "{}\n{}\n",
                     RowsOfOneColumn("a", {"null"}),
                     "{}\n" + RowsOfOneColumn("a", {123, 456}),
                 },
                 &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int64Type>({{false, false}, {false}, {false, true, true}},
                                    {{0, 0}, {0}, {0, 123, 456}}, &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

TEST(InferringChunkedArrayBuilder, SingleChunkDouble) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(
      builder,
      {
          "{}\n" + RowsOfOneColumn("a", {0.0, 12.5}) + RowsOfOneColumn("a", {"null"}),
      },
      &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<DoubleType>({{false, true, true, false}},
                                     {{0.0, 0.0, 12.5, 0.0}}, &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

TEST(InferringChunkedArrayBuilder, MultipleChunkDouble) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder,
                 {
                     "{}\n{}\n",
                     RowsOfOneColumn("a", {"null"}),
                     RowsOfOneColumn("a", {8}),
                     RowsOfOneColumn("a", {"null", "12.5"}),
                 },
                 &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<DoubleType>({{false, false}, {false}, {true}, {false, true}},
                                     {{0.0, 0.0}, {0.0}, {8.0}, {0.0, 12.5}}, &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

TEST(InferringChunkedArrayBuilder, SingleChunkTimestamp) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder,
                 {
                     "{}\n" + RowsOfOneColumn("a", {"null", "\"1970-01-01\"",
                                                    "\"2018-11-13 17:11:10\""}),
                 },
                 &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<TimestampType>(timestamp(TimeUnit::SECOND),
                                        {{false, false, true, true}},
                                        {{0, 0, 0, 1542129070}}, &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

TEST(InferringChunkedArrayBuilder, MultipleChunkTimestamp) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder,
                 {
                     "{}\n{}\n",
                     RowsOfOneColumn("a", {"null"}),
                     RowsOfOneColumn("a", {"\"1970-01-01\""}),
                     RowsOfOneColumn("a", {"\"2018-11-13 17:11:10\""}),
                 },
                 &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<TimestampType>(timestamp(TimeUnit::SECOND),
                                        {{false, false}, {false}, {true}, {true}},
                                        {{0, 0}, {0}, {0}, {1542129070}}, &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

TEST(InferringChunkedArrayBuilder, SingleChunkString) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(
      builder,
      {
          "{}\n" + RowsOfOneColumn("a", {"null", "\"\"", "null", "\"foo\"", "\"baré\""}),
      },
      &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<StringType, std::string>(
      {{false, false, true, false, true, true}}, {{"", "", "", "", "foo", "baré"}},
      &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

TEST(InferringChunkedArrayBuilder, MultipleChunkString) {
  auto tg = TaskGroup::MakeSerial();
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder,
                 {
                     "{}\n{}\n",
                     RowsOfOneColumn("a", {"\"\"", "null"}),
                     RowsOfOneColumn("a", {"\"1970-01-01\""}),
                     RowsOfOneColumn("a", {"\"\"", "\"baré\""}),
                 },
                 &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<StringType, std::string>(
      {{false, false}, {true, false}, {true}, {true, true}},
      {{"", ""}, {"", ""}, {"1970-01-01"}, {"", "baré"}}, &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

TEST(InferringChunkedArrayBuilder, MultipleChunkIntegerParallel) {
  auto tg = TaskGroup::MakeThreaded(GetCpuThreadPool());
  std::unique_ptr<ChunkedArrayBuilder> builder;
  ASSERT_OK(MakeChunkedArrayBuilder(tg, default_memory_pool(), GetPromotionGraph(),
                                    struct_({}), &builder));

  std::shared_ptr<ChunkedArray> actual;
  std::vector<std::string> chunks;
  std::vector<std::vector<int>> expected_chunks;
  for (int i = 0; i < 1 << 10; ++i) {
    expected_chunks.push_back({i, i + 1, i + 2, i + 3});
    chunks.push_back(RowsOfOneColumn("a", {i, i + 1, i + 2, i + 3}));
  }
  AssertBuilding(builder, chunks, &actual);

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int64Type>(expected_chunks, &expected);
  AssertFieldEqual({"a"}, actual, *expected);
}

}  // namespace json
}  // namespace arrow
