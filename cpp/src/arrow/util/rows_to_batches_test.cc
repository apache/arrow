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

#include <vector>

#include <gtest/gtest.h>

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/rows_to_batches.h"

namespace arrow::util {

const auto kTestSchema = schema(
    {field("field_1", int64()), field("field_2", int64()), field("field_3", int64())});

auto IntConvertor = [](ArrayBuilder& array_builder, int value) {
  return static_cast<Int64Builder&>(array_builder).Append(value);
};

bool CompareJson(const arrow::Table& arrow_table, const std::string& json,
                 const std::string& field_name) {
  const auto col = arrow_table.GetColumnByName(field_name);
  return arrow::ChunkedArrayFromJSON(col->type(), {json})->Equals(col);
}

TEST(RowsToBatches, BasicUsage) {
  std::vector<std::vector<int>> data = {{1, 2, 4}, {5, 6, 7}};
  auto batches = RowsToBatches(kTestSchema, data, IntConvertor).ValueOrDie();
  auto table = batches->ToTable().ValueOrDie();

  EXPECT_TRUE(CompareJson(*table, R"([1, 5])", "field_1"));
  EXPECT_TRUE(CompareJson(*table, R"([2, 6])", "field_2"));
  EXPECT_TRUE(CompareJson(*table, R"([4, 7])", "field_3"));
}

TEST(RowsToBatches, ConstRange) {
  const std::vector<std::vector<int>> data = {{1, 2, 4}, {5, 6, 7}};
  auto batches = RowsToBatches(kTestSchema, data, IntConvertor).ValueOrDie();
  auto table = batches->ToTable().ValueOrDie();

  EXPECT_TRUE(CompareJson(*table, R"([1, 5])", "field_1"));
  EXPECT_TRUE(CompareJson(*table, R"([2, 6])", "field_2"));
  EXPECT_TRUE(CompareJson(*table, R"([4, 7])", "field_3"));
}

TEST(RowsToBatches, StructAccessor) {
  struct TestStruct {
    std::vector<int> values;
  };
  std::vector<TestStruct> data = {TestStruct{{1, 2, 4}}, TestStruct{{5, 6, 7}}};

  auto accessor =
      [](const TestStruct& s) -> Result<std::reference_wrapper<const std::vector<int>>> {
    return std::cref(s.values);
  };

  auto batches = RowsToBatches(kTestSchema, data, IntConvertor, accessor).ValueOrDie();

  auto table = batches->ToTable().ValueOrDie();

  EXPECT_TRUE(CompareJson(*table, R"([1, 5])", "field_1"));
  EXPECT_TRUE(CompareJson(*table, R"([2, 6])", "field_2"));
  EXPECT_TRUE(CompareJson(*table, R"([4, 7])", "field_3"));

  // Test accessor that returns by value instead of using `std::reference_wrapper`
  auto accessor_by_value = [](const TestStruct& s) -> Result<std::set<int>> {
    return std::set<int>(std::begin(s.values), std::end(s.values));
  };
  auto batches_by_value =
      RowsToBatches(kTestSchema, data, IntConvertor, accessor_by_value).ValueOrDie();

  auto table_by_value = batches_by_value->ToTable().ValueOrDie();

  EXPECT_TRUE(CompareJson(*table_by_value, R"([1, 5])", "field_1"));
  EXPECT_TRUE(CompareJson(*table_by_value, R"([2, 6])", "field_2"));
  EXPECT_TRUE(CompareJson(*table_by_value, R"([4, 7])", "field_3"));
}

TEST(RowsToBatches, Variant) {
  auto VariantConvertor = [](ArrayBuilder& array_builder,
                             const std::variant<int, std::string>& value) {
    if (std::holds_alternative<int>(value))
      return dynamic_cast<Int64Builder&>(array_builder).Append(std::get<int>(value));
    else
      return dynamic_cast<arrow::StringBuilder&>(array_builder)
          .Append(std::get<std::string>(value));
  };

  const auto test_schema = schema({field("x", int64()), field("y", utf8())});
  std::vector<std::vector<std::variant<int, std::string>>> data = {{1, std::string("2")},
                                                                   {4, std::string("5")}};

  auto batches = RowsToBatches(test_schema, data, VariantConvertor).ValueOrDie();

  auto table = batches->ToTable().ValueOrDie();

  EXPECT_TRUE(CompareJson(*table, R"([1, 4])", "x"));
  EXPECT_TRUE(CompareJson(*table, R"(["2", "5"])", "y"));
}

}  // namespace arrow::util
