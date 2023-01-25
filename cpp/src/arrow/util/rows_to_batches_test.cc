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

#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/util/rows_to_batches.h"

namespace arrow::util {

// clang-format off
const auto kTestSchema = schema(
    {field("field_1", int64()),
     field("field_2", int64()),
     field("field_3", int64())} );
// clang-format on

auto IntConvertor = [](ArrayBuilder& array_builder, int value) {
  return static_cast<Int64Builder&>(array_builder).Append(value);
};

TEST(RowsToBatches, BasicUsage) {
  std::vector<std::vector<int>> data = {{1, 2, 4}, {5, 6, 7}};

  auto batches = RowsToBatches(kTestSchema, std::ref(data), IntConvertor).ValueOrDie();

  auto table = batches->ToTable().ValueOrDie();

  std::shared_ptr<ChunkedArray> col = table->column(0);
  EXPECT_EQ(col->length(), 2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(0).ValueOrDie())->value,
            1);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(1).ValueOrDie())->value,
            5);

  col = table->column(1);
  EXPECT_EQ(col->length(), 2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(0).ValueOrDie())->value,
            2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(1).ValueOrDie())->value,
            6);

  col = table->column(2);
  EXPECT_EQ(col->length(), 2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(0).ValueOrDie())->value,
            4);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(1).ValueOrDie())->value,
            7);
}

TEST(RowsToBatches, ConstRange) {
  const std::vector<std::vector<int>> data = {{1, 2, 4}, {5, 6, 7}};

  auto batches = RowsToBatches(kTestSchema, std::cref(data), IntConvertor).ValueOrDie();

  auto table = batches->ToTable().ValueOrDie();

  std::shared_ptr<ChunkedArray> col = table->column(0);
  EXPECT_EQ(col->length(), 2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(0).ValueOrDie())->value,
            1);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(1).ValueOrDie())->value,
            5);

  col = table->column(1);
  EXPECT_EQ(col->length(), 2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(0).ValueOrDie())->value,
            2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(1).ValueOrDie())->value,
            6);

  col = table->column(2);
  EXPECT_EQ(col->length(), 2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(0).ValueOrDie())->value,
            4);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(1).ValueOrDie())->value,
            7);
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

  auto batches =
      RowsToBatches(kTestSchema, std::ref(data), IntConvertor, accessor).ValueOrDie();

  auto table = batches->ToTable().ValueOrDie();

  std::shared_ptr<ChunkedArray> col = table->column(0);
  EXPECT_EQ(col->length(), 2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(0).ValueOrDie())->value,
            1);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(1).ValueOrDie())->value,
            5);

  col = table->column(1);
  EXPECT_EQ(col->length(), 2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(0).ValueOrDie())->value,
            2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(1).ValueOrDie())->value,
            6);

  col = table->column(2);
  EXPECT_EQ(col->length(), 2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(0).ValueOrDie())->value,
            4);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(1).ValueOrDie())->value,
            7);
}

TEST(RowsToBatches, Variant) {
  auto VariantConvertor = [](ArrayBuilder& array_builder,
                             const std::variant<int, std::string>& value) {
    if (std::holds_alternative<int>(value))
      return dynamic_cast<Int64Builder&>(array_builder).Append(std::get<int>(value));
    else
      return dynamic_cast<arrow::StringBuilder&>(array_builder)
          .Append(std::get<std::string>(value).c_str(),
                  std::get<std::string>(value).length());
  };

  const auto test_schema = schema({field("x", int64()), field("y", utf8())});
  std::vector<std::vector<std::variant<int, std::string>>> data = {{1, std::string("2")},
                                                                   {4, std::string("5")}};

  auto batches = RowsToBatches(test_schema, std::ref(data), VariantConvertor).ValueOrDie();
  auto table = batches->ToTable().ValueOrDie();

  std::shared_ptr<ChunkedArray> col = table->column(0);
  EXPECT_EQ(col->length(), 2);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(0).ValueOrDie())->value,
            1);
  EXPECT_EQ(std::dynamic_pointer_cast<Int64Scalar>(col->GetScalar(1).ValueOrDie())->value,
            4);

  col = table->column(1);
  EXPECT_EQ(col->length(), 2);
  EXPECT_EQ(
      std::dynamic_pointer_cast<StringScalar>(col->GetScalar(0).ValueOrDie())->view(),
      "2");
  EXPECT_EQ(
      std::dynamic_pointer_cast<StringScalar>(col->GetScalar(1).ValueOrDie())->view(),
      "5");
}

}  // namespace arrow::util
