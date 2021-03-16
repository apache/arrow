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

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace random {

class RandomArrayTest : public ::testing::TestWithParam<std::shared_ptr<Field>> {
 protected:
  std::shared_ptr<Field> GetField() { return GetParam(); }
};

template <typename T>
class RandomNumericArrayTest : public ::testing::Test {
 protected:
  std::shared_ptr<Field> GetField() { return field("field0", std::make_shared<T>()); }

  std::shared_ptr<NumericArray<T>> Downcast(std::shared_ptr<Array> array) {
    return internal::checked_pointer_cast<NumericArray<T>>(array);
  }
};

TEST_P(RandomArrayTest, GenerateArray) {
  auto field = GetField();
  auto batch = Generate({field}, 128, 0xDEADBEEF);
  AssertSchemaEqual(schema({field}), batch->schema());
  auto array = batch->column(0);
  ASSERT_EQ(128, array->length());
  ASSERT_OK(array->ValidateFull());
}

TEST_P(RandomArrayTest, GenerateNonNullArray) {
  auto field =
      GetField()->WithMetadata(key_value_metadata({{"null_probability", "0.0"}}));
  if (field->type()->id() == Type::type::NA) {
    GTEST_SKIP() << "Cannot generate non-null null arrays";
  }
  auto batch = Generate({field}, 128, 0xDEADBEEF);
  AssertSchemaEqual(schema({field}), batch->schema());
  auto array = batch->column(0);
  ASSERT_OK(array->ValidateFull());
  ASSERT_EQ(0, array->null_count());
}

TEST_P(RandomArrayTest, GenerateNonNullableArray) {
  auto field = GetField()->WithNullable(false);
  if (field->type()->id() == Type::type::NA) {
    GTEST_SKIP() << "Cannot generate non-null null arrays";
  }
  auto batch = Generate({field}, 128, 0xDEADBEEF);
  AssertSchemaEqual(schema({field}), batch->schema());
  auto array = batch->column(0);
  ASSERT_OK(array->ValidateFull());
  ASSERT_EQ(0, array->null_count());
}

struct FieldParamName {
  template <class ParamType>
  std::string operator()(const ::testing::TestParamInfo<ParamType>& info) const {
    return std::to_string(info.index) + info.param->name();
  }
};

auto values = ::testing::Values(
    field("null", null()), field("bool", boolean()), field("uint8", uint8()),
    field("int8", int8()), field("uint16", uint16()), field("int16", int16()),
    field("uint32", uint32()), field("int32", int32()), field("uint64", uint64()),
    field("int64", int64()), field("float16", float16()), field("float32", float32()),
    field("float64", float64()), field("string", utf8()), field("binary", binary()),
    field("fixed_size_binary", fixed_size_binary(8)),
    field("decimal128", decimal128(8, 3)), field("decimal256", decimal256(16, 4)),
    field("date32", date32()), field("date64", date64()),
    field("timestampns", timestamp(TimeUnit::NANO)),
    field("timestamps", timestamp(TimeUnit::SECOND, "America/Phoenix")),
    field("time32ms", time32(TimeUnit::MILLI)), field("time64ns", time64(TimeUnit::NANO)),
    field("time32s", time32(TimeUnit::SECOND)),
    field("time64us", time64(TimeUnit::MICRO)), field("month_interval", month_interval()),
    field("daytime_interval", day_time_interval()), field("listint8", list(int8())),
    field("listlistint8", list(list(int8()))),
    field("listint8emptynulls", list(int8()), true,
          key_value_metadata({{"force_empty_nulls", "true"}})),
    field("listint81024values", list(int8()), true,
          key_value_metadata({{"values", "1024"}})),
    field("structints", struct_({
                            field("int8", int8()),
                            field("int16", int16()),
                            field("int32", int32()),
                        })),
    field("structnested", struct_({
                              field("string", utf8()),
                              field("list", list(int64())),
                              field("timestamp", timestamp(TimeUnit::MILLI)),
                          })),
    field("sparseunion", sparse_union({
                             field("int8", int8()),
                             field("int16", int16()),
                             field("int32", int32()),
                         })),
    field("denseunion", dense_union({
                            field("int8", int8()),
                            field("int16", int16()),
                            field("int32", int32()),
                        })),
    field("dictionary", dictionary(int8(), utf8())), field("map", map(int8(), utf8())),
    field("fixedsizelist", fixed_size_list(int8(), 4)),
    field("durationns", duration(TimeUnit::NANO)), field("largestring", large_utf8()),
    field("largebinary", large_binary()),
    field("largelistlistint8", large_list(list(int8()))));

INSTANTIATE_TEST_SUITE_P(
    TestRandomArrayGeneration, RandomArrayTest, values,
    [](const ::testing::TestParamInfo<RandomArrayTest::ParamType>& info) {
      return std::to_string(info.index) + info.param->name();
    });

using NumericTypes =
    ::testing::Types<UInt8Type, Int8Type, UInt16Type, Int16Type, UInt32Type, Int32Type,
                     HalfFloatType, FloatType, DoubleType>;
TYPED_TEST_SUITE(RandomNumericArrayTest, NumericTypes);

TYPED_TEST(RandomNumericArrayTest, GenerateMinMax) {
  auto field = this->GetField()->WithMetadata(
      key_value_metadata({{"min", "0"}, {"max", "127"}, {"nan_probability", "0.0"}}));
  auto batch = Generate({field}, 128, 0xDEADBEEF);
  AssertSchemaEqual(schema({field}), batch->schema());
  auto array = this->Downcast(batch->column(0));
  auto it = array->begin();
  while (it != array->end()) {
    if ((*it).has_value()) {
      ASSERT_GE(**it, typename TypeParam::c_type(0));
      ASSERT_LE(**it, typename TypeParam::c_type(127));
    }
    it++;
  }
}

TEST(TypeSpecificTests, FloatNan) {
  auto field = arrow::field("float32", float32())
                   ->WithMetadata(key_value_metadata({{"nan_probability", "1.0"}}));
  auto batch = Generate({field}, 128, 0xDEADBEEF);
  AssertSchemaEqual(schema({field}), batch->schema());
  auto array = internal::checked_pointer_cast<NumericArray<FloatType>>(batch->column(0));
  auto it = array->begin();
  while (it != array->end()) {
    if ((*it).has_value()) {
      ASSERT_TRUE(std::isnan(**it));
    }
    it++;
  }
}

TEST(TypeSpecificTests, RepeatedStrings) {
  auto field =
      arrow::field("string", utf8())->WithMetadata(key_value_metadata({{"unique", "1"}}));
  auto batch = Generate({field}, 128, 0xDEADBEEF);
  AssertSchemaEqual(schema({field}), batch->schema());
  auto array = internal::checked_pointer_cast<StringArray>(batch->column(0));
  auto it = array->begin();
  util::optional<util::string_view> singular_value;
  while (it != array->end()) {
    if ((*it).has_value()) {
      if (!singular_value.has_value()) {
        singular_value = *it;
      } else {
        ASSERT_EQ(*singular_value, **it);
      }
    }
    it++;
  }
}

}  // namespace random
}  // namespace arrow
