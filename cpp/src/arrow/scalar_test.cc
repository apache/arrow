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
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_traits.h"

namespace arrow {

using internal::checked_cast;

TEST(TestNullScalar, Basics) {
  NullScalar scalar;
  ASSERT_FALSE(scalar.is_valid);
  ASSERT_TRUE(scalar.type->Equals(*null()));
}

template <typename T>
class TestNumericScalar : public ::testing::Test {
 public:
  TestNumericScalar() {}
};

TYPED_TEST_CASE(TestNumericScalar, NumericArrowTypes);

TYPED_TEST(TestNumericScalar, Basics) {
  using T = typename TypeParam::c_type;
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;

  T value = static_cast<T>(1);

  auto scalar_val = std::make_shared<ScalarType>(value);
  ASSERT_EQ(value, scalar_val->value);
  ASSERT_TRUE(scalar_val->is_valid);

  auto expected_type = TypeTraits<TypeParam>::type_singleton();
  ASSERT_TRUE(scalar_val->type->Equals(*expected_type));

  T other_value = static_cast<T>(2);
  auto scalar_other = std::make_shared<ScalarType>(other_value);
  ASSERT_NE(*scalar_other, *scalar_val);

  scalar_val->value = other_value;
  ASSERT_EQ(other_value, scalar_val->value);
  ASSERT_EQ(*scalar_other, *scalar_val);

  ScalarType stack_val;
  ASSERT_FALSE(stack_val.is_valid);

  auto null_value = std::make_shared<ScalarType>();
  ASSERT_FALSE(null_value->is_valid);

  // Nulls should be equals to itself following Array::Equals
  ASSERT_EQ(*null_value, stack_val);

  auto dyn_null_value = MakeNullScalar(expected_type);
  ASSERT_EQ(*null_value, *dyn_null_value);
}

TYPED_TEST(TestNumericScalar, MakeScalar) {
  using T = typename TypeParam::c_type;
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  auto type = TypeTraits<TypeParam>::type_singleton();

  std::shared_ptr<Scalar> three = MakeScalar(static_cast<T>(3));
  ASSERT_EQ(ScalarType(3), *three);

  ASSERT_OK_AND_ASSIGN(three, MakeScalar(type, static_cast<T>(3)));
  ASSERT_EQ(ScalarType(3), *three);

  ASSERT_OK_AND_ASSIGN(three, Scalar::Parse(type, "3"));
  ASSERT_EQ(ScalarType(3), *three);
}

TEST(TestBinaryScalar, Basics) {
  std::string data = "test data";
  auto buf = std::make_shared<Buffer>(data);

  BinaryScalar value(buf);
  ASSERT_TRUE(value.value->Equals(*buf));
  ASSERT_TRUE(value.is_valid);
  ASSERT_TRUE(value.type->Equals(*binary()));

  auto ref_count = buf.use_count();
  // Check that destructor doesn't fail to clean up a buffer
  std::shared_ptr<Scalar> base_ref = std::make_shared<BinaryScalar>(buf);
  base_ref = nullptr;
  ASSERT_EQ(ref_count, buf.use_count());

  BinaryScalar null_value;
  ASSERT_FALSE(null_value.is_valid);

  StringScalar value2(buf);
  ASSERT_TRUE(value2.value->Equals(*buf));
  ASSERT_TRUE(value2.is_valid);
  ASSERT_TRUE(value2.type->Equals(*utf8()));

  // Same buffer, different type.
  ASSERT_NE(value2, value);

  StringScalar value3(buf);
  // Same buffer, same type.
  ASSERT_EQ(value2, value3);

  StringScalar null_value2;
  ASSERT_FALSE(null_value2.is_valid);
}

TEST(TestStringScalar, MakeScalar) {
  auto three = MakeScalar("three");
  ASSERT_EQ(StringScalar("three"), *three);

  ASSERT_OK_AND_ASSIGN(three, MakeScalar(utf8(), Buffer::FromString("three")));
  ASSERT_EQ(StringScalar("three"), *three);

  ASSERT_OK_AND_ASSIGN(three, Scalar::Parse(utf8(), "three"));
  ASSERT_EQ(StringScalar("three"), *three);
}

TEST(TestFixedSizeBinaryScalar, Basics) {
  std::string data = "test data";
  auto buf = std::make_shared<Buffer>(data);

  auto ex_type = fixed_size_binary(9);

  FixedSizeBinaryScalar value(buf, ex_type);
  ASSERT_TRUE(value.value->Equals(*buf));
  ASSERT_TRUE(value.is_valid);
  ASSERT_TRUE(value.type->Equals(*ex_type));
}

TEST(TestFixedSizeBinaryScalar, MakeScalar) {
  std::string data = "test data";
  auto buf = std::make_shared<Buffer>(data);
  auto type = fixed_size_binary(9);

  ASSERT_OK_AND_ASSIGN(auto s, MakeScalar(type, buf));
  ASSERT_EQ(FixedSizeBinaryScalar(buf, type), *s);

  ASSERT_OK_AND_ASSIGN(s, Scalar::Parse(type, util::string_view(data)));
  ASSERT_EQ(FixedSizeBinaryScalar(buf, type), *s);

  // wrong length:
  ASSERT_RAISES(Invalid, MakeScalar(type, Buffer::FromString(data.substr(3))).status());
  ASSERT_RAISES(Invalid, Scalar::Parse(type, util::string_view(data).substr(3)).status());
}

TEST(TestDateScalars, Basics) {
  int32_t i32_val = 1;
  Date32Scalar date32_val(i32_val);
  Date32Scalar date32_null;
  ASSERT_TRUE(date32_val.type->Equals(*date32()));
  ASSERT_TRUE(date32_val.is_valid);
  ASSERT_FALSE(date32_null.is_valid);

  int64_t i64_val = 2;
  Date64Scalar date64_val(i64_val);
  Date64Scalar date64_null;
  ASSERT_EQ(i64_val, date64_val.value);
  ASSERT_TRUE(date64_val.type->Equals(*date64()));
  ASSERT_TRUE(date64_val.is_valid);
  ASSERT_FALSE(date64_null.is_valid);
}

TEST(TestDateScalars, MakeScalar) {
  ASSERT_OK_AND_ASSIGN(auto s, MakeScalar(date32(), int32_t(1)));
  ASSERT_EQ(Date32Scalar(1), *s);

  ASSERT_OK_AND_ASSIGN(s, MakeScalar(date64(), int64_t(1)));
  ASSERT_EQ(Date64Scalar(1), *s);

  ASSERT_RAISES(NotImplemented, Scalar::Parse(date64(), ""));
}

TEST(TestTimeScalars, Basics) {
  auto type1 = time32(TimeUnit::MILLI);
  auto type2 = time32(TimeUnit::SECOND);
  auto type3 = time64(TimeUnit::MICRO);
  auto type4 = time64(TimeUnit::NANO);

  int32_t i32_val = 1;
  Time32Scalar time32_val(i32_val, type1);
  Time32Scalar time32_null(type2);
  ASSERT_EQ(i32_val, time32_val.value);
  ASSERT_TRUE(time32_val.type->Equals(*type1));
  ASSERT_TRUE(time32_val.is_valid);
  ASSERT_FALSE(time32_null.is_valid);
  ASSERT_TRUE(time32_null.type->Equals(*type2));

  int64_t i64_val = 2;
  Time64Scalar time64_val(i64_val, type3);
  Time64Scalar time64_null(type4);
  ASSERT_EQ(i64_val, time64_val.value);
  ASSERT_TRUE(time64_val.type->Equals(*type3));
  ASSERT_TRUE(time64_val.is_valid);
  ASSERT_FALSE(time64_null.is_valid);
  ASSERT_TRUE(time64_null.type->Equals(*type4));
}

TEST(TestTimeScalars, MakeScalar) {
  auto type1 = time32(TimeUnit::MILLI);
  auto type2 = time32(TimeUnit::SECOND);
  auto type3 = time64(TimeUnit::MICRO);
  auto type4 = time64(TimeUnit::NANO);

  ASSERT_OK_AND_ASSIGN(auto s, MakeScalar(type1, int32_t(1)));
  ASSERT_EQ(Time32Scalar(1, type1), *s);

  ASSERT_OK_AND_ASSIGN(s, MakeScalar(type2, int32_t(1)));
  ASSERT_EQ(Time32Scalar(1, type2), *s);

  ASSERT_OK_AND_ASSIGN(s, MakeScalar(type3, int64_t(1)));
  ASSERT_EQ(Time64Scalar(1, type3), *s);

  ASSERT_OK_AND_ASSIGN(s, MakeScalar(type4, int64_t(1)));
  ASSERT_EQ(Time64Scalar(1, type4), *s);

  ASSERT_RAISES(NotImplemented, Scalar::Parse(type4, ""));
}

TEST(TestTimestampScalars, Basics) {
  auto type1 = timestamp(TimeUnit::MILLI);
  auto type2 = timestamp(TimeUnit::SECOND);

  int64_t val1 = 1;
  int64_t val2 = 2;
  TimestampScalar ts_val1(val1, type1);
  TimestampScalar ts_val2(val2, type2);
  TimestampScalar ts_null(type1);
  ASSERT_EQ(val1, ts_val1.value);

  ASSERT_TRUE(ts_val1.type->Equals(*type1));
  ASSERT_TRUE(ts_val2.type->Equals(*type2));
  ASSERT_TRUE(ts_val1.is_valid);
  ASSERT_FALSE(ts_null.is_valid);
  ASSERT_TRUE(ts_null.type->Equals(*type1));

  ASSERT_NE(ts_val1, ts_val2);
  ASSERT_NE(ts_val1, ts_null);
  ASSERT_NE(ts_val2, ts_null);
}

TEST(TestTimestampScalars, MakeScalar) {
  auto type1 = timestamp(TimeUnit::MILLI);
  auto type2 = timestamp(TimeUnit::SECOND);
  auto type3 = timestamp(TimeUnit::MICRO);
  auto type4 = timestamp(TimeUnit::NANO);

  util::string_view epoch_plus_1s = "1970-01-01 00:00:01";

  ASSERT_OK_AND_ASSIGN(auto s, MakeScalar(type1, int64_t(1)));
  ASSERT_EQ(TimestampScalar(1, type1), *s);
  ASSERT_OK_AND_ASSIGN(s, Scalar::Parse(type1, epoch_plus_1s));
  ASSERT_EQ(TimestampScalar(1000, type1), *s);

  ASSERT_OK_AND_ASSIGN(s, MakeScalar(type2, int64_t(1)));
  ASSERT_EQ(TimestampScalar(1, type2), *s);
  ASSERT_OK_AND_ASSIGN(s, Scalar::Parse(type2, epoch_plus_1s));
  ASSERT_EQ(TimestampScalar(1, type2), *s);

  ASSERT_OK_AND_ASSIGN(s, MakeScalar(type3, int64_t(1)));
  ASSERT_EQ(TimestampScalar(1, type3), *s);
  ASSERT_OK_AND_ASSIGN(s, Scalar::Parse(type3, epoch_plus_1s));
  ASSERT_EQ(TimestampScalar(1000 * 1000, type3), *s);

  ASSERT_OK_AND_ASSIGN(s, MakeScalar(type4, int64_t(1)));
  ASSERT_EQ(TimestampScalar(1, type4), *s);
  ASSERT_OK_AND_ASSIGN(s, Scalar::Parse(type4, epoch_plus_1s));
  ASSERT_EQ(TimestampScalar(1000 * 1000 * 1000, type4), *s);
}

TEST(TestTimestampScalars, Cast) {
  auto convert = [](TimeUnit::type in, TimeUnit::type out, int64_t value) -> int64_t {
    auto scalar =
        TimestampScalar(value, timestamp(in)).CastTo(timestamp(out)).ValueOrDie();
    return internal::checked_pointer_cast<TimestampScalar>(scalar)->value;
  };

  EXPECT_EQ(convert(TimeUnit::SECOND, TimeUnit::MILLI, 1), 1000);
  EXPECT_EQ(convert(TimeUnit::SECOND, TimeUnit::NANO, 1), 1000000000);

  EXPECT_EQ(convert(TimeUnit::NANO, TimeUnit::MICRO, 1234), 1);
  EXPECT_EQ(convert(TimeUnit::MICRO, TimeUnit::MILLI, 4567), 4);
}

TEST(TestDurationScalars, Basics) {
  auto type1 = duration(TimeUnit::MILLI);
  auto type2 = duration(TimeUnit::SECOND);

  int64_t val1 = 1;
  int64_t val2 = 2;
  DurationScalar ts_val1(val1, type1);
  DurationScalar ts_val2(val2, type2);
  DurationScalar ts_null(type1);
  ASSERT_EQ(val1, ts_val1.value);

  ASSERT_TRUE(ts_val1.type->Equals(*type1));
  ASSERT_TRUE(ts_val2.type->Equals(*type2));
  ASSERT_TRUE(ts_val1.is_valid);
  ASSERT_FALSE(ts_null.is_valid);
  ASSERT_TRUE(ts_null.type->Equals(*type1));

  ASSERT_NE(ts_val1, ts_val2);
  ASSERT_NE(ts_val1, ts_null);
  ASSERT_NE(ts_val2, ts_null);
}

TEST(TestMonthIntervalScalars, Basics) {
  auto type = month_interval();

  int32_t val1 = 1;
  int32_t val2 = 2;
  MonthIntervalScalar ts_val1(val1);
  MonthIntervalScalar ts_val2(val2);
  MonthIntervalScalar ts_null;
  ASSERT_EQ(val1, ts_val1.value);

  ASSERT_TRUE(ts_val1.type->Equals(*type));
  ASSERT_TRUE(ts_val2.type->Equals(*type));
  ASSERT_TRUE(ts_val1.is_valid);
  ASSERT_FALSE(ts_null.is_valid);
  ASSERT_TRUE(ts_null.type->Equals(*type));

  ASSERT_NE(ts_val1, ts_val2);
  ASSERT_NE(ts_val1, ts_null);
  ASSERT_NE(ts_val2, ts_null);
}

TEST(TestDayTimeIntervalScalars, Basics) {
  auto type = day_time_interval();

  DayTimeIntervalType::DayMilliseconds val1 = {1, 1};
  DayTimeIntervalType::DayMilliseconds val2 = {2, 2};
  DayTimeIntervalScalar ts_val1(val1);
  DayTimeIntervalScalar ts_val2(val2);
  DayTimeIntervalScalar ts_null;
  ASSERT_EQ(val1, ts_val1.value);

  ASSERT_TRUE(ts_val1.type->Equals(*type));
  ASSERT_TRUE(ts_val2.type->Equals(*type));
  ASSERT_TRUE(ts_val1.is_valid);
  ASSERT_FALSE(ts_null.is_valid);
  ASSERT_TRUE(ts_null.type->Equals(*type));

  ASSERT_NE(ts_val1, ts_val2);
  ASSERT_NE(ts_val1, ts_null);
  ASSERT_NE(ts_val2, ts_null);
}

// TODO test HalfFloatScalar

TYPED_TEST(TestNumericScalar, Cast) {
  auto type = TypeTraits<TypeParam>::type_singleton();

  for (util::string_view repr : {"0", "1", "3"}) {
    std::shared_ptr<Scalar> scalar;
    ASSERT_OK_AND_ASSIGN(scalar, Scalar::Parse(type, repr));

    // cast to and from other numeric scalars
    for (auto other_type : {float32(), int8(), int64(), uint32()}) {
      std::shared_ptr<Scalar> other_scalar;
      ASSERT_OK_AND_ASSIGN(other_scalar, Scalar::Parse(other_type, repr));

      ASSERT_OK_AND_ASSIGN(auto cast_to_other, scalar->CastTo(other_type))
      ASSERT_EQ(*cast_to_other, *other_scalar);

      ASSERT_OK_AND_ASSIGN(auto cast_from_other, other_scalar->CastTo(type))
      ASSERT_EQ(*cast_from_other, *scalar);
    }

    ASSERT_OK_AND_ASSIGN(auto cast_from_string,
                         StringScalar(repr.to_string()).CastTo(type));
    ASSERT_EQ(*cast_from_string, *scalar);

    if (is_integer_type<TypeParam>::value) {
      ASSERT_OK_AND_ASSIGN(auto cast_to_string, scalar->CastTo(utf8()));
      ASSERT_EQ(
          util::string_view(*checked_cast<const StringScalar&>(*cast_to_string).value),
          repr);
    }
  }
}

}  // namespace arrow
