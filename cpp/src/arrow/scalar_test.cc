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
#include <unordered_set>
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
using internal::checked_pointer_cast;

TEST(TestNullScalar, Basics) {
  NullScalar scalar;
  ASSERT_FALSE(scalar.is_valid);
  ASSERT_TRUE(scalar.type->Equals(*null()));

  ASSERT_OK_AND_ASSIGN(auto arr, MakeArrayOfNull(null(), 1));
  ASSERT_OK_AND_ASSIGN(auto first, arr->GetScalar(0));
  ASSERT_TRUE(first->Equals(scalar));
}

template <typename T>
class TestNumericScalar : public ::testing::Test {
 public:
  TestNumericScalar() = default;
};

TYPED_TEST_SUITE(TestNumericScalar, NumericArrowTypes);

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

  // test Array.GetScalar
  auto arr = ArrayFromJSON(expected_type, "[null, 1, 2]");
  ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(0));
  ASSERT_OK_AND_ASSIGN(auto one, arr->GetScalar(1));
  ASSERT_OK_AND_ASSIGN(auto two, arr->GetScalar(2));
  ASSERT_TRUE(null->Equals(*null_value));
  ASSERT_TRUE(one->Equals(ScalarType(1)));
  ASSERT_FALSE(one->Equals(ScalarType(2)));
  ASSERT_TRUE(two->Equals(ScalarType(2)));
  ASSERT_FALSE(two->Equals(ScalarType(3)));
}

TYPED_TEST(TestNumericScalar, Hashing) {
  using T = typename TypeParam::c_type;
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;

  std::unordered_set<std::shared_ptr<Scalar>, Scalar::Hash, Scalar::PtrsEqual> set;
  for (T i = 0; i < 10; ++i) {
    set.emplace(std::make_shared<ScalarType>(i));
  }

  for (T i = 0; i < 10; ++i) {
    ASSERT_FALSE(set.emplace(std::make_shared<ScalarType>(i)).second);
  }
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

TEST(TestDecimalScalar, Basics) {
  auto ty = decimal(3, 2);
  auto pi = Decimal128Scalar(Decimal128("3.14"), ty);
  auto null = MakeNullScalar(ty);

  ASSERT_EQ(pi.value, Decimal128("3.14"));

  // test Array.GetScalar
  auto arr = ArrayFromJSON(ty, "[null, \"3.14\"]");
  ASSERT_OK_AND_ASSIGN(auto first, arr->GetScalar(0));
  ASSERT_OK_AND_ASSIGN(auto second, arr->GetScalar(1));
  ASSERT_TRUE(first->Equals(null));
  ASSERT_FALSE(first->Equals(pi));
  ASSERT_TRUE(second->Equals(pi));
  ASSERT_FALSE(second->Equals(null));
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

  // test Array.GetScalar
  auto arr = ArrayFromJSON(binary(), "[null, \"one\", \"two\"]");
  ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(0));
  ASSERT_OK_AND_ASSIGN(auto one, arr->GetScalar(1));
  ASSERT_OK_AND_ASSIGN(auto two, arr->GetScalar(2));
  ASSERT_TRUE(null->Equals(null_value));
  ASSERT_TRUE(one->Equals(BinaryScalar(Buffer::FromString("one"))));
  ASSERT_TRUE(two->Equals(BinaryScalar(Buffer::FromString("two"))));
  ASSERT_FALSE(two->Equals(BinaryScalar(Buffer::FromString("else"))));
}

TEST(TestStringScalar, MakeScalar) {
  auto three = MakeScalar("three");
  ASSERT_EQ(StringScalar("three"), *three);

  ASSERT_OK_AND_ASSIGN(three, MakeScalar(utf8(), Buffer::FromString("three")));
  ASSERT_EQ(StringScalar("three"), *three);

  ASSERT_OK_AND_ASSIGN(three, Scalar::Parse(utf8(), "three"));
  ASSERT_EQ(StringScalar("three"), *three);

  // test Array.GetScalar
  auto arr = ArrayFromJSON(utf8(), R"([null, "one", "two"])");
  ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(0));
  ASSERT_OK_AND_ASSIGN(auto one, arr->GetScalar(1));
  ASSERT_OK_AND_ASSIGN(auto two, arr->GetScalar(2));
  ASSERT_TRUE(null->Equals(MakeNullScalar(utf8())));
  ASSERT_TRUE(one->Equals(StringScalar("one")));
  ASSERT_TRUE(two->Equals(StringScalar("two")));
  ASSERT_FALSE(two->Equals(Int64Scalar(1)));
}

TEST(TestFixedSizeBinaryScalar, Basics) {
  std::string data = "test data";
  auto buf = std::make_shared<Buffer>(data);

  auto ex_type = fixed_size_binary(9);

  FixedSizeBinaryScalar value(buf, ex_type);
  ASSERT_TRUE(value.value->Equals(*buf));
  ASSERT_TRUE(value.is_valid);
  ASSERT_TRUE(value.type->Equals(*ex_type));

  // test Array.GetScalar
  auto ty = fixed_size_binary(3);
  auto arr = ArrayFromJSON(ty, R"([null, "one", "two"])");
  ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(0));
  ASSERT_OK_AND_ASSIGN(auto one, arr->GetScalar(1));
  ASSERT_OK_AND_ASSIGN(auto two, arr->GetScalar(2));
  ASSERT_TRUE(null->Equals(MakeNullScalar(ty)));
  ASSERT_TRUE(one->Equals(FixedSizeBinaryScalar(Buffer::FromString("one"), ty)));
  ASSERT_TRUE(two->Equals(FixedSizeBinaryScalar(Buffer::FromString("two"), ty)));
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

  // test Array.GetScalar
  for (auto ty : {date32(), date64()}) {
    auto arr = ArrayFromJSON(ty, "[5, null, 42]");
    ASSERT_OK_AND_ASSIGN(auto first, arr->GetScalar(0));
    ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(1));
    ASSERT_OK_AND_ASSIGN(auto last, arr->GetScalar(2));
    ASSERT_TRUE(null->Equals(MakeNullScalar(ty)));
    ASSERT_TRUE(first->Equals(MakeScalar(ty, 5).ValueOrDie()));
    ASSERT_TRUE(last->Equals(MakeScalar(ty, 42).ValueOrDie()));
    ASSERT_FALSE(last->Equals(MakeScalar("string")));
  }
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

  // test Array.GetScalar
  for (auto ty : {type1, type2, type3, type4}) {
    auto arr = ArrayFromJSON(ty, "[5, null, 42]");
    ASSERT_OK_AND_ASSIGN(auto first, arr->GetScalar(0));
    ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(1));
    ASSERT_OK_AND_ASSIGN(auto last, arr->GetScalar(2));
    ASSERT_TRUE(null->Equals(MakeNullScalar(ty)));
    ASSERT_TRUE(first->Equals(MakeScalar(ty, 5).ValueOrDie()));
    ASSERT_TRUE(last->Equals(MakeScalar(ty, 42).ValueOrDie()));
    ASSERT_FALSE(last->Equals(MakeScalar("string")));
  }
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

  // test Array.GetScalar
  for (auto ty : {type1, type2}) {
    auto arr = ArrayFromJSON(ty, "[5, null, 42]");
    ASSERT_OK_AND_ASSIGN(auto first, arr->GetScalar(0));
    ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(1));
    ASSERT_OK_AND_ASSIGN(auto last, arr->GetScalar(2));
    ASSERT_TRUE(null->Equals(MakeNullScalar(ty)));
    ASSERT_TRUE(first->Equals(MakeScalar(ty, 5).ValueOrDie()));
    ASSERT_TRUE(last->Equals(MakeScalar(ty, 42).ValueOrDie()));
    ASSERT_FALSE(last->Equals(MakeScalar(int64(), 42).ValueOrDie()));
  }
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

  ASSERT_OK_AND_ASSIGN(auto str,
                       TimestampScalar(1024, timestamp(TimeUnit::MILLI)).CastTo(utf8()));
  EXPECT_EQ(*str, StringScalar("1024"));
  ASSERT_OK_AND_ASSIGN(auto i64,
                       TimestampScalar(1024, timestamp(TimeUnit::MILLI)).CastTo(int64()));
  EXPECT_EQ(*i64, Int64Scalar(1024));

  constexpr int64_t kMillisecondsInDay = 86400000;
  ASSERT_OK_AND_ASSIGN(
      auto d64, TimestampScalar(1024 * kMillisecondsInDay + 3, timestamp(TimeUnit::MILLI))
                    .CastTo(date64()));
  EXPECT_EQ(*d64, Date64Scalar(1024 * kMillisecondsInDay));
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

  // test Array.GetScalar
  for (auto ty : {type1, type2}) {
    auto arr = ArrayFromJSON(ty, "[5, null, 42]");
    ASSERT_OK_AND_ASSIGN(auto first, arr->GetScalar(0));
    ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(1));
    ASSERT_OK_AND_ASSIGN(auto last, arr->GetScalar(2));
    ASSERT_TRUE(null->Equals(MakeNullScalar(ty)));
    ASSERT_TRUE(first->Equals(MakeScalar(ty, 5).ValueOrDie()));
    ASSERT_TRUE(last->Equals(MakeScalar(ty, 42).ValueOrDie()));
  }
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

  // test Array.GetScalar
  auto arr = ArrayFromJSON(type, "[5, null, 42]");
  ASSERT_OK_AND_ASSIGN(auto first, arr->GetScalar(0));
  ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(1));
  ASSERT_OK_AND_ASSIGN(auto last, arr->GetScalar(2));
  ASSERT_TRUE(null->Equals(MakeNullScalar(type)));
  ASSERT_TRUE(first->Equals(MakeScalar(type, 5).ValueOrDie()));
  ASSERT_TRUE(last->Equals(MakeScalar(type, 42).ValueOrDie()));
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

  // test Array.GetScalar
  auto arr = ArrayFromJSON(type, "[[2, 2], null]");
  ASSERT_OK_AND_ASSIGN(auto first, arr->GetScalar(0));
  ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(1));
  ASSERT_TRUE(null->Equals(ts_null));
  ASSERT_TRUE(first->Equals(ts_val2));
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

TEST(TestMapScalar, Basics) {
  auto value =
      ArrayFromJSON(struct_({field("key", utf8(), false), field("value", int8())}),
                    R"([{"key": "a", "value": 1}, {"key": "b", "value": 2}])");
  auto scalar = MapScalar(value);

  auto expected_scalar_type = map(utf8(), field("value", int8()));

  ASSERT_TRUE(scalar.type->Equals(expected_scalar_type));
  ASSERT_TRUE(value->Equals(scalar.value));
}

TEST(TestStructScalar, FieldAccess) {
  StructScalar abc({MakeScalar(true), MakeNullScalar(int32()), MakeScalar("hello"),
                    MakeNullScalar(int64())},
                   struct_({field("a", boolean()), field("b", int32()),
                            field("b", utf8()), field("d", int64())}));

  ASSERT_OK_AND_ASSIGN(auto a, abc.field("a"));
  AssertScalarsEqual(*a, *abc.value[0]);

  ASSERT_RAISES(Invalid, abc.field("b").status());

  ASSERT_OK_AND_ASSIGN(auto b, abc.field(1));
  AssertScalarsEqual(*b, *abc.value[1]);

  ASSERT_RAISES(Invalid, abc.field(5).status());
  ASSERT_RAISES(Invalid, abc.field("c").status());

  ASSERT_OK_AND_ASSIGN(auto d, abc.field("d"));
  ASSERT_TRUE(d->Equals(MakeNullScalar(int64())));
  ASSERT_FALSE(d->Equals(MakeScalar(int64(), 12).ValueOrDie()));
}

TEST(TestDictionaryScalar, Basics) {
  for (auto index_ty : all_dictionary_index_types()) {
    auto ty = dictionary(index_ty, utf8());
    auto dict = ArrayFromJSON(utf8(), R"(["alpha", "beta", "gamma"])");

    DictionaryScalar::ValueType alpha;
    ASSERT_OK_AND_ASSIGN(alpha.index, MakeScalar(index_ty, 0));
    alpha.dictionary = dict;

    DictionaryScalar::ValueType gamma;
    ASSERT_OK_AND_ASSIGN(gamma.index, MakeScalar(index_ty, 2));
    gamma.dictionary = dict;

    auto scalar_null = MakeNullScalar(ty);
    auto scalar_alpha = DictionaryScalar(alpha, ty);
    auto scalar_gamma = DictionaryScalar(gamma, ty);

    ASSERT_OK_AND_ASSIGN(
        auto encoded_null,
        checked_cast<const DictionaryScalar&>(*scalar_null).GetEncodedValue());
    ASSERT_TRUE(encoded_null->Equals(MakeNullScalar(utf8())));

    ASSERT_OK_AND_ASSIGN(
        auto encoded_alpha,
        checked_cast<const DictionaryScalar&>(scalar_alpha).GetEncodedValue());
    ASSERT_TRUE(encoded_alpha->Equals(MakeScalar("alpha")));

    ASSERT_OK_AND_ASSIGN(
        auto encoded_gamma,
        checked_cast<const DictionaryScalar&>(scalar_gamma).GetEncodedValue());
    ASSERT_TRUE(encoded_gamma->Equals(MakeScalar("gamma")));

    // test Array.GetScalar
    DictionaryArray arr(ty, ArrayFromJSON(index_ty, "[2, 0, 1, null]"), dict);
    ASSERT_OK_AND_ASSIGN(auto first, arr.GetScalar(0));
    ASSERT_OK_AND_ASSIGN(auto second, arr.GetScalar(1));
    ASSERT_OK_AND_ASSIGN(auto last, arr.GetScalar(3));

    ASSERT_TRUE(first->Equals(scalar_gamma));
    ASSERT_TRUE(second->Equals(scalar_alpha));
    ASSERT_TRUE(last->Equals(scalar_null));
  }
}

TEST(TestDictionaryScalar, Cast) {
  for (auto index_ty : all_dictionary_index_types()) {
    auto ty = dictionary(index_ty, utf8());
    auto dict = checked_pointer_cast<StringArray>(
        ArrayFromJSON(utf8(), R"(["alpha", "beta", "gamma"])"));

    for (int64_t i = 0; i < dict->length(); ++i) {
      auto alpha = MakeScalar(dict->GetString(i));
      ASSERT_OK_AND_ASSIGN(auto cast_alpha, alpha->CastTo(ty));
      ASSERT_OK_AND_ASSIGN(
          auto roundtripped_alpha,
          checked_cast<const DictionaryScalar&>(*cast_alpha).GetEncodedValue());

      ASSERT_OK_AND_ASSIGN(auto i_scalar, MakeScalar(index_ty, i));
      auto alpha_dict = DictionaryScalar({i_scalar, dict}, ty);
      ASSERT_OK_AND_ASSIGN(
          auto encoded_alpha,
          checked_cast<const DictionaryScalar&>(alpha_dict).GetEncodedValue());

      AssertScalarsEqual(*alpha, *roundtripped_alpha);
      AssertScalarsEqual(*encoded_alpha, *roundtripped_alpha);

      // dictionaries differ, though encoded values are identical
      ASSERT_FALSE(alpha_dict.Equals(cast_alpha));
    }
  }
}

TEST(TestSparseUnionScalar, Basics) {
  auto ty = sparse_union({field("string", utf8()), field("number", uint64())});

  auto alpha = MakeScalar("alpha");
  auto beta = MakeScalar("beta");
  ASSERT_OK_AND_ASSIGN(auto two, MakeScalar(uint64(), 2));

  auto scalar_alpha = SparseUnionScalar(alpha, ty);
  auto scalar_beta = SparseUnionScalar(beta, ty);
  auto scalar_two = SparseUnionScalar(two, ty);

  // test Array.GetScalar
  std::vector<std::shared_ptr<Array>> children{
      ArrayFromJSON(utf8(), R"(["alpha", "", "beta", null, "gamma"])"),
      ArrayFromJSON(uint64(), "[1, 2, 11, 22, null]")};

  auto type_ids = ArrayFromJSON(int8(), "[0, 1, 0, 0, 1]");
  SparseUnionArray arr(ty, 5, children, type_ids->data()->buffers[1]);
  ASSERT_OK(arr.ValidateFull());

  ASSERT_OK_AND_ASSIGN(auto first, arr.GetScalar(0));
  ASSERT_TRUE(first->Equals(scalar_alpha));

  const auto& first_as_union = checked_cast<const SparseUnionScalar&>(*first);
  ASSERT_TRUE(first_as_union.is_valid);
  ASSERT_TRUE(first_as_union.value->Equals(alpha));

  ASSERT_OK_AND_ASSIGN(auto second, arr.GetScalar(1));
  ASSERT_TRUE(second->Equals(scalar_two));

  const auto& second_as_union = checked_cast<const SparseUnionScalar&>(*second);
  ASSERT_TRUE(second_as_union.is_valid);
  ASSERT_TRUE(second_as_union.value->Equals(two));

  ASSERT_OK_AND_ASSIGN(auto third, arr.GetScalar(2));
  ASSERT_TRUE(third->Equals(scalar_beta));

  const auto& third_as_union = checked_cast<const SparseUnionScalar&>(*third);
  ASSERT_TRUE(third_as_union.is_valid);
  ASSERT_TRUE(third_as_union.value->Equals(beta));

  ASSERT_OK_AND_ASSIGN(auto fourth, arr.GetScalar(3));
  ASSERT_TRUE(fourth->Equals(MakeNullScalar(ty)));

  const auto& fourth_as_union = checked_cast<const SparseUnionScalar&>(*fourth);
  ASSERT_FALSE(fourth_as_union.is_valid);

  ASSERT_OK_AND_ASSIGN(auto fifth, arr.GetScalar(4));
  ASSERT_TRUE(fifth->Equals(MakeNullScalar(ty)));

  const auto& fifth_as_union = checked_cast<const SparseUnionScalar&>(*fifth);
  ASSERT_FALSE(fifth_as_union.is_valid);
}

TEST(TestDenseUnionScalar, Basics) {
  auto ty = dense_union({field("string", utf8()), field("number", uint64())});

  auto alpha = MakeScalar("alpha");
  auto beta = MakeScalar("beta");
  ASSERT_OK_AND_ASSIGN(auto two, MakeScalar(uint64(), 2));
  ASSERT_OK_AND_ASSIGN(auto three, MakeScalar(uint64(), 3));

  auto scalar_alpha = DenseUnionScalar(alpha, ty);
  auto scalar_beta = DenseUnionScalar(beta, ty);
  auto scalar_two = DenseUnionScalar(two, ty);
  auto scalar_three = DenseUnionScalar(three, ty);

  // test Array.GetScalar
  std::vector<std::shared_ptr<Array>> children = {
      ArrayFromJSON(utf8(), R"(["alpha", "beta", null])"),
      ArrayFromJSON(uint64(), "[2, 3]")};

  auto type_ids = ArrayFromJSON(int8(), "[0, 1, 0, 0, 1]");
  auto offsets = ArrayFromJSON(int32(), "[0, 0, 1, 2, 1]");
  DenseUnionArray arr(ty, 5, children, type_ids->data()->buffers[1],
                      offsets->data()->buffers[1]);
  ASSERT_OK(arr.ValidateFull());

  ASSERT_OK_AND_ASSIGN(auto first, arr.GetScalar(0));
  ASSERT_TRUE(first->Equals(scalar_alpha));

  const auto& first_as_union = checked_cast<const DenseUnionScalar&>(*first);
  ASSERT_TRUE(first_as_union.value->Equals(alpha));
  ASSERT_TRUE(first_as_union.is_valid);

  ASSERT_OK_AND_ASSIGN(auto second, arr.GetScalar(1));
  ASSERT_TRUE(second->Equals(scalar_two));

  const auto& second_as_union = checked_cast<const DenseUnionScalar&>(*second);
  ASSERT_TRUE(second_as_union.value->Equals(two));
  ASSERT_TRUE(second_as_union.is_valid);

  ASSERT_OK_AND_ASSIGN(auto third, arr.GetScalar(2));
  ASSERT_TRUE(third->Equals(scalar_beta));

  const auto& third_as_union = checked_cast<const DenseUnionScalar&>(*third);
  ASSERT_TRUE(third_as_union.value->Equals(beta));
  ASSERT_TRUE(third_as_union.is_valid);

  ASSERT_OK_AND_ASSIGN(auto fourth, arr.GetScalar(3));
  ASSERT_TRUE(fourth->Equals(MakeNullScalar(ty)));

  const auto& fourth_as_union = checked_cast<const DenseUnionScalar&>(*fourth);
  ASSERT_FALSE(fourth_as_union.is_valid);

  ASSERT_OK_AND_ASSIGN(auto fifth, arr.GetScalar(4));
  ASSERT_TRUE(fifth->Equals(scalar_three));

  const auto& fifth_as_union = checked_cast<const DenseUnionScalar&>(*fifth);
  ASSERT_TRUE(fifth_as_union.value->Equals(three));
  ASSERT_TRUE(fifth_as_union.is_valid);
}

}  // namespace arrow
