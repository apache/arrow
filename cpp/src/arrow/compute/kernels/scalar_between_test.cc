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

#include <algorithm>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/string_view.h"

namespace arrow {

using internal::BitmapReader;

namespace compute {

using util::string_view;

template <typename ArrowType>
static void ValidateBetween(const Datum& val, const Datum& lhs, const Datum& rhs,
                            const Datum& expected) {
  ASSERT_OK_AND_ASSIGN(Datum result, Between(val, lhs, rhs));
  AssertArraysEqual(*expected.make_array(), *result.make_array(),
                    /*verbose=*/true);
}

template <typename ArrowType>
static void ValidateBetween(const char* value_str, const Datum& lhs, const Datum& rhs,
                            const char* expected_str) {
  auto value = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), value_str);
  auto expected = ArrayFromJSON(TypeTraits<BooleanType>::type_singleton(), expected_str);
  ValidateBetween<ArrowType>(value, lhs, rhs, expected);
}

template <>
void ValidateBetween<StringType>(const char* value_str, const Datum& lhs,
                                 const Datum& rhs, const char* expected_str) {
  auto value = ArrayFromJSON(utf8(), value_str);
  auto expected = ArrayFromJSON(TypeTraits<BooleanType>::type_singleton(), expected_str);
  ValidateBetween<StringType>(value, lhs, rhs, expected);
}

template <typename ArrowType>
class TestNumericBetweenKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestNumericBetweenKernel, NumericArrowTypes);
TYPED_TEST(TestNumericBetweenKernel, SimpleBetweenArrayScalarScalar) {
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using CType = typename TypeTraits<TypeParam>::CType;

  Datum zero(std::make_shared<ScalarType>(CType(0)));
  Datum four(std::make_shared<ScalarType>(CType(4)));
  ValidateBetween<TypeParam>("[]", zero, four, "[]");
  ValidateBetween<TypeParam>("[null]", zero, four, "[null]");
  ValidateBetween<TypeParam>("[0,0,1,1,2,2]", zero, four, "[0,0,1,1,1,1]");
  ValidateBetween<TypeParam>("[0,1,2,3,4,5]", zero, four, "[0,1,1,1,0,0]");
  ValidateBetween<TypeParam>("[5,4,3,2,1,0]", zero, four, "[0,0,1,1,1,0]");
  ValidateBetween<TypeParam>("[null,0,1,1]", zero, four, "[null,0,1,1]");
}

TEST(TestSimpleBetweenKernel, SimpleStringTest) {
  using ScalarType = typename TypeTraits<StringType>::ScalarType;
  auto l = Datum(std::make_shared<ScalarType>("abc"));
  auto r = Datum(std::make_shared<ScalarType>("zzz"));
  ValidateBetween<StringType>("[]", l, r, "[]");
  ValidateBetween<StringType>("[null]", l, r, "[null]");
  ValidateBetween<StringType>(R"(["aaa", "aaaa", "ccc", "z"])", l, r,
                              R"([false, false, true, true])");
  ValidateBetween<StringType>(R"(["a", "aaaa", "c", "z"])", l, r,
                              R"([false, false, true, true])");
  ValidateBetween<StringType>(R"(["a", "aaaa", "fff", "zzzz"])", l, r,
                              R"([false, false, true, false])");
  ValidateBetween<StringType>(R"(["abd", null, null, "zzx"])", l, r,
                              R"([true, null, null, true])");
}

TEST(TestSimpleBetweenKernel, SimpleTimestampTest) {
  using ScalarType = typename TypeTraits<TimestampType>::ScalarType;
  auto checkTimestampArray = [](std::shared_ptr<DataType> type, const char* input_str,
                                const Datum& lhs, const Datum& rhs,
                                const char* expected_str) {
    auto value = ArrayFromJSON(type, input_str);
    auto expected = ArrayFromJSON(boolean(), expected_str);
    ValidateBetween<TimestampType>(value, lhs, rhs, expected);
  };
  auto unit = TimeUnit::SECOND;
  auto l = Datum(std::make_shared<ScalarType>(923184000, timestamp(unit)));
  auto r = Datum(std::make_shared<ScalarType>(1602032602, timestamp(unit)));
  checkTimestampArray(timestamp(unit), "[]", l, r, "[]");
  checkTimestampArray(timestamp(unit), "[null]", l, r, "[null]");
  checkTimestampArray(timestamp(unit), R"(["1970-01-01","2000-02-29","1900-02-28"])", l,
                      r, "[false,true,false]");
  checkTimestampArray(timestamp(unit), R"(["1970-01-01","2000-02-29","2004-02-28"])", l,
                      r, "[false,true,true]");
  checkTimestampArray(timestamp(unit), R"(["2018-01-01","1999-04-04","1900-02-28"])", l,
                      r, "[true,false,false]");
}

TYPED_TEST(TestNumericBetweenKernel, SimpleBetweenArrayArrayArray) {

 ValidateBetween<TypeParam>(
      "[]", ArrayFromJSON(TypeTraits<TypeParam>::type_singleton(), "[]"),
      ArrayFromJSON(TypeTraits<TypeParam>::type_singleton(), "[]"), "[]");
  ValidateBetween<TypeParam>(
      "[null]", ArrayFromJSON(TypeTraits<TypeParam>::type_singleton(), "[null]"),
      ArrayFromJSON(TypeTraits<TypeParam>::type_singleton(), "[null]"), "[null]");
  ValidateBetween<TypeParam>(
      "[1,1,2,2,2]",
      ArrayFromJSON(TypeTraits<TypeParam>::type_singleton(), "[0,0,1,3,3]"),
      ArrayFromJSON(TypeTraits<TypeParam>::type_singleton(), "[10,10,2,5,5]"),
      "[true,true,false,false,false]");
  ValidateBetween<TypeParam>(
      "[1,1,2,2,2,2]",
      ArrayFromJSON(TypeTraits<TypeParam>::type_singleton(), "[0,0,1,null,3,3]"),
      ArrayFromJSON(TypeTraits<TypeParam>::type_singleton(), "[10,10,2,2,5,5]"),
      "[true,true,false,null,false,false]");
}

TEST(TestSimpleBetweenKernel, StringArrayArrayArrayTest) {
  ValidateBetween<StringType>(
      R"(["david","hello","world"])",
      ArrayFromJSON(TypeTraits<StringType>::type_singleton(), R"(["adam","hi","whirl"])"),
      ArrayFromJSON(TypeTraits<StringType>::type_singleton(),
                    R"(["robert","goeiemoreen","whirlwind"])"),
      "[true, false, false]");
  ValidateBetween<StringType>(
      R"(["x","a","f"])",
      ArrayFromJSON(TypeTraits<StringType>::type_singleton(), R"(["w","a","e"])"),
      ArrayFromJSON(TypeTraits<StringType>::type_singleton(), R"(["z","a","g"])"),
      "[true, false, true]");
  ValidateBetween<StringType>(
      R"(["block","bit","binary"])",
      ArrayFromJSON(TypeTraits<StringType>::type_singleton(),
                    R"(["bit","nibble","ternary"])"),
      ArrayFromJSON(TypeTraits<StringType>::type_singleton(), R"(["word","d","xyz"])"),
      "[true, false, false]");
  ValidateBetween<StringType>(
      R"(["Ayumi","アユミ","王梦莹"])",
      ArrayFromJSON(TypeTraits<StringType>::type_singleton(),
                    R"(["たなか","あゆみ","歩美"])"),
      ArrayFromJSON(TypeTraits<StringType>::type_singleton(), R"(["李平之","田中","たなか"])"),
      "[false, true, false]");
}

TEST(TestSimpleBetweenKernel, TimestampArrayArrayArrayTest) {
  auto checkTimestampArray = [](std::shared_ptr<DataType> type, const char* input_str,
                                const char* lhs, const char* rhs,
                                const char* expected_str) {
    auto value = ArrayFromJSON(type, input_str);
    auto left = ArrayFromJSON(type, lhs);
    auto right = ArrayFromJSON(type, rhs);
    auto expected = ArrayFromJSON(boolean(), expected_str);
    ValidateBetween<TimestampType>(value, left, right, expected);
  };
  auto unit = TimeUnit::SECOND;
  checkTimestampArray(timestamp(unit), R"(["1970-01-01","2000-02-29","1900-02-28"])",
                      R"(["1970-01-01","2000-02-26","1900-02-28"])",
                      R"(["1970-01-01","2000-03-15","1900-02-28"])",
                      "[false,true,false]");
  checkTimestampArray(timestamp(unit), R"(["1970-01-01","2000-02-29","2004-02-28"])",
                      R"(["1970-05-01","2000-01-26","1900-02-27"])",
                      R"(["1970-01-01","2000-03-15","2020-02-28"])", "[false,true,true]");
  checkTimestampArray(timestamp(unit), R"(["2018-01-01","1999-04-04","1900-02-28"])",
                      R"(["1970-05-01","2000-01-26","1900-02-27"])",
                      R"(["2019-01-01","2000-03-15","1900-02-28"])",
                      "[true,false,false]");
}

}  // namespace compute
}  // namespace arrow
