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

#include <numeric>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/compute/registry.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace compute {

// Helper that combines a dictionary and the value type so it can
// later be used with DictArrayFromJSON
struct JsonDict {
  std::shared_ptr<DataType> type;
  std::string value;
};

// Helper that makes a list of dictionary indices
std::shared_ptr<Array> MakeListOfDict(const std::shared_ptr<Array>& indices,
                                      const std::shared_ptr<Array>& backing_array) {
  EXPECT_OK_AND_ASSIGN(auto result, ListArray::FromArrays(*indices, *backing_array));
  return result;
}

void CheckIfElseOutput(const Datum& cond, const Datum& left, const Datum& right,
                       const Datum& expected) {
  ASSERT_OK_AND_ASSIGN(Datum datum_out, IfElse(cond, left, right));
  if (datum_out.is_array()) {
    std::shared_ptr<Array> result = datum_out.make_array();
    ValidateOutput(*result);
    std::shared_ptr<Array> expected_ = expected.make_array();
    AssertArraysEqual(*expected_, *result, /*verbose=*/true);
  } else {  // expecting scalar
    const std::shared_ptr<Scalar>& result = datum_out.scalar();
    const std::shared_ptr<Scalar>& expected_ = expected.scalar();
    AssertScalarsEqual(*expected_, *result, /*verbose=*/true);
  }
}

class TestIfElseKernel : public ::testing::Test {};

template <typename Type>
class TestIfElsePrimitive : public ::testing::Test {};

// There are a lot of tests here if we cover all the types and it gets slow on valgrind
// so we overrdie the standard type sets with a smaller range
#ifdef ARROW_VALGRIND
using IfElseNumericBasedTypes =
    ::testing::Types<UInt32Type, FloatType, Date32Type, Time32Type, TimestampType,
                     MonthIntervalType>;
using BaseBinaryArrowTypes = ::testing::Types<BinaryType>;
using ListArrowTypes = ::testing::Types<ListType>;
using IntegralArrowTypes = ::testing::Types<Int32Type>;
#else
using IfElseNumericBasedTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, Date32Type, Date64Type,
                     Time32Type, Time64Type, TimestampType, MonthIntervalType>;
#endif

TYPED_TEST_SUITE(TestIfElsePrimitive, IfElseNumericBasedTypes);

TYPED_TEST(TestIfElsePrimitive, IfElseFixedSizeRand) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  auto type = default_type_instance<TypeParam>();

  random::RandomArrayGenerator rand(/*seed=*/0);
  int64_t len = 1000;

  // adding 64 consecutive 1's and 0's in the cond array to test all-true/ all-false
  // word code paths
  ASSERT_OK_AND_ASSIGN(auto temp1, MakeArrayFromScalar(BooleanScalar(true), 64));
  ASSERT_OK_AND_ASSIGN(auto temp2, MakeArrayFromScalar(BooleanScalar(false), 64));
  auto temp3 = rand.ArrayOf(boolean(), len - 64 * 2, /*null_probability=*/0.01);

  ASSERT_OK_AND_ASSIGN(auto concat, Concatenate({temp1, temp2, temp3}));
  auto cond = std::static_pointer_cast<BooleanArray>(concat);
  auto left = std::static_pointer_cast<ArrayType>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));
  auto right = std::static_pointer_cast<ArrayType>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));

  typename TypeTraits<TypeParam>::BuilderType builder(type, default_memory_pool());

  for (int64_t i = 0; i < len; ++i) {
    if (!cond->IsValid(i) || (cond->Value(i) && !left->IsValid(i)) ||
        (!cond->Value(i) && !right->IsValid(i))) {
      ASSERT_OK(builder.AppendNull());
      continue;
    }

    if (cond->Value(i)) {
      ASSERT_OK(builder.Append(left->Value(i)));
    } else {
      ASSERT_OK(builder.Append(right->Value(i)));
    }
  }
  ASSERT_OK_AND_ASSIGN(auto expected_data, builder.Finish());

  CheckIfElseOutput(cond, left, right, expected_data);
}

void CheckWithDifferentShapes(const std::shared_ptr<Array>& cond,
                              const std::shared_ptr<Array>& left,
                              const std::shared_ptr<Array>& right,
                              const std::shared_ptr<Array>& expected) {
  // this will check for whole arrays, every scalar at i'th index and slicing (offset)
  CheckScalar("if_else", {cond, left, right}, expected);

  auto len = left->length();
  std::vector<int64_t> array_indices = {-1};  // sentinel for make_input
  std::vector<int64_t> scalar_indices(len);
  std::iota(scalar_indices.begin(), scalar_indices.end(), 0);
  auto make_input = [&](const std::shared_ptr<Array>& array, int64_t index, Datum* input,
                        Datum* input_broadcast, std::string* trace) {
    if (index >= 0) {
      // Use scalar from array[index] as input; broadcast scalar for computing expected
      // result
      ASSERT_OK_AND_ASSIGN(auto scalar, array->GetScalar(index));
      *trace += "@" + std::to_string(index) + "=" + scalar->ToString();
      *input = std::move(scalar);
      ASSERT_OK_AND_ASSIGN(*input_broadcast, MakeArrayFromScalar(*input->scalar(), len));
    } else {
      // Use array as input
      *trace += "=Array";
      *input = *input_broadcast = array;
    }
  };

  enum { COND_SCALAR = 1, LEFT_SCALAR = 2, RIGHT_SCALAR = 4 };
  for (int mask = 1; mask <= (COND_SCALAR | LEFT_SCALAR | RIGHT_SCALAR); ++mask) {
    for (int64_t cond_idx : (mask & COND_SCALAR) ? scalar_indices : array_indices) {
      Datum cond_in, cond_bcast;
      std::string trace_cond = "Cond";
      make_input(cond, cond_idx, &cond_in, &cond_bcast, &trace_cond);

      for (int64_t left_idx : (mask & LEFT_SCALAR) ? scalar_indices : array_indices) {
        Datum left_in, left_bcast;
        std::string trace_left = "Left";
        make_input(left, left_idx, &left_in, &left_bcast, &trace_left);

        for (int64_t right_idx : (mask & RIGHT_SCALAR) ? scalar_indices : array_indices) {
          Datum right_in, right_bcast;
          std::string trace_right = "Right";
          make_input(right, right_idx, &right_in, &right_bcast, &trace_right);

          SCOPED_TRACE(trace_right);
          SCOPED_TRACE(trace_left);
          SCOPED_TRACE(trace_cond);

          Datum expected;
          ASSERT_OK_AND_ASSIGN(auto actual, IfElse(cond_in, left_in, right_in));
          if (mask == (COND_SCALAR | LEFT_SCALAR | RIGHT_SCALAR)) {
            const auto& scalar = cond_in.scalar_as<BooleanScalar>();
            if (scalar.is_valid) {
              expected = scalar.value ? left_in : right_in;
            } else {
              expected = MakeNullScalar(left_in.type());
            }
            if (!left_in.type()->Equals(*right_in.type())) {
              ASSERT_OK_AND_ASSIGN(expected,
                                   Cast(expected, CastOptions::Safe(actual.type())));
            }
          } else {
            ASSERT_OK_AND_ASSIGN(expected, IfElse(cond_bcast, left_bcast, right_bcast));
          }
          AssertDatumsEqual(expected, actual, /*verbose=*/true);
        }
      }
    }
  }  // for (mask)
}

TYPED_TEST(TestIfElsePrimitive, IfElseFixedSize) {
  auto type = default_type_instance<TypeParam>();

  if (std::is_same<TypeParam, Date64Type>::value) {
    CheckWithDifferentShapes(
        ArrayFromJSON(boolean(), "[true, true, true, false]"),
        ArrayFromJSON(type, "[86400000, 172800000, 259200000, 345600000]"),
        ArrayFromJSON(type, "[432000000, 518400000, 604800000, 691200000]"),
        ArrayFromJSON(type, "[86400000, 172800000, 259200000, 691200000]"));

    CheckWithDifferentShapes(
        ArrayFromJSON(boolean(), "[true, true, true, false]"),
        ArrayFromJSON(type, "[86400000, 172800000, 259200000, 345600000]"),
        ArrayFromJSON(type, "[432000000, 518400000, 604800000, null]"),
        ArrayFromJSON(type, "[86400000, 172800000, 259200000, null]"));

    CheckWithDifferentShapes(
        ArrayFromJSON(boolean(), "[true, true, true, false]"),
        ArrayFromJSON(type, "[86400000, 172800000, null, 345600000]"),
        ArrayFromJSON(type, "[432000000, 518400000, 604800000, null]"),
        ArrayFromJSON(type, "[86400000, 172800000, null, null]"));

    CheckWithDifferentShapes(
        ArrayFromJSON(boolean(), "[true, true, true, false]"),
        ArrayFromJSON(type, "[86400000, 172800000, null, 345600000]"),
        ArrayFromJSON(type, "[432000000, 518400000, 604800000, 691200000]"),
        ArrayFromJSON(type, "[86400000, 172800000, null, 691200000]"));

    CheckWithDifferentShapes(
        ArrayFromJSON(boolean(), "[null, true, true, false]"),
        ArrayFromJSON(type, "[86400000, 172800000, null, 345600000]"),
        ArrayFromJSON(type, "[432000000, 518400000, 604800000, 691200000]"),
        ArrayFromJSON(type, "[null, 172800000, null, 691200000]"));

    CheckWithDifferentShapes(
        ArrayFromJSON(boolean(), "[null, true, true, false]"),
        ArrayFromJSON(type, "[86400000, 172800000, null, 345600000]"),
        ArrayFromJSON(type, "[432000000, 518400000, 604800000, null]"),
        ArrayFromJSON(type, "[null, 172800000, null, null]"));

    CheckWithDifferentShapes(
        ArrayFromJSON(boolean(), "[null, true, true, false]"),
        ArrayFromJSON(type, "[86400000, 172800000, 259200000, 345600000]"),
        ArrayFromJSON(type, "[432000000, 518400000, 604800000, null]"),
        ArrayFromJSON(type, "[null, 172800000, 259200000, null]"));

    CheckWithDifferentShapes(
        ArrayFromJSON(boolean(), "[null, true, true, false]"),
        ArrayFromJSON(type, "[86400000, 172800000, 259200000, 345600000]"),
        ArrayFromJSON(type, "[432000000, 518400000, 604800000, 691200000]"),
        ArrayFromJSON(type, "[null, 172800000, 259200000, 691200000]"));
  } else {
    CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                             ArrayFromJSON(type, "[1, 2, 3, 4]"),
                             ArrayFromJSON(type, "[5, 6, 7, 8]"),
                             ArrayFromJSON(type, "[1, 2, 3, 8]"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                             ArrayFromJSON(type, "[1, 2, 3, 4]"),
                             ArrayFromJSON(type, "[5, 6, 7, null]"),
                             ArrayFromJSON(type, "[1, 2, 3, null]"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                             ArrayFromJSON(type, "[1, 2, null, 4]"),
                             ArrayFromJSON(type, "[5, 6, 7, null]"),
                             ArrayFromJSON(type, "[1, 2, null, null]"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                             ArrayFromJSON(type, "[1, 2, null, 4]"),
                             ArrayFromJSON(type, "[5, 6, 7, 8]"),
                             ArrayFromJSON(type, "[1, 2, null, 8]"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[null, true, true, false]"),
                             ArrayFromJSON(type, "[1, 2, null, 4]"),
                             ArrayFromJSON(type, "[5, 6, 7, 8]"),
                             ArrayFromJSON(type, "[null, 2, null, 8]"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[null, true, true, false]"),
                             ArrayFromJSON(type, "[1, 2, null, 4]"),
                             ArrayFromJSON(type, "[5, 6, 7, null]"),
                             ArrayFromJSON(type, "[null, 2, null, null]"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[null, true, true, false]"),
                             ArrayFromJSON(type, "[1, 2, 3, 4]"),
                             ArrayFromJSON(type, "[5, 6, 7, null]"),
                             ArrayFromJSON(type, "[null, 2, 3, null]"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[null, true, true, false]"),
                             ArrayFromJSON(type, "[1, 2, 3, 4]"),
                             ArrayFromJSON(type, "[5, 6, 7, 8]"),
                             ArrayFromJSON(type, "[null, 2, 3, 8]"));
  }
}

TEST_F(TestIfElseKernel, IfElseBoolean) {
  auto type = boolean();

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                           ArrayFromJSON(type, "[false, false, false, false]"),
                           ArrayFromJSON(type, "[true, true, true, true]"),
                           ArrayFromJSON(type, "[false, false, false, true]"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                           ArrayFromJSON(type, "[false, false, false, false]"),
                           ArrayFromJSON(type, "[true, true, true, null]"),
                           ArrayFromJSON(type, "[false, false, false, null]"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                           ArrayFromJSON(type, "[false, false, null, false]"),
                           ArrayFromJSON(type, "[true, true, true, null]"),
                           ArrayFromJSON(type, "[false, false, null, null]"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                           ArrayFromJSON(type, "[false, false, null, false]"),
                           ArrayFromJSON(type, "[true, true, true, true]"),
                           ArrayFromJSON(type, "[false, false, null, true]"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[null, true, true, false]"),
                           ArrayFromJSON(type, "[false, false, null, false]"),
                           ArrayFromJSON(type, "[true, true, true, true]"),
                           ArrayFromJSON(type, "[null, false, null, true]"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[null, true, true, false]"),
                           ArrayFromJSON(type, "[false, false, null, false]"),
                           ArrayFromJSON(type, "[true, true, true, null]"),
                           ArrayFromJSON(type, "[null, false, null, null]"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[null, true, true, false]"),
                           ArrayFromJSON(type, "[false, false, false, false]"),
                           ArrayFromJSON(type, "[true, true, true, null]"),
                           ArrayFromJSON(type, "[null, false, false, null]"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[null, true, true, false]"),
                           ArrayFromJSON(type, "[false, false, false, false]"),
                           ArrayFromJSON(type, "[true, true, true, true]"),
                           ArrayFromJSON(type, "[null, false, false, true]"));
}

TEST_F(TestIfElseKernel, IfElseBooleanRand) {
  auto type = boolean();
  random::RandomArrayGenerator rand(/*seed=*/0);
  int64_t len = 1000;
  auto cond = std::static_pointer_cast<BooleanArray>(
      rand.ArrayOf(boolean(), len, /*null_probability=*/0.01));
  auto left = std::static_pointer_cast<BooleanArray>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));
  auto right = std::static_pointer_cast<BooleanArray>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));

  BooleanBuilder builder;
  for (int64_t i = 0; i < len; ++i) {
    if (!cond->IsValid(i) || (cond->Value(i) && !left->IsValid(i)) ||
        (!cond->Value(i) && !right->IsValid(i))) {
      ASSERT_OK(builder.AppendNull());
      continue;
    }

    if (cond->Value(i)) {
      ASSERT_OK(builder.Append(left->Value(i)));
    } else {
      ASSERT_OK(builder.Append(right->Value(i)));
    }
  }
  ASSERT_OK_AND_ASSIGN(auto expected_data, builder.Finish());

  CheckIfElseOutput(cond, left, right, expected_data);
}

TEST_F(TestIfElseKernel, IfElseNull) {
  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[null, null, null, null]"),
                           ArrayFromJSON(null(), "[null, null, null, null]"),
                           ArrayFromJSON(null(), "[null, null, null, null]"),
                           ArrayFromJSON(null(), "[null, null, null, null]"));
}

TEST_F(TestIfElseKernel, IfElseMultiType) {
  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                           ArrayFromJSON(int32(), "[1, 2, 3, 4]"),
                           ArrayFromJSON(float32(), "[5, 6, 7, 8]"),
                           ArrayFromJSON(float32(), "[1, 2, 3, 8]"));
}

TEST_F(TestIfElseKernel, TimestampTypes) {
  for (const auto unit : TimeUnit::values()) {
    auto ty = timestamp(unit);
    CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                             ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                             ArrayFromJSON(ty, "[5, 6, 7, 8]"),
                             ArrayFromJSON(ty, "[1, 2, 3, 8]"));

    ty = timestamp(unit, "America/Phoenix");
    CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                             ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                             ArrayFromJSON(ty, "[5, 6, 7, 8]"),
                             ArrayFromJSON(ty, "[1, 2, 3, 8]"));
  }
}

TEST_F(TestIfElseKernel, TemporalTypes) {
  for (const auto& ty : TemporalTypes()) {
    if (ty->name() == "date64") {
      CheckWithDifferentShapes(
          ArrayFromJSON(boolean(), "[true, true, true, false]"),
          ArrayFromJSON(ty, "[86400000, 172800000, 259200000, 4]"),
          ArrayFromJSON(ty, "[5, 6, 7, 691200000]"),
          ArrayFromJSON(ty, "[86400000, 172800000, 259200000, 691200000]"));
    } else {
      CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                               ArrayFromJSON(ty, "[1, 2, 3, 4]"),
                               ArrayFromJSON(ty, "[5, 6, 7, 8]"),
                               ArrayFromJSON(ty, "[1, 2, 3, 8]"));
    }
  }
}

TEST_F(TestIfElseKernel, DayTimeInterval) {
  auto ty = day_time_interval();
  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[true, true, true, false]"),
      ArrayFromJSON(ty, "[[1, 2], [3, -4], [-5, 6], [-7, -8]]"),
      ArrayFromJSON(ty, "[[-9, -10], [11, -12], [-13, 14], [15, 16]]"),
      ArrayFromJSON(ty, "[[1, 2], [3, -4], [-5, 6], [15, 16]]"));
}

TEST_F(TestIfElseKernel, MonthDayNanoInterval) {
  auto ty = month_day_nano_interval();
  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[true, true, true, false]"),
      ArrayFromJSON(ty, "[[1, 2, -3], [3, -4, 5], [-5, 6, 7], [-7, -8, -9]]"),
      ArrayFromJSON(ty, "[[-9, -10, 11], [11, -12, 0], [-13, 14, -1], [15, 16, 2]]"),
      ArrayFromJSON(ty, "[[1, 2, -3], [3, -4, 5], [-5, 6, 7], [15, 16, 2]]"));
}

TEST_F(TestIfElseKernel, IfElseDispatchBest) {
  std::string name = "if_else";
  ASSERT_OK_AND_ASSIGN(auto function, GetFunctionRegistry()->GetFunction(name));
  CheckDispatchBest(name, {boolean(), int32(), int32()}, {boolean(), int32(), int32()});
  CheckDispatchBest(name, {boolean(), int32(), null()}, {boolean(), int32(), int32()});
  CheckDispatchBest(name, {boolean(), null(), int32()}, {boolean(), int32(), int32()});

  CheckDispatchBest(name, {boolean(), int32(), int8()}, {boolean(), int32(), int32()});
  CheckDispatchBest(name, {boolean(), int32(), int16()}, {boolean(), int32(), int32()});
  CheckDispatchBest(name, {boolean(), int32(), int32()}, {boolean(), int32(), int32()});
  CheckDispatchBest(name, {boolean(), int32(), int64()}, {boolean(), int64(), int64()});

  CheckDispatchBest(name, {boolean(), int32(), uint8()}, {boolean(), int32(), int32()});
  CheckDispatchBest(name, {boolean(), int32(), uint16()}, {boolean(), int32(), int32()});
  CheckDispatchBest(name, {boolean(), int32(), uint32()}, {boolean(), int64(), int64()});
  CheckDispatchBest(name, {boolean(), int32(), uint64()}, {boolean(), int64(), int64()});

  CheckDispatchBest(name, {boolean(), uint8(), uint8()}, {boolean(), uint8(), uint8()});
  CheckDispatchBest(name, {boolean(), uint8(), uint16()},
                    {boolean(), uint16(), uint16()});

  CheckDispatchBest(name, {boolean(), int32(), float32()},
                    {boolean(), float32(), float32()});
  CheckDispatchBest(name, {boolean(), float32(), int64()},
                    {boolean(), float32(), float32()});
  CheckDispatchBest(name, {boolean(), float64(), int32()},
                    {boolean(), float64(), float64()});

  CheckDispatchBest(name, {null(), uint8(), int8()}, {boolean(), int16(), int16()});

  CheckDispatchBest(name,
                    {boolean(), timestamp(TimeUnit::SECOND), timestamp(TimeUnit::MILLI)},
                    {boolean(), timestamp(TimeUnit::MILLI), timestamp(TimeUnit::MILLI)});
  CheckDispatchBest(name, {boolean(), date32(), timestamp(TimeUnit::MILLI)},
                    {boolean(), timestamp(TimeUnit::MILLI), timestamp(TimeUnit::MILLI)});
  CheckDispatchBest(name, {boolean(), date32(), date64()},
                    {boolean(), date64(), date64()});
  CheckDispatchBest(name, {boolean(), date32(), date32()},
                    {boolean(), date32(), date32()});
}

template <typename Type>
class TestIfElseBaseBinary : public ::testing::Test {};

TYPED_TEST_SUITE(TestIfElseBaseBinary, BaseBinaryArrowTypes);

TYPED_TEST(TestIfElseBaseBinary, IfElseBaseBinary) {
  auto type = TypeTraits<TypeParam>::type_singleton();

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                           ArrayFromJSON(type, R"(["a", "ab", "abc", "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmn", "lm", "l"])"),
                           ArrayFromJSON(type, R"(["a", "ab", "abc", "l"])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([true, true, true, false])"),
                           ArrayFromJSON(type, R"(["a", "ab", "abc", "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmn", "lm", null])"),
                           ArrayFromJSON(type, R"(["a", "ab", "abc", null])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([true, true, true, false])"),
                           ArrayFromJSON(type, R"(["a", "ab", null, "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmn", "lm", null])"),
                           ArrayFromJSON(type, R"(["a", "ab", null, null])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([true, true, true, false])"),
                           ArrayFromJSON(type, R"(["a", "ab", null, "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmn", "lm", "l"])"),
                           ArrayFromJSON(type, R"(["a", "ab", null, "l"])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                           ArrayFromJSON(type, R"(["a", "ab", null, "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmn", "lm", "l"])"),
                           ArrayFromJSON(type, R"([null, "ab", null, "l"])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                           ArrayFromJSON(type, R"(["a", "ab", null, "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmn", "lm", null])"),
                           ArrayFromJSON(type, R"([null, "ab", null, null])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                           ArrayFromJSON(type, R"(["a", "ab", "abc", "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmn", "lm", null])"),
                           ArrayFromJSON(type, R"([null, "ab", "abc", null])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                           ArrayFromJSON(type, R"(["a", "ab", "abc", "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmn", "lm", "l"])"),
                           ArrayFromJSON(type, R"([null, "ab", "abc", "l"])"));
}

TYPED_TEST(TestIfElseBaseBinary, IfElseBaseBinaryRand) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  using OffsetType = typename TypeTraits<TypeParam>::OffsetType::c_type;
  auto type = TypeTraits<TypeParam>::type_singleton();

  random::RandomArrayGenerator rand(/*seed=*/0);
  int64_t len = 1000;

  //  this is to check the BitBlockCount::AllSet/ NoneSet code paths
  ASSERT_OK_AND_ASSIGN(auto temp1, MakeArrayFromScalar(BooleanScalar(true), 64));
  ASSERT_OK_AND_ASSIGN(auto temp2, MakeArrayFromScalar(BooleanScalar(false), 64));
  auto temp3 = rand.ArrayOf(boolean(), len - 64 * 2, /*null_probability=*/0.01);

  ASSERT_OK_AND_ASSIGN(auto concat, Concatenate({temp1, temp2, temp3}));
  auto cond = std::static_pointer_cast<BooleanArray>(concat);

  auto left = std::static_pointer_cast<ArrayType>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));
  auto right = std::static_pointer_cast<ArrayType>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));

  typename TypeTraits<TypeParam>::BuilderType builder;

  for (int64_t i = 0; i < len; ++i) {
    if (!cond->IsValid(i) || (cond->Value(i) && !left->IsValid(i)) ||
        (!cond->Value(i) && !right->IsValid(i))) {
      ASSERT_OK(builder.AppendNull());
      continue;
    }

    OffsetType offset;
    const uint8_t* val;
    if (cond->Value(i)) {
      val = left->GetValue(i, &offset);
    } else {
      val = right->GetValue(i, &offset);
    }
    ASSERT_OK(builder.Append(val, offset));
  }
  ASSERT_OK_AND_ASSIGN(auto expected_data, builder.Finish());

  CheckIfElseOutput(cond, left, right, expected_data);
}

TEST_F(TestIfElseKernel, IfElseFSBinary) {
  auto type = fixed_size_binary(4);

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", "abca", "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmnl", "lmlm", "llll"])"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", "abca", "llll"])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([true, true, true, false])"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", "abca", "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmnl", "lmlm", null])"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", "abca", null])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([true, true, true, false])"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", null, "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmnl", "lmlm", null])"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", null, null])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([true, true, true, false])"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", null, "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmnl", "lmlm", "llll"])"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", null, "llll"])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", null, "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmnl", "lmlm", "llll"])"),
                           ArrayFromJSON(type, R"([null, "abab", null, "llll"])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", null, "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmnl", "lmlm", null])"),
                           ArrayFromJSON(type, R"([null, "abab", null, null])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", "abca", "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmnl", "lmlm", null])"),
                           ArrayFromJSON(type, R"([null, "abab", "abca", null])"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                           ArrayFromJSON(type, R"(["aaaa", "abab", "abca", "abcd"])"),
                           ArrayFromJSON(type, R"(["lmno", "lmnl", "lmlm", "llll"])"),
                           ArrayFromJSON(type, R"([null, "abab", "abca", "llll"])"));
}

TEST_F(TestIfElseKernel, IfElseFSBinaryRand) {
  auto type = fixed_size_binary(5);

  random::RandomArrayGenerator rand(/*seed=*/0);
  int64_t len = 1000;

  //  this is to check the BitBlockCount::AllSet/ NoneSet code paths
  ASSERT_OK_AND_ASSIGN(auto temp1, MakeArrayFromScalar(BooleanScalar(true), 64));
  ASSERT_OK_AND_ASSIGN(auto temp2, MakeArrayFromScalar(BooleanScalar(false), 64));
  auto temp3 = rand.ArrayOf(boolean(), len - 64 * 2, /*null_probability=*/0.01);

  ASSERT_OK_AND_ASSIGN(auto concat, Concatenate({temp1, temp2, temp3}));
  auto cond = std::static_pointer_cast<BooleanArray>(concat);

  auto left = std::static_pointer_cast<FixedSizeBinaryArray>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));
  auto right = std::static_pointer_cast<FixedSizeBinaryArray>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));

  FixedSizeBinaryBuilder builder(type);

  for (int64_t i = 0; i < len; ++i) {
    if (!cond->IsValid(i) || (cond->Value(i) && !left->IsValid(i)) ||
        (!cond->Value(i) && !right->IsValid(i))) {
      ASSERT_OK(builder.AppendNull());
      continue;
    }

    const uint8_t* val;
    if (cond->Value(i)) {
      val = left->GetValue(i);
    } else {
      val = right->GetValue(i);
    }
    ASSERT_OK(builder.Append(val));
  }
  ASSERT_OK_AND_ASSIGN(auto expected_data, builder.Finish());

  CheckIfElseOutput(cond, left, right, expected_data);
}

TEST_F(TestIfElseKernel, Decimal) {
  for (const auto& ty : {decimal128(3, 2), decimal256(3, 2)}) {
    CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", "-1.23", "3.45"])"),
                             ArrayFromJSON(ty, R"(["1.34", "-2.34", "0.00", "-4.56"])"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", "-1.23", "-4.56"])"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([true, true, true, false])"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", "-1.23", "3.45"])"),
                             ArrayFromJSON(ty, R"(["1.34", "-2.34", "0.00", null])"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", "-1.23", null])"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([true, true, true, false])"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", null, "3.45"])"),
                             ArrayFromJSON(ty, R"(["1.34", "-2.34", "0.00", null])"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", null, null])"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([true, true, true, false])"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", null, "3.45"])"),
                             ArrayFromJSON(ty, R"(["1.34", "-2.34", "0.00", "-4.56"])"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", null, "-4.56"])"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", null, "3.45"])"),
                             ArrayFromJSON(ty, R"(["1.34", "-2.34", "0.00", "-4.56"])"),
                             ArrayFromJSON(ty, R"([null, "2.34", null, "-4.56"])"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", null, "3.45"])"),
                             ArrayFromJSON(ty, R"(["1.34", "-2.34", "0.00", null])"),
                             ArrayFromJSON(ty, R"([null, "2.34", null, null])"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", "-1.23", "3.45"])"),
                             ArrayFromJSON(ty, R"(["1.34", "-2.34", "0.00", null])"),
                             ArrayFromJSON(ty, R"([null, "2.34", "-1.23", null])"));

    CheckWithDifferentShapes(ArrayFromJSON(boolean(), R"([null, true, true, false])"),
                             ArrayFromJSON(ty, R"(["1.23", "2.34", "-1.23", "3.45"])"),
                             ArrayFromJSON(ty, R"(["1.34", "-2.34", "0.00", "-4.56"])"),
                             ArrayFromJSON(ty, R"([null, "2.34", "-1.23", "-4.56"])"));
  }
}

template <typename Type>
class TestIfElseList : public ::testing::Test {};

TYPED_TEST_SUITE(TestIfElseList, ListArrowTypes);

TYPED_TEST(TestIfElseList, ListOfInt) {
  auto type = std::make_shared<TypeParam>(int32());
  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, false, false]"),
                           ArrayFromJSON(type, "[[], null, [1, null], [2, 3]]"),
                           ArrayFromJSON(type, "[[4, 5, 6], [7], [null], null]"),
                           ArrayFromJSON(type, "[[], null, [null], null]"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[null, null, null, null]"),
                           ArrayFromJSON(type, "[[], [2, 3, 4, 5], null, null]"),
                           ArrayFromJSON(type, "[[4, 5, 6], null, [null], null]"),
                           ArrayFromJSON(type, "[null, null, null, null]"));
}

TYPED_TEST(TestIfElseList, ListOfString) {
  auto type = std::make_shared<TypeParam>(utf8());
  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[true, true, false, false]"),
      ArrayFromJSON(type, R"([[], null, ["xyz", null], ["ab", "c"]])"),
      ArrayFromJSON(type, R"([["hi", "jk", "l"], ["defg"], [null], null])"),
      ArrayFromJSON(type, R"([[], null, [null], null])"));

  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[null, null, null, null]"),
      ArrayFromJSON(type, R"([[], ["b", "cd", "efg", "h"], null, null])"),
      ArrayFromJSON(type, R"([["hi", "jk", "l"], null, [null], null])"),
      ArrayFromJSON(type, R"([null, null, null, null])"));
}

TEST_F(TestIfElseKernel, FixedSizeList) {
  auto type = fixed_size_list(int32(), 2);
  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, false, false]"),
                           ArrayFromJSON(type, "[[1, 2], null, [1, null], [2, 3]]"),
                           ArrayFromJSON(type, "[[4, 5], [6, 7], [null, 8], null]"),
                           ArrayFromJSON(type, "[[1, 2], null, [null, 8], null]"));

  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[null, null, null, null]"),
                           ArrayFromJSON(type, "[[2, 3], [4, 5], null, null]"),
                           ArrayFromJSON(type, "[[4, 5], null, [6, null], null]"),
                           ArrayFromJSON(type, "[null, null, null, null]"));
}

TEST_F(TestIfElseKernel, StructPrimitive) {
  auto type = struct_({field("int", uint16()), field("str", utf8())});
  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[true, true, false, false]"),
      ArrayFromJSON(type, R"([[null, "foo"], null, [1, null], [2, "spam"]])"),
      ArrayFromJSON(type, R"([[1, "a"], [42, ""], [24, null], null])"),
      ArrayFromJSON(type, R"([[null, "foo"], null, [24, null], null])"));

  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[null, null, null, null]"),
      ArrayFromJSON(type, R"([[null, "foo"], [4, "abcd"], null, null])"),
      ArrayFromJSON(type, R"([[1, "a"], null, [24, null], null])"),
      ArrayFromJSON(type, R"([null, null, null, null])"));
}

TEST_F(TestIfElseKernel, StructNested) {
  auto type = struct_({field("date", date32()), field("list", list(int32()))});
  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[true, true, false, false]"),
      ArrayFromJSON(type, R"([[-1, [null]], null, [1, null], [2, [3, 4]]])"),
      ArrayFromJSON(type, R"([[4, [5]], [6, [7, 8]], [null, [1, null, 42]], null])"),
      ArrayFromJSON(type, R"([[-1, [null]], null, [null, [1, null, 42]], null])"));

  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[null, null, null, null]"),
      ArrayFromJSON(type, R"([[-1, [null]], [4, [5, 6]], null, null])"),
      ArrayFromJSON(type, R"([[4, [5]], null, [null, [1, null, 42]], null])"),
      ArrayFromJSON(type, R"([null, null, null, null])"));
}

TEST_F(TestIfElseKernel, ParameterizedTypes) {
  auto cond = ArrayFromJSON(boolean(), "[true]");

  auto type0 = fixed_size_binary(4);
  auto type1 = fixed_size_binary(5);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("All types must be compatible, expected: "
                           "fixed_size_binary[4], but got: fixed_size_binary[5]"),
      CallFunction("if_else", {cond, ArrayFromJSON(type0, R"(["aaaa"])"),
                               ArrayFromJSON(type1, R"(["aaaaa"])")}));

  // TODO(ARROW-14105): in principle many of these could be implicitly castable too

  type0 = struct_({field("a", int32())});
  type1 = struct_({field("a", int64())});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("All types must be compatible, expected: struct<a: int32>, "
                           "but got: struct<a: int64>"),
      CallFunction("if_else",
                   {cond, ArrayFromJSON(type0, "[[0]]"), ArrayFromJSON(type1, "[[0]]")}));

  type0 = dense_union({field("a", int32())});
  type1 = dense_union({field("a", int64())});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("All types must be compatible, expected: dense_union<a: "
                           "int32=0>, but got: dense_union<a: int64=0>"),
      CallFunction("if_else", {cond, ArrayFromJSON(type0, "[[0, -1]]"),
                               ArrayFromJSON(type1, "[[0, -1]]")}));

  type0 = list(int16());
  type1 = list(int32());
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("All types must be compatible, expected: list<item: int16>, "
                           "but got: list<item: int32>"),
      CallFunction("if_else",
                   {cond, ArrayFromJSON(type0, "[[0]]"), ArrayFromJSON(type1, "[[0]]")}));

  type0 = timestamp(TimeUnit::SECOND);
  type1 = timestamp(TimeUnit::MILLI);
  CheckWithDifferentShapes(ArrayFromJSON(boolean(), "[true, true, true, false]"),
                           ArrayFromJSON(type0, "[1, 2, 3, 4]"),
                           ArrayFromJSON(type1, "[5, 6, 7, 8]"),
                           ArrayFromJSON(type1, "[1000, 2000, 3000, 8]"));

  type0 = timestamp(TimeUnit::SECOND);
  type1 = timestamp(TimeUnit::SECOND, "America/Phoenix");
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("All types must be compatible, expected: timestamp[s], "
                           "but got: timestamp[s, tz=America/Phoenix]"),
      CallFunction("if_else",
                   {cond, ArrayFromJSON(type0, "[0]"), ArrayFromJSON(type1, "[1]")}));

  type0 = timestamp(TimeUnit::SECOND, "America/New_York");
  type1 = timestamp(TimeUnit::SECOND, "America/Phoenix");
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr(
          "All types must be compatible, expected: timestamp[s, tz=America/New_York], "
          "but got: timestamp[s, tz=America/Phoenix]"),
      CallFunction("if_else",
                   {cond, ArrayFromJSON(type0, "[0]"), ArrayFromJSON(type1, "[1]")}));

  type0 = timestamp(TimeUnit::MILLI, "America/New_York");
  type1 = timestamp(TimeUnit::SECOND, "America/Phoenix");
  // Casting fails so we never get to the kernel in the first place (since the units don't
  // match)
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      NotImplemented,
      ::testing::HasSubstr("Function 'if_else' has no kernel matching input types "
                           "(bool, timestamp[ms, tz=America/New_York], "
                           "timestamp[s, tz=America/Phoenix]"),
      CallFunction("if_else",
                   {cond, ArrayFromJSON(type0, "[0]"), ArrayFromJSON(type1, "[1]")}));
}

template <typename Type>
class TestIfElseUnion : public ::testing::Test {};

TYPED_TEST_SUITE(TestIfElseUnion, UnionArrowTypes);

TYPED_TEST(TestIfElseUnion, UnionPrimitive) {
  std::vector<std::shared_ptr<Field>> fields = {field("int", uint16()),
                                                field("str", utf8())};
  std::vector<int8_t> codes = {2, 7};
  auto type = std::make_shared<TypeParam>(fields, codes);
  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[true, true, false, false]"),
      ArrayFromJSON(type, R"([[7, "foo"], [7, null], [7, null], [7, "spam"]])"),
      ArrayFromJSON(type, R"([[2, 15], [2, null], [2, 42], [2, null]])"),
      ArrayFromJSON(type, R"([[7, "foo"], [7, null], [2, 42], [2, null]])"));

  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[null, null, null, null]"),
      ArrayFromJSON(type, R"([[7, "foo"], [7, null], [7, null], [7, "spam"]])"),
      ArrayFromJSON(type, R"([[2, 15], [2, null], [2, 42], [2, null]])"),
      ArrayFromJSON(type, R"([null, null, null, null])"));
}

TYPED_TEST(TestIfElseUnion, UnionNested) {
  std::vector<std::shared_ptr<Field>> fields = {field("int", uint16()),
                                                field("list", list(int16()))};
  std::vector<int8_t> codes = {2, 7};
  auto type = std::make_shared<TypeParam>(fields, codes);
  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[true, true, false, false]"),
      ArrayFromJSON(type, R"([[7, [1, 2]], [7, null], [7, []], [7, [3]]])"),
      ArrayFromJSON(type, R"([[2, 15], [2, null], [2, 42], [2, null]])"),
      ArrayFromJSON(type, R"([[7, [1, 2]], [7, null], [2, 42], [2, null]])"));

  CheckWithDifferentShapes(
      ArrayFromJSON(boolean(), "[null, null, null, null]"),
      ArrayFromJSON(type, R"([[7, [1, 2]], [7, null], [7, []], [7, [3]]])"),
      ArrayFromJSON(type, R"([[2, 15], [2, null], [2, 42], [2, null]])"),
      ArrayFromJSON(type, R"([null, null, null, null])"));
}

template <typename Type>
class TestIfElseDict : public ::testing::Test {};

TYPED_TEST_SUITE(TestIfElseDict, IntegralArrowTypes);

TYPED_TEST(TestIfElseDict, Simple) {
  auto cond = ArrayFromJSON(boolean(), "[true, false, true, null]");
  for (const auto& dict :
       {JsonDict{utf8(), R"(["a", null, "bc", "def"])"},
        JsonDict{int64(), "[1, null, 2, 3]"},
        JsonDict{decimal256(3, 2), R"(["1.23", null, "3.45", "6.78"])"}}) {
    auto type = dictionary(default_type_instance<TypeParam>(), dict.type);
    auto values_null = DictArrayFromJSON(type, "[null, null, null, null]", dict.value);
    auto values1 = DictArrayFromJSON(type, "[0, null, 3, 1]", dict.value);
    auto values2 = DictArrayFromJSON(type, "[2, 1, null, 0]", dict.value);
    auto scalar = DictScalarFromJSON(type, "3", dict.value);

    // Easy case: all arguments have the same dictionary
    CheckDictionary("if_else", {cond, values1, values2});
    CheckDictionary("if_else", {cond, values1, scalar});
    CheckDictionary("if_else", {cond, scalar, values2});
    CheckDictionary("if_else", {cond, values_null, values2});
    CheckDictionary("if_else", {cond, values1, values_null});
    CheckDictionary("if_else", {Datum(true), values1, values2});
    CheckDictionary("if_else", {Datum(false), values1, values2});
    CheckDictionary("if_else", {Datum(true), scalar, values2});
    CheckDictionary("if_else", {Datum(true), values1, scalar});
    CheckDictionary("if_else", {Datum(false), values1, scalar});
    CheckDictionary("if_else", {Datum(false), scalar, values2});
    CheckDictionary("if_else", {MakeNullScalar(boolean()), values1, values2});
  }
}

TYPED_TEST(TestIfElseDict, Mixed) {
  auto index_type = default_type_instance<TypeParam>();
  auto type = dictionary(index_type, utf8());
  auto cond = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto dict = R"(["a", null, "bc", "def"])";
  auto values_null = DictArrayFromJSON(type, "[null, null, null, null]", dict);
  auto values1_dict = DictArrayFromJSON(type, "[0, null, 3, 1]", dict);
  auto values1_decoded = ArrayFromJSON(utf8(), R"(["a", null, "def", null])");
  auto values2_dict = DictArrayFromJSON(type, "[2, 1, null, 0]", dict);
  auto values2_decoded = ArrayFromJSON(utf8(), R"(["bc", null, null, "a"])");
  auto scalar = ScalarFromJSON(utf8(), R"("bc")");

  // If we have mixed dictionary/non-dictionary arguments, we decode dictionaries
  CheckDictionary("if_else", {cond, values1_dict, values2_decoded},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {cond, values1_dict, scalar}, /*result_is_encoded=*/false);
  CheckDictionary("if_else", {cond, scalar, values2_dict}, /*result_is_encoded=*/false);
  CheckDictionary("if_else", {cond, values_null, values2_decoded},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {cond, values1_decoded, values_null},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(true), values1_decoded, values2_dict},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(false), values1_decoded, values2_dict},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(true), scalar, values2_dict},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(true), values1_dict, scalar},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(false), values1_dict, scalar},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(false), scalar, values2_dict},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {MakeNullScalar(boolean()), values1_decoded, values2_dict},
                  /*result_is_encoded=*/false);

  // If we have mismatched dictionary types, we decode (for now)
  auto values3_dict =
      DictArrayFromJSON(dictionary(index_type, binary()), "[2, 1, null, 0]", dict);
  auto values4_dict = DictArrayFromJSON(
      dictionary(index_type->id() == Type::UINT8 ? int8() : uint8(), utf8()),
      "[2, 1, null, 0]", dict);
  CheckDictionary("if_else", {cond, values1_dict, values3_dict},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {cond, values1_dict, values4_dict},
                  /*result_is_encoded=*/false);
}

TYPED_TEST(TestIfElseDict, NestedSimple) {
  auto index_type = default_type_instance<TypeParam>();
  auto inner_type = dictionary(index_type, utf8());
  auto type = list(inner_type);
  auto dict = R"(["a", null, "bc", "def"])";
  auto cond = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto values_null = MakeListOfDict(ArrayFromJSON(int32(), "[null, null, null, null, 0]"),
                                    DictArrayFromJSON(inner_type, "[]", dict));
  auto values1_backing = DictArrayFromJSON(inner_type, "[0, null, 3, 1]", dict);
  auto values2_backing = DictArrayFromJSON(inner_type, "[2, 1, null, 0]", dict);
  auto values1 =
      MakeListOfDict(ArrayFromJSON(int32(), "[0, 2, 2, 3, 4]"), values1_backing);
  auto values2 =
      MakeListOfDict(ArrayFromJSON(int32(), "[0, 1, 2, 2, 4]"), values2_backing);
  auto scalar =
      Datum(std::make_shared<ListScalar>(DictArrayFromJSON(inner_type, "[0, 1]", dict)));

  CheckDictionary("if_else", {cond, values1, values2}, /*result_is_encoded=*/false);
  CheckDictionary("if_else", {cond, values1, scalar}, /*result_is_encoded=*/false);
  CheckDictionary("if_else", {cond, scalar, values2}, /*result_is_encoded=*/false);
  CheckDictionary("if_else", {cond, values_null, values2}, /*result_is_encoded=*/false);
  CheckDictionary("if_else", {cond, values1, values_null}, /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(true), values1, values2},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(false), values1, values2},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(true), scalar, values2}, /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(true), values1, scalar}, /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(false), values1, scalar},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {Datum(false), scalar, values2},
                  /*result_is_encoded=*/false);
  CheckDictionary("if_else", {MakeNullScalar(boolean()), values1, values2},
                  /*result_is_encoded=*/false);
}

TYPED_TEST(TestIfElseDict, DifferentDictionaries) {
  auto type = dictionary(default_type_instance<TypeParam>(), utf8());
  auto cond = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto dict1 = R"(["a", null, "bc", "def"])";
  auto dict2 = R"(["bc", "foo", null, "a"])";
  auto values1_null = DictArrayFromJSON(type, "[null, null, null, null]", dict1);
  auto values2_null = DictArrayFromJSON(type, "[null, null, null, null]", dict2);
  auto values1 = DictArrayFromJSON(type, "[null, 0, 3, 1]", dict1);
  auto values2 = DictArrayFromJSON(type, "[2, 1, 0, null]", dict2);
  auto scalar1 = DictScalarFromJSON(type, "0", dict1);
  auto scalar2 = DictScalarFromJSON(type, "0", dict2);

  CheckDictionary("if_else", {cond, values1, values2});
  CheckDictionary("if_else", {cond, values1, scalar2});
  CheckDictionary("if_else", {cond, scalar1, values2});
  CheckDictionary("if_else", {cond, values1_null, values2});
  CheckDictionary("if_else", {cond, values1, values2_null});
  CheckDictionary("if_else", {Datum(true), values1, values2});
  CheckDictionary("if_else", {Datum(false), values1, values2});
  CheckDictionary("if_else", {Datum(true), scalar1, values2});
  CheckDictionary("if_else", {Datum(true), values1, scalar2});
  CheckDictionary("if_else", {Datum(false), values1, scalar2});
  CheckDictionary("if_else", {Datum(false), scalar1, values2});
  CheckDictionary("if_else", {MakeNullScalar(boolean()), values1, values2});
}

Datum MakeStruct(const std::vector<Datum>& conds) {
  if (conds.size() == 0) {
    // The tests below want a struct scalar when no condition values passed,
    // not a StructArray of length 0
    ScalarVector value;
    return std::make_shared<StructScalar>(value, struct_({}));
  } else {
    EXPECT_OK_AND_ASSIGN(Datum result, CallFunction("make_struct", conds));
    return result;
  }
}

void TestCaseWhenFixedSize(const std::shared_ptr<DataType>& type) {
  auto cond_true = ScalarFromJSON(boolean(), "true");
  auto cond_false = ScalarFromJSON(boolean(), "false");
  auto cond_null = ScalarFromJSON(boolean(), "null");
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");

  if (type->id() == Type::DATE64) {
    auto scalar1 = ScalarFromJSON(type, "86400000");
    auto scalar2 = ScalarFromJSON(type, "172800000");
    auto values1 = ArrayFromJSON(type, "[259200000, null, 432000000, 518400000]");
    auto values2 = ArrayFromJSON(type, "[604800000, 691200000, null, 864000000]");

    CheckScalar("case_when", {MakeStruct({}), values1}, values1);
    CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

    CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
                *MakeArrayFromScalar(*scalar1, 4));
    CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

    CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
    CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
    CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
    CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
    CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
    CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

    CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
                values1);
    CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
                values_null);
    CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
                values1);
    CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
                values2);
    CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
                values2);
    CheckScalar("case_when",
                {MakeStruct({cond_false, cond_false}), values1, values2, values2},
                values2);

    CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
                ArrayFromJSON(type, "[86400000, 86400000, 172800000, null]"));
    CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
    CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
                ArrayFromJSON(type, "[null, null, 86400000, 86400000]"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
                ArrayFromJSON(type, "[86400000, 86400000, 172800000, 86400000]"));

    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
                ArrayFromJSON(type, "[259200000, null, null, null]"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
                ArrayFromJSON(type, "[259200000, null, null, 518400000]"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
                ArrayFromJSON(type, "[null, null, null, 518400000]"));

    CheckScalar(
        "case_when",
        {MakeStruct(
             {ArrayFromJSON(boolean(),
                            "[true, true, true, false, false, false, null, null, null]"),
              ArrayFromJSON(
                  boolean(),
                  "[true, false, null, true, false, null, true, false, null]")}),
         ArrayFromJSON(type,
                       "[864000000, 950400000, 1036800000, 1123200000, 1209600000, "
                       "1296000000, 1382400000, 1468800000, 1555200000]"),
         ArrayFromJSON(type,
                       "[1728000000, 1814400000, 1900800000, 1987200000, 2073600000, "
                       "2160000000, 2246400000, 2332800000, 2419200000]")},
        ArrayFromJSON(type,
                      "[864000000, 950400000, 1036800000, 1987200000, null, null, "
                      "2246400000, null, null]"));
    CheckScalar(
        "case_when",
        {MakeStruct(
             {ArrayFromJSON(boolean(),
                            "[true, true, true, false, false, false, null, null, null]"),
              ArrayFromJSON(
                  boolean(),
                  "[true, false, null, true, false, null, true, false, null]")}),
         ArrayFromJSON(type,
                       "[864000000, 950400000, 1036800000, 1123200000, 1209600000, "
                       "1296000000, 1382400000, 1468800000, 1555200000]"),

         ArrayFromJSON(type,
                       "[1728000000, 1814400000, 1900800000, 1987200000, 2073600000, "
                       "2160000000, 2246400000, 2332800000, 2419200000]"),
         ArrayFromJSON(type,
                       "[2592000000, 2678400000, 2764800000, 2851200000, 2937600000, "
                       "null, 3110400000, 3196800000, null]")},
        ArrayFromJSON(type,
                      "[864000000, 950400000, 1036800000, 1987200000, 2937600000, null, "
                      "2246400000, 3196800000, null]"));

    // Error cases
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr("cond struct must not be a null scalar or "
                             "have top-level nulls"),
        CallFunction("case_when",
                     {MakeNullScalar(struct_({field("", boolean())})), Datum(scalar1)}));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr("cond struct must not be a null scalar or "
                             "have top-level nulls"),
        CallFunction("case_when",
                     {Datum(*MakeArrayOfNull(struct_({field("", boolean())}), 4)),
                      Datum(values1)}));

  } else {
    auto scalar1 = ScalarFromJSON(type, "1");
    auto scalar2 = ScalarFromJSON(type, "2");
    auto values1 = ArrayFromJSON(type, "[3, null, 5, 6]");
    auto values2 = ArrayFromJSON(type, "[7, 8, null, 10]");

    CheckScalar("case_when", {MakeStruct({}), values1}, values1);
    CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

    CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
                *MakeArrayFromScalar(*scalar1, 4));
    CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

    CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
    CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
    CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
    CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
    CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
    CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

    CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
                values1);
    CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
                values_null);
    CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
                values1);
    CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
                values2);
    CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
                values2);
    CheckScalar("case_when",
                {MakeStruct({cond_false, cond_false}), values1, values2, values2},
                values2);

    CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
                ArrayFromJSON(type, "[1, 1, 2, null]"));
    CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
    CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
                ArrayFromJSON(type, "[null, null, 1, 1]"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
                ArrayFromJSON(type, "[1, 1, 2, 1]"));

    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
                ArrayFromJSON(type, "[3, null, null, null]"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
                ArrayFromJSON(type, "[3, null, null, 6]"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
                ArrayFromJSON(type, "[null, null, null, 6]"));

    CheckScalar(
        "case_when",
        {MakeStruct(
             {ArrayFromJSON(boolean(),
                            "[true, true, true, false, false, false, null, null, null]"),
              ArrayFromJSON(
                  boolean(),
                  "[true, false, null, true, false, null, true, false, null]")}),
         ArrayFromJSON(type, "[10, 11, 12, 13, 14, 15, 16, 17, 18]"),
         ArrayFromJSON(type, "[20, 21, 22, 23, 24, 25, 26, 27, 28]")},
        ArrayFromJSON(type, "[10, 11, 12, 23, null, null, 26, null, null]"));
    CheckScalar(
        "case_when",
        {MakeStruct(
             {ArrayFromJSON(boolean(),
                            "[true, true, true, false, false, false, null, null, null]"),
              ArrayFromJSON(
                  boolean(),
                  "[true, false, null, true, false, null, true, false, null]")}),
         ArrayFromJSON(type, "[10, 11, 12, 13, 14, 15, 16, 17, 18]"),

         ArrayFromJSON(type, "[20, 21, 22, 23, 24, 25, 26, 27, 28]"),
         ArrayFromJSON(type, "[30, 31, 32, 33, 34, null, 36, 37, null]")},
        ArrayFromJSON(type, "[10, 11, 12, 23, 34, null, 26, 37, null]"));

    // Error cases
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr("cond struct must not be a null scalar or "
                             "have top-level nulls"),
        CallFunction("case_when",
                     {MakeNullScalar(struct_({field("", boolean())})), Datum(scalar1)}));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr("cond struct must not be a null scalar or "
                             "have top-level nulls"),
        CallFunction("case_when",
                     {Datum(*MakeArrayOfNull(struct_({field("", boolean())}), 4)),
                      Datum(values1)}));
  }
}

void TestCaseWhenRandom(const std::shared_ptr<DataType>& type, int64_t len = 300) {
  random::RandomArrayGenerator rand(/*seed=*/0);

  // Adding 64 consecutive 1's and 0's in the cond array to test all-true/ all-false
  // word code paths
  ASSERT_OK_AND_ASSIGN(auto always_true, MakeArrayFromScalar(BooleanScalar(true), 64));
  ASSERT_OK_AND_ASSIGN(auto always_false, MakeArrayFromScalar(BooleanScalar(false), 64));
  auto maybe_true_with_nulls =
      rand.ArrayOf(boolean(), len - 64 * 2, /*null_probability=*/0.04);
  auto maybe_true_all_valid =
      rand.ArrayOf(boolean(), len - 64 * 2, /*null_probability=*/0.0);
  ASSERT_OK_AND_ASSIGN(auto concat1,
                       Concatenate({always_true, always_false, maybe_true_with_nulls}));
  auto cond1 = checked_pointer_cast<BooleanArray>(concat1);
  ASSERT_OK_AND_ASSIGN(auto concat2,
                       Concatenate({always_true, maybe_true_all_valid, always_false}));
  auto cond2 = checked_pointer_cast<BooleanArray>(concat2);

  auto value1 = rand.ArrayOf(type, len, /*null_probability=*/0.04);
  auto value2 = rand.ArrayOf(type, len, /*null_probability=*/0.04);
  auto value_else = rand.ArrayOf(type, len, /*null_probability=*/0.04);

  auto value1_span = ArraySpan(*value1->data());
  auto value2_span = ArraySpan(*value2->data());
  auto value_else_span = ArraySpan(*value_else->data());

  for (const bool has_else : {true, false}) {
    ASSERT_OK_AND_ASSIGN(auto builder, MakeBuilder(type));
    ASSERT_OK(builder->Reserve(len));
    for (int64_t i = 0; i < len; ++i) {
      if (cond1->IsValid(i) && cond1->Value(i)) {
        ASSERT_OK(builder->AppendArraySlice(value1_span, i, /*length=*/1));
      } else if (cond2->IsValid(i) && cond2->Value(i)) {
        ASSERT_OK(builder->AppendArraySlice(value2_span, i, /*length=*/1));
      } else if (has_else) {
        ASSERT_OK(builder->AppendArraySlice(value_else_span, i, /*length=*/1));
      } else {
        ASSERT_OK(builder->AppendNull());
      }
    }
    ASSERT_OK_AND_ASSIGN(auto expected_data, builder->Finish());

    if (has_else) {
      CheckScalar("case_when", {MakeStruct({cond1, cond2}), value1, value2, value_else},
                  expected_data);
    } else {
      CheckScalar("case_when", {MakeStruct({cond1, cond2}), value1, value2},
                  expected_data);
    }
  }
}

template <typename Type>
class TestCaseWhenNumeric : public ::testing::Test {};

TYPED_TEST_SUITE(TestCaseWhenNumeric, IfElseNumericBasedTypes);

TYPED_TEST(TestCaseWhenNumeric, FixedSize) {
  TestCaseWhenFixedSize(default_type_instance<TypeParam>());
}

TYPED_TEST(TestCaseWhenNumeric, Random) {
  TestCaseWhenRandom(default_type_instance<TypeParam>());
}

TYPED_TEST(TestCaseWhenNumeric, ListOfType) {
  // More minimal test to check type coverage
  auto type = list(default_type_instance<TypeParam>());
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");

  if (std::is_same<TypeParam, Date64Type>::value) {
    auto values1 = ArrayFromJSON(
        type,
        R"([[86400000, 172800000], null, [259200000, 345600000, 432000000], [518400000, null]])");
    auto values2 = ArrayFromJSON(
        type, R"([[691200000, 777600000, 864000000], [950400000], null, [1036800000]])");

    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
                ArrayFromJSON(type, R"([[86400000, 172800000], null, null, null])"));
    CheckScalar(
        "case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
        ArrayFromJSON(type, R"([[86400000, 172800000], null, null, [518400000, null]])"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
                ArrayFromJSON(type, R"([null, null, null, [518400000, null]])"));
  } else {
    auto values1 = ArrayFromJSON(type, R"([[1, 2], null, [3, 4, 5], [6, null]])");
    auto values2 = ArrayFromJSON(type, R"([[8, 9, 10], [11], null, [12]])");

    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
                ArrayFromJSON(type, R"([[1, 2], null, null, null])"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
                ArrayFromJSON(type, R"([[1, 2], null, null, [6, null]])"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
                ArrayFromJSON(type, R"([null, null, null, [6, null]])"));
  }
}

template <typename Type>
class TestCaseWhenDict : public ::testing::Test {};

TYPED_TEST_SUITE(TestCaseWhenDict, IntegralArrowTypes);

TYPED_TEST(TestCaseWhenDict, Simple) {
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  for (const auto& dict :
       {JsonDict{utf8(), R"(["a", null, "bc", "def"])"},
        JsonDict{int64(), "[1, null, 2, 3]"},
        JsonDict{decimal256(3, 2), R"(["1.23", null, "3.45", "6.78"])"}}) {
    auto type = dictionary(default_type_instance<TypeParam>(), dict.type);
    auto values_null = DictArrayFromJSON(type, "[null, null, null, null]", dict.value);
    auto values1 = DictArrayFromJSON(type, "[0, null, 3, 1]", dict.value);
    auto values2 = DictArrayFromJSON(type, "[2, 1, null, 0]", dict.value);

    // Easy case: all arguments have the same dictionary
    CheckDictionary("case_when", {MakeStruct({cond1, cond2}), values1, values2});
    CheckDictionary("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1});
    CheckDictionary("case_when",
                    {MakeStruct({cond1, cond2}), values_null, values2, values1});
  }
}

TYPED_TEST(TestCaseWhenDict, Mixed) {
  auto index_type = default_type_instance<TypeParam>();
  auto type = dictionary(index_type, utf8());
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto dict = R"(["a", null, "bc", "def"])";
  auto values_null = DictArrayFromJSON(type, "[null, null, null, null]", dict);
  auto values1_dict = DictArrayFromJSON(type, "[0, null, 3, 1]", dict);
  auto values1_decoded = ArrayFromJSON(utf8(), R"(["a", null, "def", null])");
  auto values2_dict = DictArrayFromJSON(type, "[2, 1, null, 0]", dict);
  auto values2_decoded = ArrayFromJSON(utf8(), R"(["bc", null, null, "a"])");

  // If we have mixed dictionary/non-dictionary arguments, we decode dictionaries
  CheckDictionary("case_when",
                  {MakeStruct({cond1, cond2}), values1_dict, values2_decoded},
                  /*result_is_encoded=*/false);
  CheckDictionary("case_when",
                  {MakeStruct({cond1, cond2}), values1_decoded, values2_dict},
                  /*result_is_encoded=*/false);
  CheckDictionary(
      "case_when",
      {MakeStruct({cond1, cond2}), values1_dict, values2_dict, values1_decoded},
      /*result_is_encoded=*/false);
  CheckDictionary(
      "case_when",
      {MakeStruct({cond1, cond2}), values_null, values2_dict, values1_decoded},
      /*result_is_encoded=*/false);

  // If we have mismatched dictionary types, we decode (for now)
  auto values3_dict =
      DictArrayFromJSON(dictionary(index_type, binary()), "[2, 1, null, 0]", dict);
  auto values4_dict = DictArrayFromJSON(
      dictionary(index_type->id() == Type::UINT8 ? int8() : uint8(), utf8()),
      "[2, 1, null, 0]", dict);
  CheckDictionary("case_when", {MakeStruct({cond1, cond2}), values1_dict, values3_dict},
                  /*result_is_encoded=*/false);
  CheckDictionary("case_when", {MakeStruct({cond1, cond2}), values1_dict, values4_dict},
                  /*result_is_encoded=*/false);
}

TYPED_TEST(TestCaseWhenDict, NestedSimple) {
  auto index_type = default_type_instance<TypeParam>();
  auto inner_type = dictionary(index_type, utf8());
  auto type = list(inner_type);
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto dict = R"(["a", null, "bc", "def"])";
  auto values_null = MakeListOfDict(ArrayFromJSON(int32(), "[null, null, null, null, 0]"),
                                    DictArrayFromJSON(inner_type, "[]", dict));
  auto values1_backing = DictArrayFromJSON(inner_type, "[0, null, 3, 1]", dict);
  auto values2_backing = DictArrayFromJSON(inner_type, "[2, 1, null, 0]", dict);
  auto values1 =
      MakeListOfDict(ArrayFromJSON(int32(), "[0, 2, 2, 3, 4]"), values1_backing);
  auto values2 =
      MakeListOfDict(ArrayFromJSON(int32(), "[0, 1, 2, 2, 4]"), values2_backing);

  CheckDictionary("case_when", {MakeStruct({cond1, cond2}), values1, values2},
                  /*result_is_encoded=*/false);
  CheckDictionary(
      "case_when",
      {MakeStruct({cond1, cond2}), values1,
       MakeListOfDict(ArrayFromJSON(int32(), "[0, 1, null, 2, 4]"), values2_backing)},
      /*result_is_encoded=*/false);
  CheckDictionary(
      "case_when",
      {MakeStruct({cond1, cond2}), values1,
       MakeListOfDict(ArrayFromJSON(int32(), "[0, 1, null, 2, 4]"), values2_backing),
       values1},
      /*result_is_encoded=*/false);

  CheckDictionary("case_when",
                  {
                      Datum(MakeStruct({cond1, cond2})),
                      Datum(std::make_shared<ListScalar>(
                          DictArrayFromJSON(inner_type, "[0, 1]", dict))),
                      Datum(std::make_shared<ListScalar>(
                          DictArrayFromJSON(inner_type, "[2, 3]", dict))),
                  },
                  /*result_is_encoded=*/false);

  CheckDictionary("case_when",
                  {MakeStruct({Datum(true), Datum(false)}), values1, values2},
                  /*result_is_encoded=*/false);
  CheckDictionary("case_when",
                  {MakeStruct({Datum(false), Datum(true)}), values1, values2},
                  /*result_is_encoded=*/false);
  CheckDictionary("case_when", {MakeStruct({Datum(false)}), values1, values2},
                  /*result_is_encoded=*/false);
  CheckDictionary("case_when",
                  {MakeStruct({Datum(false), Datum(false)}), values1, values2},
                  /*result_is_encoded=*/false);
}

TYPED_TEST(TestCaseWhenDict, DifferentDictionaries) {
  auto type = dictionary(default_type_instance<TypeParam>(), utf8());
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, null, true]");
  auto dict1 = R"(["a", null, "bc", "def"])";
  auto dict2 = R"(["bc", "foo", null, "a"])";
  auto dict3 = R"(["def", null, "a", "bc"])";
  auto values1_null = DictArrayFromJSON(type, "[null, null, null, null]", dict1);
  auto values2_null = DictArrayFromJSON(type, "[null, null, null, null]", dict2);
  auto values1 = DictArrayFromJSON(type, "[null, 0, 3, 1]", dict1);
  auto values2 = DictArrayFromJSON(type, "[2, 1, 0, null]", dict2);
  auto values3 = DictArrayFromJSON(type, "[0, 1, 2, 3]", dict3);

  CheckDictionary("case_when",
                  {MakeStruct({Datum(true), Datum(false)}), values1, values2});
  CheckDictionary("case_when",
                  {MakeStruct({Datum(false), Datum(true)}), values1, values2});
  CheckDictionary("case_when",
                  {MakeStruct({Datum(false), Datum(false)}), values1, values2});
  CheckDictionary("case_when",
                  {MakeStruct({Datum(false), Datum(false)}), values2, values1});

  CheckDictionary("case_when", {MakeStruct({cond1, cond2}), values1, values2});
  CheckDictionary("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1});

  CheckDictionary("case_when",
                  {MakeStruct({ArrayFromJSON(boolean(), "[true, true, false, false]")}),
                   values1, values2});
  CheckDictionary("case_when",
                  {MakeStruct({ArrayFromJSON(boolean(), "[true, false, false, true]")}),
                   values1, values2});
  CheckDictionary("case_when",
                  {MakeStruct({ArrayFromJSON(boolean(), "[true, true, false, false]"),
                               ArrayFromJSON(boolean(), "[true, false, true, false]")}),
                   values1, values2});
  CheckDictionary("case_when",
                  {MakeStruct({ArrayFromJSON(boolean(), "[false, false, false, false]"),
                               ArrayFromJSON(boolean(), "[true, true, true, true]")}),
                   values1, values3});
  CheckDictionary("case_when",
                  {MakeStruct({ArrayFromJSON(boolean(), "[null, null, null, true]"),
                               ArrayFromJSON(boolean(), "[true, true, true, true]")}),
                   values1, values3});
  CheckDictionary(
      "case_when",
      {
          MakeStruct({ArrayFromJSON(boolean(), "[true, true, false, false]")}),
          DictScalarFromJSON(type, "0", dict1),
          DictScalarFromJSON(type, "0", dict2),
      });
  CheckDictionary(
      "case_when",
      {
          MakeStruct({ArrayFromJSON(boolean(), "[true, true, false, false]"),
                      ArrayFromJSON(boolean(), "[false, false, true, true]")}),
          DictScalarFromJSON(type, "0", dict1),
          DictScalarFromJSON(type, "0", dict2),
      });
  CheckDictionary(
      "case_when",
      {
          MakeStruct({ArrayFromJSON(boolean(), "[true, true, false, false]"),
                      ArrayFromJSON(boolean(), "[false, false, true, true]")}),
          DictScalarFromJSON(type, "null", dict1),
          DictScalarFromJSON(type, "0", dict2),
      });
}

TEST(TestCaseWhen, Null) {
  auto cond_true = ScalarFromJSON(boolean(), "true");
  auto cond_false = ScalarFromJSON(boolean(), "false");
  auto cond_arr = ArrayFromJSON(boolean(), "[true, true, false, null]");
  auto scalar = ScalarFromJSON(null(), "null");
  auto array = ArrayFromJSON(null(), "[null, null, null, null]");
  CheckScalar("case_when", {MakeStruct({}), array}, array);
  CheckScalar("case_when", {MakeStruct({cond_false}), array}, array);
  CheckScalar("case_when", {MakeStruct({cond_true}), array, array}, array);
  CheckScalar("case_when", {MakeStruct({cond_arr, cond_true}), array, array}, array);
}

TEST(TestCaseWhen, NullRandom) { TestCaseWhenRandom(null()); }

TEST(TestCaseWhen, Boolean) {
  auto type = boolean();
  auto cond_true = ScalarFromJSON(boolean(), "true");
  auto cond_false = ScalarFromJSON(boolean(), "false");
  auto cond_null = ScalarFromJSON(boolean(), "null");
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, "true");
  auto scalar2 = ScalarFromJSON(type, "false");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, "[true, null, true, true]");
  auto values2 = ArrayFromJSON(type, "[false, false, null, false]");

  CheckScalar("case_when", {MakeStruct({}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

  CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
              *MakeArrayFromScalar(*scalar1, 4));
  CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

  CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
              values_null);
  CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when",
              {MakeStruct({cond_false, cond_false}), values1, values2, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
              ArrayFromJSON(type, "[true, true, false, null]"));
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
              ArrayFromJSON(type, "[null, null, true, true]"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
              ArrayFromJSON(type, "[true, true, false, true]"));

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, "[true, null, null, null]"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, "[true, null, null, true]"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, "[null, null, null, true]"));
}

TEST(TestCaseWhen, BooleanRandom) { TestCaseWhenRandom(boolean()); }

TEST(TestCaseWhen, DayTimeInterval) {
  auto type = day_time_interval();
  auto cond_true = ScalarFromJSON(boolean(), "true");
  auto cond_false = ScalarFromJSON(boolean(), "false");
  auto cond_null = ScalarFromJSON(boolean(), "null");
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, "[1, 1]");
  auto scalar2 = ScalarFromJSON(type, "[2, 2]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, "[[3, 3], null, [5, 5], [6, 6]]");
  auto values2 = ArrayFromJSON(type, "[[7, 7], [8, 8], null, [10, 10]]");

  CheckScalar("case_when", {MakeStruct({}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

  CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
              *MakeArrayFromScalar(*scalar1, 4));
  CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

  CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
              values_null);
  CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when",
              {MakeStruct({cond_false, cond_false}), values1, values2, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
              ArrayFromJSON(type, "[[1, 1], [1, 1], [2, 2], null]"));
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
              ArrayFromJSON(type, "[null, null, [1, 1], [1, 1]]"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
              ArrayFromJSON(type, "[[1, 1], [1, 1], [2, 2], [1, 1]]"));

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, "[[3, 3], null, null, null]"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, "[[3, 3], null, null, [6, 6]]"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, "[null, null, null, [6, 6]]"));
}

TEST(TestCaseWhen, DayTimeIntervalRandom) { TestCaseWhenRandom(day_time_interval()); }

TEST(TestCaseWhen, MonthDayNanoInterval) {
  auto type = month_day_nano_interval();
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, R"([[0, 1, -2], null, [-3, 4, 5], [-6, -7, -8]])");
  auto values2 = ArrayFromJSON(type, R"([[1, 2, 3], [4, 5, 6], null, [0, 2, 4]])");

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([[0, 1, -2], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([[0, 1, -2], null, null, [-6, -7, -8]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, [-6, -7, -8]])"));
}

TEST(TestCaseWhen, MonthDayNanoIntervalRandom) {
  TestCaseWhenRandom(month_day_nano_interval());
}

TEST(TestCaseWhen, Decimal) {
  for (const auto& type :
       std::vector<std::shared_ptr<DataType>>{decimal128(3, 2), decimal256(3, 2)}) {
    auto cond_true = ScalarFromJSON(boolean(), "true");
    auto cond_false = ScalarFromJSON(boolean(), "false");
    auto cond_null = ScalarFromJSON(boolean(), "null");
    auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
    auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
    auto scalar_null = ScalarFromJSON(type, "null");
    auto scalar1 = ScalarFromJSON(type, R"("1.23")");
    auto scalar2 = ScalarFromJSON(type, R"("2.34")");
    auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
    auto values1 = ArrayFromJSON(type, R"(["3.45", null, "5.67", "6.78"])");
    auto values2 = ArrayFromJSON(type, R"(["7.89", "8.90", null, "1.01"])");

    CheckScalar("case_when", {MakeStruct({}), values1}, values1);
    CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

    CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
                *MakeArrayFromScalar(*scalar1, 4));
    CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

    CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
    CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
    CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
    CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
    CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
    CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

    CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
                values1);
    CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
                values_null);
    CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
                values1);
    CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
                values2);
    CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
                values2);
    CheckScalar("case_when",
                {MakeStruct({cond_false, cond_false}), values1, values2, values2},
                values2);

    CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
                ArrayFromJSON(type, R"(["1.23", "1.23", "2.34", null])"));
    CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
    CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
                ArrayFromJSON(type, R"([null, null, "1.23", "1.23"])"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
                ArrayFromJSON(type, R"(["1.23", "1.23", "2.34", "1.23"])"));

    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
                ArrayFromJSON(type, R"(["3.45", null, null, null])"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
                ArrayFromJSON(type, R"(["3.45", null, null, "6.78"])"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
                ArrayFromJSON(type, R"([null, null, null, "6.78"])"));
  }
}

TEST(TestCaseWhen, FixedSizeBinary) {
  auto type = fixed_size_binary(3);
  auto cond_true = ScalarFromJSON(boolean(), "true");
  auto cond_false = ScalarFromJSON(boolean(), "false");
  auto cond_null = ScalarFromJSON(boolean(), "null");
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"("abc")");
  auto scalar2 = ScalarFromJSON(type, R"("bcd")");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, R"(["cde", null, "def", "efg"])");
  auto values2 = ArrayFromJSON(type, R"(["fgh", "ghi", null, "hij"])");

  CheckScalar("case_when", {MakeStruct({}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

  CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
              *MakeArrayFromScalar(*scalar1, 4));
  CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

  CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
              values_null);
  CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when",
              {MakeStruct({cond_false, cond_false}), values1, values2, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
              ArrayFromJSON(type, R"(["abc", "abc", "bcd", null])"));
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
              ArrayFromJSON(type, R"([null, null, "abc", "abc"])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
              ArrayFromJSON(type, R"(["abc", "abc", "bcd", "abc"])"));

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"(["cde", null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"(["cde", null, null, "efg"])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, "efg"])"));
}

TEST(TestCaseWhen, FixedSizeBinaryRandom) { TestCaseWhenRandom(fixed_size_binary(3)); }

template <typename Type>
class TestCaseWhenBinary : public ::testing::Test {};

TYPED_TEST_SUITE(TestCaseWhenBinary, BaseBinaryArrowTypes);

TYPED_TEST(TestCaseWhenBinary, Basics) {
  auto type = default_type_instance<TypeParam>();
  auto cond_true = ScalarFromJSON(boolean(), "true");
  auto cond_false = ScalarFromJSON(boolean(), "false");
  auto cond_null = ScalarFromJSON(boolean(), "null");
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"("aBxYz")");
  auto scalar2 = ScalarFromJSON(type, R"("b")");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, R"(["cDE", null, "degfhi", "efg"])");
  auto values2 = ArrayFromJSON(type, R"(["fghijk", "ghi", null, "hi"])");

  CheckScalar("case_when", {MakeStruct({}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

  CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
              *MakeArrayFromScalar(*scalar1, 4));
  CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

  CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
              values_null);
  CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when",
              {MakeStruct({cond_false, cond_false}), values1, values2, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
              ArrayFromJSON(type, R"(["aBxYz", "aBxYz", "b", null])"));
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
              ArrayFromJSON(type, R"([null, null, "aBxYz", "aBxYz"])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
              ArrayFromJSON(type, R"(["aBxYz", "aBxYz", "b", "aBxYz"])"));

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"(["cDE", null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"(["cDE", null, null, "efg"])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, "efg"])"));
}

TYPED_TEST(TestCaseWhenBinary, Random) {
  TestCaseWhenRandom(default_type_instance<TypeParam>());
}

template <typename Type>
class TestCaseWhenList : public ::testing::Test {};

TYPED_TEST_SUITE(TestCaseWhenList, ListArrowTypes);

TYPED_TEST(TestCaseWhenList, ListOfString) {
  auto type = std::make_shared<TypeParam>(utf8());
  auto cond_true = ScalarFromJSON(boolean(), "true");
  auto cond_false = ScalarFromJSON(boolean(), "false");
  auto cond_null = ScalarFromJSON(boolean(), "null");
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"(["aB", "xYz"])");
  auto scalar2 = ScalarFromJSON(type, R"(["b", null])");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 =
      ArrayFromJSON(type, R"([["cD", "E"], null, ["de", "gf", "hi"], ["ef", "g"]])");
  auto values2 = ArrayFromJSON(type, R"([["f", "ghi", "jk"], ["ghi"], null, ["hi"]])");

  CheckScalar("case_when", {MakeStruct({}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

  CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
              *MakeArrayFromScalar(*scalar1, 4));
  CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

  CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
              values_null);
  CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when",
              {MakeStruct({cond_false, cond_false}), values1, values2, values2}, values2);

  CheckScalar(
      "case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
      ArrayFromJSON(type, R"([["aB", "xYz"], ["aB", "xYz"], ["b", null], null])"));
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
              ArrayFromJSON(type, R"([null, null, ["aB", "xYz"], ["aB", "xYz"]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
              ArrayFromJSON(
                  type, R"([["aB", "xYz"], ["aB", "xYz"], ["b", null], ["aB", "xYz"]])"));

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([["cD", "E"], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([["cD", "E"], null, null, ["ef", "g"]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, ["ef", "g"]])"));
}

TYPED_TEST(TestCaseWhenList, ListOfStringRandom) {
  auto type = std::make_shared<TypeParam>(utf8());
  TestCaseWhenRandom(type, /*len=*/200);
}

// More minimal tests to check type coverage
TYPED_TEST(TestCaseWhenList, ListOfBool) {
  auto type = std::make_shared<TypeParam>(boolean());
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, R"([[true], null, [false], [false, null]])");
  auto values2 = ArrayFromJSON(type, R"([[false], [false], null, [true]])");

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([[true], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([[true], null, null, [false, null]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, [false, null]])"));
}

TYPED_TEST(TestCaseWhenList, ListOfBoolRandom) {
  auto type = std::make_shared<TypeParam>(boolean());
  TestCaseWhenRandom(type, /*len=*/200);
}

TYPED_TEST(TestCaseWhenList, ListOfInt) {
  auto type = std::make_shared<TypeParam>(int64());
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, R"([[1, 2], null, [3, 4, 5], [6, null]])");
  auto values2 = ArrayFromJSON(type, R"([[8, 9, 10], [11], null, [12]])");

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([[1, 2], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([[1, 2], null, null, [6, null]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, [6, null]])"));
}

TYPED_TEST(TestCaseWhenList, ListOfDayTimeInterval) {
  auto type = std::make_shared<TypeParam>(day_time_interval());
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 =
      ArrayFromJSON(type, R"([[[1, 2]], null, [[3, 4], [5, 0]], [[6, 7], null]])");
  auto values2 = ArrayFromJSON(type, R"([[[8, 9], null], [[11, 12]], null, [[12, 1]]])");

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([[[1, 2]], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([[[1, 2]], null, null, [[6, 7], null]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, [[6, 7], null]])"));
}

TYPED_TEST(TestCaseWhenList, ListOfDecimal) {
  for (const auto& decimal_ty :
       std::vector<std::shared_ptr<DataType>>{decimal128(3, 2), decimal256(3, 2)}) {
    auto type = std::make_shared<TypeParam>(decimal_ty);
    auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
    auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
    auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
    auto values1 = ArrayFromJSON(
        type, R"([["1.23", "2.34"], null, ["3.45", "4.56", "5.67"], ["6.78", null]])");
    auto values2 =
        ArrayFromJSON(type, R"([["8.90", "9.01", "1.02"], ["1.12"], null, ["1.23"]])");

    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
                ArrayFromJSON(type, R"([["1.23", "2.34"], null, null, null])"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
                ArrayFromJSON(type, R"([["1.23", "2.34"], null, null, ["6.78", null]])"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
                ArrayFromJSON(type, R"([null, null, null, ["6.78", null]])"));
  }
}

TYPED_TEST(TestCaseWhenList, ListOfFixedSizeBinary) {
  auto type = std::make_shared<TypeParam>(fixed_size_binary(4));
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(
      type, R"([["1.23", "2.34"], null, ["3.45", "4.56", "5.67"], ["6.78", null]])");
  auto values2 =
      ArrayFromJSON(type, R"([["8.90", "9.01", "1.02"], ["1.12"], null, ["1.23"]])");

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([["1.23", "2.34"], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([["1.23", "2.34"], null, null, ["6.78", null]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, ["6.78", null]])"));
}

TYPED_TEST(TestCaseWhenList, ListOfListOfInt) {
  auto type = std::make_shared<TypeParam>(list(int64()));
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 =
      ArrayFromJSON(type, R"([[[1, 2], []], null, [[3, 4, 5]], [[6, null], null]])");
  auto values2 = ArrayFromJSON(type, R"([[[8, 9, 10]], [[11]], null, [[12]]])");

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([[[1, 2], []], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([[[1, 2], []], null, null, [[6, null], null]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, [[6, null], null]])"));
}

TYPED_TEST(TestCaseWhenList, ListOfListOfIntRandom) {
  auto type = std::make_shared<TypeParam>(list(int64()));
  TestCaseWhenRandom(type, /*len=*/200);
}

TEST(TestCaseWhen, Map) {
  auto type = map(int64(), utf8());
  auto cond_true = ScalarFromJSON(boolean(), "true");
  auto cond_false = ScalarFromJSON(boolean(), "false");
  auto cond_null = ScalarFromJSON(boolean(), "null");
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"([[1, "abc"], [2, "de"]])");
  auto scalar2 = ScalarFromJSON(type, R"([[3, "fghi"]])");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 =
      ArrayFromJSON(type, R"([[[4, "kl"]], null, [[5, "mn"]], [[6, "o"], [7, "pq"]]])");
  auto values2 = ArrayFromJSON(type, R"([[[8, "r"], [9, "st"]], [[10, "u"]], null, []])");

  CheckScalar("case_when", {MakeStruct({}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

  CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
              *MakeArrayFromScalar(*scalar1, 4));
  CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

  CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
              values_null);
  CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when",
              {MakeStruct({cond_false, cond_false}), values1, values2, values2}, values2);

  CheckScalar(
      "case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
      ArrayFromJSON(
          type,
          R"([[[1, "abc"], [2, "de"]], [[1, "abc"], [2, "de"]], [[3, "fghi"]], null])"));
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
  CheckScalar(
      "case_when", {MakeStruct({cond1}), scalar_null, scalar1},
      ArrayFromJSON(type,
                    R"([null, null, [[1, "abc"], [2, "de"]], [[1, "abc"], [2, "de"]]])"));
  CheckScalar(
      "case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
      ArrayFromJSON(
          type,
          R"([[[1, "abc"], [2, "de"]], [[1, "abc"], [2, "de"]], [[3, "fghi"]], [[1, "abc"], [2, "de"]]])"));

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([[[4, "kl"]], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([[[4, "kl"]], null, null, [[6, "o"], [7, "pq"]]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, [[6, "o"], [7, "pq"]]])"));
}

TEST(TestCaseWhen, FixedSizeListOfInt) {
  auto type = fixed_size_list(int64(), 2);
  auto cond_true = ScalarFromJSON(boolean(), "true");
  auto cond_false = ScalarFromJSON(boolean(), "false");
  auto cond_null = ScalarFromJSON(boolean(), "null");
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"([1, 2])");
  auto scalar2 = ScalarFromJSON(type, R"([3, null])");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, R"([[4, 5], null, [6, 7], [8,  9]])");
  auto values2 = ArrayFromJSON(type, R"([[10, 11], [12, null], null, [null, 13]])");

  CheckScalar("case_when", {MakeStruct({}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

  CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
              *MakeArrayFromScalar(*scalar1, 4));
  CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

  CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
              values_null);
  CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when",
              {MakeStruct({cond_false, cond_false}), values1, values2, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
              ArrayFromJSON(type, R"([[1, 2], [1, 2], [3, null], null])"));
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
              ArrayFromJSON(type, R"([null, null, [1, 2], [1, 2]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
              ArrayFromJSON(type, R"([[1, 2], [1, 2], [3, null], [1, 2]])"));

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([[4, 5], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([[4, 5], null, null, [8, 9]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, [8, 9]])"));
}

TEST(TestCaseWhen, FixedSizeListOfIntRandom) {
  auto type = fixed_size_list(int64(), 2);
  TestCaseWhenRandom(type);
}

TEST(TestCaseWhen, FixedSizeListOfString) {
  auto type = fixed_size_list(utf8(), 2);
  auto cond_true = ScalarFromJSON(boolean(), "true");
  auto cond_false = ScalarFromJSON(boolean(), "false");
  auto cond_null = ScalarFromJSON(boolean(), "null");
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"(["aB", "xYz"])");
  auto scalar2 = ScalarFromJSON(type, R"(["b", null])");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 =
      ArrayFromJSON(type, R"([["cD", "E"], null, ["de", "gfhi"], ["ef", "g"]])");
  auto values2 =
      ArrayFromJSON(type, R"([["fghi", "jk"], ["ghi", null], null, [null, "hi"]])");

  CheckScalar("case_when", {MakeStruct({}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

  CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
              *MakeArrayFromScalar(*scalar1, 4));
  CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

  CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
              values_null);
  CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when",
              {MakeStruct({cond_false, cond_false}), values1, values2, values2}, values2);

  CheckScalar(
      "case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
      ArrayFromJSON(type, R"([["aB", "xYz"], ["aB", "xYz"], ["b", null], null])"));
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
              ArrayFromJSON(type, R"([null, null, ["aB", "xYz"], ["aB", "xYz"]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
              ArrayFromJSON(
                  type, R"([["aB", "xYz"], ["aB", "xYz"], ["b", null], ["aB", "xYz"]])"));

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([["cD", "E"], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([["cD", "E"], null, null, ["ef", "g"]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, ["ef", "g"]])"));
}

TEST(TestCaseWhen, StructOfInt) {
  auto type = struct_({field("a", uint32()), field("b", int64())});
  auto cond_true = ScalarFromJSON(boolean(), "true");
  auto cond_false = ScalarFromJSON(boolean(), "false");
  auto cond_null = ScalarFromJSON(boolean(), "null");
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"([1, -2])");
  auto scalar2 = ScalarFromJSON(type, R"([null, 3])");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, R"([[4, null], null, [5, -6], [7, -8]])");
  auto values2 = ArrayFromJSON(type, R"([[9, 10], [11, -12], null, [null, null]])");

  CheckScalar("case_when", {MakeStruct({}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

  CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
              *MakeArrayFromScalar(*scalar1, 4));
  CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

  CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
  CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
  CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
  CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
              values_null);
  CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
              values1);
  CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
              values2);
  CheckScalar("case_when",
              {MakeStruct({cond_false, cond_false}), values1, values2, values2}, values2);

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
              ArrayFromJSON(type, R"([[1, -2], [1, -2], [null, 3], null])"));
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
              ArrayFromJSON(type, R"([null, null, [1, -2], [1, -2]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
              ArrayFromJSON(type, R"([[1, -2], [1, -2], [null, 3], [1, -2]])"));

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([[4, null], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([[4, null], null, null, [7, -8]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, [7, -8]])"));
}

TEST(TestCaseWhen, StructOfIntRandom) {
  auto type = struct_({field("a", uint32()), field("b", int64())});
  TestCaseWhenRandom(type);
}

TEST(TestCaseWhen, StructOfString) {
  // More minimal test to check type coverage
  auto type = struct_({field("a", utf8()), field("b", large_utf8())});
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"(["a", "bc"])");
  auto scalar2 = ScalarFromJSON(type, R"([null, "d"])");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 =
      ArrayFromJSON(type, R"([["efg", null], null, [null, null], [null, "hi"]])");
  auto values2 =
      ArrayFromJSON(type, R"([["j", "k"], [null, "lmnop"], null, ["qr", "stu"]])");
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
              ArrayFromJSON(type, R"([["a", "bc"], ["a", "bc"], [null, "d"], null])"));
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
              ArrayFromJSON(type, R"([null, null, ["a", "bc"], ["a", "bc"]])"));
  CheckScalar(
      "case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
      ArrayFromJSON(type, R"([["a", "bc"], ["a", "bc"], [null, "d"], ["a", "bc"]])"));

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([["efg", null], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([["efg", null], null, null, [null, "hi"]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, [null, "hi"]])"));
}

TEST(TestCaseWhen, StructOfStringRandom) {
  auto type = struct_({field("a", utf8()), field("b", large_utf8())});
  TestCaseWhenRandom(type);
}

TEST(TestCaseWhen, StructOfListOfInt) {
  // More minimal test to check type coverage
  auto type = struct_({field("a", utf8()), field("b", list(int64()))});
  auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
  auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"([null, [1, null]])");
  auto scalar2 = ScalarFromJSON(type, R"(["b", null])");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 =
      ArrayFromJSON(type, R"([["efg", null], null, [null, null], [null, [null, 1]]])");
  auto values2 =
      ArrayFromJSON(type, R"([["j", [2, 3]], [null, [4, 5, 6]], null, ["qr", [7]]])");
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
              ArrayFromJSON(
                  type, R"([[null, [1, null]], [null, [1, null]], ["b", null], null])"));
  CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
  CheckScalar(
      "case_when", {MakeStruct({cond1}), scalar_null, scalar1},
      ArrayFromJSON(type, R"([null, null, [null, [1, null]], [null, [1, null]]])"));
  CheckScalar(
      "case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
      ArrayFromJSON(
          type,
          R"([[null, [1, null]], [null, [1, null]], ["b", null], [null, [1, null]]])"));

  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
              ArrayFromJSON(type, R"([["efg", null], null, null, null])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
              ArrayFromJSON(type, R"([["efg", null], null, null, [null, [null, 1]]])"));
  CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
              ArrayFromJSON(type, R"([null, null, null, [null, [null, 1]]])"));
}

TEST(TestCaseWhen, UnionBoolString) {
  for (const auto& type : std::vector<std::shared_ptr<DataType>>{
           sparse_union({field("a", boolean()), field("b", utf8())}, {2, 7}),
           dense_union({field("a", boolean()), field("b", utf8())}, {2, 7})}) {
    ARROW_SCOPED_TRACE(type->ToString());
    auto cond_true = ScalarFromJSON(boolean(), "true");
    auto cond_false = ScalarFromJSON(boolean(), "false");
    auto cond_null = ScalarFromJSON(boolean(), "null");
    auto cond1 = ArrayFromJSON(boolean(), "[true, true, null, null]");
    auto cond2 = ArrayFromJSON(boolean(), "[true, false, true, null]");
    auto scalar_null = ScalarFromJSON(type, "null");
    auto scalar1 = ScalarFromJSON(type, R"([2, null])");
    auto scalar2 = ScalarFromJSON(type, R"([7, "foo"])");
    auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
    auto values1 = ArrayFromJSON(type, R"([[2, true], null, [7, "bar"], [7, "baz"]])");
    auto values2 = ArrayFromJSON(type, R"([[7, "spam"], [2, null], null, [7, null]])");

    CheckScalar("case_when", {MakeStruct({}), values1}, values1);
    CheckScalar("case_when", {MakeStruct({}), values_null}, values_null);

    CheckScalar("case_when", {MakeStruct({cond_true}), scalar1, values1},
                *MakeArrayFromScalar(*scalar1, 4));
    CheckScalar("case_when", {MakeStruct({cond_false}), scalar1, values1}, values1);

    CheckScalar("case_when", {MakeStruct({cond_true}), values1}, values1);
    CheckScalar("case_when", {MakeStruct({cond_false}), values1}, values_null);
    CheckScalar("case_when", {MakeStruct({cond_null}), values1}, values_null);
    CheckScalar("case_when", {MakeStruct({cond_true}), values1, values2}, values1);
    CheckScalar("case_when", {MakeStruct({cond_false}), values1, values2}, values2);
    CheckScalar("case_when", {MakeStruct({cond_null}), values1, values2}, values2);

    CheckScalar("case_when", {MakeStruct({cond_true, cond_true}), values1, values2},
                values1);
    CheckScalar("case_when", {MakeStruct({cond_false, cond_false}), values1, values2},
                values_null);
    CheckScalar("case_when", {MakeStruct({cond_true, cond_false}), values1, values2},
                values1);
    CheckScalar("case_when", {MakeStruct({cond_false, cond_true}), values1, values2},
                values2);
    CheckScalar("case_when", {MakeStruct({cond_null, cond_true}), values1, values2},
                values2);
    CheckScalar("case_when",
                {MakeStruct({cond_false, cond_false}), values1, values2, values2},
                values2);

    CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2},
                ArrayFromJSON(type, R"([[2, null], [2, null], [7, "foo"], null])"));
    CheckScalar("case_when", {MakeStruct({cond1}), scalar_null}, values_null);
    CheckScalar("case_when", {MakeStruct({cond1}), scalar_null, scalar1},
                ArrayFromJSON(type, R"([null, null, [2, null], [2, null]])"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), scalar1, scalar2, scalar1},
                ArrayFromJSON(type, R"([[2, null], [2, null], [7, "foo"], [2, null]])"));

    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2},
                ArrayFromJSON(type, R"([[2, true], null, null, null])"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values1, values2, values1},
                ArrayFromJSON(type, R"([[2, true], null, null, [7, "baz"]])"));
    CheckScalar("case_when", {MakeStruct({cond1, cond2}), values_null, values2, values1},
                ArrayFromJSON(type, R"([null, null, null, [7, "baz"]])"));
  }
}

// FIXME(GH-15192): enabling this test produces test failures

// TEST(TestCaseWhen, UnionBoolStringRandom) {
//   for (const auto& type : std::vector<std::shared_ptr<DataType>>{
//            sparse_union({field("a", boolean()), field("b", utf8())}, {2, 7}),
//            dense_union({field("a", boolean()), field("b", utf8())}, {2, 7})}) {
//     ARROW_SCOPED_TRACE(type->ToString());
//     TestCaseWhenRandom(type);
//   }
// }

TEST(TestCaseWhen, DispatchBest) {
  CheckDispatchBest("case_when", {struct_({field("", boolean())}), int64(), int32()},
                    {struct_({field("", boolean())}), int64(), int64()});
  CheckDispatchBest("case_when",
                    {struct_({field("", boolean())}), binary(), large_utf8()},
                    {struct_({field("", boolean())}), large_binary(), large_binary()});
  CheckDispatchBest(
      "case_when",
      {struct_({field("", boolean())}), timestamp(TimeUnit::SECOND), date32()},
      {struct_({field("", boolean())}), timestamp(TimeUnit::SECOND),
       timestamp(TimeUnit::SECOND)});
  CheckDispatchBest(
      "case_when", {struct_({field("", boolean())}), decimal128(38, 0), decimal128(1, 1)},
      {struct_({field("", boolean())}), decimal256(39, 1), decimal256(39, 1)});

  ASSERT_RAISES(Invalid, CallFunction("case_when", ExecBatch({}, 0)));
  // Too many/too few conditions
  ASSERT_RAISES(
      Invalid, CallFunction("case_when", {MakeStruct({ArrayFromJSON(boolean(), "[]")})}));
  ASSERT_RAISES(Invalid,
                CallFunction("case_when", {MakeStruct({}), ArrayFromJSON(int64(), "[]"),
                                           ArrayFromJSON(int64(), "[]")}));
  // Conditions must be struct of boolean
  ASSERT_RAISES(TypeError,
                CallFunction("case_when", {MakeStruct({ArrayFromJSON(int64(), "[]")}),
                                           ArrayFromJSON(int64(), "[]")}));
  ASSERT_RAISES(TypeError, CallFunction("case_when", {ArrayFromJSON(boolean(), "[true]"),
                                                      ArrayFromJSON(int32(), "[0]")}));
  // Values must have compatible types
  ASSERT_RAISES(NotImplemented,
                CallFunction("case_when", {MakeStruct({ArrayFromJSON(boolean(), "[]")}),
                                           ArrayFromJSON(int64(), "[]"),
                                           ArrayFromJSON(utf8(), "[]")}));

  // Do not dictionary-decode when we have only dictionary values
  CheckDispatchBest("case_when",
                    {struct_({field("", boolean())}), dictionary(int64(), utf8()),
                     dictionary(int64(), utf8())},
                    {struct_({field("", boolean())}), dictionary(int64(), utf8()),
                     dictionary(int64(), utf8())});

  // Dictionary-decode if we have a mix
  CheckDispatchBest(
      "case_when", {struct_({field("", boolean())}), dictionary(int64(), utf8()), utf8()},
      {struct_({field("", boolean())}), utf8(), utf8()});
}

template <typename Type>
class TestCoalesceNumeric : public ::testing::Test {};
template <typename Type>
class TestCoalesceBinary : public ::testing::Test {};
template <typename Type>
class TestCoalesceList : public ::testing::Test {};

TYPED_TEST_SUITE(TestCoalesceNumeric, IfElseNumericBasedTypes);
TYPED_TEST_SUITE(TestCoalesceBinary, BaseBinaryArrowTypes);
TYPED_TEST_SUITE(TestCoalesceList, ListArrowTypes);

TYPED_TEST(TestCoalesceNumeric, Basics) {
  auto type = default_type_instance<TypeParam>();
  auto scalar_null = ScalarFromJSON(type, "null");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");

  if (std::is_same<TypeParam, Date64Type>::value) {
    auto scalar1 = ScalarFromJSON(type, "1728000000");
    auto values1 = ArrayFromJSON(type, "[null, 864000000, 950400000, 1036800000]");
    auto values2 =
        ArrayFromJSON(type, "[1123200000, 1209600000, 1296000000, 1382400000]");
    auto values3 = ArrayFromJSON(type, "[17, 18, 19, null]");
    // N.B. all-scalar cases are checked in CheckScalar
    CheckScalar("coalesce", {values_null}, values_null);
    CheckScalar("coalesce", {values_null, scalar1},
                ArrayFromJSON(type, "[1728000000, 1728000000, 1728000000, 1728000000]"));
    CheckScalar("coalesce", {values_null, values1}, values1);
    CheckScalar("coalesce", {values_null, values2}, values2);
    CheckScalar("coalesce", {values1, values_null}, values1);
    CheckScalar("coalesce", {values2, values_null}, values2);
    CheckScalar("coalesce", {scalar_null, values1}, values1);
    CheckScalar("coalesce", {values1, scalar_null}, values1);
    CheckScalar("coalesce", {values2, values1, values_null}, values2);
    CheckScalar("coalesce", {values1, scalar1},
                ArrayFromJSON(type, "[1728000000, 864000000, 950400000, 1036800000]"));
    CheckScalar("coalesce", {values1, values2},
                ArrayFromJSON(type, "[1123200000, 864000000, 950400000, 1036800000]"));
    CheckScalar("coalesce", {values1, values2, values3},
                ArrayFromJSON(type, "[1123200000, 864000000, 950400000, 1036800000]"));
    CheckScalar("coalesce", {scalar1, values1},
                ArrayFromJSON(type, "[1728000000, 1728000000, 1728000000, 1728000000]"));
  } else {
    auto scalar1 = ScalarFromJSON(type, "20");
    auto values1 = ArrayFromJSON(type, "[null, 10, 11, 12]");
    auto values2 = ArrayFromJSON(type, "[13, 14, 15, 16]");
    auto values3 = ArrayFromJSON(type, "[17, 18, 19, null]");
    // N.B. all-scalar cases are checked in CheckScalar
    CheckScalar("coalesce", {values_null}, values_null);
    CheckScalar("coalesce", {values_null, scalar1},
                ArrayFromJSON(type, "[20, 20, 20, 20]"));
    CheckScalar("coalesce", {values_null, values1}, values1);
    CheckScalar("coalesce", {values_null, values2}, values2);
    CheckScalar("coalesce", {values1, values_null}, values1);
    CheckScalar("coalesce", {values2, values_null}, values2);
    CheckScalar("coalesce", {scalar_null, values1}, values1);
    CheckScalar("coalesce", {values1, scalar_null}, values1);
    CheckScalar("coalesce", {values2, values1, values_null}, values2);
    CheckScalar("coalesce", {values1, scalar1}, ArrayFromJSON(type, "[20, 10, 11, 12]"));
    CheckScalar("coalesce", {values1, values2}, ArrayFromJSON(type, "[13, 10, 11, 12]"));
    CheckScalar("coalesce", {values1, values2, values3},
                ArrayFromJSON(type, "[13, 10, 11, 12]"));
    CheckScalar("coalesce", {scalar1, values1}, ArrayFromJSON(type, "[20, 20, 20, 20]"));
  }
}

TYPED_TEST(TestCoalesceNumeric, ListOfType) {
  auto type = list(default_type_instance<TypeParam>());
  auto scalar_null = ScalarFromJSON(type, "null");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");

  if (std::is_same<TypeParam, Date64Type>::value) {
    auto scalar1 = ScalarFromJSON(type, "[1728000000, 2073600000]");
    auto values1 =
        ArrayFromJSON(type, "[null, [864000000, null, 1728000000], [], [null, null]]");
    auto values2 = ArrayFromJSON(
        type,
        "[[1987200000], [1209600000, 2073600000], [null, 1296000000], [1382400000]]");
    auto values3 =
        ArrayFromJSON(type, "[[1468800000, 1555200000], [1641600000], [], null]");
    CheckScalar("coalesce", {values_null}, values_null);
    CheckScalar("coalesce", {values_null, scalar1},
                ArrayFromJSON(type,
                              "[[1728000000, 2073600000], [1728000000, 2073600000], "
                              "[1728000000, 2073600000], [1728000000, 2073600000]]"));
    CheckScalar("coalesce", {values_null, values1}, values1);
    CheckScalar("coalesce", {values_null, values2}, values2);
    CheckScalar("coalesce", {values1, values_null}, values1);
    CheckScalar("coalesce", {values2, values_null}, values2);
    CheckScalar("coalesce", {scalar_null, values1}, values1);
    CheckScalar("coalesce", {values1, scalar_null}, values1);
    CheckScalar("coalesce", {values2, values1, values_null}, values2);
    CheckScalar("coalesce", {values1, scalar1},
                ArrayFromJSON(type,
                              "[[1728000000, 2073600000], [864000000, null, 1728000000], "
                              "[], [null, null]]"));
    CheckScalar(
        "coalesce", {values1, values2},
        ArrayFromJSON(type,
                      "[[1987200000], [864000000, null, 1728000000], [], [null, null]]"));
    CheckScalar(
        "coalesce", {values1, values2, values3},
        ArrayFromJSON(type,
                      "[[1987200000], [864000000, null, 1728000000], [], [null, null]]"));
    CheckScalar("coalesce", {scalar1, values1},
                ArrayFromJSON(type,
                              "[[1728000000, 2073600000], [1728000000, 2073600000], "
                              "[1728000000, 2073600000], [1728000000, 2073600000]]"));
  } else {
    auto scalar1 = ScalarFromJSON(type, "[20, 24]");
    auto values1 = ArrayFromJSON(type, "[null, [10, null, 20], [], [null, null]]");
    auto values2 = ArrayFromJSON(type, "[[23], [14, 24], [null, 15], [16]]");
    auto values3 = ArrayFromJSON(type, "[[17, 18], [19], [], null]");
    CheckScalar("coalesce", {values_null}, values_null);
    CheckScalar("coalesce", {values_null, scalar1},
                ArrayFromJSON(type, "[[20, 24], [20, 24], [20, 24], [20, 24]]"));
    CheckScalar("coalesce", {values_null, values1}, values1);
    CheckScalar("coalesce", {values_null, values2}, values2);
    CheckScalar("coalesce", {values1, values_null}, values1);
    CheckScalar("coalesce", {values2, values_null}, values2);
    CheckScalar("coalesce", {scalar_null, values1}, values1);
    CheckScalar("coalesce", {values1, scalar_null}, values1);
    CheckScalar("coalesce", {values2, values1, values_null}, values2);
    CheckScalar("coalesce", {values1, scalar1},
                ArrayFromJSON(type, "[[20, 24], [10, null, 20], [], [null, null]]"));
    CheckScalar("coalesce", {values1, values2},
                ArrayFromJSON(type, "[[23], [10, null, 20], [], [null, null]]"));
    CheckScalar("coalesce", {values1, values2, values3},
                ArrayFromJSON(type, "[[23], [10, null, 20], [], [null, null]]"));
    CheckScalar("coalesce", {scalar1, values1},
                ArrayFromJSON(type, "[[20, 24], [20, 24], [20, 24], [20, 24]]"));
  }
}

TYPED_TEST(TestCoalesceBinary, Basics) {
  auto type = default_type_instance<TypeParam>();
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"("a")");
  auto values_null = ArrayFromJSON(type, R"([null, null, null, null])");
  auto values1 = ArrayFromJSON(type, R"([null, "bc", "def", "ghij"])");
  auto values2 = ArrayFromJSON(type, R"(["klmno", "p", "qr", "stu"])");
  auto values3 = ArrayFromJSON(type, R"(["vwxy", "zabc", "d", null])");
  // N.B. all-scalar cases are checked in CheckScalar
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar("coalesce", {values_null, scalar1},
              ArrayFromJSON(type, R"(["a", "a", "a", "a"])"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values_null, values2}, values2);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {values2, values_null}, values2);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
  CheckScalar("coalesce", {values2, values1, values_null}, values2);
  CheckScalar("coalesce", {values1, scalar1},
              ArrayFromJSON(type, R"(["a", "bc", "def", "ghij"])"));
  CheckScalar("coalesce", {values1, values2},
              ArrayFromJSON(type, R"(["klmno", "bc", "def", "ghij"])"));
  CheckScalar("coalesce", {values1, values2, values3},
              ArrayFromJSON(type, R"(["klmno", "bc", "def", "ghij"])"));
  CheckScalar("coalesce", {scalar1, values1},
              ArrayFromJSON(type, R"(["a", "a", "a", "a"])"));
}

TYPED_TEST(TestCoalesceList, ListOfString) {
  auto type = std::make_shared<TypeParam>(utf8());
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"([null, "a"])");
  auto values_null = ArrayFromJSON(type, R"([null, null, null, null])");
  auto values1 = ArrayFromJSON(type, R"([null, ["bc", null], ["def"], []])");
  auto values2 = ArrayFromJSON(type, R"([["klmno"], ["p"], ["qr", null], ["stu"]])");
  auto values3 = ArrayFromJSON(type, R"([["vwxy"], [], ["d"], null])");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar(
      "coalesce", {values_null, scalar1},
      ArrayFromJSON(type, R"([[null, "a"], [null, "a"], [null, "a"], [null, "a"]])"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values_null, values2}, values2);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {values2, values_null}, values2);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
  CheckScalar("coalesce", {values2, values1, values_null}, values2);
  CheckScalar("coalesce", {values1, scalar1},
              ArrayFromJSON(type, R"([[null, "a"], ["bc", null], ["def"], []])"));
  CheckScalar("coalesce", {values1, values2},
              ArrayFromJSON(type, R"([["klmno"], ["bc", null], ["def"], []])"));
  CheckScalar("coalesce", {values1, values2, values3},
              ArrayFromJSON(type, R"([["klmno"], ["bc", null], ["def"], []])"));
  CheckScalar(
      "coalesce", {scalar1, values1},
      ArrayFromJSON(type, R"([[null, "a"], [null, "a"], [null, "a"], [null, "a"]])"));
}

// More minimal tests to check type coverage
TYPED_TEST(TestCoalesceList, ListOfBool) {
  auto type = std::make_shared<TypeParam>(boolean());
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, "[true, false, null]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, "[null, [true, null, true], [], [null, null]]");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar("coalesce", {values_null, scalar1},
              ArrayFromJSON(type,
                            "[[true, false, null], [true, false, null], [true, false, "
                            "null], [true, false, null]]"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
}

TYPED_TEST(TestCoalesceList, ListOfInt) {
  auto type = std::make_shared<TypeParam>(int64());
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, "[20, 24]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, "[null, [10, null, 20], [], [null, null]]");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar("coalesce", {values_null, scalar1},
              ArrayFromJSON(type, "[[20, 24], [20, 24], [20, 24], [20, 24]]"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
}

TYPED_TEST(TestCoalesceList, ListOfDayTimeInterval) {
  auto type = std::make_shared<TypeParam>(day_time_interval());
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, "[[20, 24], null]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 =
      ArrayFromJSON(type, "[null, [[10, 12], null, [20, 22]], [], [null, null]]");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar(
      "coalesce", {values_null, scalar1},
      ArrayFromJSON(
          type,
          "[[[20, 24], null], [[20, 24], null], [[20, 24], null], [[20, 24], null]]"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
}

TYPED_TEST(TestCoalesceList, ListOfDecimal) {
  for (auto ty : {decimal128(3, 2), decimal256(3, 2)}) {
    auto type = std::make_shared<TypeParam>(ty);
    auto scalar_null = ScalarFromJSON(type, "null");
    auto scalar1 = ScalarFromJSON(type, R"(["0.42", null])");
    auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
    auto values1 = ArrayFromJSON(type, R"([null, ["1.23"], [], [null, null]])");
    CheckScalar("coalesce", {values_null}, values_null);
    CheckScalar(
        "coalesce", {values_null, scalar1},
        ArrayFromJSON(
            type, R"([["0.42", null], ["0.42", null], ["0.42", null], ["0.42", null]])"));
    CheckScalar("coalesce", {values_null, values1}, values1);
    CheckScalar("coalesce", {values1, values_null}, values1);
    CheckScalar("coalesce", {scalar_null, values1}, values1);
    CheckScalar("coalesce", {values1, scalar_null}, values1);
  }
}

TYPED_TEST(TestCoalesceList, ListOfFixedSizeBinary) {
  auto type = std::make_shared<TypeParam>(fixed_size_binary(3));
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"(["ab!", null])");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, R"([null, ["def"], [], [null, null]])");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar(
      "coalesce", {values_null, scalar1},
      ArrayFromJSON(type,
                    R"([["ab!", null], ["ab!", null], ["ab!", null], ["ab!", null]])"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
}

TYPED_TEST(TestCoalesceList, ListOfListOfInt) {
  auto type = std::make_shared<TypeParam>(std::make_shared<TypeParam>(int64()));
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, "[[20], null]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, "[null, [[10, 12], null, []], [], [null, null]]");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar(
      "coalesce", {values_null, scalar1},
      ArrayFromJSON(type, "[[[20], null], [[20], null], [[20], null], [[20], null]]"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
}

TYPED_TEST(TestCoalesceList, Errors) {
  auto type1 = std::make_shared<TypeParam>(int64());
  auto type2 = std::make_shared<TypeParam>(utf8());
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError, ::testing::HasSubstr("All types must be compatible"),
      CallFunction("coalesce", {
                                   ArrayFromJSON(type1, "[null]"),
                                   ArrayFromJSON(type2, "[null]"),
                               }));
}

template <typename Type>
class TestCoalesceDict : public ::testing::Test {};

TYPED_TEST_SUITE(TestCoalesceDict, IntegralArrowTypes);

TYPED_TEST(TestCoalesceDict, Simple) {
  for (const auto& dict :
       {JsonDict{utf8(), R"(["a", null, "bc", "def"])"},
        JsonDict{int64(), "[1, null, 2, 3]"},
        JsonDict{decimal256(3, 2), R"(["1.23", null, "3.45", "6.78"])"}}) {
    auto type = dictionary(default_type_instance<TypeParam>(), dict.type);
    auto values_null = DictArrayFromJSON(type, "[null, null, null, null]", dict.value);
    auto values1 = DictArrayFromJSON(type, "[0, null, 3, null]", dict.value);
    auto values2 = DictArrayFromJSON(type, "[2, 1, null, null]", dict.value);
    auto scalar = DictScalarFromJSON(type, "2", dict.value);

    // Easy case: all arguments have the same dictionary
    CheckDictionary("coalesce", {values1, values2});
    CheckDictionary("coalesce", {values1, values2, values1});
    CheckDictionary("coalesce", {values_null, values1});
    CheckDictionary("coalesce", {values1, values_null});
    CheckDictionary("coalesce", {values1, scalar});
    CheckDictionary("coalesce", {values_null, scalar});
    CheckDictionary("coalesce", {scalar, values1});
  }
}

TYPED_TEST(TestCoalesceDict, Mixed) {
  auto index_type = default_type_instance<TypeParam>();
  auto type = dictionary(index_type, utf8());
  auto dict = R"(["a", null, "bc", "def"])";
  auto values_null = DictArrayFromJSON(type, "[null, null, null, null]", dict);
  auto values1_dict = DictArrayFromJSON(type, "[0, null, 3, 1]", dict);
  auto values1_decoded = ArrayFromJSON(utf8(), R"(["a", null, "def", null])");
  auto values2_dict = DictArrayFromJSON(type, "[2, 1, null, 0]", dict);
  auto values2_decoded = ArrayFromJSON(utf8(), R"(["bc", null, null, "a"])");
  auto scalar = ScalarFromJSON(utf8(), R"("bc")");

  // If we have mixed dictionary/non-dictionary arguments, we decode dictionaries
  CheckDictionary("coalesce", {values1_dict, values2_decoded},
                  /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {values1_decoded, values2_dict},
                  /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {values1_dict, values2_dict, values1_decoded},
                  /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {values_null, values2_dict, values1_decoded},
                  /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {values_null, scalar}, /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {scalar, values_null}, /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {values1_dict, scalar}, /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {scalar, values2_dict}, /*result_is_encoded=*/false);

  // If we have mismatched dictionary types, we decode (for now)
  auto values3_dict =
      DictArrayFromJSON(dictionary(index_type, binary()), "[2, 1, null, 0]", dict);
  auto values4_dict = DictArrayFromJSON(
      dictionary(index_type->id() == Type::UINT8 ? int8() : uint8(), utf8()),
      "[2, 1, null, 0]", dict);
  CheckDictionary("coalesce", {values1_dict, values3_dict}, /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {values1_dict, values4_dict}, /*result_is_encoded=*/false);
}

TYPED_TEST(TestCoalesceDict, NestedSimple) {
  auto index_type = default_type_instance<TypeParam>();
  auto inner_type = dictionary(index_type, utf8());
  auto type = list(inner_type);
  auto dict = R"(["a", null, "bc", "def"])";
  auto values_null = MakeListOfDict(ArrayFromJSON(int32(), "[null, null, null, null, 0]"),
                                    DictArrayFromJSON(inner_type, "[]", dict));
  auto values1_backing = DictArrayFromJSON(inner_type, "[0, null, 3, 1]", dict);
  auto values2_backing = DictArrayFromJSON(inner_type, "[2, 1, null, 0]", dict);
  auto values1 =
      MakeListOfDict(ArrayFromJSON(int32(), "[0, 2, 2, 3, 4]"), values1_backing);
  auto values2 =
      MakeListOfDict(ArrayFromJSON(int32(), "[0, 1, null, 2, 4]"), values2_backing);
  auto scalar =
      Datum(std::make_shared<ListScalar>(DictArrayFromJSON(inner_type, "[0, 1]", dict)));

  CheckDictionary("coalesce", {values1, values2}, /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {values1, scalar}, /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {scalar, values2}, /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {values_null, values2}, /*result_is_encoded=*/false);
  CheckDictionary("coalesce", {values1, values_null}, /*result_is_encoded=*/false);
}

TYPED_TEST(TestCoalesceDict, DifferentDictionaries) {
  auto type = dictionary(default_type_instance<TypeParam>(), utf8());
  auto dict1 = R"(["a", "", "bc", "def"])";
  auto dict2 = R"(["bc", "foo", "", "a"])";
  auto values1_null = DictArrayFromJSON(type, "[null, null, null, null]", dict1);
  auto values2_null = DictArrayFromJSON(type, "[null, null, null, null]", dict2);
  auto values1 = DictArrayFromJSON(type, "[null, 0, 3, 1]", dict1);
  auto values2 = DictArrayFromJSON(type, "[2, 1, 0, null]", dict2);
  auto scalar1 = DictScalarFromJSON(type, "0", dict1);
  auto scalar2 = DictScalarFromJSON(type, "0", dict2);

  CheckDictionary("coalesce", {values1, values2});
  CheckDictionary("coalesce", {values1, scalar2});
  CheckDictionary("coalesce", {scalar1, values2});
  CheckDictionary("coalesce", {values1, scalar2});
  CheckDictionary("coalesce", {values1_null, values2});
  CheckDictionary("coalesce", {values1, values2_null});

  // Test dictionaries with nulls (where decoding before/after calling coalesce changes
  // the results)
  dict1 = R"(["a", null, "bc", "def"])";
  dict2 = R"(["bc", "foo", null, "a"])";
  values1 = DictArrayFromJSON(type, "[null, 0, 3, 1]", dict1);
  values2 = DictArrayFromJSON(type, "[2, 1, 0, null]", dict2);
  scalar1 = DictScalarFromJSON(type, "0", dict1);

  // Note this is sensitive to the implementation. Nulls are emitted here
  // because a non-null index mapped to a null dictionary value and was emitted
  // as a null (instead of encoding null in the dictionary)
  CheckScalarNonRecursive(
      "coalesce", {values1, values2},
      DictArrayFromJSON(type, "[null, 0, 1, null]", R"(["a", "def"])"));
  CheckScalarNonRecursive("coalesce", {values1, scalar1},
                          DictArrayFromJSON(type, "[0, 0, 1, null]", R"(["a", "def"])"));
  // The dictionary gets preserved since a leading non-null scalar just gets
  // broadcasted and returned without going through the rest of the kernel
  // implementation
  CheckScalarNonRecursive("coalesce", {scalar1, values1},
                          DictArrayFromJSON(type, "[0, 0, 0, 0]", dict1));
}

TEST(TestCoalesce, Null) {
  auto type = null();
  auto scalar_null = ScalarFromJSON(type, "null");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar("coalesce", {values_null, scalar_null}, values_null);
}

TEST(TestCoalesce, Boolean) {
  auto type = boolean();
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, "false");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, "[null, true, false, true]");
  auto values2 = ArrayFromJSON(type, "[true, false, true, false]");
  auto values3 = ArrayFromJSON(type, "[false, true, false, null]");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar("coalesce", {values_null, scalar1},
              ArrayFromJSON(type, "[false, false, false, false]"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values_null, values2}, values2);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {values2, values_null}, values2);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
  CheckScalar("coalesce", {values2, values1, values_null}, values2);
  CheckScalar("coalesce", {values1, scalar1},
              ArrayFromJSON(type, "[false, true, false, true]"));
  CheckScalar("coalesce", {values1, values2},
              ArrayFromJSON(type, "[true, true, false, true]"));
  CheckScalar("coalesce", {values1, values2, values3},
              ArrayFromJSON(type, "[true, true, false, true]"));
  CheckScalar("coalesce", {scalar1, values1},
              ArrayFromJSON(type, "[false, false, false, false]"));
}

TEST(TestCoalesce, DayTimeInterval) {
  auto type = day_time_interval();
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, "[1, 2]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, "[null, [3, 4], [5, 6], [7, 8]]");
  auto values2 = ArrayFromJSON(type, "[[9, 10], [11, 12], [13, 14], [15, 16]]");
  auto values3 = ArrayFromJSON(type, "[[17, 18], [19, 20], [21, 22], null]");
  // N.B. all-scalar cases are checked in CheckScalar
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar("coalesce", {values_null, scalar1},
              ArrayFromJSON(type, "[[1, 2], [1, 2], [1, 2], [1, 2]]"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values_null, values2}, values2);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {values2, values_null}, values2);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
  CheckScalar("coalesce", {values2, values1, values_null}, values2);
  CheckScalar("coalesce", {values1, scalar1},
              ArrayFromJSON(type, "[[1, 2], [3, 4], [5, 6], [7, 8]]"));
  CheckScalar("coalesce", {values1, values2},
              ArrayFromJSON(type, "[[9, 10], [3, 4], [5, 6], [7, 8]]"));
  CheckScalar("coalesce", {values1, values2, values3},
              ArrayFromJSON(type, "[[9, 10], [3, 4], [5, 6], [7, 8]]"));
  CheckScalar("coalesce", {scalar1, values1},
              ArrayFromJSON(type, "[[1, 2], [1, 2], [1, 2], [1, 2]]"));
}

TEST(TestCoalesce, MonthDayNanoInterval) {
  auto type = month_day_nano_interval();
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, "[1, 2, 3]");
  auto values_null = ArrayFromJSON(type, "[null, null, null, null]");
  auto values1 = ArrayFromJSON(type, "[null, [3, 4, 5], [5, 6, 7], [7, 8, 9]]");
  auto values2 =
      ArrayFromJSON(type, "[[9, 10, 0], [11, 12, 1], [13, 14, 2], [15, 16, 3]]");
  auto values3 = ArrayFromJSON(type, "[[17, 18, 4], [19, 20, 5], [21, 22, 6], null]");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar("coalesce", {values_null, scalar1},
              ArrayFromJSON(type, "[[1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3]]"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values_null, values2}, values2);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {values2, values_null}, values2);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
  CheckScalar("coalesce", {values2, values1, values_null}, values2);
  CheckScalar("coalesce", {values1, scalar1},
              ArrayFromJSON(type, "[[1, 2, 3], [3, 4, 5], [5, 6, 7], [7, 8, 9]]"));
  CheckScalar("coalesce", {values1, values2},
              ArrayFromJSON(type, "[[9, 10, 0], [3, 4, 5], [5, 6, 7], [7, 8, 9]]"));
  CheckScalar("coalesce", {values1, values2, values3},
              ArrayFromJSON(type, "[[9, 10, 0], [3, 4, 5], [5, 6, 7], [7, 8, 9]]"));
  CheckScalar("coalesce", {scalar1, values1},
              ArrayFromJSON(type, "[[1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3]]"));
}

TEST(TestCoalesce, Decimal) {
  for (const auto& type :
       std::vector<std::shared_ptr<DataType>>{decimal128(3, 2), decimal256(3, 2)}) {
    auto scalar_null = ScalarFromJSON(type, "null");
    auto scalar1 = ScalarFromJSON(type, R"("1.23")");
    auto values_null = ArrayFromJSON(type, R"([null, null, null, null])");
    auto values1 = ArrayFromJSON(type, R"([null, "4.56", "7.89", "1.34"])");
    auto values2 = ArrayFromJSON(type, R"(["1.45", "2.34", "3.45", "4.56"])");
    auto values3 = ArrayFromJSON(type, R"(["5.67", "6.78", "7.91", null])");
    CheckScalar("coalesce", {values_null}, values_null);
    CheckScalar("coalesce", {values_null, scalar1},
                ArrayFromJSON(type, R"(["1.23", "1.23", "1.23", "1.23"])"));
    CheckScalar("coalesce", {values_null, values1}, values1);
    CheckScalar("coalesce", {values_null, values2}, values2);
    CheckScalar("coalesce", {values1, values_null}, values1);
    CheckScalar("coalesce", {values2, values_null}, values2);
    CheckScalar("coalesce", {scalar_null, values1}, values1);
    CheckScalar("coalesce", {values1, scalar_null}, values1);
    CheckScalar("coalesce", {values2, values1, values_null}, values2);
    CheckScalar("coalesce", {values1, scalar1},
                ArrayFromJSON(type, R"(["1.23", "4.56", "7.89", "1.34"])"));
    CheckScalar("coalesce", {values1, values2},
                ArrayFromJSON(type, R"(["1.45", "4.56", "7.89", "1.34"])"));
    CheckScalar("coalesce", {values1, values2, values3},
                ArrayFromJSON(type, R"(["1.45", "4.56", "7.89", "1.34"])"));
    CheckScalar("coalesce", {scalar1, values1},
                ArrayFromJSON(type, R"(["1.23", "1.23", "1.23", "1.23"])"));
  }
  // Ensure promotion
  CheckScalar("coalesce",
              {
                  ArrayFromJSON(decimal128(3, 2), R"(["1.23", null])"),
                  ArrayFromJSON(decimal128(4, 1), R"([null, "1.0"])"),
              },
              ArrayFromJSON(decimal128(5, 2), R"(["1.23", "1.00"])"));
}

TEST(TestCoalesce, FixedSizeBinary) {
  auto type = fixed_size_binary(3);
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"("abc")");
  auto values_null = ArrayFromJSON(type, R"([null, null, null, null])");
  auto values1 = ArrayFromJSON(type, R"([null, "def", "ghi", "jkl"])");
  auto values2 = ArrayFromJSON(type, R"(["mno", "pqr", "stu", "vwx"])");
  auto values3 = ArrayFromJSON(type, R"(["yza", "bcd", "efg", null])");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar("coalesce", {values_null, scalar1},
              ArrayFromJSON(type, R"(["abc", "abc", "abc", "abc"])"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values_null, values2}, values2);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {values2, values_null}, values2);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
  CheckScalar("coalesce", {values2, values1, values_null}, values2);
  CheckScalar("coalesce", {values1, scalar1},
              ArrayFromJSON(type, R"(["abc", "def", "ghi", "jkl"])"));
  CheckScalar("coalesce", {values1, values2},
              ArrayFromJSON(type, R"(["mno", "def", "ghi", "jkl"])"));
  CheckScalar("coalesce", {values1, values2, values3},
              ArrayFromJSON(type, R"(["mno", "def", "ghi", "jkl"])"));
  CheckScalar("coalesce", {scalar1, values1},
              ArrayFromJSON(type, R"(["abc", "abc", "abc", "abc"])"));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("All types must be compatible, expected: "
                           "fixed_size_binary[3], but got: fixed_size_binary[2]"),
      CallFunction("coalesce", {
                                   ArrayFromJSON(type, "[null]"),
                                   ArrayFromJSON(fixed_size_binary(2), "[null]"),
                               }));
}

TEST(TestCoalesce, FixedSizeListOfInt) {
  auto type = fixed_size_list(uint8(), 2);
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"([42, null])");
  auto values_null = ArrayFromJSON(type, R"([null, null, null, null])");
  auto values1 = ArrayFromJSON(type, R"([null, [2, null], [4, 8], [null, null]])");
  auto values2 = ArrayFromJSON(type, R"([[1, 5], [16, 32], [64, null], [null, 128]])");
  auto values3 = ArrayFromJSON(type, R"([[null, null], [1, 3], [9, 27], null])");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar("coalesce", {values_null, scalar1},
              ArrayFromJSON(type, R"([[42, null], [42, null], [42, null], [42, null]])"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values_null, values2}, values2);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {values2, values_null}, values2);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
  CheckScalar("coalesce", {values2, values1, values_null}, values2);
  CheckScalar("coalesce", {values1, scalar1},
              ArrayFromJSON(type, R"([[42, null], [2, null], [4, 8], [null, null]])"));
  CheckScalar("coalesce", {values1, values2},
              ArrayFromJSON(type, R"([[1, 5], [2, null], [4, 8], [null, null]])"));
  CheckScalar("coalesce", {values1, values2, values3},
              ArrayFromJSON(type, R"([[1, 5], [2, null], [4, 8], [null, null]])"));
  CheckScalar("coalesce", {scalar1, values1},
              ArrayFromJSON(type, R"([[42, null], [42, null], [42, null], [42, null]])"));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr(
          "All types must be compatible, expected: fixed_size_list<item: "
          "uint8>[2], but got: fixed_size_list<item: uint8>[3]"),
      CallFunction("coalesce", {
                                   ArrayFromJSON(type, "[null]"),
                                   ArrayFromJSON(fixed_size_list(uint8(), 3), "[null]"),
                               }));
}

TEST(TestCoalesce, FixedSizeListOfString) {
  auto type = fixed_size_list(utf8(), 2);
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"(["abc", null])");
  auto values_null = ArrayFromJSON(type, R"([null, null, null, null])");
  auto values1 =
      ArrayFromJSON(type, R"([null, ["d", null], ["ghi", "jkl"], [null, null]])");
  auto values2 = ArrayFromJSON(
      type, R"([["mno", "pq"], ["pqr", "ab"], ["stu", null], [null, "vwx"]])");
  auto values3 =
      ArrayFromJSON(type, R"([[null, null], ["a", "bcd"], ["d", "efg"], null])");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar(
      "coalesce", {values_null, scalar1},
      ArrayFromJSON(type,
                    R"([["abc", null], ["abc", null], ["abc", null], ["abc", null]])"));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values_null, values2}, values2);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {values2, values_null}, values2);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
  CheckScalar("coalesce", {values2, values1, values_null}, values2);
  CheckScalar("coalesce", {values1, scalar1},
              ArrayFromJSON(
                  type, R"([["abc", null], ["d", null], ["ghi", "jkl"], [null, null]])"));
  CheckScalar("coalesce", {values1, values2},
              ArrayFromJSON(
                  type, R"([["mno", "pq"], ["d", null], ["ghi", "jkl"], [null, null]])"));
  CheckScalar("coalesce", {values1, values2, values3},
              ArrayFromJSON(
                  type, R"([["mno", "pq"], ["d", null], ["ghi", "jkl"], [null, null]])"));
  CheckScalar(
      "coalesce", {scalar1, values1},
      ArrayFromJSON(type,
                    R"([["abc", null], ["abc", null], ["abc", null], ["abc", null]])"));
}

TEST(TestCoalesce, Map) {
  auto type = map(int64(), utf8());
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"([[1, "a"], [5, "bc"]])");
  auto values_null = ArrayFromJSON(type, R"([null, null, null, null])");
  auto values1 =
      ArrayFromJSON(type, R"([null, [[2, "foo"], [4, null]], [[3, "test"]], []])");
  auto values2 = ArrayFromJSON(
      type, R"([[[1, "b"]], [[2, "c"]], [[5, "c"], [6, "d"]], [[7, "abc"]]])");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar("coalesce", {values_null, scalar1}, *MakeArrayFromScalar(*scalar1, 4));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values_null, values2}, values2);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {values2, values_null}, values2);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
  CheckScalar("coalesce", {values2, values1, values_null}, values2);
  CheckScalar(
      "coalesce", {values1, scalar1},
      ArrayFromJSON(
          type,
          R"([[[1, "a"], [5, "bc"]], [[2, "foo"], [4, null]], [[3, "test"]], []])"));
  CheckScalar(
      "coalesce", {values1, values2},
      ArrayFromJSON(type, R"([[[1, "b"]], [[2, "foo"], [4, null]], [[3, "test"]], []])"));
  CheckScalar("coalesce", {scalar1, values1}, *MakeArrayFromScalar(*scalar1, 4));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("All types must be compatible, expected: map<int64, "
                           "string>, but got: map<int64, int32>"),
      CallFunction("coalesce", {
                                   ArrayFromJSON(type, "[null]"),
                                   ArrayFromJSON(map(int64(), int32()), "[null]"),
                               }));
}

TEST(TestCoalesce, Struct) {
  auto type = struct_(
      {field("int", uint32()), field("str", utf8()), field("list", list(int8()))});
  auto scalar_null = ScalarFromJSON(type, "null");
  auto scalar1 = ScalarFromJSON(type, R"([42, "spam", [null, -1]])");
  auto values_null = ArrayFromJSON(type, R"([null, null, null, null])");
  auto values1 = ArrayFromJSON(
      type, R"([null, [null, "eggs", []], [0, "", [null]], [32, "abc", [1, 2, 3]]])");
  auto values2 = ArrayFromJSON(
      type,
      R"([[21, "foobar", [1, null, 2]], [5, "bar", []], [20, null, null], [1, "", [null]]])");
  CheckScalar("coalesce", {values_null}, values_null);
  CheckScalar("coalesce", {values_null, scalar1}, *MakeArrayFromScalar(*scalar1, 4));
  CheckScalar("coalesce", {values_null, values1}, values1);
  CheckScalar("coalesce", {values_null, values2}, values2);
  CheckScalar("coalesce", {values1, values_null}, values1);
  CheckScalar("coalesce", {values2, values_null}, values2);
  CheckScalar("coalesce", {scalar_null, values1}, values1);
  CheckScalar("coalesce", {values1, scalar_null}, values1);
  CheckScalar("coalesce", {values2, values1, values_null}, values2);
  CheckScalar(
      "coalesce", {values1, scalar1},
      ArrayFromJSON(
          type,
          R"([[42, "spam", [null, -1]], [null, "eggs", []], [0, "", [null]], [32, "abc", [1, 2, 3]]])"));
  CheckScalar(
      "coalesce", {values1, values2},
      ArrayFromJSON(
          type,
          R"([[21, "foobar", [1, null, 2]], [null, "eggs", []], [0, "", [null]], [32, "abc", [1, 2, 3]]])"));
  CheckScalar("coalesce", {scalar1, values1}, *MakeArrayFromScalar(*scalar1, 4));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("All types must be compatible, expected: struct<str: "
                           "string>, but got: struct<int: uint16>"),
      CallFunction("coalesce",
                   {
                       ArrayFromJSON(struct_({field("str", utf8())}), "[null]"),
                       ArrayFromJSON(struct_({field("int", uint16())}), "[null]"),
                   }));
}

TEST(TestCoalesce, UnionBoolString) {
  for (const auto& type : {
           sparse_union({field("a", boolean()), field("b", utf8())}, {2, 7}),
           dense_union({field("a", boolean()), field("b", utf8())}, {2, 7}),
       }) {
    auto scalar_null = ScalarFromJSON(type, "null");
    auto scalar1 = ScalarFromJSON(type, R"([7, "foo"])");
    auto values_null = ArrayFromJSON(type, R"([null, null, null, null])");
    auto values1 = ArrayFromJSON(type, R"([null, [2, false], [7, "bar"], [7, "baz"]])");
    auto values2 =
        ArrayFromJSON(type, R"([[2, true], [2, false], [7, "foo"], [7, "bar"]])");
    CheckScalar("coalesce", {values_null}, values_null);
    CheckScalar("coalesce", {values_null, scalar1}, *MakeArrayFromScalar(*scalar1, 4));
    CheckScalar("coalesce", {values_null, values1}, values1);
    CheckScalar("coalesce", {values_null, values2}, values2);
    CheckScalar("coalesce", {values1, values_null}, values1);
    CheckScalar("coalesce", {values2, values_null}, values2);
    CheckScalar("coalesce", {scalar_null, values1}, values1);
    CheckScalar("coalesce", {values1, scalar_null}, values1);
    CheckScalar("coalesce", {values2, values1, values_null}, values2);
    CheckScalar(
        "coalesce", {values1, scalar1},
        ArrayFromJSON(type, R"([[7, "foo"], [2, false], [7, "bar"], [7, "baz"]])"));
    CheckScalar(
        "coalesce", {values1, values2},
        ArrayFromJSON(type, R"([[2, true], [2, false], [7, "bar"], [7, "baz"]])"));
    CheckScalar("coalesce", {scalar1, values1}, *MakeArrayFromScalar(*scalar1, 4));
  }

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("All types must be compatible, expected: "
                           "sparse_union<a: bool=0>, but got: sparse_union<a: int64=0>"),
      CallFunction(
          "coalesce",
          {
              ArrayFromJSON(sparse_union({field("a", boolean())}), "[[0, true]]"),
              ArrayFromJSON(sparse_union({field("a", int64())}), "[[0, 1]]"),
          }));
}

TEST(TestCoalesce, DispatchBest) {
  CheckDispatchBest("coalesce", {int8(), float64()}, {float64(), float64()});
  CheckDispatchBest("coalesce", {int8(), uint32()}, {int64(), int64()});
  CheckDispatchBest("coalesce", {binary(), utf8()}, {binary(), binary()});
  CheckDispatchBest("coalesce", {binary(), large_binary()},
                    {large_binary(), large_binary()});
  CheckDispatchBest("coalesce", {int32(), decimal128(3, 2)},
                    {decimal128(12, 2), decimal128(12, 2)});
  CheckDispatchBest("coalesce", {float32(), decimal128(3, 2)}, {float64(), float64()});
  CheckDispatchBest("coalesce", {decimal128(3, 2), decimal256(3, 2)},
                    {decimal256(3, 2), decimal256(3, 2)});
  CheckDispatchBest("coalesce", {timestamp(TimeUnit::SECOND), date32()},
                    {timestamp(TimeUnit::SECOND), timestamp(TimeUnit::SECOND)});
  CheckDispatchBest("coalesce", {timestamp(TimeUnit::SECOND), timestamp(TimeUnit::MILLI)},
                    {timestamp(TimeUnit::MILLI), timestamp(TimeUnit::MILLI)});
  CheckDispatchFails("coalesce", {
                                     sparse_union({field("a", boolean())}),
                                     dense_union({field("a", boolean())}),
                                 });
  CheckDispatchBest("coalesce",
                    {dictionary(int8(), binary()), dictionary(int16(), large_utf8())},
                    {large_binary(), large_binary()});
}

template <typename Type>
class TestChooseNumeric : public ::testing::Test {};
template <typename Type>
class TestChooseBinary : public ::testing::Test {};

TYPED_TEST_SUITE(TestChooseNumeric, IfElseNumericBasedTypes);
TYPED_TEST_SUITE(TestChooseBinary, BaseBinaryArrowTypes);

TYPED_TEST(TestChooseNumeric, FixedSize) {
  auto type = default_type_instance<TypeParam>();
  auto indices1 = ArrayFromJSON(int64(), "[0, 1, 0, 1, null]");
  auto scalar_null = ScalarFromJSON(type, "null");
  auto nulls = ArrayFromJSON(type, "[null, null, null, null, null]");

  if (std::is_same<TypeParam, Date64Type>::value) {
    auto values1 = ArrayFromJSON(type, "[864000000, 950400000, null, null, 1209600000]");
    auto values2 =
        ArrayFromJSON(type, "[1728000000, 1814400000, null, null, 2073600000]");
    auto scalar1 = ScalarFromJSON(type, "3628800000");

    CheckScalar("choose", {indices1, values1, values2},
                ArrayFromJSON(type, "[864000000, 1814400000, null, null, null]"));
    CheckScalar("choose", {indices1, ScalarFromJSON(type, "864000000"), values1},
                ArrayFromJSON(type, "[864000000, 950400000, 864000000, null, null]"));
    // Mixed scalar and array (note CheckScalar checks all-scalar cases for us)
    CheckScalar("choose", {ScalarFromJSON(int64(), "0"), values1, values2}, values1);
    CheckScalar("choose", {ScalarFromJSON(int64(), "1"), values1, values2}, values2);
    CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
    CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar1, values2},
                *MakeArrayFromScalar(*scalar1, 5));
    CheckScalar("choose", {ScalarFromJSON(int64(), "1"), scalar1, values2}, values2);
    CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
    CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar_null, values2},
                *MakeArrayOfNull(type, 5));
  } else {
    auto values1 = ArrayFromJSON(type, "[10, 11, null, null, 14]");
    auto values2 = ArrayFromJSON(type, "[20, 21, null, null, 24]");
    auto scalar1 = ScalarFromJSON(type, "42");

    CheckScalar("choose", {indices1, values1, values2},
                ArrayFromJSON(type, "[10, 21, null, null, null]"));
    CheckScalar("choose", {indices1, ScalarFromJSON(type, "1"), values1},
                ArrayFromJSON(type, "[1, 11, 1, null, null]"));
    // Mixed scalar and array (note CheckScalar checks all-scalar cases for us)
    CheckScalar("choose", {ScalarFromJSON(int64(), "0"), values1, values2}, values1);
    CheckScalar("choose", {ScalarFromJSON(int64(), "1"), values1, values2}, values2);
    CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
    CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar1, values2},
                *MakeArrayFromScalar(*scalar1, 5));
    CheckScalar("choose", {ScalarFromJSON(int64(), "1"), scalar1, values2}, values2);
    CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
    CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar_null, values2},
                *MakeArrayOfNull(type, 5));
  }
}

TYPED_TEST(TestChooseBinary, Basics) {
  auto type = default_type_instance<TypeParam>();
  auto indices1 = ArrayFromJSON(int64(), "[0, 1, 0, 1, null]");
  auto values1 = ArrayFromJSON(type, R"(["a", "bc", null, null, "def"])");
  auto values2 = ArrayFromJSON(type, R"(["ghij", "klmno", null, null, "pqrstu"])");
  auto nulls = ArrayFromJSON(type, "[null, null, null, null, null]");
  CheckScalar("choose", {indices1, values1, values2},
              ArrayFromJSON(type, R"(["a", "klmno", null, null, null])"));
  CheckScalar("choose", {indices1, ScalarFromJSON(type, R"("foo")"), values1},
              ArrayFromJSON(type, R"(["foo", "bc", "foo", null, null])"));
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), values1, values2}, values1);
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), values1, values2}, values2);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
  auto scalar1 = ScalarFromJSON(type, R"("abcd")");
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar1, values2},
              *MakeArrayFromScalar(*scalar1, 5));
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), scalar1, values2}, values2);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
  auto scalar_null = ScalarFromJSON(type, "null");
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar_null, values2},
              *MakeArrayOfNull(type, 5));
}

TEST(TestChoose, Null) {
  auto type = null();
  auto indices1 = ArrayFromJSON(int64(), "[0, 1, 0, 1, null]");
  auto nulls = *MakeArrayOfNull(type, 5);
  CheckScalar("choose", {indices1, nulls, nulls}, nulls);
  CheckScalar("choose", {indices1, MakeNullScalar(type), nulls}, nulls);
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), nulls, nulls}, nulls);
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), nulls, nulls}, nulls);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), nulls, nulls}, nulls);
  auto scalar_null = ScalarFromJSON(type, "null");
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar_null, nulls}, nulls);
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), scalar_null, nulls}, nulls);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), nulls, nulls}, nulls);
}

TEST(TestChoose, Boolean) {
  auto type = boolean();
  auto indices1 = ArrayFromJSON(int64(), "[0, 1, 0, 1, null]");
  auto values1 = ArrayFromJSON(type, "[true, true, null, null, true]");
  auto values2 = ArrayFromJSON(type, "[false, false, null, null, false]");
  auto nulls = ArrayFromJSON(type, "[null, null, null, null, null]");
  CheckScalar("choose", {indices1, values1, values2},
              ArrayFromJSON(type, "[true, false, null, null, null]"));
  CheckScalar("choose", {indices1, ScalarFromJSON(type, "false"), values1},
              ArrayFromJSON(type, "[false, true, false, null, null]"));
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), values1, values2}, values1);
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), values1, values2}, values2);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
  auto scalar1 = ScalarFromJSON(type, "true");
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar1, values2},
              *MakeArrayFromScalar(*scalar1, 5));
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), scalar1, values2}, values2);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
  auto scalar_null = ScalarFromJSON(type, "null");
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar_null, values2},
              *MakeArrayOfNull(type, 5));
}

TEST(TestChoose, DayTimeInterval) {
  auto type = day_time_interval();
  auto indices1 = ArrayFromJSON(int64(), "[0, 1, 0, 1, null]");
  auto values1 = ArrayFromJSON(type, "[[10, 1], [10, 1], null, null, [10, 1]]");
  auto values2 = ArrayFromJSON(type, "[[2, 20], [2, 20], null, null, [2, 20]]");
  auto nulls = ArrayFromJSON(type, "[null, null, null, null, null]");
  CheckScalar("choose", {indices1, values1, values2},
              ArrayFromJSON(type, "[[10, 1], [2, 20], null, null, null]"));
  CheckScalar("choose", {indices1, ScalarFromJSON(type, "[1, 2]"), values1},
              ArrayFromJSON(type, "[[1, 2], [10, 1], [1, 2], null, null]"));
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), values1, values2}, values1);
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), values1, values2}, values2);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
  auto scalar1 = ScalarFromJSON(type, "[10, 1]");
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar1, values2},
              *MakeArrayFromScalar(*scalar1, 5));
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), scalar1, values2}, values2);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
  auto scalar_null = ScalarFromJSON(type, "null");
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar_null, values2},
              *MakeArrayOfNull(type, 5));
}

TEST(TestChoose, MonthDayNanoInterval) {
  auto type = month_day_nano_interval();
  auto indices1 = ArrayFromJSON(int64(), "[0, 1, 0, 1, null]");
  auto values1 = ArrayFromJSON(type, "[[10, 1, 0], [10, 1, 0], null, null, [10, 1, 0]]");
  auto values2 = ArrayFromJSON(type, "[[2, 20, 4], [2, 20, 4], null, null, [2, 20, 4]]");
  auto nulls = ArrayFromJSON(type, "[null, null, null, null, null]");
  CheckScalar("choose", {indices1, values1, values2},
              ArrayFromJSON(type, "[[10, 1, 0], [2, 20, 4], null, null, null]"));
  CheckScalar("choose", {indices1, ScalarFromJSON(type, "[1, 2, 3]"), values1},
              ArrayFromJSON(type, "[[1, 2, 3], [10, 1, 0], [1, 2, 3], null, null]"));
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), values1, values2}, values1);
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), values1, values2}, values2);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
  auto scalar1 = ScalarFromJSON(type, "[10, 1, 0]");
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar1, values2},
              *MakeArrayFromScalar(*scalar1, 5));
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), scalar1, values2}, values2);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
  auto scalar_null = ScalarFromJSON(type, "null");
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar_null, values2},
              *MakeArrayOfNull(type, 5));
}

TEST(TestChoose, Decimal) {
  for (const auto& type : {decimal128(3, 2), decimal256(3, 2)}) {
    auto indices1 = ArrayFromJSON(int64(), "[0, 1, 0, 1, null]");
    auto values1 = ArrayFromJSON(type, R"(["1.23", "1.24", null, null, "1.25"])");
    auto values2 = ArrayFromJSON(type, R"(["4.56", "4.57", null, null, "4.58"])");
    auto nulls = ArrayFromJSON(type, "[null, null, null, null, null]");
    CheckScalar("choose", {indices1, values1, values2},
                ArrayFromJSON(type, R"(["1.23", "4.57", null, null, null])"));
    CheckScalar("choose", {indices1, ScalarFromJSON(type, R"("2.34")"), values1},
                ArrayFromJSON(type, R"(["2.34", "1.24", "2.34", null, null])"));
    CheckScalar("choose", {ScalarFromJSON(int64(), "0"), values1, values2}, values1);
    CheckScalar("choose", {ScalarFromJSON(int64(), "1"), values1, values2}, values2);
    CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
    auto scalar1 = ScalarFromJSON(type, R"("1.23")");
    CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar1, values2},
                *MakeArrayFromScalar(*scalar1, 5));
    CheckScalar("choose", {ScalarFromJSON(int64(), "1"), scalar1, values2}, values2);
    CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
    auto scalar_null = ScalarFromJSON(type, "null");
    CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar_null, values2},
                *MakeArrayOfNull(type, 5));
  }
}

TEST(TestChoose, FixedSizeBinary) {
  auto type = fixed_size_binary(3);
  auto indices1 = ArrayFromJSON(int64(), "[0, 1, 0, 1, null]");
  auto values1 = ArrayFromJSON(type, R"(["abc", "abd", null, null, "abe"])");
  auto values2 = ArrayFromJSON(type, R"(["def", "deg", null, null, "deh"])");
  auto nulls = ArrayFromJSON(type, "[null, null, null, null, null]");
  CheckScalar("choose", {indices1, values1, values2},
              ArrayFromJSON(type, R"(["abc", "deg", null, null, null])"));
  CheckScalar("choose", {indices1, ScalarFromJSON(type, R"("xyz")"), values1},
              ArrayFromJSON(type, R"(["xyz", "abd", "xyz", null, null])"));
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), values1, values2}, values1);
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), values1, values2}, values2);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
  auto scalar1 = ScalarFromJSON(type, R"("abc")");
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar1, values2},
              *MakeArrayFromScalar(*scalar1, 5));
  CheckScalar("choose", {ScalarFromJSON(int64(), "1"), scalar1, values2}, values2);
  CheckScalar("choose", {ScalarFromJSON(int64(), "null"), values1, values2}, nulls);
  auto scalar_null = ScalarFromJSON(type, "null");
  CheckScalar("choose", {ScalarFromJSON(int64(), "0"), scalar_null, values2},
              *MakeArrayOfNull(type, 5));
}

TEST(TestChooseKernel, DispatchBest) {
  ASSERT_OK_AND_ASSIGN(auto function, GetFunctionRegistry()->GetFunction("choose"));
  auto Check = [&](std::vector<TypeHolder> original_values) {
    auto values = original_values;
    ARROW_EXPECT_OK(function->DispatchBest(&values));
    return values;
  };

  // Since DispatchBest for this kernel pulls tricks, we can't compare it to DispatchExact
  // as CheckDispatchBest does
  for (auto ty :
       {int8(), int16(), int32(), int64(), uint8(), uint16(), uint32(), uint64()}) {
    // Index always promoted to int64
    EXPECT_EQ((std::vector<TypeHolder>{int64(), ty}), Check({ty, ty}));
    EXPECT_EQ((std::vector<TypeHolder>{int64(), int64(), int64()}),
              Check({ty, ty, int64()}));
  }
  // Other arguments promoted separately from index
  EXPECT_EQ((std::vector<TypeHolder>{int64(), int32(), int32()}),
            Check({int8(), int32(), uint8()}));
}

TEST(TestChooseKernel, Errors) {
  ASSERT_RAISES(Invalid, CallFunction("choose", ExecBatch({}, 0)));
  ASSERT_RAISES(Invalid, CallFunction("choose", {ArrayFromJSON(int64(), "[]")}));
  ASSERT_RAISES(Invalid, CallFunction("choose", {ArrayFromJSON(utf8(), "[\"a\"]"),
                                                 ArrayFromJSON(int64(), "[0]")}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      IndexError, ::testing::HasSubstr("choose: index 1 out of range"),
      CallFunction("choose",
                   {ArrayFromJSON(int64(), "[1]"), ArrayFromJSON(int32(), "[0]")}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      IndexError, ::testing::HasSubstr("choose: index -1 out of range"),
      CallFunction("choose",
                   {ArrayFromJSON(int64(), "[-1]"), ArrayFromJSON(int32(), "[0]")}));
}

}  // namespace compute
}  // namespace arrow
