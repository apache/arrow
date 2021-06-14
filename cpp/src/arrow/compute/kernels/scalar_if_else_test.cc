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

#include <arrow/array.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/kernels/test_util.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

namespace arrow {
namespace compute {

void CheckIfElseOutput(const Datum& cond, const Datum& left, const Datum& right,
                       const Datum& expected) {
  ASSERT_OK_AND_ASSIGN(Datum datum_out, IfElse(cond, left, right));
  if (datum_out.is_array()) {
    std::shared_ptr<Array> result = datum_out.make_array();
    ASSERT_OK(result->ValidateFull());
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

using PrimitiveTypes = ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type,
                                        Int32Type, UInt32Type, Int64Type, UInt64Type,
                                        FloatType, DoubleType, Date32Type, Date64Type>;

TYPED_TEST_SUITE(TestIfElsePrimitive, PrimitiveTypes);

TYPED_TEST(TestIfElsePrimitive, IfElseFixedSizeRand) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  auto type = TypeTraits<TypeParam>::type_singleton();

  random::RandomArrayGenerator rand(/*seed=*/0);
  int64_t len = 1000;
  auto cond = std::static_pointer_cast<BooleanArray>(
      rand.ArrayOf(boolean(), len, /*null_probability=*/0.01));
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

  enum { COND_SCALAR = 1, LEFT_SCALAR = 2, RIGHT_SCALAR = 4 };
  for (int mask = 0; mask < (COND_SCALAR | LEFT_SCALAR | RIGHT_SCALAR); ++mask) {
    for (int64_t cond_idx = 0; cond_idx < len; ++cond_idx) {
      Datum cond_in, cond_bcast;
      std::string trace_cond = "Cond";
      if (mask & COND_SCALAR) {
        ASSERT_OK_AND_ASSIGN(cond_in, cond->GetScalar(cond_idx));
        ASSERT_OK_AND_ASSIGN(cond_bcast, MakeArrayFromScalar(*cond_in.scalar(), len));
        trace_cond += "@" + std::to_string(cond_idx) + "=" + cond_in.scalar()->ToString();
      } else {
        cond_in = cond_bcast = cond;
      }
      SCOPED_TRACE(trace_cond);

      for (int64_t left_idx = 0; left_idx < len; ++left_idx) {
        Datum left_in, left_bcast;
        std::string trace_left = "Left";
        if (mask & LEFT_SCALAR) {
          ASSERT_OK_AND_ASSIGN(left_in, left->GetScalar(left_idx).As<Datum>());
          ASSERT_OK_AND_ASSIGN(left_bcast, MakeArrayFromScalar(*left_in.scalar(), len));
          trace_cond +=
              "@" + std::to_string(left_idx) + "=" + left_in.scalar()->ToString();
        } else {
          left_in = left_bcast = left;
        }
        SCOPED_TRACE(trace_left);

        for (int64_t right_idx = 0; right_idx < len; ++right_idx) {
          Datum right_in, right_bcast;
          std::string trace_right = "Right";
          if (mask & RIGHT_SCALAR) {
            ASSERT_OK_AND_ASSIGN(right_in, right->GetScalar(right_idx));
            ASSERT_OK_AND_ASSIGN(right_bcast,
                                 MakeArrayFromScalar(*right_in.scalar(), len));
            trace_right +=
                "@" + std::to_string(right_idx) + "=" + right_in.scalar()->ToString();
          } else {
            right_in = right_bcast = right;
          }
          SCOPED_TRACE(trace_right);

          ASSERT_OK_AND_ASSIGN(auto exp, IfElse(cond_bcast, left_bcast, right_bcast));
          ASSERT_OK_AND_ASSIGN(auto actual, IfElse(cond_in, left_in, right_in));
          AssertDatumsEqual(exp, actual, /*verbose=*/true);

          if (right_in.is_array()) break;
        }
        if (left_in.is_array()) break;
      }
      if (cond_in.is_array()) break;
    }
  }  // for (mask)
}

TYPED_TEST(TestIfElsePrimitive, IfElseFixedSize) {
  auto type = TypeTraits<TypeParam>::type_singleton();

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
  CheckIfElseOutput(ArrayFromJSON(boolean(), "[null, null, null, null]"),
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

TEST_F(TestIfElseKernel, IfElseDispatchBest) {
  std::string name = "if_else";
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
}

}  // namespace compute
}  // namespace arrow
