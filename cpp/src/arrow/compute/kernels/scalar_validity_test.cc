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
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace compute {

template <typename ArrowType>
class TestValidityKernels : public ::testing::Test {
 protected:
  static std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

using TestBooleanValidityKernels = TestValidityKernels<BooleanType>;

TEST_F(TestBooleanValidityKernels, ArrayIsValid) {
  CheckScalarUnary("is_valid", type_singleton(), "[]", type_singleton(), "[]");
  CheckScalarUnary("is_valid", type_singleton(), "[null]", type_singleton(), "[false]");
  CheckScalarUnary("is_valid", type_singleton(), "[1]", type_singleton(), "[true]");
  CheckScalarUnary("is_valid", type_singleton(), "[null, 1, 0, null]", type_singleton(),
                   "[false, true, true, false]");
}

TEST_F(TestBooleanValidityKernels, TrueUnlessNull) {
  CheckScalarUnary("true_unless_null", type_singleton(), "[]", type_singleton(), "[]");
  CheckScalarUnary("true_unless_null", type_singleton(), "[null]", type_singleton(),
                   "[null]");
  CheckScalarUnary("true_unless_null", type_singleton(), "[0, 1]", type_singleton(),
                   "[true, true]");
  CheckScalarUnary("true_unless_null", type_singleton(), "[null, 1, 0, null]",
                   type_singleton(), "[null, true, true, null]");
}

TEST_F(TestBooleanValidityKernels, IsValidIsNullNullType) {
  CheckScalarUnary("is_null", std::make_shared<NullArray>(5),
                   ArrayFromJSON(boolean(), "[true, true, true, true, true]"));
  CheckScalarUnary("is_valid", std::make_shared<NullArray>(5),
                   ArrayFromJSON(boolean(), "[false, false, false, false, false]"));
  CheckScalarUnary("true_unless_null", std::make_shared<NullArray>(5),
                   ArrayFromJSON(boolean(), "[null, null, null, null, null]"));
}

TEST_F(TestBooleanValidityKernels, IsNull) {
  auto ty = type_singleton();
  NullOptions default_options;
  NullOptions nan_is_null_options(/*nan_is_null=*/true);

  CheckScalarUnary("is_null", ty, "[]", boolean(), "[]");
  CheckScalarUnary("is_null", ty, "[]", boolean(), "[]", &default_options);
  CheckScalarUnary("is_null", ty, "[]", boolean(), "[]", &nan_is_null_options);

  CheckScalarUnary("is_null", ty, "[null]", boolean(), "[true]");
  CheckScalarUnary("is_null", ty, "[null]", boolean(), "[true]", &default_options);
  CheckScalarUnary("is_null", ty, "[null]", boolean(), "[true]", &nan_is_null_options);

  CheckScalarUnary("is_null", ty, "[1]", boolean(), "[false]");
  CheckScalarUnary("is_null", ty, "[1]", boolean(), "[false]", &default_options);
  CheckScalarUnary("is_null", ty, "[1]", boolean(), "[false]", &nan_is_null_options);

  CheckScalarUnary("is_null", ty, "[null, 1, 0, null]", boolean(),
                   "[true, false, false, true]");
  CheckScalarUnary("is_null", ty, "[null, 1, 0, null]", boolean(),
                   "[true, false, false, true]", &default_options);
  CheckScalarUnary("is_null", ty, "[null, 1, 0, null]", boolean(),
                   "[true, false, false, true]", &nan_is_null_options);
}

TEST(TestValidityKernels, IsFinite) {
  for (const auto& ty : IntTypes()) {
    CheckScalar("is_finite", {ArrayFromJSON(ty, "[0, 1, 42, null]")},
                ArrayFromJSON(boolean(), "[true, true, true, null]"));
  }
  for (const auto& ty : {decimal128(4, 2), decimal256(4, 2)}) {
    CheckScalar("is_finite", {ArrayFromJSON(ty, R"(["0.00", "1.01", "-42.00", null])")},
                ArrayFromJSON(boolean(), "[true, true, true, null]"));
  }
  CheckScalar("is_finite", {std::make_shared<NullArray>(4)},
              ArrayFromJSON(boolean(), "[null, null, null, null]"));
  CheckScalar("is_finite",
              {ArrayFromJSON(duration(TimeUnit::SECOND), "[0, 1, 42, null]")},
              ArrayFromJSON(boolean(), "[true, true, true, null]"));
}

TEST(TestValidityKernels, IsInf) {
  for (const auto& ty : IntTypes()) {
    CheckScalar("is_inf", {ArrayFromJSON(ty, "[0, 1, 42, null]")},
                ArrayFromJSON(boolean(), "[false, false, false, null]"));
  }
  for (const auto& ty : {decimal128(4, 2), decimal256(4, 2)}) {
    CheckScalar("is_inf", {ArrayFromJSON(ty, R"(["0.00", "1.01", "-42.00", null])")},
                ArrayFromJSON(boolean(), "[false, false, false, null]"));
  }
  CheckScalar("is_inf", {std::make_shared<NullArray>(4)},
              ArrayFromJSON(boolean(), "[null, null, null, null]"));
  CheckScalar("is_inf", {ArrayFromJSON(duration(TimeUnit::SECOND), "[0, 1, 42, null]")},
              ArrayFromJSON(boolean(), "[false, false, false, null]"));
}

TEST(TestValidityKernels, IsNan) {
  for (const auto& ty : IntTypes()) {
    CheckScalar("is_nan", {ArrayFromJSON(ty, "[0, 1, 42, null]")},
                ArrayFromJSON(boolean(), "[false, false, false, null]"));
  }
  for (const auto& ty : {decimal128(4, 2), decimal256(4, 2)}) {
    CheckScalar("is_nan", {ArrayFromJSON(ty, R"(["0.00", "1.01", "-42.00", null])")},
                ArrayFromJSON(boolean(), "[false, false, false, null]"));
  }
  CheckScalar("is_nan", {std::make_shared<NullArray>(4)},
              ArrayFromJSON(boolean(), "[null, null, null, null]"));
  CheckScalar("is_nan", {ArrayFromJSON(duration(TimeUnit::SECOND), "[0, 1, 42, null]")},
              ArrayFromJSON(boolean(), "[false, false, false, null]"));
}

TEST(TestValidityKernels, IsValidIsNullNullType) {
  CheckScalarUnary("is_null", std::make_shared<NullArray>(5),
                   ArrayFromJSON(boolean(), "[true, true, true, true, true]"));
  CheckScalarUnary("is_valid", std::make_shared<NullArray>(5),
                   ArrayFromJSON(boolean(), "[false, false, false, false, false]"));
}

TEST(TestValidityKernels, IsNullSetsZeroNullCount) {
  auto arr = ArrayFromJSON(int32(), "[1, 2, 3, 4, null]");
  ASSERT_OK_AND_ASSIGN(Datum out, IsNull(arr));
  ASSERT_EQ(out.array()->null_count, 0);
}

template <typename ArrowType>
class TestFloatingPointValidityKernels : public TestValidityKernels<ArrowType> {
 public:
  void TestIsNull() {
    NullOptions default_options;
    NullOptions nan_is_null_options(/*nan_is_null=*/true);

    auto ty = this->type_singleton();
    auto arr = ArrayFromJSON(ty, "[]");
    CheckScalarUnary("is_null", arr, ArrayFromJSON(boolean(), "[]"));
    CheckScalarUnary("is_null", arr, ArrayFromJSON(boolean(), "[]"), &default_options);
    CheckScalarUnary("is_null", arr, ArrayFromJSON(boolean(), "[]"),
                     &nan_is_null_options);

    // Without nulls
    arr = ArrayFromJSON(ty, "[1.5, 0.0, -0.0, Inf, -Inf, NaN]");
    CheckScalarUnary(
        "is_null", arr,
        ArrayFromJSON(boolean(), "[false, false, false, false, false, false]"));
    CheckScalarUnary(
        "is_null", arr,
        ArrayFromJSON(boolean(), "[false, false, false, false, false, false]"),
        &default_options);
    CheckScalarUnary(
        "is_null", arr,
        ArrayFromJSON(boolean(), "[false, false, false, false, false, true]"),
        &nan_is_null_options);

    // With nulls
    arr = ArrayFromJSON(ty, "[1.5, -0.0, null, Inf, -Inf, NaN]");
    CheckScalarUnary(
        "is_null", arr,
        ArrayFromJSON(boolean(), "[false, false, true, false, false, false]"));
    CheckScalarUnary(
        "is_null", arr,
        ArrayFromJSON(boolean(), "[false, false, true, false, false, false]"),
        &default_options);
    CheckScalarUnary("is_null", arr,
                     ArrayFromJSON(boolean(), "[false, false, true, false, false, true]"),
                     &nan_is_null_options);

    // Only nulls
    arr = ArrayFromJSON(ty, "[null, null, null]");
    CheckScalarUnary("is_null", arr, ArrayFromJSON(boolean(), "[true, true, true]"));
    CheckScalarUnary("is_null", arr, ArrayFromJSON(boolean(), "[true, true, true]"),
                     &default_options);
    CheckScalarUnary("is_null", arr, ArrayFromJSON(boolean(), "[true, true, true]"),
                     &nan_is_null_options);
  }

  void TestIsFinite() {
    auto ty = this->type_singleton();
    CheckScalarUnary("is_finite", ArrayFromJSON(ty, "[]"),
                     ArrayFromJSON(boolean(), "[]"));

    // All Inf
    CheckScalarUnary("is_finite", ArrayFromJSON(ty, "[Inf, -Inf, Inf, -Inf, Inf]"),
                     ArrayFromJSON(boolean(), "[false, false, false, false, false]"));
    // No Inf
    CheckScalarUnary("is_finite", ArrayFromJSON(ty, "[0.0, 1.0, 2.0, 3.0, NaN, null]"),
                     ArrayFromJSON(boolean(), "[true, true, true, true, false, null]"));
    // Some Inf
    CheckScalarUnary("is_finite", ArrayFromJSON(ty, "[0.0, Inf, 2.0, -Inf, NaN, null]"),
                     ArrayFromJSON(boolean(), "[true, false, true, false, false, null]"));
  }

  void TestIsInf() {
    auto ty = this->type_singleton();
    CheckScalarUnary("is_inf", ArrayFromJSON(ty, "[]"), ArrayFromJSON(boolean(), "[]"));

    // All Inf
    CheckScalarUnary("is_inf", ArrayFromJSON(ty, "[Inf, -Inf, Inf, -Inf, Inf]"),
                     ArrayFromJSON(boolean(), "[true, true, true, true, true]"));
    // No Inf
    CheckScalarUnary(
        "is_inf", ArrayFromJSON(ty, "[0.0, 1.0, 2.0, 3.0, NaN, null]"),
        ArrayFromJSON(boolean(), "[false, false, false, false, false, null]"));
    // Some Inf
    CheckScalarUnary("is_inf", ArrayFromJSON(ty, "[0.0, Inf, 2.0, -Inf, NaN, null]"),
                     ArrayFromJSON(boolean(), "[false, true, false, true, false, null]"));
  }

  void TestIsNan() {
    auto ty = this->type_singleton();
    CheckScalarUnary("is_nan", ArrayFromJSON(ty, "[]"), ArrayFromJSON(boolean(), "[]"));

    // All NaN
    CheckScalarUnary("is_nan", ArrayFromJSON(ty, "[NaN, NaN, NaN, NaN, NaN]"),
                     ArrayFromJSON(boolean(), "[true, true, true, true, true]"));
    // No NaN
    CheckScalarUnary(
        "is_nan", ArrayFromJSON(ty, "[0.0, 1.0, 2.0, 3.0, Inf, null]"),
        ArrayFromJSON(boolean(), "[false, false, false, false, false, null]"));
    // Some NaNs
    CheckScalarUnary("is_nan", ArrayFromJSON(ty, "[0.0, NaN, 2.0, NaN, Inf, null]"),
                     ArrayFromJSON(boolean(), "[false, true, false, true, false, null]"));
  }
};

TYPED_TEST_SUITE(TestFloatingPointValidityKernels, RealArrowTypes);

TYPED_TEST(TestFloatingPointValidityKernels, IsNull) { this->TestIsNull(); }

TYPED_TEST(TestFloatingPointValidityKernels, IsFinite) { this->TestIsFinite(); }

TYPED_TEST(TestFloatingPointValidityKernels, IsInf) { this->TestIsInf(); }

TYPED_TEST(TestFloatingPointValidityKernels, IsNan) { this->TestIsNan(); }

}  // namespace compute
}  // namespace arrow
