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

#include <chrono>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

std::shared_ptr<Scalar> CheckMakeNullScalar(const std::shared_ptr<DataType>& type) {
  const auto scalar = MakeNullScalar(type);
  ARROW_EXPECT_OK(scalar->Validate());
  ARROW_EXPECT_OK(scalar->ValidateFull());
  AssertTypeEqual(*type, *scalar->type);
  EXPECT_FALSE(scalar->is_valid);
  return scalar;
}

template <typename... MakeScalarArgs>
void AssertMakeScalar(const Scalar& expected, MakeScalarArgs&&... args) {
  ASSERT_OK_AND_ASSIGN(auto scalar, MakeScalar(std::forward<MakeScalarArgs>(args)...));
  ASSERT_OK(scalar->Validate());
  ASSERT_OK(scalar->ValidateFull());
  AssertScalarsEqual(expected, *scalar, /*verbose=*/true);
}

void AssertParseScalar(const std::shared_ptr<DataType>& type, std::string_view s,
                       const Scalar& expected) {
  ASSERT_OK_AND_ASSIGN(auto scalar, Scalar::Parse(type, s));
  ASSERT_OK(scalar->Validate());
  ASSERT_OK(scalar->ValidateFull());
  AssertScalarsEqual(expected, *scalar, /*verbose=*/true);
}

void AssertValidationFails(const Scalar& scalar) {
  ASSERT_RAISES(Invalid, scalar.Validate());
  ASSERT_RAISES(Invalid, scalar.ValidateFull());
}

TEST(TestNullScalar, Basics) {
  NullScalar scalar;
  ASSERT_FALSE(scalar.is_valid);
  ASSERT_TRUE(scalar.type->Equals(*null()));
  ASSERT_OK(scalar.ValidateFull());

  ASSERT_OK_AND_ASSIGN(auto arr, MakeArrayOfNull(null(), 1));
  ASSERT_OK_AND_ASSIGN(auto first, arr->GetScalar(0));
  ASSERT_TRUE(first->Equals(scalar));
  ASSERT_OK(first->ValidateFull());
}

TEST(TestNullScalar, ValidateErrors) {
  NullScalar scalar;
  scalar.is_valid = true;
  AssertValidationFails(scalar);
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
  ASSERT_OK(scalar_val->ValidateFull());

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
  ASSERT_OK(stack_val.ValidateFull());

  auto null_value = std::make_shared<ScalarType>();
  ASSERT_FALSE(null_value->is_valid);
  ASSERT_OK(null_value->ValidateFull());

  // Nulls should be equals to itself following Array::Equals
  ASSERT_EQ(*null_value, stack_val);

  auto dyn_null_value = CheckMakeNullScalar(expected_type);
  ASSERT_EQ(*null_value, *dyn_null_value);

  // test Array.GetScalar
  auto arr = ArrayFromJSON(expected_type, "[null, 1, 2]");
  ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(0));
  ASSERT_OK_AND_ASSIGN(auto one, arr->GetScalar(1));
  ASSERT_OK_AND_ASSIGN(auto two, arr->GetScalar(2));
  ASSERT_OK(null->ValidateFull());
  ASSERT_OK(one->ValidateFull());
  ASSERT_OK(two->ValidateFull());

  ASSERT_TRUE(null->Equals(*null_value));
  ASSERT_TRUE(one->Equals(ScalarType(1)));
  ASSERT_FALSE(one->Equals(ScalarType(2)));
  ASSERT_TRUE(two->Equals(ScalarType(2)));
  ASSERT_FALSE(two->Equals(ScalarType(3)));

  ASSERT_TRUE(null->ApproxEquals(*null_value));
  ASSERT_TRUE(one->ApproxEquals(ScalarType(1)));
  ASSERT_FALSE(one->ApproxEquals(ScalarType(2)));
  ASSERT_TRUE(two->ApproxEquals(ScalarType(2)));
  ASSERT_FALSE(two->ApproxEquals(ScalarType(3)));
}

TYPED_TEST(TestNumericScalar, Hashing) {
  using T = typename TypeParam::c_type;
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;

  std::unordered_set<std::shared_ptr<Scalar>, Scalar::Hash, Scalar::PtrsEqual> set;
  set.emplace(std::make_shared<ScalarType>());
  for (T i = 0; i < 10; ++i) {
    set.emplace(std::make_shared<ScalarType>(i));
  }

  ASSERT_FALSE(set.emplace(std::make_shared<ScalarType>()).second);
  for (T i = 0; i < 10; ++i) {
    ASSERT_FALSE(set.emplace(std::make_shared<ScalarType>(i)).second);
  }
}

TYPED_TEST(TestNumericScalar, MakeScalar) {
  using T = typename TypeParam::c_type;
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  auto type = TypeTraits<TypeParam>::type_singleton();

  std::shared_ptr<Scalar> three = MakeScalar(static_cast<T>(3));
  ASSERT_OK(three->ValidateFull());
  ASSERT_EQ(ScalarType(3), *three);

  AssertMakeScalar(ScalarType(3), type, static_cast<T>(3));

  AssertParseScalar(type, "3", ScalarType(3));
}

template <typename T>
class TestRealScalar : public ::testing::Test {
 public:
  using CType = typename T::c_type;
  using ScalarType = typename TypeTraits<T>::ScalarType;

  void SetUp() {
    type_ = TypeTraits<T>::type_singleton();

    scalar_val_ = std::make_shared<ScalarType>(static_cast<CType>(1));
    ASSERT_TRUE(scalar_val_->is_valid);

    scalar_other_ = std::make_shared<ScalarType>(static_cast<CType>(1.1));
    ASSERT_TRUE(scalar_other_->is_valid);

    scalar_zero_ = std::make_shared<ScalarType>(static_cast<CType>(0.0));
    scalar_other_zero_ = std::make_shared<ScalarType>(static_cast<CType>(0.0));
    scalar_neg_zero_ = std::make_shared<ScalarType>(static_cast<CType>(-0.0));

    const CType nan_value = std::numeric_limits<CType>::quiet_NaN();
    scalar_nan_ = std::make_shared<ScalarType>(nan_value);
    ASSERT_TRUE(scalar_nan_->is_valid);

    const CType other_nan_value = std::numeric_limits<CType>::quiet_NaN();
    scalar_other_nan_ = std::make_shared<ScalarType>(other_nan_value);
    ASSERT_TRUE(scalar_other_nan_->is_valid);
  }

  void TestNanEquals() {
    EqualOptions options = EqualOptions::Defaults();
    ASSERT_FALSE(scalar_nan_->Equals(*scalar_val_, options));
    ASSERT_FALSE(scalar_nan_->Equals(*scalar_nan_, options));
    ASSERT_FALSE(scalar_nan_->Equals(*scalar_other_nan_, options));

    options = options.nans_equal(true);
    ASSERT_FALSE(scalar_nan_->Equals(*scalar_val_, options));
    ASSERT_TRUE(scalar_nan_->Equals(*scalar_nan_, options));
    ASSERT_TRUE(scalar_nan_->Equals(*scalar_other_nan_, options));
  }

  void TestSignedZeroEquals() {
    EqualOptions options = EqualOptions::Defaults();
    ASSERT_FALSE(scalar_zero_->Equals(*scalar_val_, options));
    ASSERT_TRUE(scalar_zero_->Equals(*scalar_other_zero_, options));
    ASSERT_TRUE(scalar_zero_->Equals(*scalar_neg_zero_, options));

    options = options.signed_zeros_equal(false);
    ASSERT_FALSE(scalar_zero_->Equals(*scalar_val_, options));
    ASSERT_TRUE(scalar_zero_->Equals(*scalar_other_zero_, options));
    ASSERT_FALSE(scalar_zero_->Equals(*scalar_neg_zero_, options));
  }

  void TestApproxEquals() {
    // The scalars are unequal with the small delta
    EqualOptions options = EqualOptions::Defaults().atol(0.05);
    ASSERT_FALSE(scalar_val_->ApproxEquals(*scalar_other_, options));
    ASSERT_FALSE(scalar_other_->ApproxEquals(*scalar_val_, options));
    ASSERT_FALSE(scalar_nan_->ApproxEquals(*scalar_val_, options));
    ASSERT_FALSE(scalar_nan_->ApproxEquals(*scalar_other_nan_, options));

    // After enlarging the delta, they become equal
    options = options.atol(0.15);
    ASSERT_TRUE(scalar_val_->ApproxEquals(*scalar_other_, options));
    ASSERT_TRUE(scalar_other_->ApproxEquals(*scalar_val_, options));
    ASSERT_FALSE(scalar_nan_->ApproxEquals(*scalar_val_, options));
    ASSERT_FALSE(scalar_nan_->ApproxEquals(*scalar_other_nan_, options));

    options = options.nans_equal(true);
    ASSERT_TRUE(scalar_val_->ApproxEquals(*scalar_other_, options));
    ASSERT_TRUE(scalar_other_->ApproxEquals(*scalar_val_, options));
    ASSERT_FALSE(scalar_nan_->ApproxEquals(*scalar_val_, options));
    ASSERT_TRUE(scalar_nan_->ApproxEquals(*scalar_other_nan_, options));

    options = options.atol(0.05);
    ASSERT_FALSE(scalar_val_->ApproxEquals(*scalar_other_, options));
    ASSERT_FALSE(scalar_other_->ApproxEquals(*scalar_val_, options));
    ASSERT_FALSE(scalar_nan_->ApproxEquals(*scalar_val_, options));
    ASSERT_TRUE(scalar_nan_->ApproxEquals(*scalar_other_nan_, options));

    // Negative zeros
    ASSERT_FALSE(scalar_zero_->ApproxEquals(*scalar_val_, options));
    ASSERT_TRUE(scalar_zero_->ApproxEquals(*scalar_other_zero_, options));
    ASSERT_TRUE(scalar_zero_->ApproxEquals(*scalar_neg_zero_, options));

    options = options.signed_zeros_equal(false);
    ASSERT_FALSE(scalar_zero_->ApproxEquals(*scalar_val_, options));
    ASSERT_TRUE(scalar_zero_->ApproxEquals(*scalar_other_zero_, options));
    ASSERT_FALSE(scalar_zero_->ApproxEquals(*scalar_neg_zero_, options));
  }

  void TestStructOf() {
    auto ty = struct_({field("float", type_)});

    StructScalar struct_val({scalar_val_}, ty);
    StructScalar struct_other_val({scalar_other_}, ty);
    StructScalar struct_nan({scalar_nan_}, ty);
    StructScalar struct_other_nan({scalar_other_nan_}, ty);

    EqualOptions options = EqualOptions::Defaults().atol(0.05);
    ASSERT_FALSE(struct_val.Equals(struct_other_val, options));
    ASSERT_FALSE(struct_other_val.Equals(struct_val, options));
    ASSERT_FALSE(struct_nan.Equals(struct_val, options));
    ASSERT_FALSE(struct_nan.Equals(struct_nan, options));
    ASSERT_FALSE(struct_nan.Equals(struct_other_nan, options));
    ASSERT_FALSE(struct_val.ApproxEquals(struct_other_val, options));
    ASSERT_FALSE(struct_other_val.ApproxEquals(struct_val, options));
    ASSERT_FALSE(struct_nan.ApproxEquals(struct_val, options));
    ASSERT_FALSE(struct_nan.ApproxEquals(struct_nan, options));
    ASSERT_FALSE(struct_nan.ApproxEquals(struct_other_nan, options));

    options = options.atol(0.15);
    ASSERT_FALSE(struct_val.Equals(struct_other_val, options));
    ASSERT_FALSE(struct_other_val.Equals(struct_val, options));
    ASSERT_FALSE(struct_nan.Equals(struct_val, options));
    ASSERT_FALSE(struct_nan.Equals(struct_nan, options));
    ASSERT_FALSE(struct_nan.Equals(struct_other_nan, options));
    ASSERT_TRUE(struct_val.ApproxEquals(struct_other_val, options));
    ASSERT_TRUE(struct_other_val.ApproxEquals(struct_val, options));
    ASSERT_FALSE(struct_nan.ApproxEquals(struct_val, options));
    ASSERT_FALSE(struct_nan.ApproxEquals(struct_nan, options));
    ASSERT_FALSE(struct_nan.ApproxEquals(struct_other_nan, options));

    options = options.nans_equal(true);
    ASSERT_FALSE(struct_val.Equals(struct_other_val, options));
    ASSERT_FALSE(struct_other_val.Equals(struct_val, options));
    ASSERT_FALSE(struct_nan.Equals(struct_val, options));
    ASSERT_TRUE(struct_nan.Equals(struct_nan, options));
    ASSERT_TRUE(struct_nan.Equals(struct_other_nan, options));
    ASSERT_TRUE(struct_val.ApproxEquals(struct_other_val, options));
    ASSERT_TRUE(struct_other_val.ApproxEquals(struct_val, options));
    ASSERT_FALSE(struct_nan.ApproxEquals(struct_val, options));
    ASSERT_TRUE(struct_nan.ApproxEquals(struct_nan, options));
    ASSERT_TRUE(struct_nan.ApproxEquals(struct_other_nan, options));

    options = options.atol(0.05);
    ASSERT_FALSE(struct_val.Equals(struct_other_val, options));
    ASSERT_FALSE(struct_other_val.Equals(struct_val, options));
    ASSERT_FALSE(struct_nan.Equals(struct_val, options));
    ASSERT_TRUE(struct_nan.Equals(struct_nan, options));
    ASSERT_TRUE(struct_nan.Equals(struct_other_nan, options));
    ASSERT_FALSE(struct_val.ApproxEquals(struct_other_val, options));
    ASSERT_FALSE(struct_other_val.ApproxEquals(struct_val, options));
    ASSERT_FALSE(struct_nan.ApproxEquals(struct_val, options));
    ASSERT_TRUE(struct_nan.ApproxEquals(struct_nan, options));
    ASSERT_TRUE(struct_nan.ApproxEquals(struct_other_nan, options));
  }

  void TestListOf() {
    auto ty = list(type_);

    ListScalar list_val(ArrayFromJSON(type_, "[0, null, 1.0]"), ty);
    ListScalar list_other_val(ArrayFromJSON(type_, "[0, null, 1.1]"), ty);
    ListScalar list_nan(ArrayFromJSON(type_, "[0, null, NaN]"), ty);
    ListScalar list_other_nan(ArrayFromJSON(type_, "[0, null, NaN]"), ty);

    EqualOptions options = EqualOptions::Defaults().atol(0.05);
    ASSERT_TRUE(list_val.Equals(list_val, options));
    ASSERT_FALSE(list_val.Equals(list_other_val, options));
    ASSERT_FALSE(list_nan.Equals(list_val, options));
    ASSERT_FALSE(list_nan.Equals(list_nan, options));
    ASSERT_FALSE(list_nan.Equals(list_other_nan, options));
    ASSERT_TRUE(list_val.ApproxEquals(list_val, options));
    ASSERT_FALSE(list_val.ApproxEquals(list_other_val, options));
    ASSERT_FALSE(list_nan.ApproxEquals(list_val, options));
    ASSERT_FALSE(list_nan.ApproxEquals(list_nan, options));
    ASSERT_FALSE(list_nan.ApproxEquals(list_other_nan, options));

    options = options.atol(0.15);
    ASSERT_TRUE(list_val.Equals(list_val, options));
    ASSERT_FALSE(list_val.Equals(list_other_val, options));
    ASSERT_FALSE(list_nan.Equals(list_val, options));
    ASSERT_FALSE(list_nan.Equals(list_nan, options));
    ASSERT_FALSE(list_nan.Equals(list_other_nan, options));
    ASSERT_TRUE(list_val.ApproxEquals(list_val, options));
    ASSERT_TRUE(list_val.ApproxEquals(list_other_val, options));
    ASSERT_FALSE(list_nan.ApproxEquals(list_val, options));
    ASSERT_FALSE(list_nan.ApproxEquals(list_nan, options));
    ASSERT_FALSE(list_nan.ApproxEquals(list_other_nan, options));

    options = options.nans_equal(true);
    ASSERT_TRUE(list_val.Equals(list_val, options));
    ASSERT_FALSE(list_val.Equals(list_other_val, options));
    ASSERT_FALSE(list_nan.Equals(list_val, options));
    ASSERT_TRUE(list_nan.Equals(list_nan, options));
    ASSERT_TRUE(list_nan.Equals(list_other_nan, options));
    ASSERT_TRUE(list_val.ApproxEquals(list_val, options));
    ASSERT_TRUE(list_val.ApproxEquals(list_other_val, options));
    ASSERT_FALSE(list_nan.ApproxEquals(list_val, options));
    ASSERT_TRUE(list_nan.ApproxEquals(list_nan, options));
    ASSERT_TRUE(list_nan.ApproxEquals(list_other_nan, options));

    options = options.atol(0.05);
    ASSERT_TRUE(list_val.Equals(list_val, options));
    ASSERT_FALSE(list_val.Equals(list_other_val, options));
    ASSERT_FALSE(list_nan.Equals(list_val, options));
    ASSERT_TRUE(list_nan.Equals(list_nan, options));
    ASSERT_TRUE(list_nan.Equals(list_other_nan, options));
    ASSERT_TRUE(list_val.ApproxEquals(list_val, options));
    ASSERT_FALSE(list_val.ApproxEquals(list_other_val, options));
    ASSERT_FALSE(list_nan.ApproxEquals(list_val, options));
    ASSERT_TRUE(list_nan.ApproxEquals(list_nan, options));
    ASSERT_TRUE(list_nan.ApproxEquals(list_other_nan, options));
  }

 protected:
  std::shared_ptr<DataType> type_;
  std::shared_ptr<Scalar> scalar_val_, scalar_other_, scalar_nan_, scalar_other_nan_,
      scalar_zero_, scalar_other_zero_, scalar_neg_zero_;
};

TYPED_TEST_SUITE(TestRealScalar, RealArrowTypes);

TYPED_TEST(TestRealScalar, NanEquals) { this->TestNanEquals(); }

TYPED_TEST(TestRealScalar, SignedZeroEquals) { this->TestSignedZeroEquals(); }

TYPED_TEST(TestRealScalar, ApproxEquals) { this->TestApproxEquals(); }

TYPED_TEST(TestRealScalar, StructOf) { this->TestStructOf(); }

TYPED_TEST(TestRealScalar, ListOf) { this->TestListOf(); }

template <typename T>
class TestDecimalScalar : public ::testing::Test {
 public:
  using ScalarType = typename TypeTraits<T>::ScalarType;
  using ValueType = typename ScalarType::ValueType;

  void TestBasics() {
    const auto ty = std::make_shared<T>(3, 2);
    const auto pi = ScalarType(ValueType(314), ty);
    const auto pi2 = ScalarType(ValueType(628), ty);
    const auto null = CheckMakeNullScalar(ty);

    ASSERT_OK(pi.ValidateFull());
    ASSERT_TRUE(pi.is_valid);
    ASSERT_EQ(pi.value, ValueType("3.14"));

    ASSERT_OK(null->ValidateFull());
    ASSERT_FALSE(null->is_valid);

    ASSERT_FALSE(pi.Equals(pi2));

    // Test Array::GetScalar
    auto arr = ArrayFromJSON(ty, "[null, \"3.14\"]");
    ASSERT_OK_AND_ASSIGN(auto first, arr->GetScalar(0));
    ASSERT_OK_AND_ASSIGN(auto second, arr->GetScalar(1));
    ASSERT_OK(first->ValidateFull());
    ASSERT_OK(second->ValidateFull());

    ASSERT_TRUE(first->Equals(*null));
    ASSERT_FALSE(first->Equals(pi));
    ASSERT_TRUE(second->Equals(pi));
    ASSERT_FALSE(second->Equals(*null));

    auto invalid = ScalarType(ValueType::GetMaxValue(6), std::make_shared<T>(5, 2));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("does not fit in precision of"),
                                    invalid.ValidateFull());
  }
};

TYPED_TEST_SUITE(TestDecimalScalar, DecimalArrowTypes);

TYPED_TEST(TestDecimalScalar, Basics) { this->TestBasics(); }

TEST(TestBinaryScalar, Basics) {
  std::string data = "test data";
  auto buf = std::make_shared<Buffer>(data);

  BinaryScalar value(buf);
  ASSERT_OK(value.ValidateFull());
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
  ASSERT_EQ(null_value.value, nullptr);
  ASSERT_OK(null_value.ValidateFull());

  StringScalar value2(buf);
  ASSERT_OK(value2.ValidateFull());
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
  ASSERT_OK(null->ValidateFull());
  ASSERT_OK(one->ValidateFull());
  ASSERT_OK(two->ValidateFull());

  ASSERT_TRUE(null->Equals(null_value));
  ASSERT_TRUE(one->Equals(BinaryScalar(Buffer::FromString("one"))));
  ASSERT_TRUE(two->Equals(BinaryScalar(Buffer::FromString("two"))));
  ASSERT_FALSE(two->Equals(BinaryScalar(Buffer::FromString("else"))));
}

TEST(TestBinaryScalar, Hashing) {
  auto FromInt = [](int i) {
    return std::make_shared<BinaryScalar>(Buffer::FromString(std::to_string(i)));
  };

  std::unordered_set<std::shared_ptr<Scalar>, Scalar::Hash, Scalar::PtrsEqual> set;
  set.emplace(std::make_shared<BinaryScalar>());
  for (int i = 0; i < 10; ++i) {
    set.emplace(FromInt(i));
  }

  ASSERT_FALSE(set.emplace(std::make_shared<BinaryScalar>()).second);
  for (int i = 0; i < 10; ++i) {
    ASSERT_FALSE(set.emplace(FromInt(i)).second);
  }
}

TEST(TestBinaryScalar, ValidateErrors) {
  // Value must be null when the scalar is null
  BinaryScalar scalar(Buffer::FromString("xxx"));
  scalar.is_valid = false;
  AssertValidationFails(scalar);

  // Value must be non-null
  auto null_scalar = MakeNullScalar(binary());
  null_scalar->is_valid = true;
  AssertValidationFails(*null_scalar);
}

template <typename T>
class TestStringScalar : public ::testing::Test {
 public:
  using ScalarType = typename TypeTraits<T>::ScalarType;

  void SetUp() { type_ = TypeTraits<T>::type_singleton(); }

  void TestMakeScalar() {
    AssertMakeScalar(ScalarType("three"), type_, Buffer::FromString("three"));

    AssertParseScalar(type_, "three", ScalarType("three"));
  }

  void TestArrayGetScalar() {
    auto arr = ArrayFromJSON(type_, R"([null, "one", "two"])");
    ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(0));
    ASSERT_OK_AND_ASSIGN(auto one, arr->GetScalar(1));
    ASSERT_OK_AND_ASSIGN(auto two, arr->GetScalar(2));
    ASSERT_OK(null->ValidateFull());
    ASSERT_OK(one->ValidateFull());
    ASSERT_OK(two->ValidateFull());

    ASSERT_TRUE(null->Equals(*CheckMakeNullScalar(type_)));
    ASSERT_TRUE(one->Equals(ScalarType("one")));
    ASSERT_TRUE(two->Equals(ScalarType("two")));
    ASSERT_FALSE(two->Equals(Int64Scalar(1)));
  }

  void TestValidateErrors() {
    // Inconsistent is_valid / value
    ScalarType scalar(Buffer::FromString("xxx"));
    scalar.is_valid = false;
    AssertValidationFails(scalar);

    auto null_scalar = MakeNullScalar(type_);
    null_scalar->is_valid = true;
    AssertValidationFails(*null_scalar);

    // Invalid UTF8
    scalar = ScalarType(Buffer::FromString("\xff"));
    ASSERT_OK(scalar.Validate());
    ASSERT_RAISES(Invalid, scalar.ValidateFull());
  }

 protected:
  std::shared_ptr<DataType> type_;
};

TYPED_TEST_SUITE(TestStringScalar, StringArrowTypes);

TYPED_TEST(TestStringScalar, MakeScalar) { this->TestMakeScalar(); }

TYPED_TEST(TestStringScalar, ArrayGetScalar) { this->TestArrayGetScalar(); }

TYPED_TEST(TestStringScalar, ValidateErrors) { this->TestValidateErrors(); }

TEST(TestStringScalar, MakeScalarImplicit) {
  // MakeScalar("string literal") creates a StringScalar
  auto three = MakeScalar("three");
  ASSERT_OK(three->ValidateFull());
  ASSERT_EQ(StringScalar("three"), *three);
}

TEST(TestStringScalar, MakeScalarString) {
  // MakeScalar(std::string) creates a StringScalar via FromBuffer
  std::string buf = "three";
  auto three = MakeScalar(std::move(buf));
  ASSERT_OK(three->ValidateFull());
  ASSERT_EQ(StringScalar("three"), *three);
}

TEST(TestFixedSizeBinaryScalar, Basics) {
  std::string data = "test data";
  auto buf = std::make_shared<Buffer>(data);

  auto ex_type = fixed_size_binary(9);

  FixedSizeBinaryScalar value(buf, ex_type);
  ASSERT_OK(value.ValidateFull());
  ASSERT_TRUE(value.value->Equals(*buf));
  ASSERT_TRUE(value.is_valid);
  ASSERT_TRUE(value.type->Equals(*ex_type));

  FixedSizeBinaryScalar null_value(buf, ex_type, /*is_valid=*/false);
  ASSERT_OK(null_value.ValidateFull());
  ASSERT_FALSE(null_value.is_valid);
  ASSERT_TRUE(null_value.value->Equals(*buf));

  // test Array.GetScalar
  auto ty = fixed_size_binary(3);
  auto arr = ArrayFromJSON(ty, R"([null, "one", "two"])");
  ASSERT_OK_AND_ASSIGN(auto null, arr->GetScalar(0));
  ASSERT_OK_AND_ASSIGN(auto one, arr->GetScalar(1));
  ASSERT_OK_AND_ASSIGN(auto two, arr->GetScalar(2));
  ASSERT_OK(null->ValidateFull());
  ASSERT_OK(one->ValidateFull());
  ASSERT_OK(two->ValidateFull());

  ASSERT_TRUE(null->Equals(*CheckMakeNullScalar(ty)));
  ASSERT_TRUE(one->Equals(FixedSizeBinaryScalar(Buffer::FromString("one"), ty)));
  ASSERT_TRUE(two->Equals(FixedSizeBinaryScalar(Buffer::FromString("two"), ty)));
}

TEST(TestFixedSizeBinaryScalar, MakeScalar) {
  std::string data = "test data";
  auto buf = std::make_shared<Buffer>(data);
  auto type = fixed_size_binary(9);

  AssertMakeScalar(FixedSizeBinaryScalar(buf, type), type, buf);

  AssertParseScalar(type, std::string_view(data), FixedSizeBinaryScalar(buf, type));

  // Wrong length
  ASSERT_RAISES(Invalid, MakeScalar(type, Buffer::FromString(data.substr(3))).status());
  ASSERT_RAISES(Invalid, Scalar::Parse(type, std::string_view(data).substr(3)).status());
}

TEST(TestFixedSizeBinaryScalar, ValidateErrors) {
  std::string data = "test data";
  auto buf = std::make_shared<Buffer>(data);
  auto type = fixed_size_binary(9);

  FixedSizeBinaryScalar scalar(buf, type);
  ASSERT_OK(scalar.ValidateFull());

  scalar.value = SliceBuffer(buf, 1);
  AssertValidationFails(scalar);
}

TEST(TestDateScalars, Basics) {
  int32_t i32_val = 1;
  Date32Scalar date32_val(i32_val);
  Date32Scalar date32_null;
  ASSERT_OK(date32_val.ValidateFull());
  ASSERT_OK(date32_null.ValidateFull());

  ASSERT_TRUE(date32_val.type->Equals(*date32()));
  ASSERT_TRUE(date32_val.is_valid);
  ASSERT_FALSE(date32_null.is_valid);

  int64_t i64_val = 2;
  Date64Scalar date64_val(i64_val);
  Date64Scalar date64_null;
  ASSERT_OK(date64_val.ValidateFull());
  ASSERT_OK(date64_null.ValidateFull());

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
    ASSERT_OK(first->ValidateFull());
    ASSERT_OK(null->ValidateFull());
    ASSERT_OK(last->ValidateFull());

    ASSERT_TRUE(null->Equals(*CheckMakeNullScalar(ty)));
    ASSERT_TRUE(first->Equals(*MakeScalar(ty, 5).ValueOrDie()));
    ASSERT_TRUE(last->Equals(*MakeScalar(ty, 42).ValueOrDie()));
    ASSERT_FALSE(last->Equals(*MakeScalar("string")));
  }
}

TEST(TestDateScalars, MakeScalar) {
  AssertMakeScalar(Date32Scalar(1), date32(), int32_t(1));
  AssertParseScalar(date32(), "1454-10-22", Date32Scalar(-188171));

  AssertMakeScalar(Date64Scalar(1), date64(), int64_t(1));
  AssertParseScalar(date64(), "1454-10-22",
                    Date64Scalar(-188171LL * 24 * 60 * 60 * 1000));
}

TEST(TestTimeScalars, Basics) {
  auto type1 = time32(TimeUnit::MILLI);
  auto type2 = time32(TimeUnit::SECOND);
  auto type3 = time64(TimeUnit::MICRO);
  auto type4 = time64(TimeUnit::NANO);

  int32_t i32_val = 1;
  Time32Scalar time32_val(i32_val, type1);
  Time32Scalar time32_null(type2);
  ASSERT_OK(time32_val.ValidateFull());
  ASSERT_OK(time32_null.ValidateFull());

  ASSERT_EQ(i32_val, time32_val.value);
  ASSERT_TRUE(time32_val.type->Equals(*type1));
  ASSERT_TRUE(time32_val.is_valid);
  ASSERT_FALSE(time32_null.is_valid);
  ASSERT_TRUE(time32_null.type->Equals(*type2));

  int64_t i64_val = 2;
  Time64Scalar time64_val(i64_val, type3);
  Time64Scalar time64_null(type4);
  ASSERT_OK(time64_val.ValidateFull());
  ASSERT_OK(time64_null.ValidateFull());

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
    ASSERT_OK(first->ValidateFull());
    ASSERT_OK(null->ValidateFull());
    ASSERT_OK(last->ValidateFull());

    ASSERT_TRUE(null->Equals(*CheckMakeNullScalar(ty)));
    ASSERT_TRUE(first->Equals(*MakeScalar(ty, 5).ValueOrDie()));
    ASSERT_TRUE(last->Equals(*MakeScalar(ty, 42).ValueOrDie()));
    ASSERT_FALSE(last->Equals(*MakeScalar("string")));
  }
}

TEST(TestTimeScalars, MakeScalar) {
  auto type1 = time32(TimeUnit::SECOND);
  auto type2 = time32(TimeUnit::MILLI);
  auto type3 = time64(TimeUnit::MICRO);
  auto type4 = time64(TimeUnit::NANO);

  AssertMakeScalar(Time32Scalar(1, type1), type1, int32_t(1));
  AssertMakeScalar(Time32Scalar(1, type2), type2, int32_t(1));
  AssertMakeScalar(Time64Scalar(1, type3), type3, int32_t(1));
  AssertMakeScalar(Time64Scalar(1, type4), type4, int32_t(1));

  int64_t tententen = 60 * (60 * (10) + 10) + 10;
  AssertParseScalar(type1, "10:10:10",
                    Time32Scalar(static_cast<int32_t>(tententen), type1));

  tententen = 1000 * tententen + 123;
  AssertParseScalar(type2, "10:10:10.123",
                    Time32Scalar(static_cast<int32_t>(tententen), type2));

  tententen = 1000 * tententen + 456;
  AssertParseScalar(type3, "10:10:10.123456", Time64Scalar(tententen, type3));

  tententen = 1000 * tententen + 789;
  AssertParseScalar(type4, "10:10:10.123456789", Time64Scalar(tententen, type4));
}

TEST(TestTimestampScalars, Basics) {
  auto type1 = timestamp(TimeUnit::MILLI);
  auto type2 = timestamp(TimeUnit::SECOND);

  int64_t val1 = 1;
  int64_t val2 = 2;
  TimestampScalar ts_val1(val1, type1);
  TimestampScalar ts_val2(val2, type2);
  TimestampScalar ts_null(type1);
  ASSERT_OK(ts_val1.ValidateFull());
  ASSERT_OK(ts_val2.ValidateFull());
  ASSERT_OK(ts_null.ValidateFull());

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
    ASSERT_OK(first->ValidateFull());
    ASSERT_OK(null->ValidateFull());
    ASSERT_OK(last->ValidateFull());

    ASSERT_TRUE(null->Equals(*CheckMakeNullScalar(ty)));
    ASSERT_TRUE(first->Equals(*MakeScalar(ty, 5).ValueOrDie()));
    ASSERT_TRUE(last->Equals(*MakeScalar(ty, 42).ValueOrDie()));
    ASSERT_FALSE(last->Equals(*MakeScalar(int64(), 42).ValueOrDie()));
  }
}

TEST(TestTimestampScalars, MakeScalar) {
  auto type1 = timestamp(TimeUnit::MILLI);
  auto type2 = timestamp(TimeUnit::SECOND);
  auto type3 = timestamp(TimeUnit::MICRO);
  auto type4 = timestamp(TimeUnit::NANO);

  std::string_view epoch_plus_1s = "1970-01-01 00:00:01";

  AssertMakeScalar(TimestampScalar(1, type1), type1, int64_t(1));
  AssertParseScalar(type1, epoch_plus_1s, TimestampScalar(1000, type1));

  AssertMakeScalar(TimestampScalar(1, type2), type2, int64_t(1));
  AssertParseScalar(type2, epoch_plus_1s, TimestampScalar(1, type2));

  AssertMakeScalar(TimestampScalar(1, type3), type3, int64_t(1));
  AssertParseScalar(type3, epoch_plus_1s, TimestampScalar(1000 * 1000, type3));

  AssertMakeScalar(TimestampScalar(1, type4), type4, int64_t(1));
  AssertParseScalar(type4, epoch_plus_1s, TimestampScalar(1000 * 1000 * 1000, type4));
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
  EXPECT_EQ(*str, StringScalar("1970-01-01 00:00:01.024"));
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
  ASSERT_OK(ts_val1.ValidateFull());
  ASSERT_OK(ts_val2.ValidateFull());
  ASSERT_OK(ts_null.ValidateFull());

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
    ASSERT_OK(first->ValidateFull());
    ASSERT_OK(null->ValidateFull());
    ASSERT_OK(last->ValidateFull());

    ASSERT_TRUE(null->Equals(*CheckMakeNullScalar(ty)));
    ASSERT_TRUE(first->Equals(*MakeScalar(ty, 5).ValueOrDie()));
    ASSERT_TRUE(last->Equals(*MakeScalar(ty, 42).ValueOrDie()));
  }

  EXPECT_EQ(DurationScalar{std::chrono::nanoseconds{1235}},
            DurationScalar(1235, TimeUnit::NANO));

  EXPECT_EQ(DurationScalar{std::chrono::microseconds{58}},
            DurationScalar(58, TimeUnit::MICRO));

  EXPECT_EQ(DurationScalar{std::chrono::milliseconds{952}},
            DurationScalar(952, TimeUnit::MILLI));

  EXPECT_EQ(DurationScalar{std::chrono::seconds{625}},
            DurationScalar(625, TimeUnit::SECOND));

  EXPECT_EQ(DurationScalar{std::chrono::minutes{2}},
            DurationScalar(120, TimeUnit::SECOND));

  // finer than nanoseconds; we can't represent this without truncation
  using picoseconds = std::chrono::duration<int64_t, std::pico>;
  static_assert(!std::is_constructible_v<DurationScalar, picoseconds>);

  // between seconds and milliseconds; we could represent this as milliseconds safely, but
  // it's a pain to support
  using centiseconds = std::chrono::duration<int64_t, std::centi>;
  static_assert(!std::is_constructible_v<DurationScalar, centiseconds>);
}

TEST(TestMonthIntervalScalars, Basics) {
  auto type = month_interval();

  int32_t val1 = 1;
  int32_t val2 = 2;
  MonthIntervalScalar ts_val1(val1);
  MonthIntervalScalar ts_val2(val2);
  MonthIntervalScalar ts_null;
  ASSERT_OK(ts_val1.ValidateFull());
  ASSERT_OK(ts_val2.ValidateFull());
  ASSERT_OK(ts_null.ValidateFull());

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
  ASSERT_OK(first->ValidateFull());
  ASSERT_OK(null->ValidateFull());
  ASSERT_OK(last->ValidateFull());

  ASSERT_TRUE(null->Equals(*CheckMakeNullScalar(type)));
  ASSERT_TRUE(first->Equals(*MakeScalar(type, 5).ValueOrDie()));
  ASSERT_TRUE(last->Equals(*MakeScalar(type, 42).ValueOrDie()));
}

TEST(TestDayTimeIntervalScalars, Basics) {
  auto type = day_time_interval();

  DayTimeIntervalType::DayMilliseconds val1 = {1, 1};
  DayTimeIntervalType::DayMilliseconds val2 = {2, 2};
  DayTimeIntervalScalar ts_val1(val1);
  DayTimeIntervalScalar ts_val2(val2);
  DayTimeIntervalScalar ts_null;
  ASSERT_OK(ts_val1.ValidateFull());
  ASSERT_OK(ts_val2.ValidateFull());
  ASSERT_OK(ts_null.ValidateFull());

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
  ASSERT_OK(first->ValidateFull());
  ASSERT_OK(null->ValidateFull());

  ASSERT_TRUE(null->Equals(ts_null));
  ASSERT_TRUE(first->Equals(ts_val2));
}

// TODO test HalfFloatScalar

TYPED_TEST(TestNumericScalar, Cast) {
  auto type = TypeTraits<TypeParam>::type_singleton();

  for (std::string_view repr : {"0", "1", "3"}) {
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
                         StringScalar(std::string(repr)).CastTo(type));
    ASSERT_EQ(*cast_from_string, *scalar);

    if (is_integer_type<TypeParam>::value) {
      ASSERT_OK_AND_ASSIGN(auto cast_to_string, scalar->CastTo(utf8()));
      ASSERT_EQ(
          std::string_view(*checked_cast<const StringScalar&>(*cast_to_string).value),
          repr);
    }
  }
}

template <typename T>
std::shared_ptr<DataType> MakeListType(std::shared_ptr<DataType> value_type,
                                       int32_t list_size) {
  return std::make_shared<T>(std::move(value_type));
}

template <>
std::shared_ptr<DataType> MakeListType<FixedSizeListType>(
    std::shared_ptr<DataType> value_type, int32_t list_size) {
  return fixed_size_list(std::move(value_type), list_size);
}

template <typename T>
class TestListScalar : public ::testing::Test {
 public:
  using ScalarType = typename TypeTraits<T>::ScalarType;

  void SetUp() {
    type_ = MakeListType<T>(int16(), 3);
    value_ = ArrayFromJSON(int16(), "[1, 2, null]");
  }

  void TestBasics() {
    ScalarType scalar(value_);
    ASSERT_OK(scalar.ValidateFull());
    ASSERT_TRUE(scalar.is_valid);
    AssertTypeEqual(scalar.type, type_);
    // list<item: int16>[1, 2, null]
    ASSERT_THAT(scalar.ToString(), ::testing::AllOf(::testing::HasSubstr("item: int16"),
                                                    ::testing::EndsWith("[1, 2, null]")));

    auto null_scalar = CheckMakeNullScalar(type_);
    ASSERT_OK(null_scalar->ValidateFull());
    ASSERT_FALSE(null_scalar->is_valid);
    AssertTypeEqual(null_scalar->type, type_);
    ASSERT_EQ(null_scalar->ToString(), "null");
  }

  void TestValidateErrors() {
    ScalarType scalar(value_);
    scalar.is_valid = false;
    ASSERT_OK(scalar.ValidateFull());

    // Value must be defined
    scalar = ScalarType(value_);
    scalar.value = nullptr;
    AssertValidationFails(scalar);

    // Inconsistent child type
    scalar = ScalarType(value_);
    scalar.value = ArrayFromJSON(int32(), "[1, 2, null]");
    AssertValidationFails(scalar);

    // Invalid UTF8 in child data
    scalar = ScalarType(ArrayFromJSON(utf8(), "[null, null, \"\xff\"]"));
    ASSERT_OK(scalar.Validate());
    ASSERT_RAISES(Invalid, scalar.ValidateFull());
  }

  void TestHashing() {
    // GH-35521: the hash value of a non-null list scalar should not
    // depend on the presence or absence of a null bitmap in the underlying
    // list values.
    ScalarType empty_bitmap_scalar(ArrayFromJSON(int16(), "[1, 2, 3]"));
    ASSERT_OK(empty_bitmap_scalar.ValidateFull());
    // Underlying list array doesn't have a null bitmap
    ASSERT_EQ(empty_bitmap_scalar.value->data()->buffers[0], nullptr);

    auto list_array = ArrayFromJSON(type_, "[[1, 2, 3], [4, 5, null]]");
    ASSERT_OK_AND_ASSIGN(auto set_bitmap_scalar_uncasted, list_array->GetScalar(0));
    auto set_bitmap_scalar = checked_pointer_cast<ScalarType>(set_bitmap_scalar_uncasted);
    // Underlying list array has a null bitmap
    ASSERT_NE(set_bitmap_scalar->value->data()->buffers[0], nullptr);
    // ... yet it's hashing equal to the other scalar
    ASSERT_EQ(empty_bitmap_scalar.hash(), set_bitmap_scalar->hash());

    // GH-35360: the hash value of a scalar from a list of structs should
    // pay attention to the offset so it hashes the equivalent validity bitmap
    auto list_struct_type = list(struct_({field("a", int64())}));
    auto a =
        ArrayFromJSON(list_struct_type, R"([[{"a": 5}, {"a": 6}], [{"a": 7}, null]])");
    auto b = ArrayFromJSON(list_struct_type, R"([[{"a": 7}, null]])");
    EXPECT_OK_AND_ASSIGN(auto a0, a->GetScalar(0));
    EXPECT_OK_AND_ASSIGN(auto a1, a->GetScalar(1));
    EXPECT_OK_AND_ASSIGN(auto b0, b->GetScalar(0));
    ASSERT_EQ(a1->hash(), b0->hash());
    ASSERT_NE(a0->hash(), b0->hash());
  }

 protected:
  std::shared_ptr<DataType> type_;
  std::shared_ptr<Array> value_;
};

using ListScalarTestTypes = ::testing::Types<ListType, LargeListType, FixedSizeListType>;

TYPED_TEST_SUITE(TestListScalar, ListScalarTestTypes);

TYPED_TEST(TestListScalar, Basics) { this->TestBasics(); }

TYPED_TEST(TestListScalar, ValidateErrors) { this->TestValidateErrors(); }

TYPED_TEST(TestListScalar, TestHashing) { this->TestHashing(); }

TEST(TestFixedSizeListScalar, ValidateErrors) {
  const auto ty = fixed_size_list(int16(), 3);
  FixedSizeListScalar scalar(ArrayFromJSON(int16(), "[1, 2, 5]"), ty);
  ASSERT_OK(scalar.ValidateFull());

  scalar.type = fixed_size_list(int16(), 4);
  AssertValidationFails(scalar);
}

TEST(TestMapScalar, Basics) {
  auto value =
      ArrayFromJSON(struct_({field("key", utf8(), false), field("value", int8())}),
                    R"([{"key": "a", "value": 1}, {"key": "b", "value": 2}])");
  auto scalar = MapScalar(value);
  ASSERT_OK(scalar.ValidateFull());

  auto expected_scalar_type = map(utf8(), field("value", int8()));

  ASSERT_TRUE(scalar.type->Equals(expected_scalar_type));
  ASSERT_TRUE(value->Equals(scalar.value));
}

TEST(TestMapScalar, NullScalar) {
  CheckMakeNullScalar(map(utf8(), field("value", int8())));
}

TEST(TestStructScalar, FieldAccess) {
  StructScalar abc({MakeScalar(true), MakeNullScalar(int32()), MakeScalar("hello"),
                    MakeNullScalar(int64())},
                   struct_({field("a", boolean()), field("b", int32()),
                            field("b", utf8()), field("d", int64())}));
  ASSERT_OK(abc.ValidateFull());

  ASSERT_OK_AND_ASSIGN(auto a, abc.field("a"));
  AssertScalarsEqual(*a, *abc.value[0]);

  ASSERT_RAISES(Invalid, abc.field("b").status());

  ASSERT_OK_AND_ASSIGN(auto b, abc.field(1));
  AssertScalarsEqual(*b, *abc.value[1]);

  ASSERT_RAISES(Invalid, abc.field(5).status());
  ASSERT_RAISES(Invalid, abc.field("c").status());

  ASSERT_OK_AND_ASSIGN(auto d, abc.field("d"));
  ASSERT_TRUE(d->Equals(*MakeNullScalar(int64())));
  ASSERT_FALSE(d->Equals(*MakeScalar(int64(), 12).ValueOrDie()));
}

TEST(TestStructScalar, NullScalar) {
  auto ty = struct_({field("a", boolean()), field("b", int32()), field("b", utf8()),
                     field("d", int64())});

  StructScalar null_scalar({MakeNullScalar(boolean()), MakeNullScalar(int32()),
                            MakeNullScalar(utf8()), MakeNullScalar(int64())},
                           ty, /*is_valid=*/false);
  ASSERT_OK(null_scalar.ValidateFull());
  ASSERT_FALSE(null_scalar.is_valid);

  const auto scalar = CheckMakeNullScalar(ty);
  ASSERT_TRUE(scalar->Equals(null_scalar));
}

TEST(TestStructScalar, EmptyStruct) {
  auto ty = struct_({});

  StructScalar null_scalar({}, ty, /*is_valid=*/false);
  ASSERT_OK(null_scalar.ValidateFull());
  ASSERT_FALSE(null_scalar.is_valid);

  auto scalar = CheckMakeNullScalar(ty);
  ASSERT_FALSE(scalar->is_valid);
  ASSERT_TRUE(scalar->Equals(null_scalar));

  StructScalar valid_scalar({}, ty);
  ASSERT_OK(valid_scalar.ValidateFull());
  ASSERT_TRUE(valid_scalar.is_valid);
  ASSERT_FALSE(valid_scalar.Equals(null_scalar));

  auto arr = ArrayFromJSON(ty, "[{}]");
  ASSERT_TRUE(arr->IsValid(0));
  ASSERT_OK_AND_ASSIGN(scalar, arr->GetScalar(0));
  ASSERT_OK(scalar->ValidateFull());
  ASSERT_TRUE(scalar->is_valid);
  ASSERT_TRUE(scalar->Equals(valid_scalar));
}

TEST(TestStructScalar, ValidateErrors) {
  auto ty = struct_({field("a", utf8())});

  // Values must always be defined
  StructScalar scalar({MakeScalar("hello")}, ty);
  scalar.is_valid = false;
  ASSERT_OK(scalar.ValidateFull());

  scalar = StructScalar({}, ty, /*is_valid=*/false);
  scalar.is_valid = true;
  AssertValidationFails(scalar);

  // Inconsistent number of fields
  scalar = StructScalar({}, ty);
  AssertValidationFails(scalar);

  scalar = StructScalar({MakeScalar("foo"), MakeScalar("bar")}, ty);
  AssertValidationFails(scalar);

  // Inconsistent child value type
  scalar = StructScalar({MakeScalar(42)}, ty);
  AssertValidationFails(scalar);

  // Child value has invalid UTF8 data
  scalar = StructScalar({MakeScalar("\xff")}, ty);
  ASSERT_OK(scalar.Validate());
  ASSERT_RAISES(Invalid, scalar.ValidateFull());
}

TEST(TestDictionaryScalar, Basics) {
  for (auto index_ty : all_dictionary_index_types()) {
    auto ty = dictionary(index_ty, utf8());
    auto dict = ArrayFromJSON(utf8(), R"(["alpha", null, "gamma"])");

    DictionaryScalar::ValueType alpha;
    ASSERT_OK_AND_ASSIGN(alpha.index, MakeScalar(index_ty, 0));
    alpha.dictionary = dict;

    DictionaryScalar::ValueType gamma;
    ASSERT_OK_AND_ASSIGN(gamma.index, MakeScalar(index_ty, 2));
    gamma.dictionary = dict;

    DictionaryScalar::ValueType null_value;
    ASSERT_OK_AND_ASSIGN(null_value.index, MakeScalar(index_ty, 1));
    null_value.dictionary = dict;

    auto scalar_null = MakeNullScalar(ty);
    checked_cast<DictionaryScalar&>(*scalar_null).value.dictionary = dict;
    ASSERT_OK(scalar_null->ValidateFull());

    auto scalar_alpha = DictionaryScalar(alpha, ty);
    ASSERT_OK(scalar_alpha.ValidateFull());
    auto scalar_gamma = DictionaryScalar(gamma, ty);
    ASSERT_OK(scalar_gamma.ValidateFull());

    // NOTE: index is valid, though corresponding value is null
    auto scalar_null_value = DictionaryScalar(null_value, ty);
    ASSERT_OK(scalar_null_value.ValidateFull());

    ASSERT_OK_AND_ASSIGN(
        auto encoded_null,
        checked_cast<const DictionaryScalar&>(*scalar_null).GetEncodedValue());
    ASSERT_OK(encoded_null->ValidateFull());
    ASSERT_TRUE(encoded_null->Equals(*MakeNullScalar(utf8())));

    ASSERT_OK_AND_ASSIGN(
        auto encoded_null_value,
        checked_cast<const DictionaryScalar&>(scalar_null_value).GetEncodedValue());
    ASSERT_OK(encoded_null_value->ValidateFull());
    ASSERT_TRUE(encoded_null_value->Equals(*MakeNullScalar(utf8())));

    ASSERT_OK_AND_ASSIGN(
        auto encoded_alpha,
        checked_cast<const DictionaryScalar&>(scalar_alpha).GetEncodedValue());
    ASSERT_OK(encoded_alpha->ValidateFull());
    ASSERT_TRUE(encoded_alpha->Equals(*MakeScalar("alpha")));

    ASSERT_OK_AND_ASSIGN(
        auto encoded_gamma,
        checked_cast<const DictionaryScalar&>(scalar_gamma).GetEncodedValue());
    ASSERT_OK(encoded_gamma->ValidateFull());
    ASSERT_TRUE(encoded_gamma->Equals(*MakeScalar("gamma")));

    // test Array.GetScalar
    DictionaryArray arr(ty, ArrayFromJSON(index_ty, "[2, 0, 1, null]"), dict);
    ASSERT_OK_AND_ASSIGN(auto first, arr.GetScalar(0));
    ASSERT_OK_AND_ASSIGN(auto second, arr.GetScalar(1));
    ASSERT_OK_AND_ASSIGN(auto third, arr.GetScalar(1));
    ASSERT_OK_AND_ASSIGN(auto last, arr.GetScalar(3));
    ASSERT_OK(first->ValidateFull());
    ASSERT_OK(second->ValidateFull());
    ASSERT_OK(last->ValidateFull());

    ASSERT_TRUE(first->is_valid);
    ASSERT_TRUE(second->is_valid);
    ASSERT_TRUE(third->is_valid);  // valid because of valid index, despite null value
    ASSERT_FALSE(last->is_valid);

    ASSERT_TRUE(first->Equals(scalar_gamma));
    ASSERT_TRUE(second->Equals(scalar_alpha));
    ASSERT_TRUE(last->Equals(*scalar_null));

    auto first_dict_scalar = checked_cast<const DictionaryScalar&>(*first);
    ASSERT_TRUE(first_dict_scalar.value.dictionary->Equals(arr.dictionary()));

    auto second_dict_scalar = checked_cast<const DictionaryScalar&>(*second);
    ASSERT_TRUE(second_dict_scalar.value.dictionary->Equals(arr.dictionary()));
  }
}

TEST(TestDictionaryScalar, ValidateErrors) {
  auto index_ty = int16();
  auto value_ty = utf8();
  auto dict = ArrayFromJSON(value_ty, R"(["alpha", null, "gamma"])");
  auto dict_ty = dictionary(index_ty, value_ty);

  DictionaryScalar::ValueType alpha;
  ASSERT_OK_AND_ASSIGN(alpha.index, MakeScalar(index_ty, 0));
  alpha.dictionary = dict;

  // Valid index, null underlying value
  DictionaryScalar::ValueType null_value;
  ASSERT_OK_AND_ASSIGN(null_value.index, MakeScalar(index_ty, 1));
  null_value.dictionary = dict;

  // Null index, no value
  DictionaryScalar::ValueType null{MakeNullScalar(index_ty), dict};

  // Inconsistent index type
  auto scalar = DictionaryScalar(alpha, dictionary(int32(), value_ty));
  AssertValidationFails(scalar);

  // Inconsistent index type
  scalar = DictionaryScalar(alpha, dictionary(index_ty, binary()));
  AssertValidationFails(scalar);

  // Inconsistent is_valid / value
  scalar = DictionaryScalar(alpha, dict_ty);
  ASSERT_OK(scalar.ValidateFull());
  scalar.is_valid = false;
  AssertValidationFails(scalar);

  scalar = DictionaryScalar(null_value, dict_ty);
  ASSERT_OK(scalar.ValidateFull());
  scalar.is_valid = false;
  AssertValidationFails(scalar);

  scalar = DictionaryScalar(null, dict_ty);
  AssertValidationFails(scalar);
  scalar.is_valid = false;
  ASSERT_OK(scalar.ValidateFull());

  // Index value out of bounds
  for (int64_t index : {-1, 3}) {
    DictionaryScalar::ValueType invalid;
    ASSERT_OK_AND_ASSIGN(invalid.index, MakeScalar(index_ty, index));
    invalid.dictionary = dict;

    scalar = DictionaryScalar(invalid, dict_ty);
    ASSERT_OK(scalar.Validate());
    ASSERT_RAISES(Invalid, scalar.ValidateFull());
  }
}

TEST(TestDictionaryScalar, Cast) {
  for (auto index_ty : all_dictionary_index_types()) {
    auto ty = dictionary(index_ty, utf8());
    auto dict = checked_pointer_cast<StringArray>(
        ArrayFromJSON(utf8(), R"(["alpha", null, "gamma"])"));

    for (int64_t i = 0; i < dict->length(); ++i) {
      auto alpha =
          dict->IsValid(i) ? MakeScalar(dict->GetString(i)) : MakeNullScalar(utf8());
      // Cast string to dict(..., string)
      ASSERT_OK_AND_ASSIGN(auto cast_alpha, alpha->CastTo(ty));
      ASSERT_OK(cast_alpha->ValidateFull());
      ASSERT_OK_AND_ASSIGN(
          auto roundtripped_alpha,
          checked_cast<const DictionaryScalar&>(*cast_alpha).GetEncodedValue());

      ASSERT_OK_AND_ASSIGN(auto i_scalar, MakeScalar(index_ty, i));
      auto alpha_dict = DictionaryScalar({i_scalar, dict}, ty);
      ASSERT_OK(alpha_dict.ValidateFull());
      ASSERT_OK_AND_ASSIGN(
          auto encoded_alpha,
          checked_cast<const DictionaryScalar&>(alpha_dict).GetEncodedValue());

      AssertScalarsEqual(*alpha, *roundtripped_alpha);
      AssertScalarsEqual(*encoded_alpha, *roundtripped_alpha);

      // dictionaries differ, though encoded values are identical
      ASSERT_FALSE(alpha_dict.Equals(*cast_alpha));
    }
  }
}

void CheckGetValidUnionScalar(const Array& arr, int64_t index, const Scalar& expected,
                              const Scalar& expected_value) {
  ASSERT_OK_AND_ASSIGN(auto scalar, arr.GetScalar(index));
  ASSERT_OK(scalar->ValidateFull());
  ASSERT_TRUE(scalar->Equals(expected));

  ASSERT_TRUE(scalar->is_valid);
  ASSERT_TRUE(
      checked_cast<const UnionScalar&>(*scalar).child_value()->Equals(expected_value));
}

void CheckGetNullUnionScalar(const Array& arr, int64_t index) {
  ASSERT_OK_AND_ASSIGN(auto scalar, arr.GetScalar(index));
  ASSERT_TRUE(scalar->Equals(*MakeNullScalar(arr.type())));

  ASSERT_FALSE(scalar->is_valid);
  ASSERT_FALSE(checked_cast<const UnionScalar&>(*scalar).child_value()->is_valid);
}

std::shared_ptr<Scalar> MakeUnionScalar(const SparseUnionType& type,
                                        std::shared_ptr<Scalar> field_value,
                                        int field_index) {
  return SparseUnionScalar::FromValue(field_value, field_index, type.GetSharedPtr());
}

std::shared_ptr<Scalar> MakeUnionScalar(const DenseUnionType& type,
                                        std::shared_ptr<Scalar> field_value,
                                        int field_index) {
  int8_t type_code = type.type_codes()[field_index];
  return std::make_shared<DenseUnionScalar>(field_value, type_code, type.GetSharedPtr());
}

std::shared_ptr<Scalar> MakeSpecificNullScalar(const DenseUnionType& type,
                                               int field_index) {
  int8_t type_code = type.type_codes()[field_index];
  auto value = MakeNullScalar(type.field(field_index)->type());
  return std::make_shared<DenseUnionScalar>(value, type_code, type.GetSharedPtr());
}

std::shared_ptr<Scalar> MakeSpecificNullScalar(const SparseUnionType& type,
                                               int field_index) {
  ScalarVector field_values;
  for (int i = 0; i < type.num_fields(); ++i) {
    field_values.emplace_back(MakeNullScalar(type.field(i)->type()));
  }
  return std::make_shared<SparseUnionScalar>(field_values, type.type_codes()[field_index],
                                             type.GetSharedPtr());
}

template <typename Type>
class TestUnionScalar : public ::testing::Test {
 public:
  using UnionType = Type;
  using ScalarType = typename TypeTraits<UnionType>::ScalarType;

  void SetUp() {
    type_.reset(new UnionType({field("string", utf8()), field("number", uint64()),
                               field("other_number", uint64())},
                              /*type_codes=*/{3, 42, 43}));
    union_type_ = static_cast<const Type*>(type_.get());

    alpha_ = MakeScalar("alpha");
    beta_ = MakeScalar("beta");
    ASSERT_OK_AND_ASSIGN(two_, MakeScalar(uint64(), 2));
    ASSERT_OK_AND_ASSIGN(three_, MakeScalar(uint64(), 3));

    union_alpha_ = ScalarFromValue(0, alpha_);
    union_beta_ = ScalarFromValue(0, beta_);
    union_two_ = ScalarFromValue(1, two_);
    union_other_two_ = ScalarFromValue(2, two_);
    union_three_ = ScalarFromValue(1, three_);
    union_string_null_ = SpecificNull(0);
    union_number_null_ = SpecificNull(1);
  }

  std::shared_ptr<Scalar> ScalarFromValue(int field_index,
                                          std::shared_ptr<Scalar> field_value) {
    return MakeUnionScalar(*union_type_, field_value, field_index);
  }

  std::shared_ptr<Scalar> SpecificNull(int field_index) {
    return MakeSpecificNullScalar(*union_type_, field_index);
  }

  void TestValidate() {
    ASSERT_OK(union_alpha_->ValidateFull());
    ASSERT_OK(union_beta_->ValidateFull());
    ASSERT_OK(union_two_->ValidateFull());
    ASSERT_OK(union_other_two_->ValidateFull());
    ASSERT_OK(union_three_->ValidateFull());
    ASSERT_OK(union_string_null_->ValidateFull());
    ASSERT_OK(union_number_null_->ValidateFull());
  }

  void TestValidateErrors() {
    // Type code doesn't exist
    auto scalar = ScalarFromValue(0, alpha_);
    UnionScalar* union_scalar = static_cast<UnionScalar*>(scalar.get());

    // Invalid type code
    union_scalar->type_code = 0;
    AssertValidationFails(*union_scalar);

    union_scalar->is_valid = false;
    AssertValidationFails(*union_scalar);

    union_scalar->type_code = -42;
    union_scalar->is_valid = true;
    AssertValidationFails(*union_scalar);

    union_scalar->is_valid = false;
    AssertValidationFails(*union_scalar);

    // Type code doesn't correspond to child type
    if (type_->id() == ::arrow::Type::DENSE_UNION) {
      union_scalar->type_code = 42;
      union_scalar->is_valid = true;
      AssertValidationFails(*union_scalar);

      scalar = ScalarFromValue(2, two_);
      union_scalar = static_cast<UnionScalar*>(scalar.get());
      union_scalar->type_code = 3;
      AssertValidationFails(*union_scalar);
    }

    // underlying value has invalid UTF8
    scalar = ScalarFromValue(0, std::make_shared<StringScalar>("\xff"));
    ASSERT_OK(scalar->Validate());
    ASSERT_RAISES(Invalid, scalar->ValidateFull());
  }

  void TestEquals() {
    // Differing values
    ASSERT_FALSE(union_alpha_->Equals(*union_beta_));
    ASSERT_FALSE(union_two_->Equals(*union_three_));
    // Differing validities
    ASSERT_FALSE(union_alpha_->Equals(*union_string_null_));
    // Differing types
    ASSERT_FALSE(union_alpha_->Equals(*union_two_));
    ASSERT_FALSE(union_alpha_->Equals(*union_other_two_));
    // Type codes don't count when comparing union scalars: the underlying values
    // are identical even though their provenance is different.
    ASSERT_TRUE(union_two_->Equals(*union_other_two_));
    ASSERT_TRUE(union_string_null_->Equals(*union_number_null_));
  }

  void TestMakeNullScalar() {
    const auto scalar = MakeNullScalar(type_);
    const auto& as_union = checked_cast<const UnionScalar&>(*scalar);
    AssertTypeEqual(type_, as_union.type);
    ASSERT_FALSE(as_union.is_valid);

    // The first child field is chosen arbitrarily for the purposes of making
    // a null scalar
    ASSERT_EQ(as_union.type_code, 3);

    if (type_->id() == ::arrow::Type::DENSE_UNION) {
      const auto& as_dense_union = checked_cast<const DenseUnionScalar&>(*scalar);
      ASSERT_FALSE(as_dense_union.value->is_valid);
    } else {
      const auto& as_sparse_union = checked_cast<const SparseUnionScalar&>(*scalar);
      ASSERT_FALSE(as_sparse_union.value[as_sparse_union.child_id]->is_valid);
    }
  }

 protected:
  std::shared_ptr<DataType> type_;
  const UnionType* union_type_;
  std::shared_ptr<Scalar> alpha_, beta_, two_, three_;
  std::shared_ptr<Scalar> union_alpha_, union_beta_, union_two_, union_three_,
      union_other_two_, union_string_null_, union_number_null_;
};

TYPED_TEST_SUITE(TestUnionScalar, UnionArrowTypes);

TYPED_TEST(TestUnionScalar, Validate) { this->TestValidate(); }

TYPED_TEST(TestUnionScalar, ValidateErrors) { this->TestValidateErrors(); }

TYPED_TEST(TestUnionScalar, Equals) { this->TestEquals(); }

TYPED_TEST(TestUnionScalar, MakeNullScalar) { this->TestMakeNullScalar(); }

class TestSparseUnionScalar : public TestUnionScalar<SparseUnionType> {};

TEST_F(TestSparseUnionScalar, GetScalar) {
  ArrayVector children{ArrayFromJSON(utf8(), R"(["alpha", "", "beta", null, "gamma"])"),
                       ArrayFromJSON(uint64(), "[1, 2, 11, 22, null]"),
                       ArrayFromJSON(uint64(), "[100, 101, 102, 103, 104]")};

  auto type_ids = ArrayFromJSON(int8(), "[3, 42, 3, 3, 42]");
  SparseUnionArray arr(type_, 5, children, type_ids->data()->buffers[1]);
  ASSERT_OK(arr.ValidateFull());

  CheckGetValidUnionScalar(arr, 0, *union_alpha_, *alpha_);
  CheckGetValidUnionScalar(arr, 1, *union_two_, *two_);
  CheckGetValidUnionScalar(arr, 2, *union_beta_, *beta_);
  CheckGetNullUnionScalar(arr, 3);
  CheckGetNullUnionScalar(arr, 4);
}

class TestDenseUnionScalar : public TestUnionScalar<DenseUnionType> {};

TEST_F(TestDenseUnionScalar, GetScalar) {
  ArrayVector children{ArrayFromJSON(utf8(), R"(["alpha", "beta", null])"),
                       ArrayFromJSON(uint64(), "[2, 3]"), ArrayFromJSON(uint64(), "[]")};

  auto type_ids = ArrayFromJSON(int8(), "[3, 42, 3, 3, 42]");
  auto offsets = ArrayFromJSON(int32(), "[0, 0, 1, 2, 1]");
  DenseUnionArray arr(type_, 5, children, type_ids->data()->buffers[1],
                      offsets->data()->buffers[1]);
  ASSERT_OK(arr.ValidateFull());

  CheckGetValidUnionScalar(arr, 0, *union_alpha_, *alpha_);
  CheckGetValidUnionScalar(arr, 1, *union_two_, *two_);
  CheckGetValidUnionScalar(arr, 2, *union_beta_, *beta_);
  CheckGetNullUnionScalar(arr, 3);
  CheckGetValidUnionScalar(arr, 4, *union_three_, *three_);
}

template <typename RunEndType>
class TestRunEndEncodedScalar : public ::testing::Test {
 public:
  using RunEndCType = typename RunEndType::c_type;

  void SetUp() override {
    run_end_type_ = std::make_shared<RunEndType>();
    value_type_ = utf8();
    type_.reset(new RunEndEncodedType(run_end_type_, value_type_));

    alpha_ = MakeScalar("alpha");
    beta_ = MakeScalar("beta");
  }

  void TestBasics() {
    RunEndEncodedScalar scalar_alpha{alpha_, type_};
    ASSERT_OK(scalar_alpha.ValidateFull());
    ASSERT_TRUE(scalar_alpha.is_valid);
    AssertTypeEqual(scalar_alpha.type, type_);
    ASSERT_EQ(scalar_alpha.ToString(),
              "\n-- run_ends:\n"
              "  [\n"
              "    1\n"
              "  ]\n"
              "-- values:\n"
              "  [\n"
              "    \"alpha\"\n"
              "  ]");

    auto null_scalar = CheckMakeNullScalar(type_);
    ASSERT_OK(null_scalar->ValidateFull());
    ASSERT_FALSE(null_scalar->is_valid);
    AssertTypeEqual(null_scalar->type, type_);
    ASSERT_EQ(null_scalar->ToString(), "null");

    RunEndEncodedScalar scalar_beta{beta_, type_};
    ASSERT_TRUE(scalar_alpha.Equals(scalar_alpha));
    ASSERT_FALSE(scalar_alpha.Equals(scalar_beta));
    ASSERT_FALSE(scalar_beta.Equals(scalar_alpha));
    ASSERT_TRUE(scalar_beta.Equals(scalar_beta));
    ASSERT_FALSE(null_scalar->Equals(scalar_alpha));
    ASSERT_FALSE(scalar_alpha.Equals(*null_scalar));
  }

  void TestValidateErrors() {
    // Inconsistent is_valid / value
    RunEndEncodedScalar scalar_alpha{alpha_, type_};
    scalar_alpha.is_valid = false;
    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               std::string("Invalid: null run_end_encoded<run_ends: ") +
                                   run_end_type_->ToString() +
                                   ", values: string> scalar has non-null storage value",
                               scalar_alpha.Validate());

    auto null_scalar = MakeNullScalar(type_);
    null_scalar->is_valid = true;
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid,
        std::string("Invalid: non-null run_end_encoded<run_ends: ") +
            run_end_type_->ToString() + ", values: string> scalar has null storage value",
        null_scalar->Validate());

    // Bad value type
    auto ree_type = run_end_encoded(run_end_type_, int64());
    RunEndEncodedScalar scalar_beta(beta_, ree_type);
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid,
        std::string("Invalid: run_end_encoded<run_ends: ") + run_end_type_->ToString() +
            ", values: int64> scalar should have an underlying value of type int64, got "
            "string",
        scalar_beta.Validate());

    // Invalid UTF8
    auto bad_utf8 = std::make_shared<StringScalar>(Buffer::FromString("\xff"));
    RunEndEncodedScalar scalar(std::move(bad_utf8), type_);
    ASSERT_OK(scalar.Validate());
    ASSERT_RAISES(Invalid, scalar.ValidateFull());
  }

  void TestHashing() {
    std::unordered_set<std::shared_ptr<Scalar>, Scalar::Hash, Scalar::PtrsEqual> set;
    set.emplace(std::make_shared<RunEndEncodedScalar>(type_));
    for (int i = 0; i < 10; ++i) {
      const auto value = std::make_shared<StringScalar>(std::to_string(i));
      set.emplace(std::make_shared<RunEndEncodedScalar>(std::move(value), type_));
    }

    ASSERT_FALSE(set.emplace(std::make_shared<RunEndEncodedScalar>(type_)).second);
    for (int i = 0; i < 10; ++i) {
      const auto value = std::make_shared<StringScalar>(std::to_string(i));
      ASSERT_FALSE(
          set.emplace(std::make_shared<RunEndEncodedScalar>(std::move(value), type_))
              .second);
    }
  }

 private:
  std::shared_ptr<DataType> run_end_type_;
  std::shared_ptr<DataType> value_type_;
  std::shared_ptr<DataType> type_;
  std::shared_ptr<Scalar> alpha_;
  std::shared_ptr<Scalar> beta_;
};

using RunEndTestTypes = ::testing::Types<Int16Type, Int32Type, Int64Type>;
TYPED_TEST_SUITE(TestRunEndEncodedScalar, RunEndTestTypes);

TYPED_TEST(TestRunEndEncodedScalar, Basics) { this->TestBasics(); }
TYPED_TEST(TestRunEndEncodedScalar, ValidateErrors) { this->TestValidateErrors(); }
TYPED_TEST(TestRunEndEncodedScalar, Hashing) { this->TestHashing(); }

#define UUID_STRING1 "abcdefghijklmnop"
#define UUID_STRING2 "zyxwvutsrqponmlk"

class TestExtensionScalar : public ::testing::Test {
 public:
  void SetUp() {
    type_ = uuid();
    storage_type_ = fixed_size_binary(16);
    uuid_type_ = checked_cast<const UuidType*>(type_.get());
  }

 protected:
  ExtensionScalar MakeUuidScalar(std::string_view value) {
    return ExtensionScalar(std::make_shared<FixedSizeBinaryScalar>(
                               std::make_shared<Buffer>(value), storage_type_),
                           type_);
  }

  std::shared_ptr<DataType> type_, storage_type_;
  const UuidType* uuid_type_{nullptr};

  const std::string_view uuid_string1_{UUID_STRING1};
  const std::string_view uuid_string2_{UUID_STRING2};
  const std::string_view uuid_json_{"[\"" UUID_STRING1 "\", \"" UUID_STRING2 "\", null]"};
};

#undef UUID_STRING1
#undef UUID_STRING2

TEST_F(TestExtensionScalar, Basics) {
  const ExtensionScalar uuid_scalar = MakeUuidScalar(uuid_string1_);
  ASSERT_OK(uuid_scalar.ValidateFull());
  ASSERT_TRUE(uuid_scalar.is_valid);

  const ExtensionScalar uuid_scalar2 = MakeUuidScalar(uuid_string2_);
  ASSERT_OK(uuid_scalar2.ValidateFull());
  ASSERT_TRUE(uuid_scalar2.is_valid);

  const ExtensionScalar uuid_scalar3 = MakeUuidScalar(uuid_string2_);
  ASSERT_OK(uuid_scalar2.ValidateFull());
  ASSERT_TRUE(uuid_scalar2.is_valid);

  const ExtensionScalar null_scalar(MakeNullScalar(storage_type_), type_,
                                    /*is_valid=*/false);
  ASSERT_OK(null_scalar.ValidateFull());
  ASSERT_FALSE(null_scalar.is_valid);

  ASSERT_FALSE(uuid_scalar.Equals(uuid_scalar2));
  ASSERT_TRUE(uuid_scalar2.Equals(uuid_scalar3));
  ASSERT_FALSE(uuid_scalar.Equals(null_scalar));
}

TEST_F(TestExtensionScalar, MakeScalar) {
  const ExtensionScalar null_scalar(MakeNullScalar(storage_type_), type_,
                                    /*is_valid=*/false);
  const ExtensionScalar uuid_scalar = MakeUuidScalar(uuid_string1_);

  auto scalar = CheckMakeNullScalar(type_);
  ASSERT_OK(scalar->ValidateFull());
  ASSERT_FALSE(scalar->is_valid);

  ASSERT_OK_AND_ASSIGN(auto scalar2,
                       MakeScalar(type_, std::make_shared<Buffer>(uuid_string1_)));
  ASSERT_OK(scalar2->ValidateFull());
  ASSERT_TRUE(scalar2->is_valid);

  ASSERT_OK_AND_ASSIGN(auto scalar3,
                       MakeScalar(type_, std::make_shared<Buffer>(uuid_string2_)));
  ASSERT_OK(scalar3->ValidateFull());
  ASSERT_TRUE(scalar3->is_valid);

  ASSERT_TRUE(scalar->Equals(null_scalar));
  ASSERT_TRUE(scalar2->Equals(uuid_scalar));
  ASSERT_FALSE(scalar3->Equals(uuid_scalar));
}

TEST_F(TestExtensionScalar, GetScalar) {
  const ExtensionScalar null_scalar(MakeNullScalar(storage_type_), type_,
                                    /*is_valid=*/false);
  const ExtensionScalar uuid_scalar = MakeUuidScalar(uuid_string1_);
  const ExtensionScalar uuid_scalar2 = MakeUuidScalar(uuid_string2_);

  auto storage_array = ArrayFromJSON(storage_type_, uuid_json_);
  auto array = ExtensionType::WrapArray(type_, storage_array);

  ASSERT_OK_AND_ASSIGN(auto scalar, array->GetScalar(0));
  ASSERT_OK(scalar->ValidateFull());
  AssertTypeEqual(scalar->type, type_);
  ASSERT_TRUE(scalar->is_valid);
  ASSERT_TRUE(scalar->Equals(uuid_scalar));
  ASSERT_FALSE(scalar->Equals(uuid_scalar2));

  ASSERT_OK_AND_ASSIGN(scalar, array->GetScalar(1));
  ASSERT_OK(scalar->ValidateFull());
  AssertTypeEqual(scalar->type, type_);
  ASSERT_TRUE(scalar->is_valid);
  ASSERT_TRUE(scalar->Equals(uuid_scalar2));
  ASSERT_FALSE(scalar->Equals(uuid_scalar));

  ASSERT_OK_AND_ASSIGN(scalar, array->GetScalar(2));
  ASSERT_OK(scalar->ValidateFull());
  AssertTypeEqual(scalar->type, type_);
  ASSERT_FALSE(scalar->is_valid);
  ASSERT_TRUE(scalar->Equals(null_scalar));
  ASSERT_FALSE(scalar->Equals(uuid_scalar));
}

TEST_F(TestExtensionScalar, ValidateErrors) {
  // Mismatching is_valid and value
  ExtensionScalar null_scalar(MakeNullScalar(storage_type_), type_,
                              /*is_valid=*/false);
  null_scalar.is_valid = true;
  AssertValidationFails(null_scalar);

  ExtensionScalar uuid_scalar = MakeUuidScalar(uuid_string1_);
  uuid_scalar.is_valid = false;
  AssertValidationFails(uuid_scalar);

  // Null storage scalar
  auto null_storage = MakeNullScalar(storage_type_);
  ExtensionScalar scalar(null_storage, type_);
  scalar.is_valid = true;
  AssertValidationFails(scalar);

  // If the scalar is null it's okay
  scalar.is_valid = false;
  ASSERT_OK(scalar.ValidateFull());

  // Invalid storage scalar (wrong length)
  std::shared_ptr<Scalar> invalid_storage = MakeNullScalar(storage_type_);
  invalid_storage->is_valid = true;
  static_cast<FixedSizeBinaryScalar*>(invalid_storage.get())->value =
      std::make_shared<Buffer>("123");
  AssertValidationFails(*invalid_storage);
  scalar = ExtensionScalar(invalid_storage, type_);
  AssertValidationFails(scalar);
}

}  // namespace arrow
