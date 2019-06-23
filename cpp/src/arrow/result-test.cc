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

#include "arrow/result.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace arrow {

namespace {

using ::testing::Eq;

StatusCode kErrorCode = StatusCode::Invalid;
constexpr char kErrorMessage[] = "Invalid argument";

const int kIntElement = 42;
constexpr char kStringElement[] =
    "The Answer to the Ultimate Question of Life, the Universe, and Everything";

// A data type without a default constructor.
struct Foo {
  int bar;
  std::string baz;

  explicit Foo(int value) : bar(value), baz(kStringElement) {}
};

// A data type with only copy constructors.
struct CopyOnlyDataType {
  explicit CopyOnlyDataType(int x) : data(x) {}

  CopyOnlyDataType(const CopyOnlyDataType& other) = default;
  CopyOnlyDataType& operator=(const CopyOnlyDataType& other) = default;

  int data;
};

struct ImplicitlyCopyConvertible {
  ImplicitlyCopyConvertible(const CopyOnlyDataType& co)  // NOLINT(runtime/explicit)
      : copy_only(co) {}

  CopyOnlyDataType copy_only;
};

// A data type with only move constructors.
struct MoveOnlyDataType {
  explicit MoveOnlyDataType(int x) : data(new int(x)) {}

  MoveOnlyDataType(MoveOnlyDataType&& other) : data(other.data) { other.data = nullptr; }

  MoveOnlyDataType(const MoveOnlyDataType& other) = delete;
  MoveOnlyDataType& operator=(const MoveOnlyDataType& other) = delete;

  ~MoveOnlyDataType() {
    delete data;
    data = nullptr;
  }

  int* data;
};

struct ImplicitlyMoveConvertible {
  ImplicitlyMoveConvertible(MoveOnlyDataType&& mo)  // NOLINT(runtime/explicit)
      : move_only(std::move(mo)) {}

  MoveOnlyDataType move_only;
};

// A data type with dynamically-allocated data.
struct HeapAllocatedObject {
  int* value;

  HeapAllocatedObject() {
    value = new int;
    *value = kIntElement;
  }

  HeapAllocatedObject(const HeapAllocatedObject& other) {
    value = new int;
    *value = *other.value;
  }

  HeapAllocatedObject& operator=(const HeapAllocatedObject& other) {
    *value = *other.value;
    return *this;
  }

  HeapAllocatedObject(HeapAllocatedObject&& other) {
    value = other.value;
    other.value = nullptr;
  }

  ~HeapAllocatedObject() { delete value; }
};

// Constructs a Foo.
struct FooCtor {
  using value_type = Foo;

  Foo operator()() { return Foo(kIntElement); }
};

// Constructs a HeapAllocatedObject.
struct HeapAllocatedObjectCtor {
  using value_type = HeapAllocatedObject;

  HeapAllocatedObject operator()() { return HeapAllocatedObject(); }
};

// Constructs an integer.
struct IntCtor {
  using value_type = int;

  int operator()() { return kIntElement; }
};

// Constructs a string.
struct StringCtor {
  using value_type = std::string;

  std::string operator()() { return std::string(kStringElement); }
};

// Constructs a vector of strings.
struct StringVectorCtor {
  using value_type = std::vector<std::string>;

  std::vector<std::string> operator()() { return {kStringElement, kErrorMessage}; }
};

bool operator==(const Foo& lhs, const Foo& rhs) {
  return (lhs.bar == rhs.bar) && (lhs.baz == rhs.baz);
}

bool operator==(const HeapAllocatedObject& lhs, const HeapAllocatedObject& rhs) {
  return *lhs.value == *rhs.value;
}

// Returns an rvalue reference to the Result<T> object pointed to by
// |result|.
template <class T>
Result<T>&& MoveResult(Result<T>* result) {
  return std::move(*result);
}

// A test fixture is required for typed tests.
template <typename T>
class ResultTest : public ::testing::Test {};

typedef ::testing::Types<IntCtor, FooCtor, StringCtor, StringVectorCtor,
                         HeapAllocatedObjectCtor>
    TestTypes;

TYPED_TEST_CASE(ResultTest, TestTypes);

// Verify that the default constructor for Result constructs an object with a
// non-ok status.
TYPED_TEST(ResultTest, ConstructorDefault) {
  Result<typename TypeParam::value_type> result;
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), StatusCode::UnknownError);
}

// Verify that Result can be constructed from a Status object.
TYPED_TEST(ResultTest, ConstructorStatus) {
  Result<typename TypeParam::value_type> result(Status(kErrorCode, kErrorMessage));

  EXPECT_FALSE(result.ok());
  EXPECT_FALSE(result.status().ok());
  EXPECT_EQ(result.status().code(), kErrorCode);
  EXPECT_EQ(result.status().message(), kErrorMessage);
}

// Verify that Result can be constructed from an object of its element type.
TYPED_TEST(ResultTest, ConstructorElementConstReference) {
  typename TypeParam::value_type value = TypeParam()();
  Result<typename TypeParam::value_type> result(value);

  ASSERT_TRUE(result.ok());
  ASSERT_TRUE(result.status().ok());
  EXPECT_EQ(result.ValueOrDie(), value);
  EXPECT_EQ(*result, value);
}

// Verify that Result can be constructed from an rvalue reference of an object
// of its element type.
TYPED_TEST(ResultTest, ConstructorElementRValue) {
  typename TypeParam::value_type value = TypeParam()();
  typename TypeParam::value_type value_copy(value);
  Result<typename TypeParam::value_type> result(std::move(value));

  ASSERT_TRUE(result.ok());
  ASSERT_TRUE(result.status().ok());

  // Compare to a copy of the original value, since the original was moved.
  EXPECT_EQ(result.ValueOrDie(), value_copy);
}

// Verify that Result can be copy-constructed from a Result with a non-ok
// status.
TYPED_TEST(ResultTest, CopyConstructorNonOkStatus) {
  Result<typename TypeParam::value_type> result1 = Status(kErrorCode, kErrorMessage);
  Result<typename TypeParam::value_type> result2(result1);

  EXPECT_EQ(result1.ok(), result2.ok());
  EXPECT_EQ(result1.status().message(), result2.status().message());
}

// Verify that Result can be copy-constructed from a Result with an ok
// status.
TYPED_TEST(ResultTest, CopyConstructorOkStatus) {
  Result<typename TypeParam::value_type> result1((TypeParam()()));
  Result<typename TypeParam::value_type> result2(result1);

  EXPECT_EQ(result1.ok(), result2.ok());
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(result1.ValueOrDie(), result2.ValueOrDie());
  EXPECT_EQ(*result1, *result2);
}

// Verify that copy-assignment of a Result with a non-ok is working as
// expected.
TYPED_TEST(ResultTest, CopyAssignmentNonOkStatus) {
  Result<typename TypeParam::value_type> result1(Status(kErrorCode, kErrorMessage));
  Result<typename TypeParam::value_type> result2((TypeParam()()));

  // Invoke the copy-assignment operator.
  result2 = result1;
  EXPECT_EQ(result1.ok(), result2.ok());
  EXPECT_EQ(result1.status().message(), result2.status().message());
}

// Verify that copy-assignment of a Result with an ok status is working as
// expected.
TYPED_TEST(ResultTest, CopyAssignmentOkStatus) {
  Result<typename TypeParam::value_type> result1((TypeParam()()));
  Result<typename TypeParam::value_type> result2(Status(kErrorCode, kErrorMessage));

  // Invoke the copy-assignment operator.
  result2 = result1;
  EXPECT_EQ(result1.ok(), result2.ok());
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(result1.ValueOrDie(), result2.ValueOrDie());
  EXPECT_EQ(*result1, *result2);
}

// Verify that copy-assignment of a Result with a non-ok status to itself is
// properly handled.
TYPED_TEST(ResultTest, CopyAssignmentSelfNonOkStatus) {
  Status status(kErrorCode, kErrorMessage);
  Result<typename TypeParam::value_type> result(status);
  result = *&result;

  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), status.code());
}

// Verify that copy-assignment of a Result with an ok status to itself is
// properly handled.
TYPED_TEST(ResultTest, CopyAssignmentSelfOkStatus) {
  typename TypeParam::value_type value = TypeParam()();
  Result<typename TypeParam::value_type> result(value);
  result = *&result;

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie(), value);
  EXPECT_EQ(*result, value);
}

// Verify that Result can be move-constructed from a Result with a non-ok
// status.
TYPED_TEST(ResultTest, MoveConstructorNonOkStatus) {
  Status status(kErrorCode, kErrorMessage);
  Result<typename TypeParam::value_type> result1(status);
  Result<typename TypeParam::value_type> result2(std::move(result1));

  // Verify that the status of the donor object was updated.
  EXPECT_FALSE(result1.ok());

  // Verify that the destination object contains the status previously held by
  // the donor.
  EXPECT_FALSE(result2.ok());
  EXPECT_EQ(result2.status().code(), status.code());
}

// Verify that Result can be move-constructed from a Result with an ok
// status.
TYPED_TEST(ResultTest, MoveConstructorOkStatus) {
  typename TypeParam::value_type value = TypeParam()();
  Result<typename TypeParam::value_type> result1(value);
  Result<typename TypeParam::value_type> result2(std::move(result1));

  // Verify that the donor object was updated to contain a non-ok status.
  EXPECT_FALSE(result1.ok());

  // The destination object should possess the value previously held by the
  // donor.
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(result2.ValueOrDie(), value);
}

// Verify that move-assignment from a Result with a non-ok status is working
// as expected.
TYPED_TEST(ResultTest, MoveAssignmentOperatorNonOkStatus) {
  Status status(kErrorCode, kErrorMessage);
  Result<typename TypeParam::value_type> result1(status);
  Result<typename TypeParam::value_type> result2((TypeParam()()));

  // Invoke the move-assignment operator.
  result2 = std::move(result1);

  // Verify that the status of the donor object was updated.
  EXPECT_FALSE(result1.ok());

  // Verify that the destination object contains the status previously held by
  // the donor.
  EXPECT_FALSE(result2.ok());
  EXPECT_EQ(result2.status().code(), status.code());
}

// Verify that move-assignment from a Result with an ok status is working as
// expected.
TYPED_TEST(ResultTest, MoveAssignmentOperatorOkStatus) {
  typename TypeParam::value_type value = TypeParam()();
  Result<typename TypeParam::value_type> result1(value);
  Result<typename TypeParam::value_type> result2(Status(kErrorCode, kErrorMessage));

  // Invoke the move-assignment operator.
  result2 = std::move(result1);

  // Verify that the donor object was updated to contain a non-ok status.
  EXPECT_FALSE(result1.ok());

  // The destination object should possess the value previously held by the
  // donor.
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(result2.ValueOrDie(), value);
}

// Verify that move-assignment of a Result with a non-ok status to itself is
// handled properly.
TYPED_TEST(ResultTest, MoveAssignmentSelfNonOkStatus) {
  Status status(kErrorCode, kErrorMessage);
  Result<typename TypeParam::value_type> result(status);

  result = MoveResult(&result);

  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), status.code());
}

// Verify that move-assignment of a Result with an ok-status to itself is
// handled properly.
TYPED_TEST(ResultTest, MoveAssignmentSelfOkStatus) {
  typename TypeParam::value_type value = TypeParam()();
  Result<typename TypeParam::value_type> result(value);

  result = MoveResult(&result);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie(), value);
}

// Tests for move-only types. These tests use std::unique_ptr<> as the
// test type, since it is valuable to support this type in the Asylo infra.
// These tests are not part of the typed test suite for the following reasons:
//   * std::unique_ptr<> cannot be used as a type in tests that expect
//   the test type to support copy operations.
//   * std::unique_ptr<> provides an equality operator that checks equality of
//   the underlying ptr. Consequently, it is difficult to generalize existing
//   tests that verify ValueOrDie() functionality using equality comparisons.

// Verify that a Result object can be constructed from a move-only type.
TEST(ResultTest, InitializationMoveOnlyType) {
  std::string* str = new std::string(kStringElement);
  std::unique_ptr<std::string> value(str);
  Result<std::unique_ptr<std::string>> result(std::move(value));

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().get(), str);
}

// Verify that a Result object can be move-constructed from a move-only type.
TEST(ResultTest, MoveConstructorMoveOnlyType) {
  std::string* str = new std::string(kStringElement);
  std::unique_ptr<std::string> value(str);
  Result<std::unique_ptr<std::string>> result1(std::move(value));
  Result<std::unique_ptr<std::string>> result2(std::move(result1));

  // Verify that the donor object was updated to contain a non-ok status.
  EXPECT_FALSE(result1.ok());

  // The destination object should possess the value previously held by the
  // donor.
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(result2.ValueOrDie().get(), str);
}

// Verify that a Result object can be move-assigned to from a Result object
// containing a move-only type.
TEST(ResultTest, MoveAssignmentMoveOnlyType) {
  std::string* str = new std::string(kStringElement);
  std::unique_ptr<std::string> value(str);
  Result<std::unique_ptr<std::string>> result1(std::move(value));
  Result<std::unique_ptr<std::string>> result2(Status(kErrorCode, kErrorMessage));

  // Invoke the move-assignment operator.
  result2 = std::move(result1);

  // Verify that the donor object was updated to contain a non-ok status.
  EXPECT_FALSE(result1.ok());

  // The destination object should possess the value previously held by the
  // donor.
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(result2.ValueOrDie().get(), str);
}

// Verify that a value can be moved out of a Result object via ValueOrDie().
TEST(ResultTest, ValueOrDieMovedValue) {
  std::string* str = new std::string(kStringElement);
  std::unique_ptr<std::string> value(str);
  Result<std::unique_ptr<std::string>> result(std::move(value));

  std::unique_ptr<std::string> moved_value = std::move(result).ValueOrDie();
  EXPECT_EQ(moved_value.get(), str);
  EXPECT_EQ(*moved_value, kStringElement);

  // Verify that the Result object was invalidated after the value was moved.
  EXPECT_FALSE(result.ok());
}

// Verify that a Result<T> is implicitly constructible from some U, where T is
// a type which has an implicit constructor taking a const U &.
TEST(ResultTest, TemplateValueCopyConstruction) {
  CopyOnlyDataType copy_only(kIntElement);
  Result<ImplicitlyCopyConvertible> result(copy_only);

  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().copy_only.data, kIntElement);
}

// Verify that a Result<T> is implicitly constructible from some U, where T is
// a type which has an implicit constructor taking a U &&.
TEST(ResultTest, TemplateValueMoveConstruction) {
  MoveOnlyDataType move_only(kIntElement);
  Result<ImplicitlyMoveConvertible> result(std::move(move_only));

  EXPECT_TRUE(result.ok());
  EXPECT_EQ(*result.ValueOrDie().move_only.data, kIntElement);
}

// Verify that a Result<U> is assignable to a Result<T>, where T
// is a type which has an implicit constructor taking a const U &.
TEST(ResultTest, TemplateCopyAssign) {
  CopyOnlyDataType copy_only(kIntElement);
  Result<CopyOnlyDataType> result(copy_only);

  Result<ImplicitlyCopyConvertible> result2 = result;

  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().data, kIntElement);
  EXPECT_TRUE(result2.ok());
  EXPECT_EQ(result2.ValueOrDie().copy_only.data, kIntElement);
}

// Verify that a Result<U> is assignable to a Result<T>, where T is a type
// which has an implicit constructor taking a U &&.
TEST(ResultTest, TemplateMoveAssign) {
  MoveOnlyDataType move_only(kIntElement);
  Result<MoveOnlyDataType> result(std::move(move_only));

  Result<ImplicitlyMoveConvertible> result2 = std::move(result);

  EXPECT_TRUE(result2.ok());
  EXPECT_EQ(*result2.ValueOrDie().move_only.data, kIntElement);

  //  NOLINTNEXTLINE use after move.
  EXPECT_FALSE(result.ok());
  //  NOLINTNEXTLINE use after move.
}

// Verify that a Result<U> is constructible from a Result<T>, where T is a
// type which has an implicit constructor taking a const U &.
TEST(ResultTest, TemplateCopyConstruct) {
  CopyOnlyDataType copy_only(kIntElement);
  Result<CopyOnlyDataType> result(copy_only);
  Result<ImplicitlyCopyConvertible> result2(result);

  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().data, kIntElement);
  EXPECT_TRUE(result2.ok());
  EXPECT_EQ(result2.ValueOrDie().copy_only.data, kIntElement);
}

// Verify that a Result<U> is constructible from a Result<T>, where T is a
// type which has an implicit constructor taking a U &&.
TEST(ResultTest, TemplateMoveConstruct) {
  MoveOnlyDataType move_only(kIntElement);
  Result<MoveOnlyDataType> result(std::move(move_only));
  Result<ImplicitlyMoveConvertible> result2(std::move(result));

  EXPECT_TRUE(result2.ok());
  EXPECT_EQ(*result2.ValueOrDie().move_only.data, kIntElement);

  //  NOLINTNEXTLINE use after move.
  EXPECT_FALSE(result.ok());
  //  NOLINTNEXTLINE use after move.
}

}  // namespace
}  // namespace arrow
