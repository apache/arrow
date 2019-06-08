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

#include "arrow/error_or.h"

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

// Returns an rvalue reference to the ErrorOr<T> object pointed to by
// |statusor|.
template <class T>
ErrorOr<T>&& MoveErrorOr(ErrorOr<T>* statusor) {
  return std::move(*statusor);
}

// A test fixture is required for typed tests.
template <typename T>
class ErrorOrTest : public ::testing::Test {};

typedef ::testing::Types<IntCtor, FooCtor, StringCtor, StringVectorCtor,
                         HeapAllocatedObjectCtor>
    TestTypes;

TYPED_TEST_CASE(ErrorOrTest, TestTypes);

// Verify that the default constructor for ErrorOr constructs an object with a
// non-ok status.
TYPED_TEST(ErrorOrTest, ConstructorDefault) {
  ErrorOr<typename TypeParam::value_type> statusor;
  EXPECT_FALSE(statusor.ok());
  EXPECT_EQ(statusor.status().code(), StatusCode::UnknownError);
}

// Verify that ErrorOr can be constructed from a Status object.
TYPED_TEST(ErrorOrTest, ConstructorStatus) {
  ErrorOr<typename TypeParam::value_type> statusor(Status(kErrorCode, kErrorMessage));

  EXPECT_FALSE(statusor.ok());
  EXPECT_FALSE(statusor.status().ok());
  EXPECT_EQ(statusor.status().code(), kErrorCode);
  EXPECT_EQ(statusor.status().message(), kErrorMessage);
}

// Verify that ErrorOr can be constructed from an object of its element type.
TYPED_TEST(ErrorOrTest, ConstructorElementConstReference) {
  typename TypeParam::value_type value = TypeParam()();
  ErrorOr<typename TypeParam::value_type> statusor(value);

  ASSERT_TRUE(statusor.ok());
  ASSERT_TRUE(statusor.status().ok());
  EXPECT_EQ(statusor.ValueOrDie(), value);
}

// Verify that ErrorOr can be constructed from an rvalue reference of an object
// of its element type.
TYPED_TEST(ErrorOrTest, ConstructorElementRValue) {
  typename TypeParam::value_type value = TypeParam()();
  typename TypeParam::value_type value_copy(value);
  ErrorOr<typename TypeParam::value_type> statusor(std::move(value));

  ASSERT_TRUE(statusor.ok());
  ASSERT_TRUE(statusor.status().ok());

  // Compare to a copy of the original value, since the original was moved.
  EXPECT_EQ(statusor.ValueOrDie(), value_copy);
}

// Verify that ErrorOr can be copy-constructed from a ErrorOr with a non-ok
// status.
TYPED_TEST(ErrorOrTest, CopyConstructorNonOkStatus) {
  ErrorOr<typename TypeParam::value_type> statusor1 = Status(kErrorCode, kErrorMessage);
  ErrorOr<typename TypeParam::value_type> statusor2(statusor1);

  EXPECT_EQ(statusor1.ok(), statusor2.ok());
  EXPECT_EQ(statusor1.status().message(), statusor2.status().message());
}

// Verify that ErrorOr can be copy-constructed from a ErrorOr with an ok
// status.
TYPED_TEST(ErrorOrTest, CopyConstructorOkStatus) {
  ErrorOr<typename TypeParam::value_type> statusor1((TypeParam()()));
  ErrorOr<typename TypeParam::value_type> statusor2(statusor1);

  EXPECT_EQ(statusor1.ok(), statusor2.ok());
  ASSERT_TRUE(statusor2.ok());
  EXPECT_EQ(statusor1.ValueOrDie(), statusor2.ValueOrDie());
}

// Verify that copy-assignment of a ErrorOr with a non-ok is working as
// expected.
TYPED_TEST(ErrorOrTest, CopyAssignmentNonOkStatus) {
  ErrorOr<typename TypeParam::value_type> statusor1(Status(kErrorCode, kErrorMessage));
  ErrorOr<typename TypeParam::value_type> statusor2((TypeParam()()));

  // Invoke the copy-assignment operator.
  statusor2 = statusor1;
  EXPECT_EQ(statusor1.ok(), statusor2.ok());
  EXPECT_EQ(statusor1.status().message(), statusor2.status().message());
}

// Verify that copy-assignment of a ErrorOr with an ok status is working as
// expected.
TYPED_TEST(ErrorOrTest, CopyAssignmentOkStatus) {
  ErrorOr<typename TypeParam::value_type> statusor1((TypeParam()()));
  ErrorOr<typename TypeParam::value_type> statusor2(Status(kErrorCode, kErrorMessage));

  // Invoke the copy-assignment operator.
  statusor2 = statusor1;
  EXPECT_EQ(statusor1.ok(), statusor2.ok());
  ASSERT_TRUE(statusor2.ok());
  EXPECT_EQ(statusor1.ValueOrDie(), statusor2.ValueOrDie());
}

// Verify that copy-assignment of a ErrorOr with a non-ok status to itself is
// properly handled.
TYPED_TEST(ErrorOrTest, CopyAssignmentSelfNonOkStatus) {
  Status status(kErrorCode, kErrorMessage);
  ErrorOr<typename TypeParam::value_type> statusor(status);
  statusor = *&statusor;

  EXPECT_FALSE(statusor.ok());
  EXPECT_EQ(statusor.status().code(), status.code());
}

// Verify that copy-assignment of a ErrorOr with an ok status to itself is
// properly handled.
TYPED_TEST(ErrorOrTest, CopyAssignmentSelfOkStatus) {
  typename TypeParam::value_type value = TypeParam()();
  ErrorOr<typename TypeParam::value_type> statusor(value);
  statusor = *&statusor;

  ASSERT_TRUE(statusor.ok());
  EXPECT_EQ(statusor.ValueOrDie(), value);
}

// Verify that ErrorOr can be move-constructed from a ErrorOr with a non-ok
// status.
TYPED_TEST(ErrorOrTest, MoveConstructorNonOkStatus) {
  Status status(kErrorCode, kErrorMessage);
  ErrorOr<typename TypeParam::value_type> statusor1(status);
  ErrorOr<typename TypeParam::value_type> statusor2(std::move(statusor1));

  // Verify that the status of the donor object was updated.
#ifndef NDEBUG
  EXPECT_FALSE(statusor1.ok());
  EXPECT_THAT(statusor1.status().code(), Eq(StatusCode::Invalid));
  EXPECT_THAT(statusor1.status().message(), ErrorOrConstants::kStatusMoveConstructorMsg);
#endif

  // Verify that the destination object contains the status previously held by
  // the donor.
  EXPECT_FALSE(statusor2.ok());
  EXPECT_EQ(statusor2.status().code(), status.code());
}

// Verify that ErrorOr can be move-constructed from a ErrorOr with an ok
// status.
TYPED_TEST(ErrorOrTest, MoveConstructorOkStatus) {
  typename TypeParam::value_type value = TypeParam()();
  ErrorOr<typename TypeParam::value_type> statusor1(value);
  ErrorOr<typename TypeParam::value_type> statusor2(std::move(statusor1));

  // Verify that the donor object was updated to contain a non-ok status.
  EXPECT_FALSE(statusor1.ok());
  EXPECT_THAT(statusor1.status().message(),
              Eq(ErrorOrConstants::kValueMoveConstructorMsg));

  // The destination object should possess the value previously held by the
  // donor.
  ASSERT_TRUE(statusor2.ok());
  EXPECT_EQ(statusor2.ValueOrDie(), value);
}

// Verify that move-assignment from a ErrorOr with a non-ok status is working
// as expected.
TYPED_TEST(ErrorOrTest, MoveAssignmentOperatorNonOkStatus) {
  Status status(kErrorCode, kErrorMessage);
  ErrorOr<typename TypeParam::value_type> statusor1(status);
  ErrorOr<typename TypeParam::value_type> statusor2((TypeParam()()));

  // Invoke the move-assignment operator.
  statusor2 = std::move(statusor1);

  // Verify that the status of the donor object was updated.
  EXPECT_FALSE(statusor1.ok());
  EXPECT_THAT(statusor1.status().message(),
              Eq(ErrorOrConstants::kStatusMoveAssignmentMsg));

  // Verify that the destination object contains the status previously held by
  // the donor.
  EXPECT_FALSE(statusor2.ok());
  EXPECT_EQ(statusor2.status().code(), status.code());
}

// Verify that move-assignment from a ErrorOr with an ok status is working as
// expected.
TYPED_TEST(ErrorOrTest, MoveAssignmentOperatorOkStatus) {
  typename TypeParam::value_type value = TypeParam()();
  ErrorOr<typename TypeParam::value_type> statusor1(value);
  ErrorOr<typename TypeParam::value_type> statusor2(Status(kErrorCode, kErrorMessage));

  // Invoke the move-assignment operator.
  statusor2 = std::move(statusor1);

  // Verify that the donor object was updated to contain a non-ok status.
  EXPECT_FALSE(statusor1.ok());
  EXPECT_THAT(statusor1.status().message(),
              Eq(ErrorOrConstants::kValueMoveAssignmentMsg));

  // The destination object should possess the value previously held by the
  // donor.
  ASSERT_TRUE(statusor2.ok());
  EXPECT_EQ(statusor2.ValueOrDie(), value);
}

// Verify that move-assignment of a ErrorOr with a non-ok status to itself is
// handled properly.
TYPED_TEST(ErrorOrTest, MoveAssignmentSelfNonOkStatus) {
  Status status(kErrorCode, kErrorMessage);
  ErrorOr<typename TypeParam::value_type> statusor(status);

  statusor = MoveErrorOr(&statusor);

  EXPECT_FALSE(statusor.ok());
  EXPECT_EQ(statusor.status().code(), status.code());
}

// Verify that move-assignment of a ErrorOr with an ok-status to itself is
// handled properly.
TYPED_TEST(ErrorOrTest, MoveAssignmentSelfOkStatus) {
  typename TypeParam::value_type value = TypeParam()();
  ErrorOr<typename TypeParam::value_type> statusor(value);

  statusor = MoveErrorOr(&statusor);

  ASSERT_TRUE(statusor.ok());
  EXPECT_EQ(statusor.ValueOrDie(), value);
}

// Tests for move-only types. These tests use std::unique_ptr<> as the
// test type, since it is valuable to support this type in the Asylo infra.
// These tests are not part of the typed test suite for the following reasons:
//   * std::unique_ptr<> cannot be used as a type in tests that expect
//   the test type to support copy operations.
//   * std::unique_ptr<> provides an equality operator that checks equality of
//   the underlying ptr. Consequently, it is difficult to generalize existing
//   tests that verify ValueOrDie() functionality using equality comparisons.

// Verify that a ErrorOr object can be constructed from a move-only type.
TEST(ErrorOrTest, InitializationMoveOnlyType) {
  std::string* str = new std::string(kStringElement);
  std::unique_ptr<std::string> value(str);
  ErrorOr<std::unique_ptr<std::string>> statusor(std::move(value));

  ASSERT_TRUE(statusor.ok());
  EXPECT_EQ(statusor.ValueOrDie().get(), str);
}

// Verify that a ErrorOr object can be move-constructed from a move-only type.
TEST(ErrorOrTest, MoveConstructorMoveOnlyType) {
  std::string* str = new std::string(kStringElement);
  std::unique_ptr<std::string> value(str);
  ErrorOr<std::unique_ptr<std::string>> statusor1(std::move(value));
  ErrorOr<std::unique_ptr<std::string>> statusor2(std::move(statusor1));

  // Verify that the donor object was updated to contain a non-ok status.
  EXPECT_FALSE(statusor1.ok());
  EXPECT_THAT(statusor1.status().message(),
              Eq(ErrorOrConstants::kValueMoveConstructorMsg));

  // The destination object should possess the value previously held by the
  // donor.
  ASSERT_TRUE(statusor2.ok());
  EXPECT_EQ(statusor2.ValueOrDie().get(), str);
}

// Verify that a ErrorOr object can be move-assigned to from a ErrorOr object
// containing a move-only type.
TEST(ErrorOrTest, MoveAssignmentMoveOnlyType) {
  std::string* str = new std::string(kStringElement);
  std::unique_ptr<std::string> value(str);
  ErrorOr<std::unique_ptr<std::string>> statusor1(std::move(value));
  ErrorOr<std::unique_ptr<std::string>> statusor2(Status(kErrorCode, kErrorMessage));

  // Invoke the move-assignment operator.
  statusor2 = std::move(statusor1);

  // Verify that the donor object was updated to contain a non-ok status.
  EXPECT_FALSE(statusor1.ok());
  EXPECT_THAT(statusor1.status().message(),
              Eq(ErrorOrConstants::kValueMoveAssignmentMsg));

  // The destination object should possess the value previously held by the
  // donor.
  ASSERT_TRUE(statusor2.ok());
  EXPECT_EQ(statusor2.ValueOrDie().get(), str);
}

// Verify that a value can be moved out of a ErrorOr object via ValueOrDie().
TEST(ErrorOrTest, ValueOrDieMovedValue) {
  std::string* str = new std::string(kStringElement);
  std::unique_ptr<std::string> value(str);
  ErrorOr<std::unique_ptr<std::string>> statusor(std::move(value));

  std::unique_ptr<std::string> moved_value = std::move(statusor).ValueOrDie();
  EXPECT_EQ(moved_value.get(), str);
  EXPECT_EQ(*moved_value, kStringElement);

  // Verify that the ErrorOr object was invalidated after the value was moved.
  EXPECT_FALSE(statusor.ok());
  EXPECT_THAT(statusor.status().message(), Eq(ErrorOrConstants::kValueOrDieMovedMsg));
}

// Verify that a ErrorOr<T> is implicitly constructible from some U, where T is
// a type which has an implicit constructor taking a const U &.
TEST(ErrorOrTest, TemplateValueCopyConstruction) {
  CopyOnlyDataType copy_only(kIntElement);
  ErrorOr<ImplicitlyCopyConvertible> statusor(copy_only);

  EXPECT_TRUE(statusor.ok());
  EXPECT_EQ(statusor.ValueOrDie().copy_only.data, kIntElement);
}

// Verify that a ErrorOr<T> is implicitly constructible from some U, where T is
// a type which has an implicit constructor taking a U &&.
TEST(ErrorOrTest, TemplateValueMoveConstruction) {
  MoveOnlyDataType move_only(kIntElement);
  ErrorOr<ImplicitlyMoveConvertible> statusor(std::move(move_only));

  EXPECT_TRUE(statusor.ok());
  EXPECT_EQ(*statusor.ValueOrDie().move_only.data, kIntElement);
}

// Verify that a ErrorOr<U> is assignable to a ErrorOr<T>, where T
// is a type which has an implicit constructor taking a const U &.
TEST(ErrorOrTest, TemplateCopyAssign) {
  CopyOnlyDataType copy_only(kIntElement);
  ErrorOr<CopyOnlyDataType> statusor(copy_only);

  ErrorOr<ImplicitlyCopyConvertible> statusor2 = statusor;

  EXPECT_TRUE(statusor.ok());
  EXPECT_EQ(statusor.ValueOrDie().data, kIntElement);
  EXPECT_TRUE(statusor2.ok());
  EXPECT_EQ(statusor2.ValueOrDie().copy_only.data, kIntElement);
}

// Verify that a ErrorOr<U> is assignable to a ErrorOr<T>, where T is a type
// which has an implicit constructor taking a U &&.
TEST(ErrorOrTest, TemplateMoveAssign) {
  MoveOnlyDataType move_only(kIntElement);
  ErrorOr<MoveOnlyDataType> statusor(std::move(move_only));

  ErrorOr<ImplicitlyMoveConvertible> statusor2 = std::move(statusor);

  EXPECT_TRUE(statusor2.ok());
  EXPECT_EQ(*statusor2.ValueOrDie().move_only.data, kIntElement);

  //  NOLINTNEXTLINE use after move.
  EXPECT_FALSE(statusor.ok());
  //  NOLINTNEXTLINE use after move.
  EXPECT_THAT(statusor.status().message(),
              Eq(ErrorOrConstants::kValueMoveConstructorMsg));
}

// Verify that a ErrorOr<U> is constructible from a ErrorOr<T>, where T is a
// type which has an implicit constructor taking a const U &.
TEST(ErrorOrTest, TemplateCopyConstruct) {
  CopyOnlyDataType copy_only(kIntElement);
  ErrorOr<CopyOnlyDataType> statusor(copy_only);
  ErrorOr<ImplicitlyCopyConvertible> statusor2(statusor);

  EXPECT_TRUE(statusor.ok());
  EXPECT_EQ(statusor.ValueOrDie().data, kIntElement);
  EXPECT_TRUE(statusor2.ok());
  EXPECT_EQ(statusor2.ValueOrDie().copy_only.data, kIntElement);
}

// Verify that a ErrorOr<U> is constructible from a ErrorOr<T>, where T is a
// type which has an implicit constructor taking a U &&.
TEST(ErrorOrTest, TemplateMoveConstruct) {
  MoveOnlyDataType move_only(kIntElement);
  ErrorOr<MoveOnlyDataType> statusor(std::move(move_only));
  ErrorOr<ImplicitlyMoveConvertible> statusor2(std::move(statusor));

  EXPECT_TRUE(statusor2.ok());
  EXPECT_EQ(*statusor2.ValueOrDie().move_only.data, kIntElement);

  //  NOLINTNEXTLINE use after move.
  EXPECT_FALSE(statusor.ok());
  //  NOLINTNEXTLINE use after move.
  EXPECT_THAT(statusor.status().message(),
              Eq(ErrorOrConstants::kValueMoveConstructorMsg));
}

}  // namespace
}  // namespace arrow
