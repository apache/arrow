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

#include "arrow/testing/gtest_compat.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/functional.h"

namespace arrow {

namespace {

using ::testing::Eq;

StatusCode kErrorCode = StatusCode::Invalid;
constexpr const char* kErrorMessage = "Invalid argument";

const int kIntElement = 42;
constexpr const char* kStringElement =
    "The Answer to the Ultimate Question of Life, the Universe, and Everything";

// A data type without a default constructor.
struct Foo {
  int bar;
  std::string baz;

  explicit Foo(int value) : bar(value), baz(kStringElement) {}

  bool operator==(const Foo& other) const {
    return (bar == other.bar) && (baz == other.baz);
  }
};

// A data type with only copy constructors.
struct CopyOnlyDataType {
  explicit CopyOnlyDataType(int x) : data(x) {}

  CopyOnlyDataType(const CopyOnlyDataType& other) = default;
  CopyOnlyDataType& operator=(const CopyOnlyDataType& other) = default;

  int data;
};

struct ImplicitlyCopyConvertible {
  ImplicitlyCopyConvertible(const CopyOnlyDataType& co)  // NOLINT runtime/explicit
      : copy_only(co) {}

  CopyOnlyDataType copy_only;
};

// A data type with only move constructors.
struct MoveOnlyDataType {
  explicit MoveOnlyDataType(int x) : data(new int(x)) {}

  MoveOnlyDataType(const MoveOnlyDataType& other) = delete;
  MoveOnlyDataType& operator=(const MoveOnlyDataType& other) = delete;

  MoveOnlyDataType(MoveOnlyDataType&& other) { MoveFrom(&other); }
  MoveOnlyDataType& operator=(MoveOnlyDataType&& other) {
    MoveFrom(&other);
    return *this;
  }

  ~MoveOnlyDataType() { Destroy(); }

  void Destroy() {
    if (data != nullptr) {
      delete data;
      data = nullptr;
    }
  }

  void MoveFrom(MoveOnlyDataType* other) {
    Destroy();
    data = other->data;
    other->data = nullptr;
  }

  int* data = nullptr;
};

struct ImplicitlyMoveConvertible {
  ImplicitlyMoveConvertible(MoveOnlyDataType&& mo)  // NOLINT runtime/explicit
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

  bool operator==(const HeapAllocatedObject& other) const {
    return *value == *other.value;
  }
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

// Returns an rvalue reference to the Result<T> object pointed to by
// |result|.
template <class T>
Result<T>&& MoveResult(Result<T>* result) {
  return std::move(*result);
}

// A test fixture is required for typed tests.
template <typename T>
class ResultTest : public ::testing::Test {};

using TestTypes = ::testing::Types<IntCtor, FooCtor, StringCtor, StringVectorCtor,
                                   HeapAllocatedObjectCtor>;

TYPED_TEST_SUITE(ResultTest, TestTypes);

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
  typename TypeParam::value_type value = TypeParam()();
  Result<typename TypeParam::value_type> result1(value);
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
  typename TypeParam::value_type value = TypeParam()();
  Result<typename TypeParam::value_type> result2(value);

  // Invoke the copy-assignment operator.
  result2 = result1;
  EXPECT_EQ(result1.ok(), result2.ok());
  EXPECT_EQ(result1.status().message(), result2.status().message());
}

// Verify that copy-assignment of a Result with an ok status is working as
// expected.
TYPED_TEST(ResultTest, CopyAssignmentOkStatus) {
  typename TypeParam::value_type value = TypeParam()();
  Result<typename TypeParam::value_type> result1(value);
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
  typename TypeParam::value_type value = TypeParam()();
  Result<typename TypeParam::value_type> result2(value);

  // Invoke the move-assignment operator.
  result2 = std::move(result1);

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
  std::unique_ptr<std::string> value(new std::string(kStringElement));
  auto str = value.get();
  Result<std::unique_ptr<std::string>> result(std::move(value));

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().get(), str);
}

// Verify that a Result object can be move-constructed from a move-only type.
TEST(ResultTest, MoveConstructorMoveOnlyType) {
  std::unique_ptr<std::string> value(new std::string(kStringElement));
  auto str = value.get();
  Result<std::unique_ptr<std::string>> result1(std::move(value));
  Result<std::unique_ptr<std::string>> result2(std::move(result1));

  // The destination object should possess the value previously held by the
  // donor.
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(result2.ValueOrDie().get(), str);
}

// Verify that a Result object can be move-assigned to from a Result object
// containing a move-only type.
TEST(ResultTest, MoveAssignmentMoveOnlyType) {
  std::unique_ptr<std::string> value(new std::string(kStringElement));
  auto str = value.get();
  Result<std::unique_ptr<std::string>> result1(std::move(value));
  Result<std::unique_ptr<std::string>> result2(Status(kErrorCode, kErrorMessage));

  // Invoke the move-assignment operator.
  result2 = std::move(result1);

  // The destination object should possess the value previously held by the
  // donor.
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(result2.ValueOrDie().get(), str);
}

// Verify that a value can be moved out of a Result object via ValueOrDie().
TEST(ResultTest, ValueOrDieMovedValue) {
  std::unique_ptr<std::string> value(new std::string(kStringElement));
  auto str = value.get();
  Result<std::unique_ptr<std::string>> result(std::move(value));

  std::unique_ptr<std::string> moved_value = std::move(result).ValueOrDie();
  EXPECT_EQ(moved_value.get(), str);
  EXPECT_EQ(*moved_value, kStringElement);
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

// Verify that an error rvalue Result<T> allows access if an alternative is provided
TEST(ResultTest, ErrorRvalueValueOrAlternative) {
  Result<MoveOnlyDataType> result = Status::Invalid("");

  EXPECT_FALSE(result.ok());
  EXPECT_EQ(*std::move(result).ValueOr(MoveOnlyDataType{kIntElement}).data, kIntElement);
}

// Verify that an ok rvalue Result<T> will ignore a provided alternative
TEST(ResultTest, OkRvalueValueOrAlternative) {
  Result<MoveOnlyDataType> result = MoveOnlyDataType{kIntElement};

  EXPECT_TRUE(result.ok());
  EXPECT_EQ(*std::move(result).ValueOr(MoveOnlyDataType{kIntElement - 1}).data,
            kIntElement);
}

// Verify that an error rvalue Result<T> allows access if an alternative factory is
// provided
TEST(ResultTest, ErrorRvalueValueOrGeneratedAlternative) {
  Result<MoveOnlyDataType> result = Status::Invalid("");

  EXPECT_FALSE(result.ok());
  auto out = std::move(result).ValueOrElse([] { return MoveOnlyDataType{kIntElement}; });
  EXPECT_EQ(*out.data, kIntElement);
}

// Verify that an ok rvalue Result<T> allows access if an alternative factory is provided
TEST(ResultTest, OkRvalueValueOrGeneratedAlternative) {
  Result<MoveOnlyDataType> result = MoveOnlyDataType{kIntElement};

  EXPECT_TRUE(result.ok());
  auto out =
      std::move(result).ValueOrElse([] { return MoveOnlyDataType{kIntElement - 1}; });
  EXPECT_EQ(*out.data, kIntElement);
}

// Verify that a Result<T> can be unpacked to T
TEST(ResultTest, StatusReturnAdapterCopyValue) {
  Result<CopyOnlyDataType> result(CopyOnlyDataType{kIntElement});
  CopyOnlyDataType copy_only{0};

  EXPECT_TRUE(std::move(result).Value(&copy_only).ok());
  EXPECT_EQ(copy_only.data, kIntElement);
}

// Verify that a Result<T> can be unpacked to some U, where U is
// a type which has a constructor taking a const T &.
TEST(ResultTest, StatusReturnAdapterCopyAndConvertValue) {
  Result<CopyOnlyDataType> result(CopyOnlyDataType{kIntElement});
  ImplicitlyCopyConvertible implicitly_convertible(CopyOnlyDataType{0});

  EXPECT_TRUE(std::move(result).Value(&implicitly_convertible).ok());
  EXPECT_EQ(implicitly_convertible.copy_only.data, kIntElement);
}

// Verify that a Result<T> can be unpacked to T
TEST(ResultTest, StatusReturnAdapterMoveValue) {
  {
    Result<MoveOnlyDataType> result(MoveOnlyDataType{kIntElement});
    MoveOnlyDataType move_only{0};

    EXPECT_TRUE(std::move(result).Value(&move_only).ok());
    EXPECT_EQ(*move_only.data, kIntElement);
  }
  {
    Result<MoveOnlyDataType> result(MoveOnlyDataType{kIntElement});
    auto move_only = std::move(result).ValueOrDie();
    EXPECT_EQ(*move_only.data, kIntElement);
  }
  {
    Result<MoveOnlyDataType> result(MoveOnlyDataType{kIntElement});
    auto move_only = *std::move(result);
    EXPECT_EQ(*move_only.data, kIntElement);
  }
}

// Verify that a Result<T> can be unpacked to some U, where U is
// a type which has a constructor taking a T &&.
TEST(ResultTest, StatusReturnAdapterMoveAndConvertValue) {
  Result<MoveOnlyDataType> result(MoveOnlyDataType{kIntElement});
  ImplicitlyMoveConvertible implicitly_convertible(MoveOnlyDataType{0});

  EXPECT_TRUE(std::move(result).Value(&implicitly_convertible).ok());
  EXPECT_EQ(*implicitly_convertible.move_only.data, kIntElement);
}

// Verify that a Result<T> can be queried for a stored value or an alternative.
TEST(ResultTest, ValueOrAlternative) {
  EXPECT_EQ(Result<MoveOnlyDataType>(MoveOnlyDataType{kIntElement})
                .ValueOr(MoveOnlyDataType{0})
                .data[0],
            kIntElement);

  EXPECT_EQ(
      Result<MoveOnlyDataType>(Status::Invalid("")).ValueOr(MoveOnlyDataType{0}).data[0],
      0);
}

TEST(ResultTest, MapFunctionToConstValue) {
  static auto error = Status::Invalid("some error message");

  const Result<MoveOnlyDataType> result(MoveOnlyDataType{kIntElement});

  auto const_mapped =
      result.Map([](const MoveOnlyDataType& m) -> Result<int> { return *m.data; });
  EXPECT_TRUE(const_mapped.ok());
  EXPECT_EQ(const_mapped.ValueOrDie(), kIntElement);

  auto const_error =
      result.Map([](const MoveOnlyDataType& m) -> Result<int> { return error; });
  EXPECT_FALSE(const_error.ok());
  EXPECT_EQ(const_error.status(), error);
}

TEST(ResultTest, MapFunctionToRrefValue) {
  static auto error = Status::Invalid("some error message");

  auto result = [] { return Result<MoveOnlyDataType>(MoveOnlyDataType{kIntElement}); };

  auto move_mapped =
      result().Map([](MoveOnlyDataType m) -> Result<int> { return std::move(*m.data); });
  EXPECT_TRUE(move_mapped.ok());
  EXPECT_EQ(move_mapped.ValueOrDie(), kIntElement);

  auto move_error = result().Map([](MoveOnlyDataType m) -> Result<int> { return error; });
  EXPECT_FALSE(move_error.ok());
  EXPECT_EQ(move_error.status(), error);
}

TEST(ResultTest, MapFunctionToConstError) {
  static auto error = Status::Invalid("some error message");
  static auto other_error = Status::Invalid("some other error message");

  const Result<MoveOnlyDataType> result(error);

  auto const_mapped =
      result.Map([](const MoveOnlyDataType& m) -> Result<int> { return *m.data; });
  EXPECT_FALSE(const_mapped.ok());
  EXPECT_EQ(const_mapped.status(), error);  // error is *not* replaced by a value

  auto const_error =
      result.Map([](const MoveOnlyDataType& m) -> Result<int> { return other_error; });
  EXPECT_FALSE(const_error.ok());
  EXPECT_EQ(const_error.status(), error);  // error is *not* replaced by other_error
}

TEST(ResultTest, MapFunctionToRrefError) {
  static auto error = Status::Invalid("some error message");
  static auto other_error = Status::Invalid("some other error message");

  auto result = [] { return Result<MoveOnlyDataType>(error); };

  auto move_mapped =
      result().Map([](MoveOnlyDataType m) -> Result<int> { return std::move(*m.data); });
  EXPECT_FALSE(move_mapped.ok());
  EXPECT_EQ(move_mapped.status(), error);  // error is *not* replaced by a value

  auto move_error =
      result().Map([](MoveOnlyDataType m) -> Result<int> { return other_error; });
  EXPECT_FALSE(move_error.ok());
  EXPECT_EQ(move_error.status(), error);  // error is *not* replaced by other_error
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
}

TEST(ResultTest, Equality) {
  EXPECT_EQ(Result<int>(), Result<int>());
  EXPECT_EQ(Result<int>(3), Result<int>(3));
  EXPECT_EQ(Result<int>(Status::Invalid("error")), Result<int>(Status::Invalid("error")));

  EXPECT_NE(Result<int>(), Result<int>(3));
  EXPECT_NE(Result<int>(Status::Invalid("error")), Result<int>(3));
  EXPECT_NE(Result<int>(3333), Result<int>(0));
  EXPECT_NE(Result<int>(Status::Invalid("error")),
            Result<int>(Status::Invalid("other error")));

  {
    Result<int> moved_from(3);
    auto moved_to = std::move(moved_from);
    EXPECT_EQ(moved_to, Result<int>(3));
  }
  {
    Result<std::vector<int>> a, b, c;
    a = std::vector<int>{1, 2, 3, 4, 5};
    b = std::vector<int>{1, 2, 3, 4, 5};
    c = std::vector<int>{1, 2, 3, 4};
    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);

    c = std::move(b);
    EXPECT_EQ(a, c);
    EXPECT_EQ(c.ValueOrDie(), (std::vector<int>{1, 2, 3, 4, 5}));
    EXPECT_NE(a, b);  // b's value was moved
  }
}

TEST(ResultTest, ViewAsStatus) {
  Result<int> ok(3);
  Result<int> err(Status::Invalid("error"));

  auto ViewAsStatus = [](const void* ptr) { return static_cast<const Status*>(ptr); };

  EXPECT_EQ(ViewAsStatus(&ok), &ok.status());
  EXPECT_EQ(ViewAsStatus(&err), &err.status());
}

TEST(ResultTest, MatcherExamples) {
  EXPECT_THAT(Result<int>(Status::Invalid("arbitrary error")),
              Raises(StatusCode::Invalid));

  EXPECT_THAT(Result<int>(Status::Invalid("arbitrary error")),
              Raises(StatusCode::Invalid, testing::HasSubstr("arbitrary")));

  // message doesn't match, so no match
  EXPECT_THAT(
      Result<int>(Status::Invalid("arbitrary error")),
      testing::Not(Raises(StatusCode::Invalid, testing::HasSubstr("reasonable"))));

  // different error code, so no match
  EXPECT_THAT(Result<int>(Status::TypeError("arbitrary error")),
              testing::Not(Raises(StatusCode::Invalid)));

  // not an error, so no match
  EXPECT_THAT(Result<int>(333), testing::Not(Raises(StatusCode::Invalid)));

  EXPECT_THAT(Result<std::string>("hello world"),
              ResultWith(testing::HasSubstr("hello")));

  EXPECT_THAT(Result<std::string>(Status::Invalid("XXX")),
              testing::Not(ResultWith(testing::HasSubstr("hello"))));

  // holds a value, but that value doesn't match the given pattern
  EXPECT_THAT(Result<std::string>("foo bar"),
              testing::Not(ResultWith(testing::HasSubstr("hello"))));
}

TEST(ResultTest, MatcherDescriptions) {
  testing::Matcher<Result<std::string>> matcher = ResultWith(testing::HasSubstr("hello"));

  {
    std::stringstream ss;
    matcher.DescribeTo(&ss);
    EXPECT_THAT(ss.str(), testing::StrEq("value has substring \"hello\""));
  }

  {
    std::stringstream ss;
    matcher.DescribeNegationTo(&ss);
    EXPECT_THAT(ss.str(), testing::StrEq("value has no substring \"hello\""));
  }
}

TEST(ResultTest, MatcherExplanations) {
  testing::Matcher<Result<std::string>> matcher = ResultWith(testing::HasSubstr("hello"));

  {
    testing::StringMatchResultListener listener;
    EXPECT_TRUE(matcher.MatchAndExplain(Result<std::string>("hello world"), &listener));
    EXPECT_THAT(listener.str(), testing::StrEq("whose value \"hello world\" matches"));
  }

  {
    testing::StringMatchResultListener listener;
    EXPECT_FALSE(matcher.MatchAndExplain(Result<std::string>("foo bar"), &listener));
    EXPECT_THAT(listener.str(), testing::StrEq("whose value \"foo bar\" doesn't match"));
  }

  {
    testing::StringMatchResultListener listener;
    EXPECT_FALSE(matcher.MatchAndExplain(Status::TypeError("XXX"), &listener));
    EXPECT_THAT(listener.str(),
                testing::StrEq("whose error \"Type error: XXX\" doesn't match"));
  }
}

TEST(ResultTest, ValueOrGeneratedMoveOnlyGenerator) {
  Result<MoveOnlyDataType> result = Status::Invalid("");
  internal::FnOnce<MoveOnlyDataType()> alternative_generator = [] {
    return MoveOnlyDataType{kIntElement};
  };
  auto out = std::move(result).ValueOrElse(std::move(alternative_generator));
  EXPECT_EQ(*out.data, kIntElement);
}

}  // namespace
}  // namespace arrow
