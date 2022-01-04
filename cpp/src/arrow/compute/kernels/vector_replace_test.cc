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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace compute {

using arrow::internal::checked_pointer_cast;

template <typename T>
class TestReplaceKernel : public ::testing::Test {
 protected:
  virtual std::shared_ptr<DataType> type() = 0;

  using ReplaceFunction = std::function<Result<Datum>(const Datum&, const Datum&,
                                                      const Datum&, ExecContext*)>;

  Datum mask_scalar(bool value) { return Datum(std::make_shared<BooleanScalar>(value)); }

  Datum null_mask_scalar() {
    auto scalar = std::make_shared<BooleanScalar>(true);
    scalar->is_valid = false;
    return Datum(std::move(scalar));
  }

  Datum scalar(const std::string& json) { return ScalarFromJSON(type(), json); }

  std::shared_ptr<Array> array(const std::string& value) {
    return ArrayFromJSON(type(), value);
  }

  std::shared_ptr<Array> mask(const std::string& value) {
    return ArrayFromJSON(boolean(), value);
  }

  Status AssertRaises(ReplaceFunction func, const std::shared_ptr<Array>& array,
                      const Datum& mask, const std::shared_ptr<Array>& replacements) {
    auto result = func(array, mask, replacements, nullptr);
    EXPECT_FALSE(result.ok());
    return result.status();
  }

  void Assert(ReplaceFunction func, const std::shared_ptr<Array>& array,
              const Datum& mask, Datum replacements,
              const std::shared_ptr<Array>& expected) {
    SCOPED_TRACE("Replacements: " + (replacements.is_array()
                                         ? replacements.make_array()->ToString()
                                         : replacements.scalar()->ToString()));
    SCOPED_TRACE("Mask: " + (mask.is_array() ? mask.make_array()->ToString()
                                             : mask.scalar()->ToString()));
    SCOPED_TRACE("Array: " + array->ToString());

    ASSERT_OK_AND_ASSIGN(auto actual, func(array, mask, replacements, nullptr));
    ASSERT_TRUE(actual.is_array());
    ASSERT_OK(actual.make_array()->ValidateFull());

    AssertArraysApproxEqual(*expected, *actual.make_array(), /*verbose=*/true);
  }

  std::shared_ptr<Array> NaiveImpl(
      const typename TypeTraits<T>::ArrayType& array, const BooleanArray& mask,
      const typename TypeTraits<T>::ArrayType& replacements) {
    auto length = array.length();
    auto builder = arrow::internal::make_unique<typename TypeTraits<T>::BuilderType>(
        default_type_instance<T>(), default_memory_pool());
    int64_t replacement_offset = 0;
    for (int64_t i = 0; i < length; ++i) {
      if (mask.IsValid(i)) {
        if (mask.Value(i)) {
          if (replacements.IsValid(replacement_offset)) {
            ARROW_EXPECT_OK(builder->Append(replacements.Value(replacement_offset++)));
          } else {
            ARROW_EXPECT_OK(builder->AppendNull());
            replacement_offset++;
          }
        } else {
          if (array.IsValid(i)) {
            ARROW_EXPECT_OK(builder->Append(array.Value(i)));
          } else {
            ARROW_EXPECT_OK(builder->AppendNull());
          }
        }
      } else {
        ARROW_EXPECT_OK(builder->AppendNull());
      }
    }
    EXPECT_OK_AND_ASSIGN(auto expected, builder->Finish());
    return expected;
  }
};

template <typename T>
class TestReplaceNumeric : public TestReplaceKernel<T> {
 protected:
  std::shared_ptr<DataType> type() override { return default_type_instance<T>(); }
};

class TestReplaceBoolean : public TestReplaceKernel<BooleanType> {
 protected:
  std::shared_ptr<DataType> type() override {
    return TypeTraits<BooleanType>::type_singleton();
  }
};

class TestReplaceFixedSizeBinary : public TestReplaceKernel<FixedSizeBinaryType> {
 protected:
  std::shared_ptr<DataType> type() override { return fixed_size_binary(3); }
};

template <typename T>
class TestReplaceDecimal : public TestReplaceKernel<T> {
 protected:
  std::shared_ptr<DataType> type() override { return default_type_instance<T>(); }
};

class TestReplaceDayTimeInterval : public TestReplaceKernel<DayTimeIntervalType> {
 protected:
  std::shared_ptr<DataType> type() override {
    return TypeTraits<DayTimeIntervalType>::type_singleton();
  }
};

class TestReplaceMonthDayNanoInterval
    : public TestReplaceKernel<MonthDayNanoIntervalType> {
 protected:
  std::shared_ptr<DataType> type() override {
    return TypeTraits<MonthDayNanoIntervalType>::type_singleton();
  }
};

template <typename T>
class TestReplaceBinary : public TestReplaceKernel<T> {
 protected:
  std::shared_ptr<DataType> type() override { return default_type_instance<T>(); }
};

using NumericBasedTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, Date32Type, Date64Type,
                     Time32Type, Time64Type, TimestampType, MonthIntervalType>;

TYPED_TEST_SUITE(TestReplaceNumeric, NumericBasedTypes);
TYPED_TEST_SUITE(TestReplaceDecimal, DecimalArrowTypes);
TYPED_TEST_SUITE(TestReplaceBinary, BaseBinaryArrowTypes);

TYPED_TEST(TestReplaceNumeric, ReplaceWithMask) {
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(false),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(true),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->null_mask_scalar(),
               this->array("[]"), this->array("[]"));

  this->Assert(ReplaceWithMask, this->array("[1]"), this->mask_scalar(false),
               this->array("[]"), this->array("[1]"));
  this->Assert(ReplaceWithMask, this->array("[1]"), this->mask_scalar(true),
               this->array("[0]"), this->array("[0]"));
  this->Assert(ReplaceWithMask, this->array("[1]"), this->mask_scalar(true),
               this->array("[2, 0]"), this->array("[2]"));
  this->Assert(ReplaceWithMask, this->array("[1]"), this->null_mask_scalar(),
               this->array("[]"), this->array("[null]"));

  this->Assert(ReplaceWithMask, this->array("[0, 0]"), this->mask_scalar(false),
               this->scalar("1"), this->array("[0, 0]"));
  this->Assert(ReplaceWithMask, this->array("[0, 0]"), this->mask_scalar(true),
               this->scalar("1"), this->array("[1, 1]"));
  this->Assert(ReplaceWithMask, this->array("[0, 0]"), this->mask_scalar(true),
               this->scalar("null"), this->array("[null, null]"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"), this->array("[]"),
               this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[0, 1, 2, 3]"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array("[0, 1, 2, 3]"));
  this->Assert(ReplaceWithMask, this->array("[0, 1, 2, 3]"),
               this->mask("[true, true, true, true]"), this->array("[10, 11, 12, 13]"),
               this->array("[10, 11, 12, 13]"));
  this->Assert(ReplaceWithMask, this->array("[0, 1, 2, 3]"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array("[null, null, null, null]"));
  this->Assert(ReplaceWithMask, this->array("[0, 1, 2, null]"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array("[0, 1, 2, null]"));
  this->Assert(ReplaceWithMask, this->array("[0, 1, 2, null]"),
               this->mask("[true, true, true, true]"), this->array("[10, 11, 12, 13]"),
               this->array("[10, 11, 12, 13]"));
  this->Assert(ReplaceWithMask, this->array("[0, 1, 2, null]"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array("[null, null, null, null]"));
  this->Assert(ReplaceWithMask, this->array("[0, 1, 2, 3, 4, 5]"),
               this->mask("[false, false, null, null, true, true]"),
               this->array("[10, null]"), this->array("[0, 1, null, null, 10, null]"));
  this->Assert(ReplaceWithMask, this->array("[null, null, null, null, null, null]"),
               this->mask("[false, false, null, null, true, true]"),
               this->array("[10, null]"),
               this->array("[null, null, null, null, 10, null]"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"), this->scalar("1"),
               this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[0, 1]"), this->mask("[true, true]"),
               this->scalar("10"), this->array("[10, 10]"));
  this->Assert(ReplaceWithMask, this->array("[0, 1]"), this->mask("[true, true]"),
               this->scalar("null"), this->array("[null, null]"));
  this->Assert(ReplaceWithMask, this->array("[0, 1, 2]"),
               this->mask("[false, null, true]"), this->scalar("10"),
               this->array("[0, null, 10]"));
}

TYPED_TEST(TestReplaceNumeric, ReplaceWithMaskForNullValuesAndMaskEnabled) {
  this->Assert(ReplaceWithMask, this->array("[1, null, 1]"),
               this->mask("[false, true, false]"), this->array("[7]"),
               this->array("[1, 7, 1]"));
  this->Assert(ReplaceWithMask, this->array("[1, null, 1, 7]"),
               this->mask("[false, true, false, true]"), this->array("[7, 20]"),
               this->array("[1, 7, 1, 20]"));
  this->Assert(ReplaceWithMask, this->array("[1, 2, 3, 4]"),
               this->mask("[false, true, false, true]"), this->array("[null, null]"),
               this->array("[1, null, 3, null]"));
  this->Assert(ReplaceWithMask, this->array("[null, 2, 3, 4]"),
               this->mask("[true, true, false, true]"), this->array("[1, null, null]"),
               this->array("[1, null, 3, null]"));
  this->Assert(ReplaceWithMask, this->array("[1, null, 1]"),
               this->mask("[false, true, false]"), this->scalar("null"),
               this->array("[1, null, 1]"));
  this->Assert(ReplaceWithMask, this->array("[1, null, 1]"),
               this->mask("[true, true, true]"), this->array("[7, 7, 7]"),
               this->array("[7, 7, 7]"));
  this->Assert(ReplaceWithMask, this->array("[1, null, 1]"),
               this->mask("[true, true, true]"), this->array("[null, null, null]"),
               this->array("[null, null, null]"));
  this->Assert(ReplaceWithMask, this->array("[1, null, 1]"),
               this->mask("[false, true, false]"), this->scalar("null"),
               this->array("[1, null, 1]"));
  this->Assert(ReplaceWithMask, this->array("[1, null, 1]"),
               this->mask("[true, true, true]"), this->scalar("null"),
               this->array("[null, null, null]"));
  this->Assert(ReplaceWithMask, this->array("[null, null]"), this->mask("[true, true]"),
               this->array("[1, 1]"), this->array("[1, 1]"));
}

TYPED_TEST(TestReplaceNumeric, ReplaceWithMaskRandom) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  using CType = typename TypeTraits<TypeParam>::CType;
  auto ty = this->type();

  random::RandomArrayGenerator rand(/*seed=*/0);
  const int64_t length = 1023;
  std::vector<std::string> values = {"0.01", "0"};
  // Clamp the range because date/time types don't print well with extreme values
  values.push_back(std::to_string(static_cast<CType>(std::min<double>(
      16384.0, static_cast<double>(std::numeric_limits<CType>::max())))));
  auto options = key_value_metadata({"null_probability", "min", "max"}, values);
  auto array =
      checked_pointer_cast<ArrayType>(rand.ArrayOf(*field("a", ty, options), length));
  auto mask = checked_pointer_cast<BooleanArray>(
      rand.ArrayOf(boolean(), length, /*null_probability=*/0.01));
  const int64_t num_replacements = std::count_if(
      mask->begin(), mask->end(),
      [](util::optional<bool> value) { return value.has_value() && *value; });
  auto replacements = checked_pointer_cast<ArrayType>(
      rand.ArrayOf(*field("a", ty, options), num_replacements));
  auto expected = this->NaiveImpl(*array, *mask, *replacements);

  this->Assert(ReplaceWithMask, array, mask, replacements, expected);
  for (int64_t slice = 1; slice <= 16; slice++) {
    auto sliced_array = checked_pointer_cast<ArrayType>(array->Slice(slice, 15));
    auto sliced_mask = checked_pointer_cast<BooleanArray>(mask->Slice(slice, 15));
    auto new_expected = this->NaiveImpl(*sliced_array, *sliced_mask, *replacements);
    this->Assert(ReplaceWithMask, sliced_array, sliced_mask, replacements, new_expected);
  }
}

TYPED_TEST(TestReplaceNumeric, ReplaceWithMaskErrors) {
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Replacement array must be of appropriate length (expected 2 "
                           "items but got 1 items)"),
      this->AssertRaises(ReplaceWithMask, this->array("[1, 2]"),
                         this->mask("[true, true]"), this->array("[0]")));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Replacement array must be of appropriate length (expected 1 "
                           "items but got 0 items)"),
      this->AssertRaises(ReplaceWithMask, this->array("[1, 2]"),
                         this->mask("[true, null]"), this->array("[]")));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Mask must be of same length as array (expected 2 "
                           "items but got 0 items)"),
      this->AssertRaises(ReplaceWithMask, this->array("[1, 2]"), this->mask("[]"),
                         this->array("[]")));
}

TEST_F(TestReplaceBoolean, ReplaceWithMask) {
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(false),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(true),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->null_mask_scalar(),
               this->array("[]"), this->array("[]"));

  this->Assert(ReplaceWithMask, this->array("[true]"), this->mask_scalar(false),
               this->array("[]"), this->array("[true]"));
  this->Assert(ReplaceWithMask, this->array("[true]"), this->mask_scalar(true),
               this->array("[false]"), this->array("[false]"));
  this->Assert(ReplaceWithMask, this->array("[true]"), this->null_mask_scalar(),
               this->array("[]"), this->array("[null]"));

  this->Assert(ReplaceWithMask, this->array("[false, false]"), this->mask_scalar(false),
               this->scalar("true"), this->array("[false, false]"));
  this->Assert(ReplaceWithMask, this->array("[false, false]"), this->mask_scalar(true),
               this->scalar("true"), this->array("[true, true]"));
  this->Assert(ReplaceWithMask, this->array("[false, false]"), this->mask_scalar(true),
               this->scalar("null"), this->array("[null, null]"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"), this->array("[]"),
               this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[true, true, true, true]"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array("[true, true, true, true]"));
  this->Assert(ReplaceWithMask, this->array("[true, true, true, true]"),
               this->mask("[true, true, true, true]"),
               this->array("[false, false, false, false]"),
               this->array("[false, false, false, false]"));
  this->Assert(ReplaceWithMask, this->array("[true, true, true, true]"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array("[null, null, null, null]"));
  this->Assert(ReplaceWithMask, this->array("[true, true, true, null]"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array("[true, true, true, null]"));
  this->Assert(ReplaceWithMask, this->array("[true, true, true, null]"),
               this->mask("[true, true, true, true]"),
               this->array("[false, false, false, false]"),
               this->array("[false, false, false, false]"));
  this->Assert(ReplaceWithMask, this->array("[true, true, true, null]"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array("[null, null, null, null]"));
  this->Assert(ReplaceWithMask, this->array("[true, true, true, true, true, true]"),
               this->mask("[false, false, null, null, true, true]"),
               this->array("[false, null]"),
               this->array("[true, true, null, null, false, null]"));
  this->Assert(ReplaceWithMask, this->array("[null, null, null, null, null, null]"),
               this->mask("[false, false, null, null, true, true]"),
               this->array("[false, null]"),
               this->array("[null, null, null, null, false, null]"));
  this->Assert(ReplaceWithMask, this->array("[true, null, true]"),
               this->mask("[false, true, false]"), this->array("[true]"),
               this->array("[true, true, true]"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"), this->scalar("true"),
               this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[null, false, true]"),
               this->mask("[true, false, false]"), this->scalar("false"),
               this->array("[false, false, true]"));
  this->Assert(ReplaceWithMask, this->array("[false, false]"), this->mask("[true, true]"),
               this->scalar("true"), this->array("[true, true]"));
  this->Assert(ReplaceWithMask, this->array("[false, false]"), this->mask("[true, true]"),
               this->scalar("null"), this->array("[null, null]"));
  this->Assert(ReplaceWithMask, this->array("[false, false, false]"),
               this->mask("[false, null, true]"), this->scalar("true"),
               this->array("[false, null, true]"));
  this->Assert(ReplaceWithMask, this->array("[null, null]"), this->mask("[true, true]"),
               this->array("[true, true]"), this->array("[true, true]"));
}

TEST_F(TestReplaceBoolean, ReplaceWithMaskErrors) {
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Replacement array must be of appropriate length (expected 2 "
                           "items but got 1 items)"),
      this->AssertRaises(ReplaceWithMask, this->array("[true, true]"),
                         this->mask("[true, true]"), this->array("[false]")));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Replacement array must be of appropriate length (expected 1 "
                           "items but got 0 items)"),
      this->AssertRaises(ReplaceWithMask, this->array("[true, true]"),
                         this->mask("[true, null]"), this->array("[]")));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Mask must be of same length as array (expected 2 "
                           "items but got 0 items)"),
      this->AssertRaises(ReplaceWithMask, this->array("[true, true]"), this->mask("[]"),
                         this->array("[]")));
}

TEST_F(TestReplaceFixedSizeBinary, ReplaceWithMask) {
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(false),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(true),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->null_mask_scalar(),
               this->array("[]"), this->array("[]"));

  this->Assert(ReplaceWithMask, this->array(R"(["foo"])"), this->mask_scalar(false),
               this->array("[]"), this->array(R"(["foo"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["foo"])"), this->mask_scalar(true),
               this->array(R"(["bar"])"), this->array(R"(["bar"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["foo"])"), this->null_mask_scalar(),
               this->array("[]"), this->array("[null]"));

  this->Assert(ReplaceWithMask, this->array(R"(["foo", "bar"])"),
               this->mask_scalar(false), this->scalar(R"("baz")"),
               this->array(R"(["foo", "bar"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["foo", "bar"])"), this->mask_scalar(true),
               this->scalar(R"("baz")"), this->array(R"(["baz", "baz"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["foo", "bar"])"), this->mask_scalar(true),
               this->scalar("null"), this->array(R"([null, null])"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"), this->array("[]"),
               this->array("[]"));
  this->Assert(ReplaceWithMask, this->array(R"(["aaa", "bbb", "ccc", "ddd"])"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array(R"(["aaa", "bbb", "ccc", "ddd"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["aaa", "bbb", "ccc", "ddd"])"),
               this->mask("[true, true, true, true]"),
               this->array(R"(["eee", "fff", "ggg", "hhh"])"),
               this->array(R"(["eee", "fff", "ggg", "hhh"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["aaa", "bbb", "ccc", "ddd"])"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array(R"([null, null, null, null])"));
  this->Assert(ReplaceWithMask, this->array(R"(["aaa", "bbb", "ccc", null])"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array(R"(["aaa", "bbb", "ccc", null])"));
  this->Assert(ReplaceWithMask, this->array(R"(["aaa", "bbb", "ccc", null])"),
               this->mask("[true, true, true, true]"),
               this->array(R"(["eee", "fff", "ggg", "hhh"])"),
               this->array(R"(["eee", "fff", "ggg", "hhh"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["aaa", "bbb", "ccc", null])"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array(R"([null, null, null, null])"));
  this->Assert(ReplaceWithMask,
               this->array(R"(["aaa", "bbb", "ccc", "ddd", "eee", "fff"])"),
               this->mask("[false, false, null, null, true, true]"),
               this->array(R"(["ggg", null])"),
               this->array(R"(["aaa", "bbb", null, null, "ggg", null])"));
  this->Assert(ReplaceWithMask, this->array(R"([null, null, null, null, null, null])"),
               this->mask("[false, false, null, null, true, true]"),
               this->array(R"(["aaa", null])"),
               this->array(R"([null, null, null, null, "aaa", null])"));
  this->Assert(ReplaceWithMask, this->array(R"(["aaa", null, "bbb"])"),
               this->mask("[false, true, false]"), this->array(R"(["aba"])"),
               this->array(R"(["aaa", "aba", "bbb"])"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"),
               this->scalar(R"("zzz")"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array(R"(["aaa", "bbb"])"),
               this->mask("[true, true]"), this->scalar(R"("zzz")"),
               this->array(R"(["zzz", "zzz"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["aaa", "bbb"])"),
               this->mask("[true, true]"), this->scalar("null"),
               this->array("[null, null]"));
  this->Assert(ReplaceWithMask, this->array(R"(["aaa", "bbb", "ccc"])"),
               this->mask("[false, null, true]"), this->scalar(R"("zzz")"),
               this->array(R"(["aaa", null, "zzz"])"));
}

TEST_F(TestReplaceFixedSizeBinary, ReplaceWithMaskErrors) {
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::AllOf(
          ::testing::HasSubstr("Replacements must be of same type (expected "),
          ::testing::HasSubstr(this->type()->ToString()),
          ::testing::HasSubstr("but got fixed_size_binary[2]")),
      this->AssertRaises(ReplaceWithMask, this->array("[]"), this->mask_scalar(true),
                         ArrayFromJSON(fixed_size_binary(2), "[]")));
}

TYPED_TEST(TestReplaceDecimal, ReplaceWithMask) {
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(false),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(true),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->null_mask_scalar(),
               this->array("[]"), this->array("[]"));

  this->Assert(ReplaceWithMask, this->array(R"(["1.00"])"), this->mask_scalar(false),
               this->array("[]"), this->array(R"(["1.00"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["1.00"])"), this->mask_scalar(true),
               this->array(R"(["0.00"])"), this->array(R"(["0.00"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["1.00"])"), this->null_mask_scalar(),
               this->array("[]"), this->array("[null]"));

  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "0.00"])"),
               this->mask_scalar(false), this->scalar(R"("1.00")"),
               this->array(R"(["0.00", "0.00"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "0.00"])"),
               this->mask_scalar(true), this->scalar(R"("1.00")"),
               this->array(R"(["1.00", "1.00"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "0.00"])"),
               this->mask_scalar(true), this->scalar("null"),
               this->array("[null, null]"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"), this->array("[]"),
               this->array("[]"));
  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "1.00", "2.00", "3.00"])"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array(R"(["0.00", "1.00", "2.00", "3.00"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "1.00", "2.00", "3.00"])"),
               this->mask("[true, true, true, true]"),
               this->array(R"(["10.00", "11.00", "12.00", "13.00"])"),
               this->array(R"(["10.00", "11.00", "12.00", "13.00"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "1.00", "2.00", "3.00"])"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array("[null, null, null, null]"));
  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "1.00", "2.00", null])"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array(R"(["0.00", "1.00", "2.00", null])"));
  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "1.00", "2.00", null])"),
               this->mask("[true, true, true, true]"),
               this->array(R"(["10.00", "11.00", "12.00", "13.00"])"),
               this->array(R"(["10.00", "11.00", "12.00", "13.00"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "1.00", "2.00", null])"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array("[null, null, null, null]"));
  this->Assert(ReplaceWithMask,
               this->array(R"(["0.00", "1.00", "2.00", "3.00", "4.00", "5.00"])"),
               this->mask("[false, false, null, null, true, true]"),
               this->array(R"(["10.00", null])"),
               this->array(R"(["0.00", "1.00", null, null, "10.00", null])"));
  this->Assert(ReplaceWithMask, this->array("[null, null, null, null, null, null]"),
               this->mask("[false, false, null, null, true, true]"),
               this->array(R"(["10.00", null])"),
               this->array(R"([null, null, null, null, "10.00", null])"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"),
               this->scalar(R"("1.00")"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "1.00"])"),
               this->mask("[true, true]"), this->scalar(R"("10.00")"),
               this->array(R"(["10.00", "10.00"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "1.00"])"),
               this->mask("[true, true]"), this->scalar("null"),
               this->array("[null, null]"));
  this->Assert(ReplaceWithMask, this->array(R"(["0.00", "1.00", "2.00"])"),
               this->mask("[false, null, true]"), this->scalar(R"("10.00")"),
               this->array(R"(["0.00", null, "10.00"])"));
}

TEST_F(TestReplaceDayTimeInterval, ReplaceWithMask) {
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(false),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(true),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->null_mask_scalar(),
               this->array("[]"), this->array("[]"));

  this->Assert(ReplaceWithMask, this->array("[[1, 2]]"), this->mask_scalar(false),
               this->array("[]"), this->array("[[1, 2]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2]]"), this->mask_scalar(true),
               this->array("[[3, 4]]"), this->array("[[3, 4]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2]]"), this->null_mask_scalar(),
               this->array("[]"), this->array("[null]"));

  this->Assert(ReplaceWithMask, this->array("[[1, 2], [3, 4]]"), this->mask_scalar(false),
               this->scalar("[7, 8]"), this->array("[[1, 2], [3, 4]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2], [3, 4]]"), this->mask_scalar(true),
               this->scalar("[7, 8]"), this->array("[[7, 8], [7, 8]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2], [3, 4]]"), this->mask_scalar(true),
               this->scalar("null"), this->array("[null, null]"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"), this->array("[]"),
               this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2], [1, 2], [1, 2], [1, 2]]"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array("[[1, 2], [1, 2], [1, 2], [1, 2]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2], [1, 2], [1, 2], [1, 2]]"),
               this->mask("[true, true, true, true]"),
               this->array("[[3, 4], [3, 4], [3, 4], [3, 4]]"),
               this->array("[[3, 4], [3, 4], [3, 4], [3, 4]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2], [1, 2], [1, 2], [1, 2]]"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array("[null, null, null, null]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2], [1, 2], [1, 2], null]"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array("[[1, 2], [1, 2], [1, 2], null]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2], [1, 2], [1, 2], null]"),
               this->mask("[true, true, true, true]"),
               this->array("[[3, 4], [3, 4], [3, 4], [3, 4]]"),
               this->array("[[3, 4], [3, 4], [3, 4], [3, 4]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2], [1, 2], [1, 2], null]"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array("[null, null, null, null]"));
  this->Assert(
      ReplaceWithMask, this->array("[[1, 2], [1, 2], [1, 2], [1, 2], [1, 2], [1, 2]]"),
      this->mask("[false, false, null, null, true, true]"), this->array("[[3, 4], null]"),
      this->array("[[1, 2], [1, 2], null, null, [3, 4], null]"));
  this->Assert(ReplaceWithMask, this->array("[null, null, null, null, null, null]"),
               this->mask("[false, false, null, null, true, true]"),
               this->array("[[3, 4], null]"),
               this->array("[null, null, null, null, [3, 4], null]"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"),
               this->scalar("[7, 8]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2], [3, 4]]"),
               this->mask("[true, true]"), this->scalar("[7, 8]"),
               this->array("[[7, 8], [7, 8]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2], [3, 4]]"),
               this->mask("[true, true]"), this->scalar("null"),
               this->array("[null, null]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2], [3, 4], [5, 6]]"),
               this->mask("[false, null, true]"), this->scalar("[7, 8]"),
               this->array("[[1, 2], null, [7, 8]]"));
}

TEST_F(TestReplaceMonthDayNanoInterval, ReplaceWithMask) {
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(false),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(true),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->null_mask_scalar(),
               this->array("[]"), this->array("[]"));

  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4]]"), this->mask_scalar(false),
               this->array("[]"), this->array("[[1, 2, 4]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4]]"), this->mask_scalar(true),
               this->array("[[3, 4, -2]]"), this->array("[[3, 4, -2]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4]]"), this->null_mask_scalar(),
               this->array("[]"), this->array("[null]"));

  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4], [3, 4, -2]]"),
               this->mask_scalar(false), this->scalar("[7, 0, 8]"),
               this->array("[[1, 2, 4], [3, 4, -2]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4], [3, 4, -2]]"),
               this->mask_scalar(true), this->scalar("[7, 0, 8]"),
               this->array("[[7, 0, 8], [7, 0, 8]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4], [3, 4, -2]]"),
               this->mask_scalar(true), this->scalar("null"),
               this->array("[null, null]"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"), this->array("[]"),
               this->array("[]"));
  this->Assert(ReplaceWithMask,
               this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4]]"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4]]"));
  this->Assert(ReplaceWithMask,
               this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4]]"),
               this->mask("[true, true, true, true]"),
               this->array("[[3, 4, -2], [3, 4, -2], [3, 4, -2], [3, 4, -2]]"),
               this->array("[[3, 4, -2], [3, 4, -2], [3, 4, -2], [3, 4, -2]]"));
  this->Assert(ReplaceWithMask,
               this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4]]"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array("[null, null, null, null]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], null]"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], null]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], null]"),
               this->mask("[true, true, true, true]"),
               this->array("[[3, 4, -2], [3, 4, -2], [3, 4, -2], [3, 4, -2]]"),
               this->array("[[3, 4, -2], [3, 4, -2], [3, 4, -2], [3, 4, -2]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], null]"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array("[null, null, null, null]"));
  this->Assert(
      ReplaceWithMask,
      this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4]]"),
      this->mask("[false, false, null, null, true, true]"),
      this->array("[[3, 4, -2], null]"),
      this->array("[[1, 2, 4], [1, 2, 4], null, null, [3, 4, -2], null]"));
  this->Assert(ReplaceWithMask, this->array("[null, null, null, null, null, null]"),
               this->mask("[false, false, null, null, true, true]"),
               this->array("[[3, 4, -2], null]"),
               this->array("[null, null, null, null, [3, 4, -2], null]"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"),
               this->scalar("[7, 0, 8]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4], [3, 4, -2]]"),
               this->mask("[true, true]"), this->scalar("[7, 0, 8]"),
               this->array("[[7, 0, 8], [7, 0, 8]]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4], [3, 4, -2]]"),
               this->mask("[true, true]"), this->scalar("null"),
               this->array("[null, null]"));
  this->Assert(ReplaceWithMask, this->array("[[1, 2, 4], [3, 4, -2], [-5, 6, 7]]"),
               this->mask("[false, null, true]"), this->scalar("[7, 0, 8]"),
               this->array("[[1, 2, 4], null, [7, 0, 8]]"));
}

TYPED_TEST(TestReplaceBinary, ReplaceWithMask) {
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(false),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->mask_scalar(true),
               this->array("[]"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array("[]"), this->null_mask_scalar(),
               this->array("[]"), this->array("[]"));

  this->Assert(ReplaceWithMask, this->array(R"(["foo"])"), this->mask_scalar(false),
               this->array("[]"), this->array(R"(["foo"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["foo"])"), this->mask_scalar(true),
               this->array(R"(["bar"])"), this->array(R"(["bar"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["foo"])"), this->null_mask_scalar(),
               this->array("[]"), this->array("[null]"));

  this->Assert(ReplaceWithMask, this->array(R"(["foo", "bar"])"),
               this->mask_scalar(false), this->scalar(R"("baz")"),
               this->array(R"(["foo", "bar"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["foo", "bar"])"), this->mask_scalar(true),
               this->scalar(R"("baz")"), this->array(R"(["baz", "baz"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["foo", "bar"])"), this->mask_scalar(true),
               this->scalar("null"), this->array(R"([null, null])"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"), this->array("[]"),
               this->array("[]"));
  this->Assert(ReplaceWithMask, this->array(R"(["a", "bb", "ccc", "dddd"])"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array(R"(["a", "bb", "ccc", "dddd"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["a", "bb", "ccc", "dddd"])"),
               this->mask("[true, true, true, true]"),
               this->array(R"(["eeeee", "f", "ggg", "hhh"])"),
               this->array(R"(["eeeee", "f", "ggg", "hhh"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["a", "bb", "ccc", "dddd"])"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array(R"([null, null, null, null])"));
  this->Assert(ReplaceWithMask, this->array(R"(["a", "bb", "ccc", null])"),
               this->mask("[false, false, false, false]"), this->array("[]"),
               this->array(R"(["a", "bb", "ccc", null])"));
  this->Assert(ReplaceWithMask, this->array(R"(["a", "bb", "ccc", null])"),
               this->mask("[true, true, true, true]"),
               this->array(R"(["eeeee", "f", "ggg", "hhh"])"),
               this->array(R"(["eeeee", "f", "ggg", "hhh"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["a", "bb", "ccc", null])"),
               this->mask("[null, null, null, null]"), this->array("[]"),
               this->array(R"([null, null, null, null])"));
  this->Assert(ReplaceWithMask,
               this->array(R"(["a", "bb", "ccc", "dddd", "eeeee", "f"])"),
               this->mask("[false, false, null, null, true, true]"),
               this->array(R"(["ggg", null])"),
               this->array(R"(["a", "bb", null, null, "ggg", null])"));
  this->Assert(ReplaceWithMask, this->array(R"([null, null, null, null, null, null])"),
               this->mask("[false, false, null, null, true, true]"),
               this->array(R"(["a", null])"),
               this->array(R"([null, null, null, null, "a", null])"));

  this->Assert(ReplaceWithMask, this->array("[]"), this->mask("[]"),
               this->scalar(R"("zzz")"), this->array("[]"));
  this->Assert(ReplaceWithMask, this->array(R"(["a", "bb"])"), this->mask("[true, true]"),
               this->scalar(R"("zzz")"), this->array(R"(["zzz", "zzz"])"));
  this->Assert(ReplaceWithMask, this->array(R"(["a", "bb"])"), this->mask("[true, true]"),
               this->scalar("null"), this->array("[null, null]"));
  this->Assert(ReplaceWithMask, this->array(R"(["a", "bb", "ccc"])"),
               this->mask("[false, null, true]"), this->scalar(R"("zzz")"),
               this->array(R"(["a", null, "zzz"])"));
}

TYPED_TEST(TestReplaceBinary, ReplaceWithMaskRandom) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  auto ty = this->type();

  random::RandomArrayGenerator rand(/*seed=*/0);
  const int64_t length = 1023;
  auto options = key_value_metadata({{"null_probability", "0.01"}, {"max_length", "5"}});
  auto array =
      checked_pointer_cast<ArrayType>(rand.ArrayOf(*field("a", ty, options), length));
  auto mask = checked_pointer_cast<BooleanArray>(
      rand.ArrayOf(boolean(), length, /*null_probability=*/0.01));
  const int64_t num_replacements = std::count_if(
      mask->begin(), mask->end(),
      [](util::optional<bool> value) { return value.has_value() && *value; });
  auto replacements = checked_pointer_cast<ArrayType>(
      rand.ArrayOf(*field("a", ty, options), num_replacements));
  auto expected = this->NaiveImpl(*array, *mask, *replacements);

  this->Assert(ReplaceWithMask, array, mask, replacements, expected);
  for (int64_t slice = 1; slice <= 16; slice++) {
    auto sliced_array = checked_pointer_cast<ArrayType>(array->Slice(slice, 15));
    auto sliced_mask = checked_pointer_cast<BooleanArray>(mask->Slice(slice, 15));
    auto new_expected = this->NaiveImpl(*sliced_array, *sliced_mask, *replacements);
    this->Assert(ReplaceWithMask, sliced_array, sliced_mask, replacements, new_expected);
  }
}

}  // namespace compute
}  // namespace arrow
