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

#include "arrow/array/concatenate.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"

#include <memory>

namespace arrow {
namespace compute {

using arrow::internal::checked_pointer_cast;

template <typename T>
class TestReplaceKernel : public ::testing::Test {
 protected:
  virtual std::shared_ptr<DataType> type() = 0;

  using ReplaceFunction = std::function<Result<Datum>(const Datum&, const Datum&,
                                                      const Datum&, ExecContext*)>;

  using FillNullFunction = std::function<Result<Datum>(const Datum&, ExecContext*)>;

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

  std::shared_ptr<ChunkedArray> chunked_array(const std::vector<std::string>& value) {
    return ChunkedArrayFromJSON(type(), value);
  }

  std::shared_ptr<Array> mask(const std::string& value) {
    return ArrayFromJSON(boolean(), value);
  }

  Status AssertRaises(ReplaceFunction func, const Datum& array, const Datum& mask,
                      const Datum& replacements) {
    auto result = func(array, mask, replacements, nullptr);
    EXPECT_FALSE(result.ok());
    return result.status();
  }

  std::string PrintDatum(const Datum& datum) {
    switch (datum.kind()) {
      case Datum::ARRAY:
        return datum.make_array()->ToString();
      case Datum::CHUNKED_ARRAY:
        return datum.chunked_array()->ToString();
      case Datum::SCALAR:
        return datum.scalar()->ToString();
      default:
        return datum.ToString();
    }
  }

  void Assert(ReplaceFunction func, const Datum& array, const Datum& mask,
              const Datum& replacements, const Datum& expected) {
    ARROW_SCOPED_TRACE("Replacements: ", PrintDatum(replacements));
    ARROW_SCOPED_TRACE("Mask: ", PrintDatum(mask));
    ARROW_SCOPED_TRACE("Array: ", PrintDatum(array));

    ASSERT_OK_AND_ASSIGN(auto actual, func(array, mask, replacements, nullptr));
    if (actual.is_array()) {
      ASSERT_OK(actual.make_array()->ValidateFull());
    } else if (actual.is_scalar()) {
      ASSERT_OK(actual.scalar()->ValidateFull());
    } else if (actual.is_arraylike()) {
      ASSERT_OK(actual.chunked_array()->ValidateFull());
    }

    AssertDatumsApproxEqual(expected, actual, /*verbose=*/true);
  }

  void AssertFillNullArray(FillNullFunction func, const std::shared_ptr<Array>& array,
                           const std::shared_ptr<Array>& expected) {
    SCOPED_TRACE("Array: " + array->ToString());

    ASSERT_OK_AND_ASSIGN(auto actual, func(array, nullptr));
    SCOPED_TRACE("Actual: " + actual.make_array()->ToString());

    ASSERT_TRUE(actual.is_array());
    ASSERT_OK(actual.make_array()->ValidateFull());

    AssertArraysApproxEqual(*expected, *actual.make_array(), /*verbose=*/true);
  }

  void AssertFillNullArraySlices(FillNullFunction func,
                                 const std::shared_ptr<Array>& first_input_array,
                                 const std::shared_ptr<Array>& array_expected_2,
                                 const std::shared_ptr<Array>& array_expected_3,
                                 const std::shared_ptr<Array>& array_expected_4) {
    uint8_t slice_length = 2;
    for (int64_t slice = 0; slice < first_input_array->length() / slice_length; slice++) {
      auto sliced_array = first_input_array->Slice(slice * slice_length, slice_length);
      auto sliced_array_expected =
          array_expected_2->Slice(slice * slice_length, slice_length);
      this->AssertFillNullArray(func, sliced_array, sliced_array_expected);
    }

    slice_length = 3;
    for (int64_t slice = 0; slice < first_input_array->length() / slice_length; slice++) {
      auto sliced_array = first_input_array->Slice(slice * slice_length, slice_length);
      auto sliced_array_expected =
          array_expected_3->Slice(slice * slice_length, slice_length);
      this->AssertFillNullArray(func, sliced_array, sliced_array_expected);
    }

    slice_length = 4;
    for (int64_t slice = 0; slice < first_input_array->length() / slice_length; slice++) {
      auto sliced_array = first_input_array->Slice(slice * slice_length, slice_length);
      auto sliced_array_expected =
          array_expected_4->Slice(slice * slice_length, slice_length);
      this->AssertFillNullArray(func, sliced_array, sliced_array_expected);
    }
  }

  void AssertFillNullChunkedArray(FillNullFunction func,
                                  const std::shared_ptr<ChunkedArray> array,
                                  const std::shared_ptr<ChunkedArray>& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, func(Datum(*array), nullptr));
    AssertChunkedEquivalent(*expected, *actual.chunked_array());
  }

  std::shared_ptr<Array> NaiveImpl(
      const typename TypeTraits<T>::ArrayType& array, const BooleanArray& mask,
      const typename TypeTraits<T>::ArrayType& replacements) {
    auto length = array.length();
    auto builder = std::make_unique<typename TypeTraits<T>::BuilderType>(
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

struct ReplaceWithMaskCase {
  Datum input;
  Datum mask;
  Datum replacements;
  Datum expected;
};

TYPED_TEST(TestReplaceNumeric, ReplaceWithMask) {
  std::vector<ReplaceWithMaskCase> cases = {
      {this->array("[]"), this->mask_scalar(false), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->mask_scalar(true), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->null_mask_scalar(), this->array("[]"), this->array("[]")},
      // Regression test for ARROW-15928
      {this->array("[1, 1, 1, 1]"), this->mask("[true, false, true, false]"),
       this->array("[2, 2]"), this->array("[2, 1, 2, 1]")},

      {this->array("[1]"), this->mask_scalar(false), this->array("[]"),
       this->array("[1]")},
      {this->array("[1]"), this->mask_scalar(true), this->array("[0]"),
       this->array("[0]")},
      {this->array("[1]"), this->mask_scalar(true), this->array("[2, 0]"),
       this->array("[2]")},
      {this->array("[1]"), this->null_mask_scalar(), this->array("[]"),
       this->array("[null]")},

      {this->array("[0, 0]"), this->mask_scalar(false), this->scalar("1"),
       this->array("[0, 0]")},
      {this->array("[0, 0]"), this->mask_scalar(true), this->scalar("1"),
       this->array("[1, 1]")},
      {this->array("[0, 0]"), this->mask_scalar(true), this->scalar("null"),
       this->array("[null, null]")},

      {this->array("[]"), this->mask("[]"), this->array("[]"), this->array("[]")},
      {this->array("[0, 1, 2, 3]"), this->mask("[false, false, false, false]"),
       this->array("[]"), this->array("[0, 1, 2, 3]")},
      {this->array("[0, 1, 2, 3]"), this->mask("[true, true, true, true]"),
       this->array("[10, 11, 12, 13]"), this->array("[10, 11, 12, 13]")},
      {this->array("[0, 1, 2, 3]"), this->mask("[null, null, null, null]"),
       this->array("[]"), this->array("[null, null, null, null]")},
      {this->array("[0, 1, 2, null]"), this->mask("[false, false, false, false]"),
       this->array("[]"), this->array("[0, 1, 2, null]")},
      {this->array("[0, 1, 2, null]"), this->mask("[true, true, true, true]"),
       this->array("[10, 11, 12, 13]"), this->array("[10, 11, 12, 13]")},
      {this->array("[0, 1, 2, null]"), this->mask("[null, null, null, null]"),
       this->array("[]"), this->array("[null, null, null, null]")},
      {this->array("[0, 1, 2, 3, 4, 5]"),
       this->mask("[false, false, null, null, true, true]"), this->array("[10, null]"),
       this->array("[0, 1, null, null, 10, null]")},
      {this->array("[null, null, null, null, null, null]"),
       this->mask("[false, false, null, null, true, true]"), this->array("[10, null]"),
       this->array("[null, null, null, null, 10, null]")},

      {this->array("[]"), this->mask("[]"), this->scalar("1"), this->array("[]")},
      {this->array("[0, 1]"), this->mask("[true, true]"), this->scalar("10"),
       this->array("[10, 10]")},
      {this->array("[0, 1]"), this->mask("[true, true]"), this->scalar("null"),
       this->array("[null, null]")},
      {this->array("[0, 1, 2]"), this->mask("[false, null, true]"), this->scalar("10"),
       this->array("[0, null, 10]")},

      // Regression tests for ARROW-14795
      {this->array("[1, null, 1]"), this->mask("[false, true, false]"),
       this->array("[7]"), this->array("[1, 7, 1]")},
      {this->array("[1, null, 1, 7]"), this->mask("[false, true, false, true]"),
       this->array("[7, 20]"), this->array("[1, 7, 1, 20]")},
      {this->array("[1, 2, 3, 4]"), this->mask("[false, true, false, true]"),
       this->array("[null, null]"), this->array("[1, null, 3, null]")},
      {this->array("[null, 2, 3, 4]"), this->mask("[true, true, false, true]"),
       this->array("[1, null, null]"), this->array("[1, null, 3, null]")},
      {this->array("[1, null, 1]"), this->mask("[false, true, false]"),
       this->scalar("null"), this->array("[1, null, 1]")},
      {this->array("[1, null, 1]"), this->mask("[true, true, true]"),
       this->array("[7, 7, 7]"), this->array("[7, 7, 7]")},
      {this->array("[1, null, 1]"), this->mask("[true, true, true]"),
       this->array("[null, null, null]"), this->array("[null, null, null]")},
      {this->array("[1, null, 1]"), this->mask("[false, true, false]"),
       this->scalar("null"), this->array("[1, null, 1]")},
      {this->array("[1, null, 1]"), this->mask("[true, true, true]"),
       this->scalar("null"), this->array("[null, null, null]")},
      {this->array("[null, null]"), this->mask("[true, true]"), this->array("[1, 1]"),
       this->array("[1, 1]")},

      // ChunkedArray tests for ARROW-15928
      {this->chunked_array({}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({}), this->mask_scalar(true), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask_scalar(false), this->array("[]"),
       this->chunked_array({})},

      {this->chunked_array({"[0, 1, 2, 3]", "[]", "[4, 5, 6, 7]"}),
       this->mask("[true, false, false, false, false, true, false, false]"),
       this->array("[10, 10]"), this->chunked_array({"[10, 1, 2, 3]", "[4, 10, 6, 7]"})},
      {this->chunked_array({"[0, 1, 2, 3, 4, 5, 6, 7]"}),
       this->mask("[true, true, true, true, true, true, true, true]"),
       this->array("[10, 10, 10, 10, 10, 10, 10, 10]"),
       this->chunked_array({"[10, 10, 10, 10, 10, 10, 10, 10]"})},
      {this->chunked_array({"[0, 1, 2, 3]", "[4, null, 6, 7]"}), this->mask_scalar(true),
       this->array("[10, 10, 10, 10, 10, 10, 10, 10]"),
       this->chunked_array({"[10, 10, 10, 10, 10, 10, 10, 10]"})},
      {this->chunked_array({"[0, 1, 2, 3]", "[4, null, 6, 7]"}), this->mask_scalar(false),
       this->array("[]"), this->chunked_array({"[0, 1, 2, 3]", "[4, null, 6, 7]"})},
      {this->chunked_array({"[0, 1, 2, 3]", "[4, null, 6, 7]"}), this->null_mask_scalar(),
       this->array("[]"),
       this->chunked_array({"[null, null, null, null]", "[null, null, null, null]"})},
      {this->chunked_array({"[0, 1, 2, 3]", "[4, null, 6, 7]"}), this->mask_scalar(true),
       this->scalar("10"), this->chunked_array({"[10, 10, 10, 10, 10, 10, 10, 10]"})},
      {this->chunked_array({"[0, 1, 2, 3]", "[4, null, 6, 7]"}), this->mask_scalar(true),
       this->scalar("null"),
       this->chunked_array({"[null, null, null, null, null, null, null, null]"})},

      {this->chunked_array(
           {"[]", "[null]", "[0, 1]", "[null, 3]", "[null, 5, 6]", "[7]"}),
       this->mask("[true, true, false, false, true, true, false, true, false]"),
       this->array("[10, 11, null, null, 14]"),
       this->chunked_array({"[10]", "[11, 1]", "[null, null]", "[null, 5, 14]", "[7]"})},
      {this->chunked_array(
           {"[]", "[null]", "[0, 1]", "[null, 3]", "[null, 5, 6]", "[7]"}),
       this->mask_scalar(true),
       this->array("[10, 11, null, null, 14, 15, null, 16, null]"),
       this->chunked_array(
           {"[10]", "[11, null]", "[null, 14]", "[15, null, 16]", "[null]"})},
      {this->chunked_array(
           {"[]", "[null]", "[0, 1]", "[null, 3]", "[null, 5, 6]", "[7]"}),
       this->mask_scalar(true), this->scalar("null"),
       this->chunked_array(
           {"[null]", "[null, null]", "[null, null]", "[null, null, null]", "[null]"})},
      {this->chunked_array(
           {"[]", "[null]", "[0, 1]", "[null, 3]", "[null, 5, 6]", "[7]"}),
       this->mask_scalar(true), this->scalar("12"),
       this->chunked_array({"[12]", "[12, 12]", "[12, 12]", "[12, 12, 12]", "[12]"})},
      {this->chunked_array(
           {"[]", "[null]", "[0, 1]", "[null, 3]", "[null, 5, 6]", "[7]"}),
       this->mask_scalar(false), this->array("[]"),
       this->chunked_array({"[null]", "[0, 1]", "[null, 3]", "[null, 5, 6]", "[7]"})},
      {this->chunked_array(
           {"[]", "[null]", "[0, 1]", "[null, 3]", "[null, 5, 6]", "[7]"}),
       this->null_mask_scalar(), this->array("[]"),
       this->chunked_array(
           {"[null]", "[null, null]", "[null, null]", "[null, null, null]", "[null]"})},
  };

  for (size_t i = 0; i < cases.size(); ++i) {
    auto test_case = cases[i];
    if (std::is_same<TypeParam, Date64Type>::value) {
      // ARROW-10924: account for Date64 value restrictions
      ASSERT_OK_AND_ASSIGN(test_case.input, Cast(test_case.input, int64()));
      ASSERT_OK_AND_ASSIGN(test_case.input, Multiply(test_case.input, Datum(86400000)));
      ASSERT_OK_AND_ASSIGN(test_case.replacements, Cast(test_case.replacements, int64()));
      ASSERT_OK_AND_ASSIGN(test_case.replacements,
                           Multiply(test_case.replacements, Datum(86400000)));
      ASSERT_OK_AND_ASSIGN(test_case.expected, Cast(test_case.expected, int64()));
      ASSERT_OK_AND_ASSIGN(test_case.expected,
                           Multiply(test_case.expected, Datum(86400000)));
    }
    this->Assert(ReplaceWithMask, test_case.input, test_case.mask, test_case.replacements,
                 test_case.expected);
  }
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
      [](std::optional<bool> value) { return value.has_value() && *value; });
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
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Replacement array must be of appropriate length (expected 2 "
                           "items but got 0 items)"),
      this->AssertRaises(ReplaceWithMask, this->array("[1, 2]"), this->mask_scalar(true),
                         this->array("[]")));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Replacements must be array or scalar, not ChunkedArray"),
      this->AssertRaises(ReplaceWithMask, this->array("[1, 2]"),
                         this->mask("[true, false]"), this->chunked_array({"[0]"})));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Mask must be array or scalar, not ChunkedArray"),
      this->AssertRaises(ReplaceWithMask, this->array("[1, 2]"),
                         ChunkedArrayFromJSON(boolean(), {"[true]", "[false]"}),
                         this->array({"[0]"})));
}

TEST_F(TestReplaceBoolean, ReplaceWithMask) {
  std::vector<ReplaceWithMaskCase> cases = {
      {this->array("[]"), this->mask_scalar(false), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->mask_scalar(true), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->null_mask_scalar(), this->array("[]"), this->array("[]")},

      {this->array("[true]"), this->mask_scalar(false), this->array("[]"),
       this->array("[true]")},
      {this->array("[true]"), this->mask_scalar(true), this->array("[false]"),
       this->array("[false]")},
      {this->array("[true]"), this->null_mask_scalar(), this->array("[]"),
       this->array("[null]")},

      {this->array("[false, false]"), this->mask_scalar(false), this->scalar("true"),
       this->array("[false, false]")},
      {this->array("[false, false]"), this->mask_scalar(true), this->scalar("true"),
       this->array("[true, true]")},
      {this->array("[false, false]"), this->mask_scalar(true), this->scalar("null"),
       this->array("[null, null]")},

      {this->array("[]"), this->mask("[]"), this->array("[]"), this->array("[]")},
      {this->array("[true, true, true, true]"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array("[true, true, true, true]")},
      {this->array("[true, true, true, true]"), this->mask("[true, true, true, true]"),
       this->array("[false, false, false, false]"),
       this->array("[false, false, false, false]")},
      {this->array("[true, true, true, true]"), this->mask("[null, null, null, null]"),
       this->array("[]"), this->array("[null, null, null, null]")},
      {this->array("[true, true, true, null]"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array("[true, true, true, null]")},
      {this->array("[true, true, true, null]"), this->mask("[true, true, true, true]"),
       this->array("[false, false, false, false]"),
       this->array("[false, false, false, false]")},
      {this->array("[true, true, true, null]"), this->mask("[null, null, null, null]"),
       this->array("[]"), this->array("[null, null, null, null]")},
      {this->array("[true, true, true, true, true, true]"),
       this->mask("[false, false, null, null, true, true]"), this->array("[false, null]"),
       this->array("[true, true, null, null, false, null]")},
      {this->array("[null, null, null, null, null, null]"),
       this->mask("[false, false, null, null, true, true]"), this->array("[false, null]"),
       this->array("[null, null, null, null, false, null]")},
      {this->array("[true, null, true]"), this->mask("[false, true, false]"),
       this->array("[true]"), this->array("[true, true, true]")},

      {this->array("[]"), this->mask("[]"), this->scalar("true"), this->array("[]")},
      {this->array("[null, false, true]"), this->mask("[true, false, false]"),
       this->scalar("false"), this->array("[false, false, true]")},
      {this->array("[false, false]"), this->mask("[true, true]"), this->scalar("true"),
       this->array("[true, true]")},
      {this->array("[false, false]"), this->mask("[true, true]"), this->scalar("null"),
       this->array("[null, null]")},
      {this->array("[false, false, false]"), this->mask("[false, null, true]"),
       this->scalar("true"), this->array("[false, null, true]")},
      {this->array("[null, null]"), this->mask("[true, true]"),
       this->array("[true, true]"), this->array("[true, true]")},
  };

  for (auto test_case : cases) {
    this->Assert(ReplaceWithMask, test_case.input, test_case.mask, test_case.replacements,
                 test_case.expected);
  }
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
  std::vector<ReplaceWithMaskCase> cases = {
      {this->array("[]"), this->mask_scalar(false), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->mask_scalar(true), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->null_mask_scalar(), this->array("[]"), this->array("[]")},

      {this->array(R"(["foo"])"), this->mask_scalar(false), this->array("[]"),
       this->array(R"(["foo"])")},
      {this->array(R"(["foo"])"), this->mask_scalar(true), this->array(R"(["bar"])"),
       this->array(R"(["bar"])")},
      {this->array(R"(["foo"])"), this->null_mask_scalar(), this->array("[]"),
       this->array("[null]")},

      {this->array(R"(["foo", "bar"])"), this->mask_scalar(false),
       this->scalar(R"("baz")"), this->array(R"(["foo", "bar"])")},
      {this->array(R"(["foo", "bar"])"), this->mask_scalar(true),
       this->scalar(R"("baz")"), this->array(R"(["baz", "baz"])")},
      {this->array(R"(["foo", "bar"])"), this->mask_scalar(true), this->scalar("null"),
       this->array(R"([null, null])")},

      {this->array("[]"), this->mask("[]"), this->array("[]"), this->array("[]")},
      {this->array(R"(["aaa", "bbb", "ccc", "ddd"])"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array(R"(["aaa", "bbb", "ccc", "ddd"])")},
      {this->array(R"(["aaa", "bbb", "ccc", "ddd"])"),
       this->mask("[true, true, true, true]"),
       this->array(R"(["eee", "fff", "ggg", "hhh"])"),
       this->array(R"(["eee", "fff", "ggg", "hhh"])")},
      {this->array(R"(["aaa", "bbb", "ccc", "ddd"])"),
       this->mask("[null, null, null, null]"), this->array("[]"),
       this->array(R"([null, null, null, null])")},
      {this->array(R"(["aaa", "bbb", "ccc", null])"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array(R"(["aaa", "bbb", "ccc", null])")},
      {this->array(R"(["aaa", "bbb", "ccc", null])"),
       this->mask("[true, true, true, true]"),
       this->array(R"(["eee", "fff", "ggg", "hhh"])"),
       this->array(R"(["eee", "fff", "ggg", "hhh"])")},
      {this->array(R"(["aaa", "bbb", "ccc", null])"),
       this->mask("[null, null, null, null]"), this->array("[]"),
       this->array(R"([null, null, null, null])")},
      {this->array(R"(["aaa", "bbb", "ccc", "ddd", "eee", "fff"])"),
       this->mask("[false, false, null, null, true, true]"),
       this->array(R"(["ggg", null])"),
       this->array(R"(["aaa", "bbb", null, null, "ggg", null])")},
      {this->array(R"([null, null, null, null, null, null])"),
       this->mask("[false, false, null, null, true, true]"),
       this->array(R"(["aaa", null])"),
       this->array(R"([null, null, null, null, "aaa", null])")},
      {this->array(R"(["aaa", null, "bbb"])"), this->mask("[false, true, false]"),
       this->array(R"(["aba"])"), this->array(R"(["aaa", "aba", "bbb"])")},

      {this->array("[]"), this->mask("[]"), this->scalar(R"("zzz")"), this->array("[]")},
      {this->array(R"(["aaa", "bbb"])"), this->mask("[true, true]"),
       this->scalar(R"("zzz")"), this->array(R"(["zzz", "zzz"])")},
      {this->array(R"(["aaa", "bbb"])"), this->mask("[true, true]"), this->scalar("null"),
       this->array("[null, null]")},
      {this->array(R"(["aaa", "bbb", "ccc"])"), this->mask("[false, null, true]"),
       this->scalar(R"("zzz")"), this->array(R"(["aaa", null, "zzz"])")},

      {this->chunked_array({}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({}), this->mask_scalar(true), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask_scalar(false), this->array("[]"),
       this->chunked_array({})},

      {this->chunked_array({R"([])", R"([null])", R"(["aaa", "bbb"])", R"([null, "ccc"])",
                            R"([null, "ddd", "eee"])", R"(["fff"])"}),
       this->mask("[true, true, false, false, true, true, false, true, false]"),
       this->array(R"(["zzz", "yyy", null, null, "xxx"])"),
       this->chunked_array({R"(["zzz"])", R"(["yyy", "bbb"])", R"([null, null])",
                            R"([null, "ddd", "xxx"])", R"(["fff"])"})},
      {this->chunked_array({R"([])", R"([null])", R"(["aaa", "bbb"])", R"([null, "ccc"])",
                            R"([null, "ddd", "eee"])", R"(["fff"])"}),
       this->mask_scalar(true),
       this->array(R"(["zzz", "yyy", null, null, "xxx", "www", null, "vvv", null])"),
       this->chunked_array({R"(["zzz"])", R"(["yyy", null])", R"([null, "xxx"])",
                            R"(["www", null, "vvv"])", R"([null])"})},
  };

  for (auto test_case : cases) {
    this->Assert(ReplaceWithMask, test_case.input, test_case.mask, test_case.replacements,
                 test_case.expected);
  }
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
  std::vector<ReplaceWithMaskCase> cases = {
      {this->array("[]"), this->mask_scalar(false), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->mask_scalar(true), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->null_mask_scalar(), this->array("[]"), this->array("[]")},

      {this->array(R"(["1.00"])"), this->mask_scalar(false), this->array("[]"),
       this->array(R"(["1.00"])")},
      {this->array(R"(["1.00"])"), this->mask_scalar(true), this->array(R"(["0.00"])"),
       this->array(R"(["0.00"])")},
      {this->array(R"(["1.00"])"), this->null_mask_scalar(), this->array("[]"),
       this->array("[null]")},

      {this->array(R"(["0.00", "0.00"])"), this->mask_scalar(false),
       this->scalar(R"("1.00")"), this->array(R"(["0.00", "0.00"])")},
      {this->array(R"(["0.00", "0.00"])"), this->mask_scalar(true),
       this->scalar(R"("1.00")"), this->array(R"(["1.00", "1.00"])")},
      {this->array(R"(["0.00", "0.00"])"), this->mask_scalar(true), this->scalar("null"),
       this->array("[null, null]")},

      {this->array("[]"), this->mask("[]"), this->array("[]"), this->array("[]")},
      {this->array(R"(["0.00", "1.00", "2.00", "3.00"])"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array(R"(["0.00", "1.00", "2.00", "3.00"])")},
      {this->array(R"(["0.00", "1.00", "2.00", "3.00"])"),
       this->mask("[true, true, true, true]"),
       this->array(R"(["10.00", "11.00", "12.00", "13.00"])"),
       this->array(R"(["10.00", "11.00", "12.00", "13.00"])")},
      {this->array(R"(["0.00", "1.00", "2.00", "3.00"])"),
       this->mask("[null, null, null, null]"), this->array("[]"),
       this->array("[null, null, null, null]")},
      {this->array(R"(["0.00", "1.00", "2.00", null])"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array(R"(["0.00", "1.00", "2.00", null])")},
      {this->array(R"(["0.00", "1.00", "2.00", null])"),
       this->mask("[true, true, true, true]"),
       this->array(R"(["10.00", "11.00", "12.00", "13.00"])"),
       this->array(R"(["10.00", "11.00", "12.00", "13.00"])")},
      {this->array(R"(["0.00", "1.00", "2.00", null])"),
       this->mask("[null, null, null, null]"), this->array("[]"),
       this->array("[null, null, null, null]")},
      {this->array(R"(["0.00", "1.00", "2.00", "3.00", "4.00", "5.00"])"),
       this->mask("[false, false, null, null, true, true]"),
       this->array(R"(["10.00", null])"),
       this->array(R"(["0.00", "1.00", null, null, "10.00", null])")},
      {this->array("[null, null, null, null, null, null]"),
       this->mask("[false, false, null, null, true, true]"),
       this->array(R"(["10.00", null])"),
       this->array(R"([null, null, null, null, "10.00", null])")},

      {this->array("[]"), this->mask("[]"), this->scalar(R"("1.00")"), this->array("[]")},
      {this->array(R"(["0.00", "1.00"])"), this->mask("[true, true]"),
       this->scalar(R"("10.00")"), this->array(R"(["10.00", "10.00"])")},
      {this->array(R"(["0.00", "1.00"])"), this->mask("[true, true]"),
       this->scalar("null"), this->array("[null, null]")},
      {this->array(R"(["0.00", "1.00", "2.00"])"), this->mask("[false, null, true]"),
       this->scalar(R"("10.00")"), this->array(R"(["0.00", null, "10.00"])")},

      {this->chunked_array({}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({}), this->mask_scalar(true), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask_scalar(false), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({R"([])", R"([null])", R"(["1.02", "-2.45"])",
                            R"([null, "-3.00"])", R"([null, "-5.45", "6.01"])",
                            R"(["7.77"])"}),
       this->mask("[true, true, false, false, true, true, false, true, false]"),
       this->array(R"(["9.01", "9.23", null, null, "9.45"])"),
       this->chunked_array({R"(["9.01"])", R"(["9.23", "-2.45"])", R"([null, null])",
                            R"([null, "-5.45", "9.45"])", R"(["7.77"])"})},
      {this->chunked_array({R"([])", R"([null])", R"(["1.02", "-2.45"])",
                            R"([null, "-3.00"])", R"([null, "-5.45", "6.01"])",
                            R"(["7.77"])"}),
       this->mask_scalar(true),
       this->array(R"(["9.01", "9.23", null, null, "9.45", "9.67", null, "9.19", null])"),
       this->chunked_array({R"(["9.01"])", R"(["9.23", null])", R"([null, "9.45"])",
                            R"(["9.67", null, "9.19"])", R"([null])"})},
  };

  for (auto test_case : cases) {
    this->Assert(ReplaceWithMask, test_case.input, test_case.mask, test_case.replacements,
                 test_case.expected);
  }
}

TEST_F(TestReplaceDayTimeInterval, ReplaceWithMask) {
  std::vector<ReplaceWithMaskCase> cases = {
      {this->array("[]"), this->mask_scalar(false), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->mask_scalar(true), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->null_mask_scalar(), this->array("[]"), this->array("[]")},

      {this->array("[[1, 2]]"), this->mask_scalar(false), this->array("[]"),
       this->array("[[1, 2]]")},
      {this->array("[[1, 2]]"), this->mask_scalar(true), this->array("[[3, 4]]"),
       this->array("[[3, 4]]")},
      {this->array("[[1, 2]]"), this->null_mask_scalar(), this->array("[]"),
       this->array("[null]")},

      {this->array("[[1, 2], [3, 4]]"), this->mask_scalar(false), this->scalar("[7, 8]"),
       this->array("[[1, 2], [3, 4]]")},
      {this->array("[[1, 2], [3, 4]]"), this->mask_scalar(true), this->scalar("[7, 8]"),
       this->array("[[7, 8], [7, 8]]")},
      {this->array("[[1, 2], [3, 4]]"), this->mask_scalar(true), this->scalar("null"),
       this->array("[null, null]")},

      {this->array("[]"), this->mask("[]"), this->array("[]"), this->array("[]")},
      {this->array("[[1, 2], [1, 2], [1, 2], [1, 2]]"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array("[[1, 2], [1, 2], [1, 2], [1, 2]]")},
      {this->array("[[1, 2], [1, 2], [1, 2], [1, 2]]"),
       this->mask("[true, true, true, true]"),
       this->array("[[3, 4], [3, 4], [3, 4], [3, 4]]"),
       this->array("[[3, 4], [3, 4], [3, 4], [3, 4]]")},
      {this->array("[[1, 2], [1, 2], [1, 2], [1, 2]]"),
       this->mask("[null, null, null, null]"), this->array("[]"),
       this->array("[null, null, null, null]")},
      {this->array("[[1, 2], [1, 2], [1, 2], null]"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array("[[1, 2], [1, 2], [1, 2], null]")},
      {this->array("[[1, 2], [1, 2], [1, 2], null]"),
       this->mask("[true, true, true, true]"),
       this->array("[[3, 4], [3, 4], [3, 4], [3, 4]]"),
       this->array("[[3, 4], [3, 4], [3, 4], [3, 4]]")},
      {this->array("[[1, 2], [1, 2], [1, 2], null]"),
       this->mask("[null, null, null, null]"), this->array("[]"),
       this->array("[null, null, null, null]")},
      {this->array("[[1, 2], [1, 2], [1, 2], [1, 2], [1, 2], [1, 2]]"),
       this->mask("[false, false, null, null, true, true]"),
       this->array("[[3, 4], null]"),
       this->array("[[1, 2], [1, 2], null, null, [3, 4], null]")},
      {this->array("[null, null, null, null, null, null]"),
       this->mask("[false, false, null, null, true, true]"),
       this->array("[[3, 4], null]"),
       this->array("[null, null, null, null, [3, 4], null]")},

      {this->array("[]"), this->mask("[]"), this->scalar("[7, 8]"), this->array("[]")},
      {this->array("[[1, 2], [3, 4]]"), this->mask("[true, true]"),
       this->scalar("[7, 8]"), this->array("[[7, 8], [7, 8]]")},
      {this->array("[[1, 2], [3, 4]]"), this->mask("[true, true]"), this->scalar("null"),
       this->array("[null, null]")},
      {this->array("[[1, 2], [3, 4], [5, 6]]"), this->mask("[false, null, true]"),
       this->scalar("[7, 8]"), this->array("[[1, 2], null, [7, 8]]")},

      {this->chunked_array({}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({}), this->mask_scalar(true), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask_scalar(false), this->array("[]"),
       this->chunked_array({})},

      {this->chunked_array({"[]", "[null]", "[[0, 0], [1, -2]]", "[null, [-3, -5]]",
                            "[null, [5, 0], [6, -6]]", "[[7, 7]]"}),
       this->mask("[true, true, false, false, true, true, false, true, false]"),
       this->array("[[10, 19], [11, 18], null, null, [14, 14]]"),
       this->chunked_array({"[[10, 19]]", "[[11, 18], [1, -2]]", "[null, null]",
                            "[null, [5, 0], [14, 14]]", "[[7, 7]]"})},
      {this->chunked_array({"[]", "[null]", "[[0, 0], [1, -2]]", "[null, [-3, -5]]",
                            "[null, [5, 0], [6, -6]]", "[[7, 7]]"}),
       this->mask_scalar(true),
       this->array(
           "[[10, 19], [11, 18], null, null, [14, 14], [15, 17], null, [16, 20], null]"),
       this->chunked_array({"[[10, 19]]", "[[11, 18], null]", "[null, [14, 14]]",
                            "[[15, 17], null, [16, 20]]", "[null]"})},
  };

  for (auto test_case : cases) {
    this->Assert(ReplaceWithMask, test_case.input, test_case.mask, test_case.replacements,
                 test_case.expected);
  }
}

TEST_F(TestReplaceMonthDayNanoInterval, ReplaceWithMask) {
  std::vector<ReplaceWithMaskCase> cases = {
      {this->array("[]"), this->mask_scalar(false), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->mask_scalar(true), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->null_mask_scalar(), this->array("[]"), this->array("[]")},

      {this->array("[[1, 2, 4]]"), this->mask_scalar(false), this->array("[]"),
       this->array("[[1, 2, 4]]")},
      {this->array("[[1, 2, 4]]"), this->mask_scalar(true), this->array("[[3, 4, -2]]"),
       this->array("[[3, 4, -2]]")},
      {this->array("[[1, 2, 4]]"), this->null_mask_scalar(), this->array("[]"),
       this->array("[null]")},

      {this->array("[[1, 2, 4], [3, 4, -2]]"), this->mask_scalar(false),
       this->scalar("[7, 0, 8]"), this->array("[[1, 2, 4], [3, 4, -2]]")},
      {this->array("[[1, 2, 4], [3, 4, -2]]"), this->mask_scalar(true),
       this->scalar("[7, 0, 8]"), this->array("[[7, 0, 8], [7, 0, 8]]")},
      {this->array("[[1, 2, 4], [3, 4, -2]]"), this->mask_scalar(true),
       this->scalar("null"), this->array("[null, null]")},

      {this->array("[]"), this->mask("[]"), this->array("[]"), this->array("[]")},
      {this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4]]"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4]]")},
      {this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4]]"),
       this->mask("[true, true, true, true]"),
       this->array("[[3, 4, -2], [3, 4, -2], [3, 4, -2], [3, 4, -2]]"),
       this->array("[[3, 4, -2], [3, 4, -2], [3, 4, -2], [3, 4, -2]]")},
      {this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4]]"),
       this->mask("[null, null, null, null]"), this->array("[]"),
       this->array("[null, null, null, null]")},
      {this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], null]"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], null]")},
      {this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], null]"),
       this->mask("[true, true, true, true]"),
       this->array("[[3, 4, -2], [3, 4, -2], [3, 4, -2], [3, 4, -2]]"),
       this->array("[[3, 4, -2], [3, 4, -2], [3, 4, -2], [3, 4, -2]]")},
      {this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], null]"),
       this->mask("[null, null, null, null]"), this->array("[]"),
       this->array("[null, null, null, null]")},
      {this->array("[[1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4], [1, 2, 4]]"),
       this->mask("[false, false, null, null, true, true]"),
       this->array("[[3, 4, -2], null]"),
       this->array("[[1, 2, 4], [1, 2, 4], null, null, [3, 4, -2], null]")},
      {this->array("[null, null, null, null, null, null]"),
       this->mask("[false, false, null, null, true, true]"),
       this->array("[[3, 4, -2], null]"),
       this->array("[null, null, null, null, [3, 4, -2], null]")},

      {this->array("[]"), this->mask("[]"), this->scalar("[7, 0, 8]"), this->array("[]")},
      {this->array("[[1, 2, 4], [3, 4, -2]]"), this->mask("[true, true]"),
       this->scalar("[7, 0, 8]"), this->array("[[7, 0, 8], [7, 0, 8]]")},
      {this->array("[[1, 2, 4], [3, 4, -2]]"), this->mask("[true, true]"),
       this->scalar("null"), this->array("[null, null]")},
      {this->array("[[1, 2, 4], [3, 4, -2], [-5, 6, 7]]"),
       this->mask("[false, null, true]"), this->scalar("[7, 0, 8]"),
       this->array("[[1, 2, 4], null, [7, 0, 8]]")},

      {this->chunked_array({}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({}), this->mask_scalar(true), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask_scalar(false), this->array("[]"),
       this->chunked_array({})},

      {this->chunked_array({"[]", "[null]", "[[0, 0, 0], [1, -2, -3]]",
                            "[null, [-3, -5, -7]]", "[null, [5, 0, 0], [6, -6, 0]]",
                            "[[7, 7, 7]]"}),
       this->mask("[true, true, false, false, true, true, false, true, false]"),
       this->array("[[10, 19, 20], [11, 18, -2], null, null, [14, 14, 14]]"),
       this->chunked_array({"[[10, 19, 20]]", "[[11, 18, -2], [1, -2, -3]]",
                            "[null, null]", "[null, [5, 0, 0], [14, 14, 14]]",
                            "[[7, 7, 7]]"})},
      {this->chunked_array({"[]", "[null]", "[[0, 0, 0], [1, -2, -3]]",
                            "[null, [-3, -5, -7]]", "[null, [5, 0, 0], [6, -6, 0]]",
                            "[[7, 7, 7]]"}),
       this->mask_scalar(true),
       this->array("[[10, 19, 20], [11, 18, -2], null, null, [14, 14, 14], [15, 17, "
                   "-30], null, [16, 20, 24], null]"),
       this->chunked_array({"[[10, 19, 20]]", "[[11, 18, -2], null]",
                            "[null, [14, 14, 14]]", "[[15, 17, -30], null, [16, 20, 24]]",
                            "[null]"})},
  };

  for (auto test_case : cases) {
    this->Assert(ReplaceWithMask, test_case.input, test_case.mask, test_case.replacements,
                 test_case.expected);
  }
}

TYPED_TEST(TestReplaceBinary, ReplaceWithMask) {
  std::vector<ReplaceWithMaskCase> cases = {
      {this->array("[]"), this->mask_scalar(false), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->mask_scalar(true), this->array("[]"), this->array("[]")},
      {this->array("[]"), this->null_mask_scalar(), this->array("[]"), this->array("[]")},

      {this->array(R"(["foo"])"), this->mask_scalar(false), this->array("[]"),
       this->array(R"(["foo"])")},
      {this->array(R"(["foo"])"), this->mask_scalar(true), this->array(R"(["bar"])"),
       this->array(R"(["bar"])")},
      {this->array(R"(["foo"])"), this->null_mask_scalar(), this->array("[]"),
       this->array("[null]")},

      {this->array(R"(["foo", "bar"])"), this->mask_scalar(false),
       this->scalar(R"("baz")"), this->array(R"(["foo", "bar"])")},
      {this->array(R"(["foo", "bar"])"), this->mask_scalar(true),
       this->scalar(R"("baz")"), this->array(R"(["baz", "baz"])")},
      {this->array(R"(["foo", "bar"])"), this->mask_scalar(true), this->scalar("null"),
       this->array(R"([null, null])")},

      {this->array("[]"), this->mask("[]"), this->array("[]"), this->array("[]")},
      {this->array(R"(["a", "bb", "ccc", "dddd"])"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array(R"(["a", "bb", "ccc", "dddd"])")},
      {this->array(R"(["a", "bb", "ccc", "dddd"])"),
       this->mask("[true, true, true, true]"),
       this->array(R"(["eeeee", "f", "ggg", "hhh"])"),
       this->array(R"(["eeeee", "f", "ggg", "hhh"])")},
      {this->array(R"(["a", "bb", "ccc", "dddd"])"),
       this->mask("[null, null, null, null]"), this->array("[]"),
       this->array(R"([null, null, null, null])")},
      {this->array(R"(["a", "bb", "ccc", null])"),
       this->mask("[false, false, false, false]"), this->array("[]"),
       this->array(R"(["a", "bb", "ccc", null])")},
      {this->array(R"(["a", "bb", "ccc", null])"), this->mask("[true, true, true, true]"),
       this->array(R"(["eeeee", "f", "ggg", "hhh"])"),
       this->array(R"(["eeeee", "f", "ggg", "hhh"])")},
      {this->array(R"(["a", "bb", "ccc", null])"), this->mask("[null, null, null, null]"),
       this->array("[]"), this->array(R"([null, null, null, null])")},
      {this->array(R"(["a", "bb", "ccc", "dddd", "eeeee", "f"])"),
       this->mask("[false, false, null, null, true, true]"),
       this->array(R"(["ggg", null])"),
       this->array(R"(["a", "bb", null, null, "ggg", null])")},
      {this->array(R"([null, null, null, null, null, null])"),
       this->mask("[false, false, null, null, true, true]"),
       this->array(R"(["a", null])"),
       this->array(R"([null, null, null, null, "a", null])")},

      {this->array("[]"), this->mask("[]"), this->scalar(R"("zzz")"), this->array("[]")},
      {this->array(R"(["a", "bb"])"), this->mask("[true, true]"),
       this->scalar(R"("zzz")"), this->array(R"(["zzz", "zzz"])")},
      {this->array(R"(["a", "bb"])"), this->mask("[true, true]"), this->scalar("null"),
       this->array("[null, null]")},
      {this->array(R"(["a", "bb", "ccc"])"), this->mask("[false, null, true]"),
       this->scalar(R"("zzz")"), this->array(R"(["a", null, "zzz"])")},

      {this->chunked_array({}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask("[]"), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({}), this->mask_scalar(true), this->array("[]"),
       this->chunked_array({})},
      {this->chunked_array({"[]"}), this->mask_scalar(false), this->array("[]"),
       this->chunked_array({})},

      {this->chunked_array({R"([])", R"([null])", R"(["a", "aba"])", R"([null, "defgh"])",
                            R"([null, "abcdefg", "e"])", R"([""])"}),
       this->mask("[true, true, false, false, true, true, false, true, false]"),
       this->array(R"(["zzz", "y", null, null, "a"])"),
       this->chunked_array({R"(["zzz"])", R"(["y", "aba"])", R"([null, null])",
                            R"([null, "abcdefg", "a"])", R"([""])"})},
      {this->chunked_array({R"([])", R"([null])", R"(["a", "aba"])", R"([null, "defgh"])",
                            R"([null, "abcdefg", "e"])", R"([""])"}),
       this->mask_scalar(true),
       this->array(R"(["zzz", "y", null, null, "a", "www", null, "vvv", null])"),
       this->chunked_array({R"(["zzz"])", R"(["y", null])", R"([null, "a"])",
                            R"(["www", null, "vvv"])", R"([null])"})},
      {this->chunked_array({R"([])", R"([null])", R"(["a", "aba"])", R"([null, "defgh"])",
                            R"([null, "abcdefg", "e"])", R"([""])"}),
       this->mask_scalar(true), this->scalar("null"),
       this->chunked_array(
           {"[null]", "[null, null]", "[null, null]", "[null, null, null]", "[null]"})},
      {this->chunked_array({R"([])", R"([null])", R"(["a", "aba"])", R"([null, "defgh"])",
                            R"([null, "abcdefg", "e"])", R"([""])"}),
       this->mask_scalar(true), this->scalar(R"("abcde")"),
       this->chunked_array({R"(["abcde"])", R"(["abcde", "abcde"])",
                            R"(["abcde", "abcde"])", R"(["abcde", "abcde", "abcde"])",
                            R"(["abcde"])"})},
      {this->chunked_array({R"([])", R"([null])", R"(["a", "aba"])", R"([null, "defgh"])",
                            R"([null, "abcdefg", "e"])", R"([""])"}),
       this->mask_scalar(false), this->array("[]"),
       this->chunked_array({R"([null])", R"(["a", "aba"])", R"([null, "defgh"])",
                            R"([null, "abcdefg", "e"])", R"([""])"})},
      {this->chunked_array({R"([])", R"([null])", R"(["a", "aba"])", R"([null, "defgh"])",
                            R"([null, "abcdefg", "e"])", R"([""])"}),
       this->null_mask_scalar(), this->array("[]"),
       this->chunked_array(
           {"[null]", "[null, null]", "[null, null]", "[null, null, null]", "[null]"})},
  };

  for (auto test_case : cases) {
    this->Assert(ReplaceWithMask, test_case.input, test_case.mask, test_case.replacements,
                 test_case.expected);
  }
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
      [](std::optional<bool> value) { return value.has_value() && *value; });
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

template <typename T>
class TestFillNullNumeric : public TestReplaceKernel<T> {
 protected:
  std::shared_ptr<DataType> type() override { return default_type_instance<T>(); }
};
template <typename T>
class TestFillNullDecimal : public TestReplaceKernel<T> {
 protected:
  std::shared_ptr<DataType> type() override { return default_type_instance<T>(); }
};
template <typename T>
class TestFillNullBinary : public TestReplaceKernel<T> {
 protected:
  std::shared_ptr<DataType> type() override { return default_type_instance<T>(); }
};

class TestFillNullType : public TestReplaceKernel<NullType> {
  std::shared_ptr<DataType> type() override { return default_type_instance<NullType>(); }
};

TYPED_TEST_SUITE(TestFillNullNumeric, NumericBasedTypes);
TYPED_TEST_SUITE(TestFillNullDecimal, DecimalArrowTypes);
TYPED_TEST_SUITE(TestFillNullBinary, BaseBinaryArrowTypes);

TYPED_TEST(TestFillNullNumeric, FillNullValuesForward) {
  this->AssertFillNullArray(FillNullForward, this->array("[]"), this->array("[]"));

  this->AssertFillNullArray(FillNullForward, this->array("[null, null, null, null]"),
                            this->array("[null, null, null, null]"));

  if (std::is_same<TypeParam, Date64Type>::value) {
    this->AssertFillNullArray(FillNullForward,
                              this->array("[null, null, null, 345600000]"),
                              this->array("[null, null, null, 345600000]"));
    this->AssertFillNullArray(FillNullForward, this->array("[null, 345600000, null]"),
                              this->array("[null, 345600000, 345600000]"));
    this->AssertFillNullArray(FillNullForward,
                              this->array("[null, null, 345600000, null]"),
                              this->array("[null, null, 345600000, 345600000]"));
    this->AssertFillNullArray(FillNullForward,
                              this->array("[null, null, null, 345600000, null]"),
                              this->array("[null, null, null, 345600000, 345600000]"));
    this->AssertFillNullArray(
        FillNullForward, this->array("[null, null, 345600000, null, 432000000, null]"),
        this->array("[null, null, 345600000, 345600000, 432000000, 432000000]"));

    this->AssertFillNullArray(FillNullForward, this->array("[86400000, 345600000, null]"),
                              this->array("[86400000, 345600000, 345600000]"));
    this->AssertFillNullArray(
        FillNullForward, this->array("[86400000, 345600000, null, null, null, null]"),
        this->array("[86400000, 345600000 ,345600000, 345600000, 345600000, 345600000]"));
    this->AssertFillNullArray(
        FillNullForward,
        this->array("[86400000, 345600000, null, 432000000, null, null]"),
        this->array("[86400000, 345600000 ,345600000, 432000000, 432000000, 432000000]"));
    this->AssertFillNullArray(
        FillNullForward,
        this->array("[86400000, 345600000, null, 432000000, null, null, 518400000]"),
        this->array("[86400000, 345600000 ,345600000, 432000000, 432000000, 432000000, "
                    "518400000]"));
    this->AssertFillNullArray(
        FillNullForward,
        this->array("[86400000, 345600000, null, 432000000, null, null, 432000000]"),
        this->array("[86400000, 345600000 ,345600000, 432000000, 432000000, 432000000, "
                    "432000000]"));
    this->AssertFillNullArray(
        FillNullForward,
        this->array("[86400000, 345600000, null, 432000000, null, 518400000, null]"),
        this->array("[86400000, 345600000 ,345600000, 432000000, 432000000, 518400000, "
                    "518400000]"));
    this->AssertFillNullArray(
        FillNullForward,
        this->array("[86400000, 345600000, null, 432000000, null, 518400000, 604800000]"),
        this->array("[86400000, 345600000 ,345600000, 432000000, 432000000, 518400000, "
                    "604800000]"));
    this->AssertFillNullArray(
        FillNullForward,
        this->array("[86400000, 345600000 ,345600000, 432000000, 432000000, 518400000, "
                    "604800000]"),
        this->array("[86400000, 345600000 ,345600000, 432000000, 432000000, 518400000, "
                    "604800000]"));
  } else {
    this->AssertFillNullArray(FillNullForward, this->array("[null, null, null, 4]"),
                              this->array("[null, null, null, 4]"));
    this->AssertFillNullArray(FillNullForward, this->array("[null, 4, null]"),
                              this->array("[null, 4, 4]"));
    this->AssertFillNullArray(FillNullForward, this->array("[null, null, 4, null]"),
                              this->array("[null, null, 4, 4]"));
    this->AssertFillNullArray(FillNullForward, this->array("[null, null, null, 4, null]"),
                              this->array("[null, null, null, 4, 4]"));
    this->AssertFillNullArray(FillNullForward,
                              this->array("[null, null, 4,null, 5, null]"),
                              this->array("[null, null, 4, 4, 5, 5]"));

    this->AssertFillNullArray(FillNullForward, this->array("[1,4,null]"),
                              this->array("[1,4,4]"));
    this->AssertFillNullArray(FillNullForward,
                              this->array("[1, 4, null, null, null, null]"),
                              this->array("[1, 4 ,4, 4, 4, 4]"));
    this->AssertFillNullArray(FillNullForward, this->array("[1, 4, null, 5, null, null]"),
                              this->array("[1, 4 ,4, 5, 5, 5]"));
    this->AssertFillNullArray(FillNullForward,
                              this->array("[1, 4, null, 5, null, null, 6]"),
                              this->array("[1, 4 ,4, 5, 5, 5, 6]"));
    this->AssertFillNullArray(FillNullForward,
                              this->array("[1, 4, null, 5, null, null, 5]"),
                              this->array("[1, 4 ,4, 5, 5, 5, 5]"));
    this->AssertFillNullArray(FillNullForward,
                              this->array("[1, 4, null, 5, null, 6, null]"),
                              this->array("[1, 4 ,4, 5, 5, 6, 6]"));
    this->AssertFillNullArray(FillNullForward, this->array("[1, 4, null, 5, null, 6, 7]"),
                              this->array("[1, 4 ,4, 5, 5, 6, 7]"));
    this->AssertFillNullArray(FillNullForward, this->array("[1, 4 ,4, 5, 5, 6, 7]"),
                              this->array("[1, 4 ,4, 5, 5, 6, 7]"));
  }
}

TYPED_TEST(TestFillNullDecimal, FillNullValuesForward) {
  this->AssertFillNullArray(FillNullForward, this->array(R"([])"), this->array(R"([])"));

  this->AssertFillNullArray(FillNullForward, this->array(R"([null, null, null, null])"),
                            this->array(R"([null, null, null, null])"));
  this->AssertFillNullArray(FillNullForward,
                            this->array(R"([null, null, null, "30.00"])"),
                            this->array(R"([null, null, null, "30.00"])"));
  this->AssertFillNullArray(FillNullForward, this->array(R"([null, "30.00", null])"),
                            this->array(R"([null, "30.00", "30.00"])"));
  this->AssertFillNullArray(FillNullForward,
                            this->array(R"([null, null, "30.00", null])"),
                            this->array(R"([null, null, "30.00", "30.00"])"));
  this->AssertFillNullArray(FillNullForward,
                            this->array(R"([null, null, null, "30.00", null])"),
                            this->array(R"([null, null, null, "30.00", "30.00"])"));
  this->AssertFillNullArray(
      FillNullForward, this->array(R"([null, null, "30.00",null, "5.00", null])"),
      this->array(R"([null, null, "30.00", "30.00", "5.00", "5.00"])"));

  this->AssertFillNullArray(FillNullForward, this->array(R"(["10.00","30.00",null])"),
                            this->array(R"(["10.00","30.00","30.00"])"));
  this->AssertFillNullArray(
      FillNullForward, this->array(R"(["10.00", "30.00", null, null, null, null])"),
      this->array(R"(["10.00", "30.00" ,"30.00", "30.00", "30.00", "30.00"])"));
  this->AssertFillNullArray(
      FillNullForward, this->array(R"(["10.00", "30.00", null, "5.00", null, null])"),
      this->array(R"(["10.00", "30.00" ,"30.00", "5.00", "5.00", "5.00"])"));
  this->AssertFillNullArray(
      FillNullForward,
      this->array(R"(["10.00", "30.00", null, "5.00", null, null, "6.00"])"),
      this->array(R"(["10.00", "30.00" ,"30.00", "5.00", "5.00", "5.00", "6.00"])"));
  this->AssertFillNullArray(
      FillNullForward,
      this->array(R"(["10.00", "30.00", null, "5.00", null, null, "5.00"])"),
      this->array(R"(["10.00", "30.00" ,"30.00", "5.00", "5.00", "5.00", "5.00"])"));
  this->AssertFillNullArray(
      FillNullForward,
      this->array(R"(["10.00", "30.00", null, "5.00", null, "6.00", null])"),
      this->array(R"(["10.00", "30.00" ,"30.00", "5.00", "5.00", "6.00", "6.00"])"));
  this->AssertFillNullArray(
      FillNullForward,
      this->array(R"(["10.00", "30.00", null, "5.00", null, "6.00", "7.00"])"),
      this->array(R"(["10.00", "30.00" ,"30.00", "5.00", "5.00", "6.00", "7.00"])"));
  this->AssertFillNullArray(
      FillNullForward,
      this->array(R"(["10.00", "30.00" ,"30.00", "5.00", "5.00", "6.00", "7.00"])"),
      this->array(R"(["10.00", "30.00" ,"30.00", "5.00", "5.00", "6.00", "7.00"])"));
}

TYPED_TEST(TestFillNullBinary, FillNullValuesForward) {
  this->AssertFillNullArray(FillNullForward, this->array(R"([])"), this->array(R"([])"));

  this->AssertFillNullArray(FillNullForward, this->array(R"([null, null, null, null])"),
                            this->array(R"([null, null, null, null])"));
  this->AssertFillNullArray(FillNullForward, this->array(R"([null, null, null, "ccc"])"),
                            this->array(R"([null, null, null, "ccc"])"));
  this->AssertFillNullArray(FillNullForward, this->array(R"([null, "ccc", null])"),
                            this->array(R"([null, "ccc", "ccc"])"));
  this->AssertFillNullArray(FillNullForward, this->array(R"([null, null, "ccc", null])"),
                            this->array(R"([null, null, "ccc", "ccc"])"));
  this->AssertFillNullArray(FillNullForward,
                            this->array(R"([null, null, null, "ccc", null])"),
                            this->array(R"([null, null, null, "ccc", "ccc"])"));
  this->AssertFillNullArray(FillNullForward,
                            this->array(R"([null, null, "ccc",null, "xyz", null])"),
                            this->array(R"([null, null, "ccc", "ccc", "xyz", "xyz"])"));

  this->AssertFillNullArray(FillNullForward, this->array(R"(["aaa","ccc",null])"),
                            this->array(R"(["aaa","ccc","ccc"])"));
  this->AssertFillNullArray(FillNullForward,
                            this->array(R"(["aaa", "ccc", null, null, null, null])"),
                            this->array(R"(["aaa", "ccc" ,"ccc", "ccc", "ccc", "ccc"])"));
  this->AssertFillNullArray(FillNullForward,
                            this->array(R"(["aaa", "ccc", null, "xyz", null, null])"),
                            this->array(R"(["aaa", "ccc" ,"ccc", "xyz", "xyz", "xyz"])"));
  this->AssertFillNullArray(
      FillNullForward, this->array(R"(["aaa", "ccc", null, "xyz", null, null, "qwert"])"),
      this->array(R"(["aaa", "ccc" ,"ccc", "xyz", "xyz", "xyz", "qwert"])"));
  this->AssertFillNullArray(
      FillNullForward, this->array(R"(["aaa", "ccc", null, "xyz", null, null, "xyz"])"),
      this->array(R"(["aaa", "ccc" ,"ccc", "xyz", "xyz", "xyz", "xyz"])"));
  this->AssertFillNullArray(
      FillNullForward, this->array(R"(["aaa", "ccc", null, "xyz", null, "qwert", null])"),
      this->array(R"(["aaa", "ccc" ,"ccc", "xyz", "xyz", "qwert", "qwert"])"));
  this->AssertFillNullArray(
      FillNullForward, this->array(R"(["aaa", "ccc", null, "xyz", null, "qwert", "uy"])"),
      this->array(R"(["aaa", "ccc" ,"ccc", "xyz", "xyz", "qwert", "uy"])"));
  this->AssertFillNullArray(
      FillNullForward,
      this->array(R"(["aaa", "ccc" ,"ccc", "xyz", "xyz", "qwert", "uy"])"),
      this->array(R"(["aaa", "ccc" ,"ccc", "xyz", "xyz", "qwert", "uy"])"));
}

TYPED_TEST(TestFillNullNumeric, FillNullValuesBackward) {
  this->AssertFillNullArray(FillNullBackward, this->array("[]"), this->array("[]"));
  this->AssertFillNullArray(FillNullBackward, this->array("[null, null, null, null]"),
                            this->array("[null, null, null, null]"));

  if (std::is_same<TypeParam, Date64Type>::value) {
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[null, 345600000, null, null, null]"),
                              this->array("[345600000, 345600000,null, null, null]"));
    this->AssertFillNullArray(
        FillNullBackward, this->array("[null, null, null, 345600000]"),
        this->array("[345600000, 345600000, 345600000, 345600000]"));
    this->AssertFillNullArray(FillNullBackward, this->array("[null, 345600000, null]"),
                              this->array("[345600000, 345600000, null]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[null, null, 345600000, null]"),
                              this->array("[345600000, 345600000, 345600000, null]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[null, null, null, 345600000, null]"),
                              this->array("[345600000, 345600000, 345600000, 345600000, "
                                          "null]"));
    this->AssertFillNullArray(
        FillNullBackward, this->array("[null, null, 345600000,null, 432000000, null]"),
        this->array("[345600000, 345600000, 345600000, 432000000, "
                    "432000000, null]"));

    this->AssertFillNullArray(FillNullBackward,
                              this->array("[86400000, 345600000, null]"),
                              this->array("[86400000, 345600000, null]"));
    this->AssertFillNullArray(
        FillNullBackward, this->array("[86400000, 345600000, null, null, null, null]"),
        this->array("[86400000, 345600000 ,null, null, null, null]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[86400000, 345600000, null, 432000000, null, "
                                          "null]"),
                              this->array("[86400000, 345600000 , 432000000, 432000000, "
                                          "null, null]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[86400000, 345600000, null, 432000000, null,"
                                          "null, 518400000]"),
                              this->array("[86400000, 345600000 ,432000000, 432000000, "
                                          "518400000, 518400000, 518400000]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[86400000, 345600000, null, 432000000, null, "
                                          "null, 432000000]"),
                              this->array("[86400000, 345600000 ,432000000 , 432000000, "
                                          "432000000, 432000000, 432000000]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[86400000, 345600000, null, 432000000, null, "
                                          "518400000, null]"),
                              this->array("[86400000, 345600000 ,432000000 , 432000000, "
                                          "518400000, 518400000, null]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[86400000, 345600000, null, 432000000, null, "
                                          "518400000, 604800000]"),
                              this->array("[86400000, 345600000 ,432000000, 432000000, "
                                          "518400000, 518400000, 604800000]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[86400000, 345600000, 432000000, 432000000, "
                                          "518400000, 518400000, 604800000]"),
                              this->array("[86400000, 345600000 ,432000000, 432000000, "
                                          "518400000, 518400000, 604800000]"));
  } else {
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[null, 4, null, null, null]"),
                              this->array("[4, 4,null, null, null]"));
    this->AssertFillNullArray(FillNullBackward, this->array("[null, null, null, 4]"),
                              this->array("[4, 4, 4, 4]"));
    this->AssertFillNullArray(FillNullBackward, this->array("[null, 4, null]"),
                              this->array("[4, 4, null]"));
    this->AssertFillNullArray(FillNullBackward, this->array("[null, null, 4, null]"),
                              this->array("[4, 4, 4, null]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[null, null, null, 4, null]"),
                              this->array("[4, 4, 4, 4, null]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[null, null, 4,null, 5, null]"),
                              this->array("[4, 4, 4, 5, 5, null]"));

    this->AssertFillNullArray(FillNullBackward, this->array("[1, 4, null]"),
                              this->array("[1, 4, null]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[1, 4, null, null, null, null]"),
                              this->array("[1, 4 ,null, null, null, null]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[1, 4, null, 5, null, null]"),
                              this->array("[1, 4 , 5, 5, null, null]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[1, 4, null, 5, null, null, 6]"),
                              this->array("[1, 4 ,5, 5, 6, 6, 6]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[1, 4, null, 5, null, null, 5]"),
                              this->array("[1, 4 ,5 , 5, 5, 5, 5]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[1, 4, null, 5, null, 6, null]"),
                              this->array("[1, 4 ,5 , 5, 6, 6, null]"));
    this->AssertFillNullArray(FillNullBackward,
                              this->array("[1, 4, null, 5, null, 6, 7]"),
                              this->array("[1, 4 ,5, 5, 6, 6, 7]"));
    this->AssertFillNullArray(FillNullBackward, this->array("[1, 4 ,5, 5, 6, 6, 7]"),
                              this->array("[1, 4 ,5, 5, 6, 6, 7]"));
  }
}

TYPED_TEST(TestFillNullDecimal, FillNullValuesBackward) {
  this->AssertFillNullArray(FillNullBackward, this->array(R"([])"), this->array(R"([])"));

  this->AssertFillNullArray(FillNullBackward, this->array(R"([null, null, null, null])"),
                            this->array(R"([null, null, null, null])"));
  this->AssertFillNullArray(FillNullBackward,
                            this->array(R"([null, "40.00", null, null, null])"),
                            this->array(R"(["40.00", "40.00",null, null, null])"));
  this->AssertFillNullArray(FillNullBackward,
                            this->array(R"([null, null, null, "40.00"])"),
                            this->array(R"(["40.00", "40.00", "40.00", "40.00"])"));
  this->AssertFillNullArray(FillNullBackward, this->array(R"([null, "40.00", null])"),
                            this->array(R"(["40.00", "40.00", null])"));
  this->AssertFillNullArray(FillNullBackward,
                            this->array(R"([null, null, "40.00", null])"),
                            this->array(R"(["40.00", "40.00", "40.00", null])"));
  this->AssertFillNullArray(FillNullBackward,
                            this->array(R"([null, null, null, "40.00", null])"),
                            this->array(R"(["40.00", "40.00", "40.00", "40.00", null])"));
  this->AssertFillNullArray(
      FillNullBackward, this->array(R"([null, null, "40.00",null, "50.00", null])"),
      this->array(R"(["40.00", "40.00", "40.00", "50.00", "50.00", null])"));

  this->AssertFillNullArray(FillNullBackward, this->array(R"(["10.00", "40.00", null])"),
                            this->array(R"(["10.00", "40.00", null])"));
  this->AssertFillNullArray(FillNullBackward,
                            this->array(R"(["10.00", "40.00", null, null, null, null])"),
                            this->array(R"(["10.00", "40.00" ,null, null, null, null])"));
  this->AssertFillNullArray(
      FillNullBackward, this->array(R"(["10.00", "40.00", null, "50.00", null, null])"),
      this->array(R"(["10.00", "40.00" , "50.00", "50.00", null, null])"));
  this->AssertFillNullArray(
      FillNullBackward,
      this->array(R"(["10.00", "40.00", null, "50.00", null, null, "6.00"])"),
      this->array(R"(["10.00", "40.00" ,"50.00", "50.00", "6.00", "6.00", "6.00"])"));
  this->AssertFillNullArray(
      FillNullBackward,
      this->array(R"(["10.00", "40.00", null, "50.00", null, null, "50.00"])"),
      this->array(R"(["10.00", "40.00" ,"50.00" , "50.00", "50.00", "50.00", "50.00"])"));
  this->AssertFillNullArray(
      FillNullBackward,
      this->array(R"(["10.00", "40.00", null, "50.00", null, "6.00", null])"),
      this->array(R"(["10.00", "40.00" ,"50.00" , "50.00", "6.00", "6.00", null])"));
  this->AssertFillNullArray(
      FillNullBackward,
      this->array(R"(["10.00", "40.00", null, "50.00", null, "6.00", "7.00"])"),
      this->array(R"(["10.00", "40.00" ,"50.00", "50.00", "6.00", "6.00", "7.00"])"));
  this->AssertFillNullArray(
      FillNullBackward,
      this->array(R"(["10.00", "40.00" ,"50.00", "50.00", "6.00", "6.00", "7.00"])"),
      this->array(R"(["10.00", "40.00" ,"50.00", "50.00", "6.00", "6.00", "7.00"])"));
}

TYPED_TEST(TestFillNullBinary, FillNullValuesBackward) {
  this->AssertFillNullArray(FillNullBackward, this->array(R"([])"), this->array(R"([])"));

  this->AssertFillNullArray(FillNullBackward, this->array(R"([null, null, null, null])"),
                            this->array(R"([null, null, null, null])"));
  this->AssertFillNullArray(FillNullBackward,
                            this->array(R"([null, "afd", null, null, null])"),
                            this->array(R"(["afd", "afd",null, null, null])"));
  this->AssertFillNullArray(FillNullBackward, this->array(R"([null, null, null, "afd"])"),
                            this->array(R"(["afd", "afd", "afd", "afd"])"));
  this->AssertFillNullArray(FillNullBackward, this->array(R"([null, "afd", null])"),
                            this->array(R"(["afd", "afd", null])"));
  this->AssertFillNullArray(FillNullBackward, this->array(R"([null, null, "afd", null])"),
                            this->array(R"(["afd", "afd", "afd", null])"));
  this->AssertFillNullArray(FillNullBackward,
                            this->array(R"([null, null, null, "afd", null])"),
                            this->array(R"(["afd", "afd", "afd", "afd", null])"));
  this->AssertFillNullArray(FillNullBackward,
                            this->array(R"([null, null, "afd",null, "qwe", null])"),
                            this->array(R"(["afd", "afd", "afd", "qwe", "qwe", null])"));

  this->AssertFillNullArray(FillNullBackward, this->array(R"(["tyu", "afd", null])"),
                            this->array(R"(["tyu", "afd", null])"));
  this->AssertFillNullArray(FillNullBackward,
                            this->array(R"(["tyu", "afd", null, null, null, null])"),
                            this->array(R"(["tyu", "afd" ,null, null, null, null])"));
  this->AssertFillNullArray(FillNullBackward,
                            this->array(R"(["tyu", "afd", null, "qwe", null, null])"),
                            this->array(R"(["tyu", "afd" , "qwe", "qwe", null, null])"));
  this->AssertFillNullArray(
      FillNullBackward,
      this->array(R"(["tyu", "afd", null, "qwe", null, null, "oiutyu"])"),
      this->array(R"(["tyu", "afd" ,"qwe", "qwe", "oiutyu", "oiutyu", "oiutyu"])"));
  this->AssertFillNullArray(
      FillNullBackward, this->array(R"(["tyu", "afd", null, "qwe", null, null, "qwe"])"),
      this->array(R"(["tyu", "afd" ,"qwe" , "qwe", "qwe", "qwe", "qwe"])"));
  this->AssertFillNullArray(
      FillNullBackward,
      this->array(R"(["tyu", "afd", null, "qwe", null, "oiutyu", null])"),
      this->array(R"(["tyu", "afd" ,"qwe" , "qwe", "oiutyu", "oiutyu", null])"));
  this->AssertFillNullArray(
      FillNullBackward,
      this->array(R"(["tyu", "afd", null, "qwe", null, "oiutyu", "aaaagggbbb"])"),
      this->array(R"(["tyu", "afd" ,"qwe", "qwe", "oiutyu", "oiutyu", "aaaagggbbb"])"));
  this->AssertFillNullArray(
      FillNullBackward,
      this->array(R"(["tyu", "afd" ,"qwe", "qwe", "oiutyu", "oiutyu", "aaaagggbbb"])"),
      this->array(R"(["tyu", "afd" ,"qwe", "qwe", "oiutyu", "oiutyu", "aaaagggbbb"])"));
}

// For Test Blocks
TYPED_TEST(TestFillNullNumeric, FillNullForwardLargeInput) {
  using CType = typename TypeTraits<TypeParam>::CType;
  random::RandomArrayGenerator rand(/*seed=*/1000);
  int64_t len_null = 500;
  int64_t len_random = 1000;
  std::shared_ptr<Array> array_random =
      rand.ArrayOf(this->type(), len_random, /*nulls=*/0);
  auto x_ptr = array_random->data()->template GetValues<CType>(1);
  ASSERT_OK_AND_ASSIGN(auto array_null, MakeArrayOfNull(array_random->type(), len_null));
  auto array_null_filled =
      ConstantArrayGenerator::Numeric<TypeParam>(len_null, x_ptr[len_random - 1]);
  {
    ASSERT_OK_AND_ASSIGN(auto value_array,
                         Concatenate({array_random, array_null, array_random}));
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         Concatenate({array_random, array_null_filled, array_random}));
    this->AssertFillNullArray(FillNullForward, value_array, result_array);
  }
}

TYPED_TEST(TestFillNullNumeric, FillNullBackwardLargeInput) {
  using CType = typename TypeTraits<TypeParam>::CType;
  random::RandomArrayGenerator rand(/*seed=*/1000);
  int64_t len_null = 500;
  int64_t len_random = 1000;
  std::shared_ptr<Array> array_random =
      rand.ArrayOf(this->type(), len_random, /*nulls=*/0);
  auto x_ptr = array_random->data()->template GetValues<CType>(1);
  ASSERT_OK_AND_ASSIGN(auto array_null, MakeArrayOfNull(array_random->type(), len_null));
  auto array_null_filled = ConstantArrayGenerator::Numeric<TypeParam>(len_null, x_ptr[0]);
  {
    ASSERT_OK_AND_ASSIGN(auto value_array,
                         Concatenate({array_random, array_null, array_random}));
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         Concatenate({array_random, array_null_filled, array_random}));
    this->AssertFillNullArray(FillNullBackward, value_array, result_array);
  }
}

TYPED_TEST(TestFillNullNumeric, FillNullForwardSliced) {
  if (std::is_same<TypeParam, Date64Type>::value) {
    auto first_input_array = this->array(
        "[86400000, 345600000, null, null, 604800000, null, null, 691200000, "
        "777600000, 432000000, null]");
    auto expected_slice_length_2 = this->array(
        "[86400000, 345600000, null, null, 604800000, 604800000, null, "
        "691200000, 777600000, 432000000, null]");
    auto expected_slice_length_3 = this->array(
        "[86400000, 345600000, 345600000, null, 604800000, 604800000, null, "
        "691200000, 777600000, 432000000, 432000000]");
    auto expected_slice_length_4 = this->array(
        "[86400000, 345600000, 345600000, 345600000, 604800000, 604800000, "
        "604800000, 691200000, 777600000, 432000000, 432000000]");
    this->AssertFillNullArraySlices(FillNullForward, first_input_array,
                                    expected_slice_length_2, expected_slice_length_3,
                                    expected_slice_length_4);
  } else {
    auto first_input_array =
        this->array("[1, 4, null, null, 7, null, null, 8, 9, 5, null]");
    auto expected_slice_length_2 =
        this->array("[1, 4, null, null, 7, 7, null, 8, 9, 5, null]");
    auto expected_slice_length_3 = this->array("[1, 4, 4, null, 7, 7, null, 8, 9, 5, 5]");
    auto expected_slice_length_4 = this->array("[1, 4, 4, 4, 7, 7, 7, 8, 9, 5, 5]");
    this->AssertFillNullArraySlices(FillNullForward, first_input_array,
                                    expected_slice_length_2, expected_slice_length_3,
                                    expected_slice_length_4);
  }
}

TYPED_TEST(TestFillNullBinary, FillNullForwardSliced) {
  auto first_input_array = this->array(
      R"(["avb", "iyy", null, null, "mnh", null, null, "ttr", "gfd", "lkj", null])");
  auto expected_slice_length_2 = this->array(
      R"(["avb", "iyy", null, null, "mnh", "mnh", null, "ttr", "gfd", "lkj", null])");
  auto expected_slice_length_3 = this->array(
      R"(["avb", "iyy", "iyy", null, "mnh", "mnh", null, "ttr", "gfd", "lkj", "lkj"])");
  auto expected_slice_length_4 = this->array(
      R"(["avb", "iyy", "iyy", "iyy", "mnh", "mnh", "mnh", "ttr", "gfd", "lkj", "lkj"])");

  this->AssertFillNullArraySlices(FillNullForward, first_input_array,
                                  expected_slice_length_2, expected_slice_length_3,
                                  expected_slice_length_4);
}

TYPED_TEST(TestFillNullNumeric, FillNullBackwardSliced) {
  if (std::is_same<TypeParam, Date64Type>::value) {
    auto first_input_array = this->array(
        "[86400000, 345600000, null, null, 604800000, null, null, 691200000, "
        "777600000, 432000000, null]");
    auto expected_slice_length_2 = this->array(
        "[86400000, 345600000, null, null, 604800000, null, 691200000, "
        "691200000, 777600000, 432000000, null]");
    auto expected_slice_length_3 = this->array(
        "[86400000, 345600000, null, 604800000, 604800000, null, 691200000, "
        "691200000, 777600000, 432000000, null]");
    auto expected_slice_length_4 = this->array(
        "[86400000, 345600000, null, null, 604800000, 691200000, 691200000, "
        "691200000, 777600000, 432000000, null]");

    this->AssertFillNullArraySlices(FillNullBackward, first_input_array,
                                    expected_slice_length_2, expected_slice_length_3,
                                    expected_slice_length_4);
  } else {
    auto first_input_array =
        this->array("[1, 4, null, null, 7, null, null, 8, 9, 5, null]");
    auto expected_slice_length_2 =
        this->array("[1, 4, null, null, 7, null, 8, 8, 9, 5, null]");
    auto expected_slice_length_3 =
        this->array("[1, 4, null, 7, 7, null, 8, 8, 9, 5, null]");
    auto expected_slice_length_4 =
        this->array("[1, 4, null, null, 7, 8, 8, 8, 9, 5, null]");

    this->AssertFillNullArraySlices(FillNullBackward, first_input_array,
                                    expected_slice_length_2, expected_slice_length_3,
                                    expected_slice_length_4);
  }
}

TYPED_TEST(TestFillNullBinary, FillNullBackwardSliced) {
  auto first_input_array = this->array(
      R"(["tre", "kjh", null, null, "mnb", null, null, "yhn", "ode", "qwe", null])");
  auto expected_slice_length_2 = this->array(
      R"(["tre", "kjh", null, null, "mnb", null, "yhn", "yhn", "ode", "qwe", null])");
  auto expected_slice_length_3 = this->array(
      R"(["tre", "kjh", null, "mnb", "mnb", null, "yhn", "yhn", "ode", "qwe", null])");
  auto expected_slice_length_4 = this->array(
      R"(["tre", "kjh", null, null, "mnb", "yhn", "yhn", "yhn", "ode", "qwe", null])");

  this->AssertFillNullArraySlices(FillNullBackward, first_input_array,
                                  expected_slice_length_2, expected_slice_length_3,
                                  expected_slice_length_4);
}

TYPED_TEST(TestFillNullNumeric, FillNullForwardChunkedArray) {
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array(
          {"[null, null, null]", "[null, null, null]", "[null]", "[null, null]"}),
      this->chunked_array(
          {"[null, null, null]", "[null, null, null]", "[null]", "[null, null]"}));

  if (std::is_same<TypeParam, Date64Type>::value) {
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[604800000, null, null]", "[null, null, null]", "[null]", "[null, null]"}),
        this->chunked_array({"[604800000, 604800000, 604800000]",
                             "[604800000, 604800000, 604800000]", "[604800000]",
                             "[604800000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[604800000, null, null]", "[null, null, null]", "[]", "[null, null]"}),
        this->chunked_array({"[604800000, 604800000, 604800000]",
                             "[604800000, 604800000, 604800000]", "[]",
                             "[604800000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[null, null, null]", "[null, null, null]", "[null]", "[null, 604800000]"}),
        this->chunked_array(
            {"[null, null, null]", "[null, null, null]", "[null]", "[null, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[null, null, null]", "[691200000, null, null]", "[null]",
                             "[null, 604800000]"}),
        this->chunked_array({"[null, null, null]", "[691200000, 691200000, 691200000]",
                             "[691200000]", "[691200000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[86400000, 172800000, 259200000, null, null, 345600000]",
                             "[432000000, null, null]", "[null, 604800000, null]"}),
        this->chunked_array(
            {"[86400000, 172800000, 259200000, 259200000, 259200000, 345600000]",
             "[432000000, 432000000, 432000000]", "[432000000, 604800000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[86400000, 172800000, 259200000, null, null, null]",
                             "[null, null, null]", "[null, 604800000, null]"}),
        this->chunked_array(
            {"[86400000, 172800000, 259200000, 259200000, 259200000, 259200000]",
             "[259200000, 259200000, 259200000]", "[259200000, 604800000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[86400000, 172800000, null]", "[null, null, null]",
                             "[null]", "[null, 604800000]"}),
        this->chunked_array({"[86400000, 172800000, 172800000]",
                             "[172800000, 172800000, 172800000]", "[172800000]",
                             "[172800000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[null, 172800000, null]", "[null, null, 259200000]",
                             "[null]", "[null, 604800000]"}),
        this->chunked_array({"[null, 172800000, 172800000]",
                             "[172800000, 172800000, 259200000]", "[259200000]",
                             "[259200000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[null, 172800000, null]", "[345600000, null, 259200000]",
                             "[null]", "[null, 604800000]"}),
        this->chunked_array({"[null, 172800000, 172800000]",
                             "[345600000, 345600000, 259200000]", "[259200000]",
                             "[259200000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[null, null, null]", "[null, null, null]", "[432000000]",
                             "[432000000, 604800000]"}),
        this->chunked_array({"[null, null, null]", "[null, null, null]", "[432000000]",
                             "[432000000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[null, null, 86400000]", "[null, null, null]",
                             "[432000000]", "[432000000, 604800000]"}),
        this->chunked_array({"[null, null, 86400000]", "[86400000, 86400000, 86400000]",
                             "[432000000]", "[432000000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[172800000, 345600000, 86400000]", "[null, null, null]",
                             "[432000000]", "[432000000, 604800000]"}),
        this->chunked_array({"[172800000, 345600000, 86400000]",
                             "[86400000, 86400000, 86400000]", "[432000000]",
                             "[432000000, 604800000]"}));
  } else {
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[7, null, null]", "[null, null, null]", "[null]", "[null, null]"}),
        this->chunked_array({"[7, 7, 7]", "[7, 7, 7]", "[7]", "[7, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[7, null, null]", "[null, null, null]", "[]", "[null, null]"}),
        this->chunked_array({"[7, 7, 7]", "[7, 7, 7]", "[]", "[7, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[null, null, null]", "[null, null, null]", "[null]", "[null, 7]"}),
        this->chunked_array(
            {"[null, null, null]", "[null, null, null]", "[null]", "[null, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[null, null, null]", "[8, null, null]", "[null]", "[null, 7]"}),
        this->chunked_array({"[null, null, null]", "[8, 8, 8]", "[8]", "[8, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[1, 2, 3, null, null, 4]", "[5, null, null]", "[null, 7, null]"}),
        this->chunked_array({"[1, 2, 3, 3, 3, 4]", "[5, 5, 5]", "[5, 7, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[1, 2, 3, null, null, null]", "[null, null, null]", "[null, 7, null]"}),
        this->chunked_array({"[1, 2, 3, 3, 3, 3]", "[3, 3, 3]", "[3, 7, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[1, 2, null]", "[null, null, null]", "[null]", "[null, 7]"}),
        this->chunked_array({"[1, 2, 2]", "[2, 2, 2]", "[2]", "[2, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[null, 2, null]", "[null, null, 3]", "[null]", "[null, 7]"}),
        this->chunked_array({"[null, 2, 2]", "[2, 2, 3]", "[3]", "[3, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[null, 2, null]", "[4, null, 3]", "[null]", "[null, 7]"}),
        this->chunked_array({"[null, 2, 2]", "[4, 4, 3]", "[3]", "[3, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array(
            {"[null, null, null]", "[null, null, null]", "[5]", "[5, 7]"}),
        this->chunked_array(
            {"[null, null, null]", "[null, null, null]", "[5]", "[5, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[null, null, 1]", "[null, null, null]", "[5]", "[5, 7]"}),
        this->chunked_array({"[null, null, 1]", "[1, 1, 1]", "[5]", "[5, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullForward,
        this->chunked_array({"[2, 4, 1]", "[null, null, null]", "[5]", "[5, 7]"}),
        this->chunked_array({"[2, 4, 1]", "[1, 1, 1]", "[5]", "[5, 7]"}));
  }
}

TYPED_TEST(TestFillNullBinary, FillNullForwardChunkedArray) {
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"([null, null, null])", R"([null, null, null])", R"([null])",
                           R"([null, null])"}),
      this->chunked_array({R"([null, null, null])", R"([null, null, null])", R"([null])",
                           R"([null, null])"}));
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"(["utrmnbv", null, null])", R"([null, null, null])",
                           R"([null])", R"([null, null])"}),
      this->chunked_array({R"(["utrmnbv", "utrmnbv", "utrmnbv"])",
                           R"(["utrmnbv", "utrmnbv", "utrmnbv"])", R"(["utrmnbv"])",
                           R"(["utrmnbv", "utrmnbv"])"}));
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"([null, null, null])", R"([null, null, null])", R"([null])",
                           R"([null, "utrmnbv"])"}),
      this->chunked_array({R"([null, null, null])", R"([null, null, null])", R"([null])",
                           R"([null, "utrmnbv"])"}));
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"([null, null, null])", R"(["mnb", null, null])", R"([null])",
                           R"([null, "utrmnbv"])"}),
      this->chunked_array({R"([null, null, null])", R"(["mnb", "mnb", "mnb"])",
                           R"(["mnb"])", R"(["mnb", "utrmnbv"])"}));
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"(["aty", "yhh", "puy", null, null, "rwq"])",
                           R"(["utr", null, null])", R"([null, "utrmnbv", null])"}),
      this->chunked_array({R"(["aty", "yhh", "puy", "puy", "puy", "rwq"])",
                           R"(["utr", "utr", "utr"])",
                           R"(["utr", "utrmnbv", "utrmnbv"])"}));
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"(["aty", "yhh", "puy", null, null, null])",
                           R"([null, null, null])", R"([null, "utrmnbv", null])"}),
      this->chunked_array({R"(["aty", "yhh", "puy", "puy", "puy", "puy"])",
                           R"(["puy", "puy", "puy"])",
                           R"(["puy", "utrmnbv", "utrmnbv"])"}));
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"(["aty", "yhh", null])", R"([null, null, null])",
                           R"([null])", R"([null, "utrmnbv"])"}),
      this->chunked_array({R"(["aty", "yhh", "yhh"])", R"(["yhh", "yhh", "yhh"])",
                           R"(["yhh"])", R"(["yhh", "utrmnbv"])"}));
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"([null, "yhh", null])", R"([null, null, "puy"])",
                           R"([null])", R"([null, "utrmnbv"])"}),
      this->chunked_array({R"([null, "yhh", "yhh"])", R"(["yhh", "yhh", "puy"])",
                           R"(["puy"])", R"(["puy", "utrmnbv"])"}));
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"([null, "yhh", null])", R"(["rwq", null, "puy"])",
                           R"([null])", R"([null, "utrmnbv"])"}),
      this->chunked_array({R"([null, "yhh", "yhh"])", R"(["rwq", "rwq", "puy"])",
                           R"(["puy"])", R"(["puy", "utrmnbv"])"}));
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"([null, null, null])", R"([null, null, null])", R"(["utr"])",
                           R"(["utr", "utrmnbv"])"}),
      this->chunked_array({R"([null, null, null])", R"([null, null, null])", R"(["utr"])",
                           R"(["utr", "utrmnbv"])"}));
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"([null, null, "aty"])", R"([null, null, null])",
                           R"(["utr"])", R"(["utr", "utrmnbv"])"}),
      this->chunked_array({R"([null, null, "aty"])", R"(["aty", "aty", "aty"])",
                           R"(["utr"])", R"(["utr", "utrmnbv"])"}));
  this->AssertFillNullChunkedArray(
      FillNullForward,
      this->chunked_array({R"(["rt", "tr", "aty"])", R"([null, null, null])",
                           R"(["utr"])", R"(["utr", "utrmnbv"])"}),
      this->chunked_array({R"(["rt", "tr", "aty"])", R"(["aty", "aty", "aty"])",
                           R"(["utr"])", R"(["utr", "utrmnbv"])"}));
}

TYPED_TEST(TestFillNullNumeric, FillBackwardChunkedArray) {
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array(
          {"[null, null, null]", "[null, null, null]", "[null]", "[null, null]"}),
      this->chunked_array(
          {"[null, null, null]", "[null, null, null]", "[null]", "[null, null]"}));

  if (std::is_same<TypeParam, Date64Type>::value) {
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array(
            {"[604800000, null, null]", "[null, null, null]", "[null]", "[null, null]"}),
        this->chunked_array(
            {"[604800000, null, null]", "[null, null, null]", "[null]", "[null, null]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array(
            {"[null, null, null]", "[null, null, null]", "[null]", "[null, 604800000]"}),
        this->chunked_array({"[604800000, 604800000, 604800000]",
                             "[604800000, 604800000, 604800000]", "[604800000]",
                             "[604800000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array({"[null, null, null]", "[691200000, null, null]", "[null]",
                             "[null, 604800000]"}),
        this->chunked_array({"[691200000, 691200000, 691200000]",
                             "[691200000, 604800000, 604800000]", "[604800000]",
                             "[604800000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array({"[86400000, 172800000, 259200000, null, null, 345600000]",
                             "[432000000, null, null]", "[null, 604800000, null]"}),
        this->chunked_array(
            {"[86400000, 172800000, 259200000, 345600000, 345600000, 345600000]",
             "[432000000, 604800000, 604800000]", "[604800000, 604800000, null]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array({"[86400000, 172800000, 259200000, null, null, null]",
                             "[null, null, null]", "[null, 604800000, null]"}),
        this->chunked_array(
            {"[86400000, 172800000, 259200000, 604800000, 604800000, 604800000]",
             "[604800000, 604800000, 604800000]", "[604800000, 604800000, null]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array({"[86400000, 172800000, null]", "[null, null, null]",
                             "[null]", "[null, 604800000]"}),
        this->chunked_array({"[86400000, 172800000, 604800000]",
                             "[604800000, 604800000, 604800000]", "[604800000]",
                             "[604800000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array({"[null, 172800000, null]", "[null, null, 259200000]",
                             "[null]", "[null, 604800000]"}),
        this->chunked_array({"[172800000, 172800000, 259200000]",
                             "[259200000, 259200000, 259200000]", "[604800000]",
                             "[604800000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array({"[null, 172800000, null]", "[345600000, null, 259200000]",
                             "[null]", "[null, 604800000]"}),
        this->chunked_array({"[172800000, 172800000, 345600000]",
                             "[345600000, 259200000, 259200000]", "[604800000]",
                             "[604800000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array({"[null, null, null]", "[null, null, null]", "[432000000]",
                             "[432000000, 604800000]"}),
        this->chunked_array({"[432000000, 432000000, 432000000]",
                             "[432000000, 432000000, 432000000]", "[432000000]",
                             "[432000000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array({"[null, null, 86400000]", "[null, null, null]",
                             "[432000000]", "[432000000, 604800000]"}),
        this->chunked_array({"[86400000, 86400000, 86400000]",
                             "[432000000, 432000000, 432000000]", "[432000000]",
                             "[432000000, 604800000]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array({"[null, null, 86400000]", "[null, null, null]",
                             "[777600000, 0]", "[432000000, 604800000]"}),
        this->chunked_array({"[86400000, 86400000, 86400000]",
                             "[777600000, 777600000, 777600000]", "[777600000, 0]",
                             "[432000000, 604800000]"}));
  } else {
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array(
            {"[7, null, null]", "[null, null, null]", "[null]", "[null, null]"}),
        this->chunked_array(
            {"[7, null, null]", "[null, null, null]", "[null]", "[null, null]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array(
            {"[null, null, null]", "[null, null, null]", "[null]", "[null, 7]"}),
        this->chunked_array({"[7, 7, 7]", "[7, 7, 7]", "[7]", "[7, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array(
            {"[null, null, null]", "[8, null, null]", "[null]", "[null, 7]"}),
        this->chunked_array({"[8, 8, 8]", "[8, 7, 7]", "[7]", "[7, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array(
            {"[1, 2, 3, null, null, 4]", "[5, null, null]", "[null, 7, null]"}),
        this->chunked_array({"[1, 2, 3, 4, 4, 4]", "[5, 7, 7]", "[7, 7, null]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array(
            {"[1, 2, 3, null, null, null]", "[null, null, null]", "[null, 7, null]"}),
        this->chunked_array({"[1, 2, 3, 7, 7, 7]", "[7, 7, 7]", "[7, 7, null]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array(
            {"[1, 2, null]", "[null, null, null]", "[null]", "[null, 7]"}),
        this->chunked_array({"[1, 2, 7]", "[7, 7, 7]", "[7]", "[7, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array(
            {"[null, 2, null]", "[null, null, 3]", "[null]", "[null, 7]"}),
        this->chunked_array({"[2, 2, 3]", "[3, 3, 3]", "[7]", "[7, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array({"[null, 2, null]", "[4, null, 3]", "[null]", "[null, 7]"}),
        this->chunked_array({"[2, 2, 4]", "[4, 3, 3]", "[7]", "[7, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array(
            {"[null, null, null]", "[null, null, null]", "[5]", "[5, 7]"}),
        this->chunked_array({"[5, 5, 5]", "[5, 5, 5]", "[5]", "[5, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array({"[null, null, 1]", "[null, null, null]", "[5]", "[5, 7]"}),
        this->chunked_array({"[1, 1, 1]", "[5, 5, 5]", "[5]", "[5, 7]"}));
    this->AssertFillNullChunkedArray(
        FillNullBackward,
        this->chunked_array(
            {"[null, null, 1]", "[null, null, null]", "[9, 0]", "[5, 7]"}),
        this->chunked_array({"[1, 1, 1]", "[9, 9, 9]", "[9, 0]", "[5, 7]"}));
  }
}

TYPED_TEST(TestFillNullBinary, FillBackwardChunkedArray) {
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array({R"([null, null, null])", R"([null, null, null])", R"([null])",
                           R"([null, null])"}),
      this->chunked_array({R"([null, null, null])", R"([null, null, null])", R"([null])",
                           R"([null, null])"}));
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array({R"(["mnz", null, null])", R"([null, null, null])", R"([null])",
                           R"([null, null])"}),
      this->chunked_array({R"(["mnz", null, null])", R"([null, null, null])", R"([null])",
                           R"([null, null])"}));
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array({R"([null, null, null])", R"([null, null, null])", R"([null])",
                           R"([null, "mnz"])"}),
      this->chunked_array({R"(["mnz", "mnz", "mnz"])", R"(["mnz", "mnz", "mnz"])",
                           R"(["mnz"])", R"(["mnz", "mnz"])"}));
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array({R"([null, null, null])", R"(["qwer", null, null])",
                           R"([null])", R"([null, "mnz"])"}),
      this->chunked_array({R"(["qwer", "qwer", "qwer"])", R"(["qwer", "mnz", "mnz"])",
                           R"(["mnz"])", R"(["mnz", "mnz"])"}));
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array({R"(["tre", "vda", "bvf", null, null, "poi"])",
                           R"(["qup", null, null])", R"([null, "mnz", null])"}),
      this->chunked_array({R"(["tre", "vda", "bvf", "poi", "poi", "poi"])",
                           R"(["qup", "mnz", "mnz"])", R"(["mnz", "mnz", null])"}));
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array({R"(["tre", "vda", "bvf", null, null, null])",
                           R"([null, null, null])", R"([null, "mnz", null])"}),
      this->chunked_array({R"(["tre", "vda", "bvf", "mnz", "mnz", "mnz"])",
                           R"(["mnz", "mnz", "mnz"])", R"(["mnz", "mnz", null])"}));
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array({R"(["tre", "vda", null])", R"([null, null, null])",
                           R"([null])", R"([null, "mnz"])"}),
      this->chunked_array({R"(["tre", "vda", "mnz"])", R"(["mnz", "mnz", "mnz"])",
                           R"(["mnz"])", R"(["mnz", "mnz"])"}));
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array({R"([null, "vda", null])", R"([null, null, "bvf"])",
                           R"([null])", R"([null, "mnz"])"}),
      this->chunked_array({R"(["vda", "vda", "bvf"])", R"(["bvf", "bvf", "bvf"])",
                           R"(["mnz"])", R"(["mnz", "mnz"])"}));
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array({R"([null, "vda", null])", R"(["poi", null, "bvf"])",
                           R"([null])", R"([null, "mnz"])"}),
      this->chunked_array({R"(["vda", "vda", "poi"])", R"(["poi", "bvf", "bvf"])",
                           R"(["mnz"])", R"(["mnz", "mnz"])"}));
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array({R"([null, null, null])", R"([null, null, null])", R"(["qup"])",
                           R"(["qup", "mnz"])"}),
      this->chunked_array({R"(["qup", "qup", "qup"])", R"(["qup", "qup", "qup"])",
                           R"(["qup"])", R"(["qup", "mnz"])"}));
  this->AssertFillNullChunkedArray(
      FillNullBackward,
      this->chunked_array({R"([null, null, "tre"])", R"([null, null, null])",
                           R"(["qup"])", R"(["qup", "mnz"])"}),
      this->chunked_array({R"(["tre", "tre", "tre"])", R"(["qup", "qup", "qup"])",
                           R"(["qup"])", R"(["qup", "mnz"])"}));
}

TEST_F(TestFillNullType, TestFillOnNullType) {
  this->AssertFillNullArray(FillNullForward, this->array(R"([null, null, null, null])"),
                            this->array(R"([null, null, null, null])"));
  this->AssertFillNullArray(FillNullForward, this->array(R"([null])"),
                            this->array(R"([null])"));
  this->AssertFillNullArray(FillNullForward, this->array(R"([null, null])"),
                            this->array(R"([null, null])"));
  this->AssertFillNullArray(FillNullBackward, this->array(R"([null, null, null, null])"),
                            this->array(R"([null, null, null, null])"));
  this->AssertFillNullArray(FillNullBackward, this->array(R"([null])"),
                            this->array(R"([null])"));
  this->AssertFillNullArray(FillNullBackward, this->array(R"([null, null])"),
                            this->array(R"([null, null])"));
}
}  // namespace compute
}  // namespace arrow
