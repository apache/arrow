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
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/concatenate.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/util/rle_util.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using std::string_view;

namespace compute {

// ----------------------------------------------------------------------

TEST(GetTakeIndices, Basics) {
  auto CheckCase = [&](const std::string& filter_json, const std::string& indices_json,
                       FilterOptions::NullSelectionBehavior null_selection,
                       const std::shared_ptr<DataType>& indices_type = uint16()) {
    auto filter = ArrayFromJSON(boolean(), filter_json);
    auto expected_indices = ArrayFromJSON(indices_type, indices_json);
    ASSERT_OK_AND_ASSIGN(auto indices,
                         internal::GetTakeIndices(*filter->data(), null_selection));
    auto indices_array = MakeArray(indices);
    ValidateOutput(indices);
    AssertArraysEqual(*expected_indices, *indices_array, /*verbose=*/true);
  };

  // Drop null cases
  CheckCase("[]", "[]", FilterOptions::DROP);
  CheckCase("[null]", "[]", FilterOptions::DROP);
  CheckCase("[null, false, true, true, false, true]", "[2, 3, 5]", FilterOptions::DROP);

  // Emit null cases
  CheckCase("[]", "[]", FilterOptions::EMIT_NULL);
  CheckCase("[null]", "[null]", FilterOptions::EMIT_NULL);
  CheckCase("[null, false, true, true]", "[null, 2, 3]", FilterOptions::EMIT_NULL);
}

TEST(GetTakeIndices, NullValidityBuffer) {
  BooleanArray filter(1, *AllocateEmptyBitmap(1), /*null_bitmap=*/nullptr);
  auto expected_indices = ArrayFromJSON(uint16(), "[]");

  ASSERT_OK_AND_ASSIGN(auto indices,
                       internal::GetTakeIndices(*filter.data(), FilterOptions::DROP));
  auto indices_array = MakeArray(indices);
  ValidateOutput(indices);
  AssertArraysEqual(*expected_indices, *indices_array, /*verbose=*/true);

  ASSERT_OK_AND_ASSIGN(
      indices, internal::GetTakeIndices(*filter.data(), FilterOptions::EMIT_NULL));
  indices_array = MakeArray(indices);
  ValidateOutput(indices);
  AssertArraysEqual(*expected_indices, *indices_array, /*verbose=*/true);
}

template <typename IndexArrayType>
void CheckGetTakeIndicesCase(const Array& untyped_filter) {
  const auto& filter = checked_cast<const BooleanArray&>(untyped_filter);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ArrayData> drop_indices,
                       internal::GetTakeIndices(*filter.data(), FilterOptions::DROP));
  // Verify DROP indices
  {
    IndexArrayType indices(drop_indices);
    ValidateOutput(indices);

    int64_t out_position = 0;
    for (int64_t i = 0; i < filter.length(); ++i) {
      if (filter.IsValid(i)) {
        if (filter.Value(i)) {
          ASSERT_EQ(indices.Value(out_position), i);
          ++out_position;
        }
      }
    }
    ASSERT_EQ(out_position, indices.length());
    // Check that the end length agrees with the output of GetFilterOutputSize
    ASSERT_EQ(out_position,
              internal::GetFilterOutputSize(*filter.data(), FilterOptions::DROP));
  }

  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<ArrayData> emit_indices,
      internal::GetTakeIndices(*filter.data(), FilterOptions::EMIT_NULL));
  // Verify EMIT_NULL indices
  {
    IndexArrayType indices(emit_indices);
    ValidateOutput(indices);

    int64_t out_position = 0;
    for (int64_t i = 0; i < filter.length(); ++i) {
      if (filter.IsValid(i)) {
        if (filter.Value(i)) {
          ASSERT_EQ(indices.Value(out_position), i);
          ++out_position;
        }
      } else {
        ASSERT_TRUE(indices.IsNull(out_position));
        ++out_position;
      }
    }

    ASSERT_EQ(out_position, indices.length());
    // Check that the end length agrees with the output of GetFilterOutputSize
    ASSERT_EQ(out_position,
              internal::GetFilterOutputSize(*filter.data(), FilterOptions::EMIT_NULL));
  }
}

TEST(GetTakeIndices, RandomlyGenerated) {
  random::RandomArrayGenerator rng(kRandomSeed);

  // Multiple of word size + 1
  const int64_t length = 6401;
  for (auto null_prob : {0.0, 0.01, 0.999, 1.0}) {
    for (auto true_prob : {0.0, 0.01, 0.999, 1.0}) {
      auto filter = rng.Boolean(length, true_prob, null_prob);
      CheckGetTakeIndicesCase<UInt16Array>(*filter);
      CheckGetTakeIndicesCase<UInt16Array>(*filter->Slice(7));
    }
  }

  // Check that the uint32 path is traveled successfully
  const int64_t uint16_max = std::numeric_limits<uint16_t>::max();
  auto filter =
      std::static_pointer_cast<BooleanArray>(rng.Boolean(uint16_max + 1, 0.99, 0.01));
  CheckGetTakeIndicesCase<UInt16Array>(*filter->Slice(1));
  CheckGetTakeIndicesCase<UInt32Array>(*filter);
}

// ----------------------------------------------------------------------
// Filter tests

std::shared_ptr<Array> CoalesceNullToFalse(std::shared_ptr<Array> filter) {
  const bool is_rle = filter->type_id() == Type::RUN_LENGTH_ENCODED;
  // work directly on run values array in case of RLE
  const ArrayData& data = is_rle ? *filter->data()->child_data[1] : *filter->data();
  if (data.GetNullCount() == 0) {
    return filter;
  }
  auto is_true = std::make_shared<BooleanArray>(data.length, data.buffers[1], nullptr, 0,
                                                data.offset);
  auto is_valid = std::make_shared<BooleanArray>(data.length, data.buffers[0], nullptr, 0,
                                                 data.offset);
  EXPECT_OK_AND_ASSIGN(Datum out_datum, And(is_true, is_valid));
  if (is_rle) {
    auto& rle_filter = dynamic_cast<RunLengthEncodedArray&>(*filter);
    return RunLengthEncodedArray::Make(rle_filter.run_ends_array(),
                                       out_datum.make_array(), rle_filter.length(),
                                       rle_filter.offset())
        .ValueOrDie();
  } else {
    return out_datum.make_array();
  }
}

class TestFilterKernel : public ::testing::Test {
 protected:
  TestFilterKernel() : emit_null_(FilterOptions::EMIT_NULL), drop_(FilterOptions::DROP) {}

  void DoAssertFilter(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<Array>& filter,
                      const std::shared_ptr<Array>& expected) {
    // test with EMIT_NULL
    {
      ARROW_SCOPED_TRACE("with EMIT_NULL");
      ASSERT_OK_AND_ASSIGN(Datum out_datum, Filter(values, filter, emit_null_));
      auto actual = out_datum.make_array();
      ValidateOutput(*actual);
      AssertArraysEqual(*expected, *actual, /*verbose=*/true);
    }

    // test with DROP using EMIT_NULL and a coalesced filter
    {
      ARROW_SCOPED_TRACE("with DROP");
      auto coalesced_filter = CoalesceNullToFalse(filter);
      ASSERT_OK_AND_ASSIGN(Datum out_datum, Filter(values, coalesced_filter, emit_null_));
      auto expected_for_drop = out_datum.make_array();
      ASSERT_OK_AND_ASSIGN(out_datum, Filter(values, filter, drop_));
      auto actual = out_datum.make_array();
      ValidateOutput(*actual);
      AssertArraysEqual(*expected_for_drop, *actual, /*verbose=*/true);
    }
  }

  void AssertFilterSliced(const std::shared_ptr<Array>& values,
                          const std::shared_ptr<Array>& filter,
                          const std::shared_ptr<Array>& expected) {
    if (values->type_id() == Type::DENSE_UNION) {
      // Concatenation of dense union not supported
      return;
    }

    // Check slicing: add M(=3) dummy values at the start and end of `values`,
    // add N(=2) dummy values at the start and end of `filter`.
    ARROW_SCOPED_TRACE("for sliced values and filter");
    ASSERT_OK_AND_ASSIGN(auto values_filler, MakeArrayOfNull(values->type(), 3));
    auto filter_filler = ArrayFromJSON(boolean(), "[true, false]");
    ASSERT_OK_AND_ASSIGN(auto values_sliced,
                         Concatenate({values_filler, values, values_filler}));
    ASSERT_OK_AND_ASSIGN(auto filter_sliced,
                         Concatenate({filter_filler, filter, filter_filler}));
    values_sliced = values_sliced->Slice(3, values->length());
    filter_sliced = filter_sliced->Slice(2, filter->length());
    DoAssertFilter(values_sliced, filter_sliced, expected);
  }

  void AssertFilterRLE(const std::shared_ptr<Array>& values,
                       const std::shared_ptr<Array>& filter,
                       const std::shared_ptr<Array>& expected) {
    // TODO: create RLE filter kernel for non-primitive types
    if (!is_primitive(values->type_id())) {
      return;
    }
    ASSERT_OK_AND_ASSIGN(auto values_rle, RunLengthEncode(values));
    ASSERT_OK_AND_ASSIGN(auto filter_rle, RunLengthEncode(filter));
    ASSERT_OK_AND_ASSIGN(auto expected_rle, RunLengthEncode(expected));
    {
      ARROW_SCOPED_TRACE("for run-length encoded non-sliced values and filter");
      DoAssertFilter(values_rle.make_array(), filter_rle.make_array(),
                     expected_rle.make_array());
    }
    {
      ARROW_SCOPED_TRACE(
          "for run-length encoded values and filter with sliced child arrays");
      auto values_rle_sliced = values_rle.array()->Copy();
      rle_util::AddArtificialOffsetInChildArray(values_rle_sliced.get(), 2);
      auto filter_rle_sliced = filter_rle.array()->Copy();
      rle_util::AddArtificialOffsetInChildArray(filter_rle_sliced.get(), 1000);
      DoAssertFilter(values_rle.make_array(), filter_rle.make_array(),
                     expected_rle.make_array());
    }
    {
      ARROW_SCOPED_TRACE("for sliced run-length encoded arrays");
      auto values_pad = ArrayFromJSON(values->type(), "[null, null]");
      auto filter_pad = ArrayFromJSON(boolean(), "[true, true]");
      ASSERT_OK_AND_ASSIGN(auto values_padded,
                           Concatenate({values_pad, values, values_pad}));
      ASSERT_OK_AND_ASSIGN(auto filter_padded,
                           Concatenate({filter_pad, filter, filter_pad}));
      ASSERT_OK_AND_ASSIGN(auto values_padded_rle, RunLengthEncode(values_padded));
      ASSERT_OK_AND_ASSIGN(auto filter_padded_rle, RunLengthEncode(filter_padded));
      int64_t originial_length = values->length();
      ASSERT_ARRAYS_EQUAL(*filter_padded_rle.make_array()->Slice(2, originial_length),
                          *filter_rle.make_array());
      DoAssertFilter(values_padded_rle.make_array()->Slice(2, originial_length),
                     filter_padded_rle.make_array()->Slice(2, originial_length),
                     expected_rle.make_array());
    }
  }

  void AssertFilter(const std::shared_ptr<Array>& values,
                    const std::shared_ptr<Array>& filter,
                    const std::shared_ptr<Array>& expected) {
    DoAssertFilter(values, filter, expected);
    AssertFilterSliced(values, filter, expected);
    AssertFilterRLE(values, filter, expected);
  }

  void AssertFilter(const std::shared_ptr<DataType>& type, const std::string& values,
                    const std::string& filter, const std::string& expected) {
    AssertFilter(ArrayFromJSON(type, values), ArrayFromJSON(boolean(), filter),
                 ArrayFromJSON(type, expected));
  }

  FilterOptions emit_null_, drop_;
};

void ValidateFilter(const std::shared_ptr<Array>& values,
                    const std::shared_ptr<Array>& filter_boxed) {
  FilterOptions emit_null(FilterOptions::EMIT_NULL);
  FilterOptions drop(FilterOptions::DROP);

  ASSERT_OK_AND_ASSIGN(Datum out_datum, Filter(values, filter_boxed, emit_null));
  auto filtered_emit_null = out_datum.make_array();
  ValidateOutput(*filtered_emit_null);

  ASSERT_OK_AND_ASSIGN(out_datum, Filter(values, filter_boxed, drop));
  auto filtered_drop = out_datum.make_array();
  ValidateOutput(*filtered_drop);

  // Create the expected arrays using Take
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<ArrayData> drop_indices,
      internal::GetTakeIndices(*filter_boxed->data(), FilterOptions::DROP));
  ASSERT_OK_AND_ASSIGN(Datum expected_drop, Take(values, Datum(drop_indices)));

  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<ArrayData> emit_null_indices,
      internal::GetTakeIndices(*filter_boxed->data(), FilterOptions::EMIT_NULL));
  ASSERT_OK_AND_ASSIGN(Datum expected_emit_null, Take(values, Datum(emit_null_indices)));

  AssertArraysEqual(*expected_drop.make_array(), *filtered_drop,
                    /*verbose=*/true);
  AssertArraysEqual(*expected_emit_null.make_array(), *filtered_emit_null,
                    /*verbose=*/true);
}

class TestFilterKernelWithNull : public TestFilterKernel {
 protected:
  void AssertFilter(const std::string& values, const std::string& filter,
                    const std::string& expected) {
    TestFilterKernel::AssertFilter(ArrayFromJSON(null(), values),
                                   ArrayFromJSON(boolean(), filter),
                                   ArrayFromJSON(null(), expected));
  }
};

TEST_F(TestFilterKernelWithNull, FilterNull) {
  this->AssertFilter("[]", "[]", "[]");

  this->AssertFilter("[null, null, null]", "[0, 1, 0]", "[null]");
  this->AssertFilter("[null, null, null]", "[1, 1, 0]", "[null, null]");
}

class TestFilterKernelWithBoolean : public TestFilterKernel {
 protected:
  void AssertFilter(const std::string& values, const std::string& filter,
                    const std::string& expected) {
    TestFilterKernel::AssertFilter(ArrayFromJSON(boolean(), values),
                                   ArrayFromJSON(boolean(), filter),
                                   ArrayFromJSON(boolean(), expected));
  }
};

TEST_F(TestFilterKernelWithBoolean, FilterBoolean) {
  this->AssertFilter("[]", "[]", "[]");

  this->AssertFilter("[true, false, true]", "[0, 1, 0]", "[false]");
  this->AssertFilter("[null, false, true]", "[0, 1, 0]", "[false]");
  this->AssertFilter("[true, false, true]", "[null, 1, 0]", "[null, false]");
}

TEST_F(TestFilterKernelWithBoolean, DefaultOptions) {
  auto values = ArrayFromJSON(int8(), "[7, 8, null, 9]");
  auto filter = ArrayFromJSON(boolean(), "[1, 1, 0, null]");

  ASSERT_OK_AND_ASSIGN(auto no_options_provided,
                       CallFunction("filter", {values, filter}));

  auto default_options = FilterOptions::Defaults();
  ASSERT_OK_AND_ASSIGN(auto explicit_defaults,
                       CallFunction("filter", {values, filter}, &default_options));

  AssertDatumsEqual(explicit_defaults, no_options_provided);
}

template <typename ArrowType>
class TestFilterKernelWithNumeric : public TestFilterKernel {
 protected:
  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_SUITE(TestFilterKernelWithNumeric, NumericArrowTypes);
TYPED_TEST(TestFilterKernelWithNumeric, FilterNumeric) {
  auto type = this->type_singleton();
  this->AssertFilter(type, "[]", "[]", "[]");

  this->AssertFilter(type, "[9]", "[0]", "[]");
  this->AssertFilter(type, "[9]", "[1]", "[9]");
  this->AssertFilter(type, "[9]", "[null]", "[null]");
  this->AssertFilter(type, "[null]", "[0]", "[]");
  this->AssertFilter(type, "[null]", "[1]", "[null]");
  this->AssertFilter(type, "[null]", "[null]", "[null]");

  this->AssertFilter(type, "[7, 8, 9]", "[0, 1, 0]", "[8]");
  this->AssertFilter(type, "[7, 8, 9]", "[1, 0, 1]", "[7, 9]");
  this->AssertFilter(type, "[null, 8, 9]", "[0, 1, 0]", "[8]");
  this->AssertFilter(type, "[7, 8, 9]", "[null, 1, 0]", "[null, 8]");
  this->AssertFilter(type, "[7, 8, 9]", "[1, null, 1]", "[7, null, 9]");

  this->AssertFilter(ArrayFromJSON(type, "[7, 8, 9]"),
                     ArrayFromJSON(boolean(), "[0, 1, 1, 1, 0, 1]")->Slice(3, 3),
                     ArrayFromJSON(type, "[7, 9]"));

  ASSERT_RAISES(Invalid, Filter(ArrayFromJSON(type, "[7, 8, 9]"),
                                ArrayFromJSON(boolean(), "[]"), this->emit_null_));
  ASSERT_RAISES(Invalid, Filter(ArrayFromJSON(type, "[7, 8, 9]"),
                                ArrayFromJSON(boolean(), "[]"), this->drop_));
}

template <typename CType>
using Comparator = bool(CType, CType);

template <typename CType>
Comparator<CType>* GetComparator(CompareOperator op) {
  static Comparator<CType>* cmp[] = {
      // EQUAL
      [](CType l, CType r) { return l == r; },
      // NOT_EQUAL
      [](CType l, CType r) { return l != r; },
      // GREATER
      [](CType l, CType r) { return l > r; },
      // GREATER_EQUAL
      [](CType l, CType r) { return l >= r; },
      // LESS
      [](CType l, CType r) { return l < r; },
      // LESS_EQUAL
      [](CType l, CType r) { return l <= r; },
  };
  return cmp[op];
}

template <typename T, typename Fn, typename CType = typename TypeTraits<T>::CType>
std::shared_ptr<Array> CompareAndFilter(const CType* data, int64_t length, Fn&& fn) {
  std::vector<CType> filtered;
  filtered.reserve(length);
  std::copy_if(data, data + length, std::back_inserter(filtered), std::forward<Fn>(fn));
  std::shared_ptr<Array> filtered_array;
  ArrayFromVector<T, CType>(filtered, &filtered_array);
  return filtered_array;
}

template <typename T, typename CType = typename TypeTraits<T>::CType>
std::shared_ptr<Array> CompareAndFilter(const CType* data, int64_t length, CType val,
                                        CompareOperator op) {
  auto cmp = GetComparator<CType>(op);
  return CompareAndFilter<T>(data, length, [&](CType e) { return cmp(e, val); });
}

template <typename T, typename CType = typename TypeTraits<T>::CType>
std::shared_ptr<Array> CompareAndFilter(const CType* data, int64_t length,
                                        const CType* other, CompareOperator op) {
  auto cmp = GetComparator<CType>(op);
  return CompareAndFilter<T>(data, length, [&](CType e) { return cmp(e, *other++); });
}

TYPED_TEST(TestFilterKernelWithNumeric, CompareScalarAndFilterRandomNumeric) {
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  using CType = typename TypeTraits<TypeParam>::CType;

  auto rand = random::RandomArrayGenerator(kRandomSeed);
  for (size_t i = 3; i < 10; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    // TODO(bkietz) rewrite with some nulls
    auto array =
        checked_pointer_cast<ArrayType>(rand.Numeric<TypeParam>(length, 0, 100, 0));
    CType c_fifty = 50;
    auto fifty = std::make_shared<ScalarType>(c_fifty);
    for (auto op : {EQUAL, NOT_EQUAL, GREATER, LESS_EQUAL}) {
      ASSERT_OK_AND_ASSIGN(
          Datum selection,
          CallFunction(CompareOperatorToFunctionName(op), {array, Datum(fifty)}));
      ASSERT_OK_AND_ASSIGN(Datum filtered, Filter(array, selection));
      auto filtered_array = filtered.make_array();
      ValidateOutput(*filtered_array);
      auto expected =
          CompareAndFilter<TypeParam>(array->raw_values(), array->length(), c_fifty, op);
      ASSERT_ARRAYS_EQUAL(*filtered_array, *expected);
    }
  }
}

TYPED_TEST(TestFilterKernelWithNumeric, CompareArrayAndFilterRandomNumeric) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  auto rand = random::RandomArrayGenerator(kRandomSeed);
  for (size_t i = 3; i < 10; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    auto lhs = checked_pointer_cast<ArrayType>(
        rand.Numeric<TypeParam>(length, 0, 100, /*null_probability=*/0.0));
    auto rhs = checked_pointer_cast<ArrayType>(
        rand.Numeric<TypeParam>(length, 0, 100, /*null_probability=*/0.0));
    for (auto op : {EQUAL, NOT_EQUAL, GREATER, LESS_EQUAL}) {
      ASSERT_OK_AND_ASSIGN(Datum selection,
                           CallFunction(CompareOperatorToFunctionName(op), {lhs, rhs}));
      ASSERT_OK_AND_ASSIGN(Datum filtered, Filter(lhs, selection));
      auto filtered_array = filtered.make_array();
      ValidateOutput(*filtered_array);
      auto expected = CompareAndFilter<TypeParam>(lhs->raw_values(), lhs->length(),
                                                  rhs->raw_values(), op);
      ASSERT_ARRAYS_EQUAL(*filtered_array, *expected);
    }
  }
}

TYPED_TEST(TestFilterKernelWithNumeric, ScalarInRangeAndFilterRandomNumeric) {
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  using CType = typename TypeTraits<TypeParam>::CType;

  auto rand = random::RandomArrayGenerator(kRandomSeed);
  for (size_t i = 3; i < 10; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    auto array = checked_pointer_cast<ArrayType>(
        rand.Numeric<TypeParam>(length, 0, 100, /*null_probability=*/0.0));
    CType c_fifty = 50, c_hundred = 100;
    auto fifty = std::make_shared<ScalarType>(c_fifty);
    auto hundred = std::make_shared<ScalarType>(c_hundred);
    ASSERT_OK_AND_ASSIGN(Datum greater_than_fifty,
                         CallFunction("greater", {array, Datum(fifty)}));
    ASSERT_OK_AND_ASSIGN(Datum less_than_hundred,
                         CallFunction("less", {array, Datum(hundred)}));
    ASSERT_OK_AND_ASSIGN(Datum selection, And(greater_than_fifty, less_than_hundred));
    ASSERT_OK_AND_ASSIGN(Datum filtered, Filter(array, selection));
    auto filtered_array = filtered.make_array();
    ValidateOutput(*filtered_array);
    auto expected = CompareAndFilter<TypeParam>(
        array->raw_values(), array->length(),
        [&](CType e) { return (e > c_fifty) && (e < c_hundred); });
    ASSERT_ARRAYS_EQUAL(*filtered_array, *expected);
  }
}

template <typename ArrowType>
class TestFilterKernelWithDecimal : public TestFilterKernel {
 protected:
  std::shared_ptr<DataType> type_singleton() { return std::make_shared<ArrowType>(3, 2); }
};

TYPED_TEST_SUITE(TestFilterKernelWithDecimal, DecimalArrowTypes);
TYPED_TEST(TestFilterKernelWithDecimal, FilterNumeric) {
  auto type = this->type_singleton();
  this->AssertFilter(type, R"([])", "[]", R"([])");

  this->AssertFilter(type, R"(["9.00"])", "[0]", R"([])");
  this->AssertFilter(type, R"(["9.00"])", "[1]", R"(["9.00"])");
  this->AssertFilter(type, R"(["9.00"])", "[null]", R"([null])");
  this->AssertFilter(type, R"([null])", "[0]", R"([])");
  this->AssertFilter(type, R"([null])", "[1]", R"([null])");
  this->AssertFilter(type, R"([null])", "[null]", R"([null])");

  this->AssertFilter(type, R"(["7.12", "8.00", "9.87"])", "[0, 1, 0]", R"(["8.00"])");
  this->AssertFilter(type, R"(["7.12", "8.00", "9.87"])", "[1, 0, 1]",
                     R"(["7.12", "9.87"])");
  this->AssertFilter(type, R"([null, "8.00", "9.87"])", "[0, 1, 0]", R"(["8.00"])");
  this->AssertFilter(type, R"(["7.12", "8.00", "9.87"])", "[null, 1, 0]",
                     R"([null, "8.00"])");
  this->AssertFilter(type, R"(["7.12", "8.00", "9.87"])", "[1, null, 1]",
                     R"(["7.12", null, "9.87"])");

  this->AssertFilter(ArrayFromJSON(type, R"(["7.12", "8.00", "9.87"])"),
                     ArrayFromJSON(boolean(), "[0, 1, 1, 1, 0, 1]")->Slice(3, 3),
                     ArrayFromJSON(type, R"(["7.12", "9.87"])"));

  ASSERT_RAISES(Invalid, Filter(ArrayFromJSON(type, R"(["7.12", "8.00", "9.87"])"),
                                ArrayFromJSON(boolean(), "[]"), this->emit_null_));
  ASSERT_RAISES(Invalid, Filter(ArrayFromJSON(type, R"(["7.12", "8.00", "9.87"])"),
                                ArrayFromJSON(boolean(), "[]"), this->drop_));
}

TEST(TestFilterKernel, NoValidityBitmapButUnknownNullCount) {
  auto values = ArrayFromJSON(int32(), "[1, 2, 3, 4]");
  auto filter = ArrayFromJSON(boolean(), "[true, true, false, true]");

  auto expected = (*Filter(values, filter)).make_array();

  filter->data()->null_count = kUnknownNullCount;
  auto result = (*Filter(values, filter)).make_array();

  AssertArraysEqual(*expected, *result);
}

template <typename TypeClass>
class TestFilterKernelWithString : public TestFilterKernel {
 protected:
  std::shared_ptr<DataType> value_type() {
    return TypeTraits<TypeClass>::type_singleton();
  }

  void AssertFilter(const std::string& values, const std::string& filter,
                    const std::string& expected) {
    TestFilterKernel::AssertFilter(ArrayFromJSON(value_type(), values),
                                   ArrayFromJSON(boolean(), filter),
                                   ArrayFromJSON(value_type(), expected));
  }

  void AssertFilterDictionary(const std::string& dictionary_values,
                              const std::string& dictionary_filter,
                              const std::string& filter,
                              const std::string& expected_filter) {
    auto dict = ArrayFromJSON(value_type(), dictionary_values);
    auto type = dictionary(int8(), value_type());
    ASSERT_OK_AND_ASSIGN(auto values,
                         DictionaryArray::FromArrays(
                             type, ArrayFromJSON(int8(), dictionary_filter), dict));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), expected_filter), dict));
    auto take_filter = ArrayFromJSON(boolean(), filter);
    TestFilterKernel::AssertFilter(values, take_filter, expected);
  }
};

TYPED_TEST_SUITE(TestFilterKernelWithString, BaseBinaryArrowTypes);

TYPED_TEST(TestFilterKernelWithString, FilterString) {
  this->AssertFilter(R"(["a", "b", "c"])", "[0, 1, 0]", R"(["b"])");
  this->AssertFilter(R"([null, "b", "c"])", "[0, 1, 0]", R"(["b"])");
  this->AssertFilter(R"(["a", "b", "c"])", "[null, 1, 0]", R"([null, "b"])");
}

TYPED_TEST(TestFilterKernelWithString, FilterDictionary) {
  auto dict = R"(["a", "b", "c", "d", "e"])";
  this->AssertFilterDictionary(dict, "[3, 4, 2]", "[0, 1, 0]", "[4]");
  this->AssertFilterDictionary(dict, "[null, 4, 2]", "[0, 1, 0]", "[4]");
  this->AssertFilterDictionary(dict, "[3, 4, 2]", "[null, 1, 0]", "[null, 4]");
}

class TestFilterKernelWithList : public TestFilterKernel {
 public:
};

TEST_F(TestFilterKernelWithList, FilterListInt32) {
  std::string list_json = "[[], [1,2], null, [3]]";
  this->AssertFilter(list(int32()), list_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(list(int32()), list_json, "[0, 1, 1, null]", "[[1,2], null, null]");
  this->AssertFilter(list(int32()), list_json, "[0, 0, 1, null]", "[null, null]");
  this->AssertFilter(list(int32()), list_json, "[1, 0, 0, 1]", "[[], [3]]");
  this->AssertFilter(list(int32()), list_json, "[1, 1, 1, 1]", list_json);
  this->AssertFilter(list(int32()), list_json, "[0, 1, 0, 1]", "[[1,2], [3]]");
}

TEST_F(TestFilterKernelWithList, FilterListListInt32) {
  std::string list_json = R"([
    [],
    [[1], [2, null, 2], []],
    null,
    [[3, null], null]
  ])";
  auto type = list(list(int32()));
  this->AssertFilter(type, list_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(type, list_json, "[0, 1, 1, null]", R"([
    [[1], [2, null, 2], []],
    null,
    null
  ])");
  this->AssertFilter(type, list_json, "[0, 0, 1, null]", "[null, null]");
  this->AssertFilter(type, list_json, "[1, 0, 0, 1]", R"([
    [],
    [[3, null], null]
  ])");
  this->AssertFilter(type, list_json, "[1, 1, 1, 1]", list_json);
  this->AssertFilter(type, list_json, "[0, 1, 0, 1]", R"([
    [[1], [2, null, 2], []],
    [[3, null], null]
  ])");
}

class TestFilterKernelWithLargeList : public TestFilterKernel {};

TEST_F(TestFilterKernelWithLargeList, FilterListInt32) {
  std::string list_json = "[[], [1,2], null, [3]]";
  this->AssertFilter(large_list(int32()), list_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(large_list(int32()), list_json, "[0, 1, 1, null]",
                     "[[1,2], null, null]");
}

class TestFilterKernelWithFixedSizeList : public TestFilterKernel {};

TEST_F(TestFilterKernelWithFixedSizeList, FilterFixedSizeListInt32) {
  std::string list_json = "[null, [1, null, 3], [4, 5, 6], [7, 8, null]]";
  this->AssertFilter(fixed_size_list(int32(), 3), list_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(fixed_size_list(int32(), 3), list_json, "[0, 1, 1, null]",
                     "[[1, null, 3], [4, 5, 6], null]");
  this->AssertFilter(fixed_size_list(int32(), 3), list_json, "[0, 0, 1, null]",
                     "[[4, 5, 6], null]");
  this->AssertFilter(fixed_size_list(int32(), 3), list_json, "[1, 1, 1, 1]", list_json);
  this->AssertFilter(fixed_size_list(int32(), 3), list_json, "[0, 1, 0, 1]",
                     "[[1, null, 3], [7, 8, null]]");
}

class TestFilterKernelWithMap : public TestFilterKernel {};

TEST_F(TestFilterKernelWithMap, FilterMapStringToInt32) {
  std::string map_json = R"([
    [["joe", 0], ["mark", null]],
    null,
    [["cap", 8]],
    []
  ])";
  this->AssertFilter(map(utf8(), int32()), map_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(map(utf8(), int32()), map_json, "[0, 1, 1, null]", R"([
    null,
    [["cap", 8]],
    null
  ])");
  this->AssertFilter(map(utf8(), int32()), map_json, "[1, 1, 1, 1]", map_json);
  this->AssertFilter(map(utf8(), int32()), map_json, "[0, 1, 0, 1]", "[null, []]");
}

class TestFilterKernelWithStruct : public TestFilterKernel {};

TEST_F(TestFilterKernelWithStruct, FilterStruct) {
  auto struct_type = struct_({field("a", int32()), field("b", utf8())});
  auto struct_json = R"([
    null,
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])";
  this->AssertFilter(struct_type, struct_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(struct_type, struct_json, "[0, 1, 1, null]", R"([
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    null
  ])");
  this->AssertFilter(struct_type, struct_json, "[1, 1, 1, 1]", struct_json);
  this->AssertFilter(struct_type, struct_json, "[1, 0, 1, 0]", R"([
    null,
    {"a": 2, "b": "hello"}
  ])");
}

class TestFilterKernelWithUnion : public TestFilterKernel {};

TEST_F(TestFilterKernelWithUnion, FilterUnion) {
  auto union_type = dense_union({field("a", int32()), field("b", utf8())}, {2, 5});
  auto union_json = R"([
      [2, null],
      [2, 222],
      [5, "hello"],
      [5, "eh"],
      [2, null],
      [2, 111],
      [5, null]
    ])";
  this->AssertFilter(union_type, union_json, "[0, 0, 0, 0, 0, 0, 0]", "[]");
  this->AssertFilter(union_type, union_json, "[0, 1, 1, null, 0, 1, 1]", R"([
      [2, 222],
      [5, "hello"],
      [2, null],
      [2, 111],
      [5, null]
    ])");
  this->AssertFilter(union_type, union_json, "[1, 0, 1, 0, 1, 0, 0]", R"([
      [2, null],
      [5, "hello"],
      [2, null]
    ])");
  this->AssertFilter(union_type, union_json, "[1, 1, 1, 1, 1, 1, 1]", union_json);

  // Sliced
  // (check this manually as concatenation of dense unions isn't supported: ARROW-4975)
  auto values = ArrayFromJSON(union_type, union_json)->Slice(2, 4);
  auto filter = ArrayFromJSON(boolean(), "[0, 1, 1, null, 0, 1, 1]")->Slice(2, 4);
  auto expected = ArrayFromJSON(union_type, R"([
      [5, "hello"],
      [2, null],
      [2, 111]
    ])");
  this->AssertFilter(values, filter, expected);
}

class TestFilterKernelWithRecordBatch : public TestFilterKernel {
 public:
  void AssertFilter(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
                    const std::string& selection, FilterOptions options,
                    const std::string& expected_batch) {
    std::shared_ptr<RecordBatch> actual;

    ASSERT_OK(this->DoFilter(schm, batch_json, selection, options, &actual));
    ValidateOutput(actual);
    ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(schm, expected_batch), *actual);
  }

  Status DoFilter(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
                  const std::string& selection, FilterOptions options,
                  std::shared_ptr<RecordBatch>* out) {
    auto batch = RecordBatchFromJSON(schm, batch_json);
    ARROW_ASSIGN_OR_RAISE(Datum out_datum,
                          Filter(batch, ArrayFromJSON(boolean(), selection), options));
    *out = out_datum.record_batch();
    return Status::OK();
  }
};

TEST_F(TestFilterKernelWithRecordBatch, FilterRecordBatch) {
  std::vector<std::shared_ptr<Field>> fields = {field("a", int32()), field("b", utf8())};
  auto schm = schema(fields);

  auto batch_json = R"([
    {"a": null, "b": "yo"},
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])";
  for (auto options : {this->emit_null_, this->drop_}) {
    this->AssertFilter(schm, batch_json, "[0, 0, 0, 0]", options, "[]");
    this->AssertFilter(schm, batch_json, "[1, 1, 1, 1]", options, batch_json);
    this->AssertFilter(schm, batch_json, "[1, 0, 1, 0]", options, R"([
      {"a": null, "b": "yo"},
      {"a": 2, "b": "hello"}
    ])");
  }

  this->AssertFilter(schm, batch_json, "[0, 1, 1, null]", this->drop_, R"([
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"}
  ])");

  this->AssertFilter(schm, batch_json, "[0, 1, 1, null]", this->emit_null_, R"([
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": null, "b": null}
  ])");
}

class TestFilterKernelWithChunkedArray : public TestFilterKernel {
 public:
  void AssertFilter(const std::shared_ptr<DataType>& type,
                    const std::vector<std::string>& values, const std::string& filter,
                    const std::vector<std::string>& expected) {
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK(this->FilterWithArray(type, values, filter, &actual));
    ValidateOutput(actual);
    AssertChunkedEqual(*ChunkedArrayFromJSON(type, expected), *actual);
  }

  void AssertChunkedFilter(const std::shared_ptr<DataType>& type,
                           const std::vector<std::string>& values,
                           const std::vector<std::string>& filter,
                           const std::vector<std::string>& expected) {
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK(this->FilterWithChunkedArray(type, values, filter, &actual));
    ValidateOutput(actual);
    AssertChunkedEqual(*ChunkedArrayFromJSON(type, expected), *actual);
  }

  Status FilterWithArray(const std::shared_ptr<DataType>& type,
                         const std::vector<std::string>& values,
                         const std::string& filter, std::shared_ptr<ChunkedArray>* out) {
    ARROW_ASSIGN_OR_RAISE(Datum out_datum, Filter(ChunkedArrayFromJSON(type, values),
                                                  ArrayFromJSON(boolean(), filter)));
    *out = out_datum.chunked_array();
    return Status::OK();
  }

  Status FilterWithChunkedArray(const std::shared_ptr<DataType>& type,
                                const std::vector<std::string>& values,
                                const std::vector<std::string>& filter,
                                std::shared_ptr<ChunkedArray>* out) {
    ARROW_ASSIGN_OR_RAISE(Datum out_datum,
                          Filter(ChunkedArrayFromJSON(type, values),
                                 ChunkedArrayFromJSON(boolean(), filter)));
    *out = out_datum.chunked_array();
    return Status::OK();
  }
};

TEST_F(TestFilterKernelWithChunkedArray, FilterChunkedArray) {
  this->AssertFilter(int8(), {"[]"}, "[]", {});
  this->AssertChunkedFilter(int8(), {"[]"}, {"[]"}, {});

  this->AssertFilter(int8(), {"[7]", "[8, 9]"}, "[0, 1, 0]", {"[8]"});
  this->AssertChunkedFilter(int8(), {"[7]", "[8, 9]"}, {"[0]", "[1, 0]"}, {"[8]"});
  this->AssertChunkedFilter(int8(), {"[7]", "[8, 9]"}, {"[0, 1]", "[0]"}, {"[8]"});

  std::shared_ptr<ChunkedArray> arr;
  ASSERT_RAISES(
      Invalid, this->FilterWithArray(int8(), {"[7]", "[8, 9]"}, "[0, 1, 0, 1, 1]", &arr));
  ASSERT_RAISES(Invalid, this->FilterWithChunkedArray(int8(), {"[7]", "[8, 9]"},
                                                      {"[0, 1, 0]", "[1, 1]"}, &arr));
}

class TestFilterKernelWithTable : public TestFilterKernel {
 public:
  void AssertFilter(const std::shared_ptr<Schema>& schm,
                    const std::vector<std::string>& table_json, const std::string& filter,
                    FilterOptions options,
                    const std::vector<std::string>& expected_table) {
    std::shared_ptr<Table> actual;

    ASSERT_OK(this->FilterWithArray(schm, table_json, filter, options, &actual));
    ValidateOutput(actual);
    ASSERT_TABLES_EQUAL(*TableFromJSON(schm, expected_table), *actual);
  }

  void AssertChunkedFilter(const std::shared_ptr<Schema>& schm,
                           const std::vector<std::string>& table_json,
                           const std::vector<std::string>& filter, FilterOptions options,
                           const std::vector<std::string>& expected_table) {
    std::shared_ptr<Table> actual;

    ASSERT_OK(this->FilterWithChunkedArray(schm, table_json, filter, options, &actual));
    ValidateOutput(actual);
    AssertTablesEqual(*TableFromJSON(schm, expected_table), *actual,
                      /*same_chunk_layout=*/false);
  }

  Status FilterWithArray(const std::shared_ptr<Schema>& schm,
                         const std::vector<std::string>& values,
                         const std::string& filter, FilterOptions options,
                         std::shared_ptr<Table>* out) {
    ARROW_ASSIGN_OR_RAISE(
        Datum out_datum,
        Filter(TableFromJSON(schm, values), ArrayFromJSON(boolean(), filter), options));
    *out = out_datum.table();
    return Status::OK();
  }

  Status FilterWithChunkedArray(const std::shared_ptr<Schema>& schm,
                                const std::vector<std::string>& values,
                                const std::vector<std::string>& filter,
                                FilterOptions options, std::shared_ptr<Table>* out) {
    ARROW_ASSIGN_OR_RAISE(Datum out_datum,
                          Filter(TableFromJSON(schm, values),
                                 ChunkedArrayFromJSON(boolean(), filter), options));
    *out = out_datum.table();
    return Status::OK();
  }
};

TEST_F(TestFilterKernelWithTable, FilterTable) {
  std::vector<std::shared_ptr<Field>> fields = {field("a", int32()), field("b", utf8())};
  auto schm = schema(fields);

  std::vector<std::string> table_json = {R"([
      {"a": null, "b": "yo"},
      {"a": 1, "b": ""}
    ])",
                                         R"([
      {"a": 2, "b": "hello"},
      {"a": 4, "b": "eh"}
    ])"};
  for (auto options : {this->emit_null_, this->drop_}) {
    this->AssertFilter(schm, table_json, "[0, 0, 0, 0]", options, {});
    this->AssertChunkedFilter(schm, table_json, {"[0]", "[0, 0, 0]"}, options, {});
    this->AssertFilter(schm, table_json, "[1, 1, 1, 1]", options, table_json);
    this->AssertChunkedFilter(schm, table_json, {"[1]", "[1, 1, 1]"}, options,
                              table_json);
  }

  std::vector<std::string> expected_emit_null = {R"([
    {"a": 1, "b": ""}
  ])",
                                                 R"([
    {"a": 2, "b": "hello"},
    {"a": null, "b": null}
  ])"};
  this->AssertFilter(schm, table_json, "[0, 1, 1, null]", this->emit_null_,
                     expected_emit_null);
  this->AssertChunkedFilter(schm, table_json, {"[0, 1, 1]", "[null]"}, this->emit_null_,
                            expected_emit_null);

  std::vector<std::string> expected_drop = {R"([{"a": 1, "b": ""}])",
                                            R"([{"a": 2, "b": "hello"}])"};
  this->AssertFilter(schm, table_json, "[0, 1, 1, null]", this->drop_, expected_drop);
  this->AssertChunkedFilter(schm, table_json, {"[0, 1, 1]", "[null]"}, this->drop_,
                            expected_drop);
}

TEST(TestFilterMetaFunction, ArityChecking) {
  ASSERT_RAISES(Invalid, CallFunction("filter", ExecBatch({}, 0)));
}

// ----------------------------------------------------------------------
// Take tests

void AssertTakeArrays(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<Array>& indices,
                      const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> actual, Take(*values, *indices));
  ValidateOutput(actual);
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

Status TakeJSON(const std::shared_ptr<DataType>& type, const std::string& values,
                const std::shared_ptr<DataType>& index_type, const std::string& indices,
                std::shared_ptr<Array>* out) {
  return Take(*ArrayFromJSON(type, values), *ArrayFromJSON(index_type, indices))
      .Value(out);
}

void CheckTake(const std::shared_ptr<DataType>& type, const std::string& values_json,
               const std::string& indices_json, const std::string& expected_json) {
  auto values = ArrayFromJSON(type, values_json);
  auto expected = ArrayFromJSON(type, expected_json);

  for (auto index_type : {int8(), uint32()}) {
    auto indices = ArrayFromJSON(index_type, indices_json);
    AssertTakeArrays(values, indices, expected);

    // Check sliced values
    if (type->id() != Type::DENSE_UNION) {
      ASSERT_OK_AND_ASSIGN(auto values_filler, MakeArrayOfNull(type, 2));
      ASSERT_OK_AND_ASSIGN(auto values_sliced,
                           Concatenate({values_filler, values, values_filler}));
      values_sliced = values_sliced->Slice(2, values->length());
      AssertTakeArrays(values_sliced, indices, expected);
    }

    // Check sliced indices
    ASSERT_OK_AND_ASSIGN(auto zero, MakeScalar(index_type, int8_t{0}));
    ASSERT_OK_AND_ASSIGN(auto indices_filler, MakeArrayFromScalar(*zero, 3));
    ASSERT_OK_AND_ASSIGN(auto indices_sliced,
                         Concatenate({indices_filler, indices, indices_filler}));
    indices_sliced = indices_sliced->Slice(3, indices->length());
    AssertTakeArrays(values, indices_sliced, expected);
  }
}

void AssertTakeNull(const std::string& values, const std::string& indices,
                    const std::string& expected) {
  CheckTake(null(), values, indices, expected);
}

void AssertTakeBoolean(const std::string& values, const std::string& indices,
                       const std::string& expected) {
  CheckTake(boolean(), values, indices, expected);
}

template <typename ValuesType, typename IndexType>
void ValidateTakeImpl(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<Array>& indices,
                      const std::shared_ptr<Array>& result) {
  using ValuesArrayType = typename TypeTraits<ValuesType>::ArrayType;
  using IndexArrayType = typename TypeTraits<IndexType>::ArrayType;
  auto typed_values = checked_pointer_cast<ValuesArrayType>(values);
  auto typed_result = checked_pointer_cast<ValuesArrayType>(result);
  auto typed_indices = checked_pointer_cast<IndexArrayType>(indices);
  for (int64_t i = 0; i < indices->length(); ++i) {
    if (typed_indices->IsNull(i) || typed_values->IsNull(typed_indices->Value(i))) {
      ASSERT_TRUE(result->IsNull(i)) << i;
    } else {
      ASSERT_FALSE(result->IsNull(i)) << i;
      ASSERT_EQ(typed_result->GetView(i), typed_values->GetView(typed_indices->Value(i)))
          << i;
    }
  }
}

template <typename ValuesType>
void ValidateTake(const std::shared_ptr<Array>& values,
                  const std::shared_ptr<Array>& indices) {
  ASSERT_OK_AND_ASSIGN(Datum out, Take(values, indices));
  auto taken = out.make_array();
  ValidateOutput(taken);
  ASSERT_EQ(indices->length(), taken->length());
  switch (indices->type_id()) {
    case Type::INT8:
      ValidateTakeImpl<ValuesType, Int8Type>(values, indices, taken);
      break;
    case Type::INT16:
      ValidateTakeImpl<ValuesType, Int16Type>(values, indices, taken);
      break;
    case Type::INT32:
      ValidateTakeImpl<ValuesType, Int32Type>(values, indices, taken);
      break;
    case Type::INT64:
      ValidateTakeImpl<ValuesType, Int64Type>(values, indices, taken);
      break;
    case Type::UINT8:
      ValidateTakeImpl<ValuesType, UInt8Type>(values, indices, taken);
      break;
    case Type::UINT16:
      ValidateTakeImpl<ValuesType, UInt16Type>(values, indices, taken);
      break;
    case Type::UINT32:
      ValidateTakeImpl<ValuesType, UInt32Type>(values, indices, taken);
      break;
    case Type::UINT64:
      ValidateTakeImpl<ValuesType, UInt64Type>(values, indices, taken);
      break;
    default:
      FAIL() << "Invalid index type";
      break;
  }
}

template <typename T>
T GetMaxIndex(int64_t values_length) {
  int64_t max_index = values_length - 1;
  if (max_index > static_cast<int64_t>(std::numeric_limits<T>::max())) {
    max_index = std::numeric_limits<T>::max();
  }
  return static_cast<T>(max_index);
}

template <>
uint64_t GetMaxIndex(int64_t values_length) {
  return static_cast<uint64_t>(values_length - 1);
}

class TestTakeKernel : public ::testing::Test {
 public:
  void TestNoValidityBitmapButUnknownNullCount(const std::shared_ptr<Array>& values,
                                               const std::shared_ptr<Array>& indices) {
    ASSERT_EQ(values->null_count(), 0);
    ASSERT_EQ(indices->null_count(), 0);
    auto expected = (*Take(values, indices)).make_array();

    auto new_values = MakeArray(values->data()->Copy());
    new_values->data()->buffers[0].reset();
    new_values->data()->null_count = kUnknownNullCount;
    auto new_indices = MakeArray(indices->data()->Copy());
    new_indices->data()->buffers[0].reset();
    new_indices->data()->null_count = kUnknownNullCount;
    auto result = (*Take(new_values, new_indices)).make_array();

    AssertArraysEqual(*expected, *result);
  }

  void TestNoValidityBitmapButUnknownNullCount(const std::shared_ptr<DataType>& type,
                                               const std::string& values,
                                               const std::string& indices) {
    TestNoValidityBitmapButUnknownNullCount(ArrayFromJSON(type, values),
                                            ArrayFromJSON(int16(), indices));
  }
};

template <typename ArrowType>
class TestTakeKernelTyped : public TestTakeKernel {};

TEST_F(TestTakeKernel, TakeNull) {
  AssertTakeNull("[null, null, null]", "[0, 1, 0]", "[null, null, null]");
  AssertTakeNull("[null, null, null]", "[0, 2]", "[null, null]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                TakeJSON(null(), "[null, null, null]", int8(), "[0, 9, 0]", &arr));
  ASSERT_RAISES(IndexError,
                TakeJSON(boolean(), "[null, null, null]", int8(), "[0, -1, 0]", &arr));
}

TEST_F(TestTakeKernel, InvalidIndexType) {
  std::shared_ptr<Array> arr;
  ASSERT_RAISES(NotImplemented, TakeJSON(null(), "[null, null, null]", float32(),
                                         "[0.0, 1.0, 0.1]", &arr));
}

TEST_F(TestTakeKernel, TakeCCEmptyIndices) {
  Datum dat = ChunkedArrayFromJSON(int8(), {"[]"});
  Datum idx = ChunkedArrayFromJSON(int32(), {});
  ASSERT_OK_AND_ASSIGN(auto out, Take(dat, idx));
  ValidateOutput(out);
  AssertDatumsEqual(ChunkedArrayFromJSON(int8(), {"[]"}), out, true);
}

TEST_F(TestTakeKernel, TakeACEmptyIndices) {
  Datum dat = ArrayFromJSON(int8(), {"[]"});
  Datum idx = ChunkedArrayFromJSON(int32(), {});
  ASSERT_OK_AND_ASSIGN(auto out, Take(dat, idx));
  ValidateOutput(out);
  AssertDatumsEqual(ChunkedArrayFromJSON(int8(), {"[]"}), out, true);
}

TEST_F(TestTakeKernel, DefaultOptions) {
  auto indices = ArrayFromJSON(int8(), "[null, 2, 0, 3]");
  auto values = ArrayFromJSON(int8(), "[7, 8, 9, null]");
  ASSERT_OK_AND_ASSIGN(auto no_options_provided, CallFunction("take", {values, indices}));

  auto default_options = TakeOptions::Defaults();
  ASSERT_OK_AND_ASSIGN(auto explicit_defaults,
                       CallFunction("take", {values, indices}, &default_options));

  AssertDatumsEqual(explicit_defaults, no_options_provided);
}

TEST_F(TestTakeKernel, TakeBoolean) {
  AssertTakeBoolean("[7, 8, 9]", "[]", "[]");
  AssertTakeBoolean("[true, false, true]", "[0, 1, 0]", "[true, false, true]");
  AssertTakeBoolean("[null, false, true]", "[0, 1, 0]", "[null, false, null]");
  AssertTakeBoolean("[true, false, true]", "[null, 1, 0]", "[null, false, true]");

  TestNoValidityBitmapButUnknownNullCount(boolean(), "[true, false, true]", "[1, 0, 0]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                TakeJSON(boolean(), "[true, false, true]", int8(), "[0, 9, 0]", &arr));
  ASSERT_RAISES(IndexError,
                TakeJSON(boolean(), "[true, false, true]", int8(), "[0, -1, 0]", &arr));
}

template <typename ArrowType>
class TestTakeKernelWithNumeric : public TestTakeKernelTyped<ArrowType> {
 protected:
  void AssertTake(const std::string& values, const std::string& indices,
                  const std::string& expected) {
    CheckTake(type_singleton(), values, indices, expected);
  }

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_SUITE(TestTakeKernelWithNumeric, NumericArrowTypes);
TYPED_TEST(TestTakeKernelWithNumeric, TakeNumeric) {
  this->AssertTake("[7, 8, 9]", "[]", "[]");
  this->AssertTake("[7, 8, 9]", "[0, 1, 0]", "[7, 8, 7]");
  this->AssertTake("[null, 8, 9]", "[0, 1, 0]", "[null, 8, null]");
  this->AssertTake("[7, 8, 9]", "[null, 1, 0]", "[null, 8, 7]");
  this->AssertTake("[null, 8, 9]", "[]", "[]");
  this->AssertTake("[7, 8, 9]", "[0, 0, 0, 0, 0, 0, 2]", "[7, 7, 7, 7, 7, 7, 9]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                TakeJSON(this->type_singleton(), "[7, 8, 9]", int8(), "[0, 9, 0]", &arr));
  ASSERT_RAISES(IndexError, TakeJSON(this->type_singleton(), "[7, 8, 9]", int8(),
                                     "[0, -1, 0]", &arr));
}

template <typename TypeClass>
class TestTakeKernelWithString : public TestTakeKernelTyped<TypeClass> {
 public:
  std::shared_ptr<DataType> value_type() {
    return TypeTraits<TypeClass>::type_singleton();
  }

  void AssertTake(const std::string& values, const std::string& indices,
                  const std::string& expected) {
    CheckTake(value_type(), values, indices, expected);
  }

  void AssertTakeDictionary(const std::string& dictionary_values,
                            const std::string& dictionary_indices,
                            const std::string& indices,
                            const std::string& expected_indices) {
    auto dict = ArrayFromJSON(value_type(), dictionary_values);
    auto type = dictionary(int8(), value_type());
    ASSERT_OK_AND_ASSIGN(auto values,
                         DictionaryArray::FromArrays(
                             type, ArrayFromJSON(int8(), dictionary_indices), dict));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), expected_indices), dict));
    auto take_indices = ArrayFromJSON(int8(), indices);
    AssertTakeArrays(values, take_indices, expected);
  }
};

TYPED_TEST_SUITE(TestTakeKernelWithString, BaseBinaryArrowTypes);

TYPED_TEST(TestTakeKernelWithString, TakeString) {
  this->AssertTake(R"(["a", "b", "c"])", "[0, 1, 0]", R"(["a", "b", "a"])");
  this->AssertTake(R"([null, "b", "c"])", "[0, 1, 0]", "[null, \"b\", null]");
  this->AssertTake(R"(["a", "b", "c"])", "[null, 1, 0]", R"([null, "b", "a"])");

  this->TestNoValidityBitmapButUnknownNullCount(this->value_type(), R"(["a", "b", "c"])",
                                                "[0, 1, 0]");

  std::shared_ptr<DataType> type = this->value_type();
  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                TakeJSON(type, R"(["a", "b", "c"])", int8(), "[0, 9, 0]", &arr));
  ASSERT_RAISES(IndexError, TakeJSON(type, R"(["a", "b", null, "ddd", "ee"])", int64(),
                                     "[2, 5]", &arr));
}

TYPED_TEST(TestTakeKernelWithString, TakeDictionary) {
  auto dict = R"(["a", "b", "c", "d", "e"])";
  this->AssertTakeDictionary(dict, "[3, 4, 2]", "[0, 1, 0]", "[3, 4, 3]");
  this->AssertTakeDictionary(dict, "[null, 4, 2]", "[0, 1, 0]", "[null, 4, null]");
  this->AssertTakeDictionary(dict, "[3, 4, 2]", "[null, 1, 0]", "[null, 4, 3]");
}

class TestTakeKernelFSB : public TestTakeKernelTyped<FixedSizeBinaryType> {
 public:
  std::shared_ptr<DataType> value_type() { return fixed_size_binary(3); }

  void AssertTake(const std::string& values, const std::string& indices,
                  const std::string& expected) {
    CheckTake(value_type(), values, indices, expected);
  }
};

TEST_F(TestTakeKernelFSB, TakeFixedSizeBinary) {
  this->AssertTake(R"(["aaa", "bbb", "ccc"])", "[0, 1, 0]", R"(["aaa", "bbb", "aaa"])");
  this->AssertTake(R"([null, "bbb", "ccc"])", "[0, 1, 0]", "[null, \"bbb\", null]");
  this->AssertTake(R"(["aaa", "bbb", "ccc"])", "[null, 1, 0]", R"([null, "bbb", "aaa"])");

  this->TestNoValidityBitmapButUnknownNullCount(this->value_type(),
                                                R"(["aaa", "bbb", "ccc"])", "[0, 1, 0]");

  std::shared_ptr<DataType> type = this->value_type();
  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                TakeJSON(type, R"(["aaa", "bbb", "ccc"])", int8(), "[0, 9, 0]", &arr));
  ASSERT_RAISES(IndexError, TakeJSON(type, R"(["aaa", "bbb", null, "ddd", "eee"])",
                                     int64(), "[2, 5]", &arr));
}

class TestTakeKernelWithList : public TestTakeKernelTyped<ListType> {};

TEST_F(TestTakeKernelWithList, TakeListInt32) {
  std::string list_json = "[[], [1,2], null, [3]]";
  CheckTake(list(int32()), list_json, "[]", "[]");
  CheckTake(list(int32()), list_json, "[3, 2, 1]", "[[3], null, [1,2]]");
  CheckTake(list(int32()), list_json, "[null, 3, 0]", "[null, [3], []]");
  CheckTake(list(int32()), list_json, "[null, null]", "[null, null]");
  CheckTake(list(int32()), list_json, "[3, 0, 0, 3]", "[[3], [], [], [3]]");
  CheckTake(list(int32()), list_json, "[0, 1, 2, 3]", list_json);
  CheckTake(list(int32()), list_json, "[0, 0, 0, 0, 0, 0, 1]",
            "[[], [], [], [], [], [], [1, 2]]");

  this->TestNoValidityBitmapButUnknownNullCount(list(int32()), "[[], [1,2], [3]]",
                                                "[0, 1, 0]");
}

TEST_F(TestTakeKernelWithList, TakeListListInt32) {
  std::string list_json = R"([
    [],
    [[1], [2, null, 2], []],
    null,
    [[3, null], null]
  ])";
  auto type = list(list(int32()));
  CheckTake(type, list_json, "[]", "[]");
  CheckTake(type, list_json, "[3, 2, 1]", R"([
    [[3, null], null],
    null,
    [[1], [2, null, 2], []]
  ])");
  CheckTake(type, list_json, "[null, 3, 0]", R"([
    null,
    [[3, null], null],
    []
  ])");
  CheckTake(type, list_json, "[null, null]", "[null, null]");
  CheckTake(type, list_json, "[3, 0, 0, 3]",
            "[[[3, null], null], [], [], [[3, null], null]]");
  CheckTake(type, list_json, "[0, 1, 2, 3]", list_json);
  CheckTake(type, list_json, "[0, 0, 0, 0, 0, 0, 1]",
            "[[], [], [], [], [], [], [[1], [2, null, 2], []]]");

  this->TestNoValidityBitmapButUnknownNullCount(
      type, "[[[1], [2, null, 2], []], [[3, null]]]", "[0, 1, 0]");
}

class TestTakeKernelWithLargeList : public TestTakeKernelTyped<LargeListType> {};

TEST_F(TestTakeKernelWithLargeList, TakeLargeListInt32) {
  std::string list_json = "[[], [1,2], null, [3]]";
  CheckTake(large_list(int32()), list_json, "[]", "[]");
  CheckTake(large_list(int32()), list_json, "[null, 1, 2, 0]", "[null, [1,2], null, []]");
}

class TestTakeKernelWithFixedSizeList : public TestTakeKernelTyped<FixedSizeListType> {};

TEST_F(TestTakeKernelWithFixedSizeList, TakeFixedSizeListInt32) {
  std::string list_json = "[null, [1, null, 3], [4, 5, 6], [7, 8, null]]";
  CheckTake(fixed_size_list(int32(), 3), list_json, "[]", "[]");
  CheckTake(fixed_size_list(int32(), 3), list_json, "[3, 2, 1]",
            "[[7, 8, null], [4, 5, 6], [1, null, 3]]");
  CheckTake(fixed_size_list(int32(), 3), list_json, "[null, 2, 0]",
            "[null, [4, 5, 6], null]");
  CheckTake(fixed_size_list(int32(), 3), list_json, "[null, null]", "[null, null]");
  CheckTake(fixed_size_list(int32(), 3), list_json, "[3, 0, 0, 3]",
            "[[7, 8, null], null, null, [7, 8, null]]");
  CheckTake(fixed_size_list(int32(), 3), list_json, "[0, 1, 2, 3]", list_json);
  CheckTake(
      fixed_size_list(int32(), 3), list_json, "[2, 2, 2, 2, 2, 2, 1]",
      "[[4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [1, null, 3]]");

  this->TestNoValidityBitmapButUnknownNullCount(fixed_size_list(int32(), 3),
                                                "[[1, null, 3], [4, 5, 6], [7, 8, null]]",
                                                "[0, 1, 0]");
}

class TestTakeKernelWithMap : public TestTakeKernelTyped<MapType> {};

TEST_F(TestTakeKernelWithMap, TakeMapStringToInt32) {
  std::string map_json = R"([
    [["joe", 0], ["mark", null]],
    null,
    [["cap", 8]],
    []
  ])";
  CheckTake(map(utf8(), int32()), map_json, "[]", "[]");
  CheckTake(map(utf8(), int32()), map_json, "[3, 1, 3, 1, 3]",
            "[[], null, [], null, []]");
  CheckTake(map(utf8(), int32()), map_json, "[2, 1, null]", R"([
    [["cap", 8]],
    null,
    null
  ])");
  CheckTake(map(utf8(), int32()), map_json, "[2, 1, 0]", R"([
    [["cap", 8]],
    null,
    [["joe", 0], ["mark", null]]
  ])");
  CheckTake(map(utf8(), int32()), map_json, "[0, 1, 2, 3]", map_json);
  CheckTake(map(utf8(), int32()), map_json, "[0, 0, 0, 0, 0, 0, 3]", R"([
    [["joe", 0], ["mark", null]],
    [["joe", 0], ["mark", null]],
    [["joe", 0], ["mark", null]],
    [["joe", 0], ["mark", null]],
    [["joe", 0], ["mark", null]],
    [["joe", 0], ["mark", null]],
    []
  ])");
}

class TestTakeKernelWithStruct : public TestTakeKernelTyped<StructType> {};

TEST_F(TestTakeKernelWithStruct, TakeStruct) {
  auto struct_type = struct_({field("a", int32()), field("b", utf8())});
  auto struct_json = R"([
    null,
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])";
  CheckTake(struct_type, struct_json, "[]", "[]");
  CheckTake(struct_type, struct_json, "[3, 1, 3, 1, 3]", R"([
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": 4, "b": "eh"}
  ])");
  CheckTake(struct_type, struct_json, "[3, 1, 0]", R"([
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    null
  ])");
  CheckTake(struct_type, struct_json, "[0, 1, 2, 3]", struct_json);
  CheckTake(struct_type, struct_json, "[0, 2, 2, 2, 2, 2, 2]", R"([
    null,
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"}
  ])");

  this->TestNoValidityBitmapButUnknownNullCount(
      struct_type, R"([{"a": 1}, {"a": 2, "b": "hello"}])", "[0, 1, 0]");
}

class TestTakeKernelWithUnion : public TestTakeKernelTyped<UnionType> {};

TEST_F(TestTakeKernelWithUnion, TakeUnion) {
  auto union_type = dense_union({field("a", int32()), field("b", utf8())}, {2, 5});
  auto union_json = R"([
      [2, null],
      [2, 222],
      [5, "hello"],
      [5, "eh"],
      [2, null],
      [2, 111],
      [5, null]
    ])";
  CheckTake(union_type, union_json, "[]", "[]");
  CheckTake(union_type, union_json, "[3, 1, 3, 1, 3]", R"([
      [5, "eh"],
      [2, 222],
      [5, "eh"],
      [2, 222],
      [5, "eh"]
    ])");
  CheckTake(union_type, union_json, "[4, 2, 1, 6]", R"([
      [2, null],
      [5, "hello"],
      [2, 222],
      [5, null]
    ])");
  CheckTake(union_type, union_json, "[0, 1, 2, 3, 4, 5, 6]", union_json);
  CheckTake(union_type, union_json, "[0, 2, 2, 2, 2, 2, 2]", R"([
      [2, null],
      [5, "hello"],
      [5, "hello"],
      [5, "hello"],
      [5, "hello"],
      [5, "hello"],
      [5, "hello"]
    ])");
}

class TestPermutationsWithTake : public ::testing::Test {
 protected:
  void DoTake(const Int16Array& values, const Int16Array& indices,
              std::shared_ptr<Int16Array>* out) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> boxed_out, Take(values, indices));
    ValidateOutput(boxed_out);
    *out = checked_pointer_cast<Int16Array>(std::move(boxed_out));
  }

  std::shared_ptr<Int16Array> DoTake(const Int16Array& values,
                                     const Int16Array& indices) {
    std::shared_ptr<Int16Array> out;
    DoTake(values, indices, &out);
    return out;
  }

  std::shared_ptr<Int16Array> DoTakeN(uint64_t n, std::shared_ptr<Int16Array> array) {
    auto power_of_2 = array;
    array = Identity(array->length());
    while (n != 0) {
      if (n & 1) {
        array = DoTake(*array, *power_of_2);
      }
      power_of_2 = DoTake(*power_of_2, *power_of_2);
      n >>= 1;
    }
    return array;
  }

  template <typename Rng>
  void Shuffle(const Int16Array& array, Rng& gen, std::shared_ptr<Int16Array>* shuffled) {
    auto byte_length = array.length() * sizeof(int16_t);
    ASSERT_OK_AND_ASSIGN(auto data, array.values()->CopySlice(0, byte_length));
    auto mutable_data = reinterpret_cast<int16_t*>(data->mutable_data());
    std::shuffle(mutable_data, mutable_data + array.length(), gen);
    shuffled->reset(new Int16Array(array.length(), data));
  }

  template <typename Rng>
  std::shared_ptr<Int16Array> Shuffle(const Int16Array& array, Rng& gen) {
    std::shared_ptr<Int16Array> out;
    Shuffle(array, gen, &out);
    return out;
  }

  void Identity(int64_t length, std::shared_ptr<Int16Array>* identity) {
    Int16Builder identity_builder;
    ASSERT_OK(identity_builder.Resize(length));
    for (int16_t i = 0; i < length; ++i) {
      identity_builder.UnsafeAppend(i);
    }
    ASSERT_OK(identity_builder.Finish(identity));
  }

  std::shared_ptr<Int16Array> Identity(int64_t length) {
    std::shared_ptr<Int16Array> out;
    Identity(length, &out);
    return out;
  }

  std::shared_ptr<Int16Array> Inverse(const std::shared_ptr<Int16Array>& permutation) {
    auto length = static_cast<int16_t>(permutation->length());

    std::vector<bool> cycle_lengths(length + 1, false);
    auto permutation_to_the_i = permutation;
    for (int16_t cycle_length = 1; cycle_length <= length; ++cycle_length) {
      cycle_lengths[cycle_length] = HasTrivialCycle(*permutation_to_the_i);
      permutation_to_the_i = DoTake(*permutation, *permutation_to_the_i);
    }

    uint64_t cycle_to_identity_length = 1;
    for (int16_t cycle_length = length; cycle_length > 1; --cycle_length) {
      if (!cycle_lengths[cycle_length]) {
        continue;
      }
      if (cycle_to_identity_length % cycle_length == 0) {
        continue;
      }
      if (cycle_to_identity_length >
          std::numeric_limits<uint64_t>::max() / cycle_length) {
        // overflow, can't compute Inverse
        return nullptr;
      }
      cycle_to_identity_length *= cycle_length;
    }

    return DoTakeN(cycle_to_identity_length - 1, permutation);
  }

  bool HasTrivialCycle(const Int16Array& permutation) {
    for (int64_t i = 0; i < permutation.length(); ++i) {
      if (permutation.Value(i) == static_cast<int16_t>(i)) {
        return true;
      }
    }
    return false;
  }
};

TEST_F(TestPermutationsWithTake, InvertPermutation) {
  for (auto seed : std::vector<random::SeedType>({0, kRandomSeed, kRandomSeed * 2 - 1})) {
    std::default_random_engine gen(seed);
    for (int16_t length = 0; length < 1 << 10; ++length) {
      auto identity = Identity(length);
      auto permutation = Shuffle(*identity, gen);
      auto inverse = Inverse(permutation);
      if (inverse == nullptr) {
        break;
      }
      ASSERT_TRUE(DoTake(*inverse, *permutation)->Equals(identity));
    }
  }
}

class TestTakeKernelWithRecordBatch : public TestTakeKernelTyped<RecordBatch> {
 public:
  void AssertTake(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
                  const std::string& indices, const std::string& expected_batch) {
    std::shared_ptr<RecordBatch> actual;

    for (auto index_type : {int8(), uint32()}) {
      ASSERT_OK(TakeJSON(schm, batch_json, index_type, indices, &actual));
      ValidateOutput(actual);
      ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(schm, expected_batch), *actual);
    }
  }

  Status TakeJSON(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
                  const std::shared_ptr<DataType>& index_type, const std::string& indices,
                  std::shared_ptr<RecordBatch>* out) {
    auto batch = RecordBatchFromJSON(schm, batch_json);
    ARROW_ASSIGN_OR_RAISE(Datum result,
                          Take(Datum(batch), Datum(ArrayFromJSON(index_type, indices))));
    *out = result.record_batch();
    return Status::OK();
  }
};

TEST_F(TestTakeKernelWithRecordBatch, TakeRecordBatch) {
  std::vector<std::shared_ptr<Field>> fields = {field("a", int32()), field("b", utf8())};
  auto schm = schema(fields);

  auto struct_json = R"([
    {"a": null, "b": "yo"},
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])";
  this->AssertTake(schm, struct_json, "[]", "[]");
  this->AssertTake(schm, struct_json, "[3, 1, 3, 1, 3]", R"([
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": 4, "b": "eh"}
  ])");
  this->AssertTake(schm, struct_json, "[3, 1, 0]", R"([
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": null, "b": "yo"}
  ])");
  this->AssertTake(schm, struct_json, "[0, 1, 2, 3]", struct_json);
  this->AssertTake(schm, struct_json, "[0, 2, 2, 2, 2, 2, 2]", R"([
    {"a": null, "b": "yo"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"}
  ])");
}

class TestTakeKernelWithChunkedArray : public TestTakeKernelTyped<ChunkedArray> {
 public:
  void AssertTake(const std::shared_ptr<DataType>& type,
                  const std::vector<std::string>& values, const std::string& indices,
                  const std::vector<std::string>& expected) {
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK(this->TakeWithArray(type, values, indices, &actual));
    ValidateOutput(actual);
    AssertChunkedEqual(*ChunkedArrayFromJSON(type, expected), *actual);
  }

  void AssertChunkedTake(const std::shared_ptr<DataType>& type,
                         const std::vector<std::string>& values,
                         const std::vector<std::string>& indices,
                         const std::vector<std::string>& expected) {
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK(this->TakeWithChunkedArray(type, values, indices, &actual));
    ValidateOutput(actual);
    AssertChunkedEqual(*ChunkedArrayFromJSON(type, expected), *actual);
  }

  Status TakeWithArray(const std::shared_ptr<DataType>& type,
                       const std::vector<std::string>& values, const std::string& indices,
                       std::shared_ptr<ChunkedArray>* out) {
    ARROW_ASSIGN_OR_RAISE(Datum result, Take(ChunkedArrayFromJSON(type, values),
                                             ArrayFromJSON(int8(), indices)));
    *out = result.chunked_array();
    return Status::OK();
  }

  Status TakeWithChunkedArray(const std::shared_ptr<DataType>& type,
                              const std::vector<std::string>& values,
                              const std::vector<std::string>& indices,
                              std::shared_ptr<ChunkedArray>* out) {
    ARROW_ASSIGN_OR_RAISE(Datum result, Take(ChunkedArrayFromJSON(type, values),
                                             ChunkedArrayFromJSON(int8(), indices)));
    *out = result.chunked_array();
    return Status::OK();
  }
};

TEST_F(TestTakeKernelWithChunkedArray, TakeChunkedArray) {
  this->AssertTake(int8(), {"[]"}, "[]", {"[]"});
  this->AssertChunkedTake(int8(), {}, {}, {});
  this->AssertChunkedTake(int8(), {}, {"[]"}, {"[]"});
  this->AssertChunkedTake(int8(), {}, {"[null]"}, {"[null]"});
  this->AssertChunkedTake(int8(), {"[]"}, {}, {});
  this->AssertChunkedTake(int8(), {"[]"}, {"[]"}, {"[]"});
  this->AssertChunkedTake(int8(), {"[]"}, {"[null]"}, {"[null]"});

  this->AssertTake(int8(), {"[7]", "[8, 9]"}, "[0, 1, 0, 2]", {"[7, 8, 7, 9]"});
  this->AssertChunkedTake(int8(), {"[7]", "[8, 9]"}, {"[0, 1, 0]", "[]", "[2]"},
                          {"[7, 8, 7]", "[]", "[9]"});
  this->AssertTake(int8(), {"[7]", "[8, 9]"}, "[2, 1]", {"[9, 8]"});

  std::shared_ptr<ChunkedArray> arr;
  ASSERT_RAISES(IndexError,
                this->TakeWithArray(int8(), {"[7]", "[8, 9]"}, "[0, 5]", &arr));
  ASSERT_RAISES(IndexError, this->TakeWithChunkedArray(int8(), {"[7]", "[8, 9]"},
                                                       {"[0, 1, 0]", "[5, 1]"}, &arr));
  ASSERT_RAISES(IndexError, this->TakeWithChunkedArray(int8(), {}, {"[0]"}, &arr));
  ASSERT_RAISES(IndexError, this->TakeWithChunkedArray(int8(), {"[]"}, {"[0]"}, &arr));
}

class TestTakeKernelWithTable : public TestTakeKernelTyped<Table> {
 public:
  void AssertTake(const std::shared_ptr<Schema>& schm,
                  const std::vector<std::string>& table_json, const std::string& filter,
                  const std::vector<std::string>& expected_table) {
    std::shared_ptr<Table> actual;

    ASSERT_OK(this->TakeWithArray(schm, table_json, filter, &actual));
    ValidateOutput(actual);
    ASSERT_TABLES_EQUAL(*TableFromJSON(schm, expected_table), *actual);
  }

  void AssertChunkedTake(const std::shared_ptr<Schema>& schm,
                         const std::vector<std::string>& table_json,
                         const std::vector<std::string>& filter,
                         const std::vector<std::string>& expected_table) {
    std::shared_ptr<Table> actual;

    ASSERT_OK(this->TakeWithChunkedArray(schm, table_json, filter, &actual));
    ValidateOutput(actual);
    ASSERT_TABLES_EQUAL(*TableFromJSON(schm, expected_table), *actual);
  }

  Status TakeWithArray(const std::shared_ptr<Schema>& schm,
                       const std::vector<std::string>& values, const std::string& indices,
                       std::shared_ptr<Table>* out) {
    ARROW_ASSIGN_OR_RAISE(Datum result, Take(Datum(TableFromJSON(schm, values)),
                                             Datum(ArrayFromJSON(int8(), indices))));
    *out = result.table();
    return Status::OK();
  }

  Status TakeWithChunkedArray(const std::shared_ptr<Schema>& schm,
                              const std::vector<std::string>& values,
                              const std::vector<std::string>& indices,
                              std::shared_ptr<Table>* out) {
    ARROW_ASSIGN_OR_RAISE(Datum result,
                          Take(Datum(TableFromJSON(schm, values)),
                               Datum(ChunkedArrayFromJSON(int8(), indices))));
    *out = result.table();
    return Status::OK();
  }
};

TEST_F(TestTakeKernelWithTable, TakeTable) {
  std::vector<std::shared_ptr<Field>> fields = {field("a", int32()), field("b", utf8())};
  auto schm = schema(fields);

  std::vector<std::string> table_json = {
      "[{\"a\": null, \"b\": \"yo\"},{\"a\": 1, \"b\": \"\"}]",
      "[{\"a\": 2, \"b\": \"hello\"},{\"a\": 4, \"b\": \"eh\"}]"};

  this->AssertTake(schm, table_json, "[]", {"[]"});
  std::vector<std::string> expected_310 = {
      "[{\"a\": 4, \"b\": \"eh\"},{\"a\": 1, \"b\": \"\"},{\"a\": null, \"b\": \"yo\"}]"};
  this->AssertTake(schm, table_json, "[3, 1, 0]", expected_310);
  this->AssertChunkedTake(schm, table_json, {"[0, 1]", "[2, 3]"}, table_json);
}

TEST(TestTakeMetaFunction, ArityChecking) {
  ASSERT_RAISES(Invalid, CallFunction("take", ExecBatch({}, 0)));
}

// ----------------------------------------------------------------------
// Random data tests

template <typename Unused = void>
struct FilterRandomTest {
  static void Test(const std::shared_ptr<DataType>& type) {
    auto rand = random::RandomArrayGenerator(kRandomSeed);
    const int64_t length = static_cast<int64_t>(1ULL << 10);
    for (auto null_probability : {0.0, 0.01, 0.1, 0.999, 1.0}) {
      for (auto true_probability : {0.0, 0.1, 0.999, 1.0}) {
        auto values = rand.ArrayOf(type, length, null_probability);
        auto filter = rand.Boolean(length + 1, true_probability, null_probability);
        auto filter_no_nulls = rand.Boolean(length + 1, true_probability, 0.0);
        ValidateFilter(values, filter->Slice(0, values->length()));
        ValidateFilter(values, filter_no_nulls->Slice(0, values->length()));
        // Test values and filter have different offsets
        ValidateFilter(values->Slice(3), filter->Slice(4));
        ValidateFilter(values->Slice(3), filter_no_nulls->Slice(4));
      }
    }
  }
};

template <typename ValuesType, typename IndexType>
void CheckTakeRandom(const std::shared_ptr<Array>& values, int64_t indices_length,
                     double null_probability, random::RandomArrayGenerator* rand) {
  using IndexCType = typename IndexType::c_type;
  IndexCType max_index = GetMaxIndex<IndexCType>(values->length());
  auto indices = rand->Numeric<IndexType>(indices_length, static_cast<IndexCType>(0),
                                          max_index, null_probability);
  auto indices_no_nulls = rand->Numeric<IndexType>(
      indices_length, static_cast<IndexCType>(0), max_index, /*null_probability=*/0.0);
  ValidateTake<ValuesType>(values, indices);
  ValidateTake<ValuesType>(values, indices_no_nulls);
  // Sliced indices array
  if (indices_length >= 2) {
    indices = indices->Slice(1, indices_length - 2);
    indices_no_nulls = indices_no_nulls->Slice(1, indices_length - 2);
    ValidateTake<ValuesType>(values, indices);
    ValidateTake<ValuesType>(values, indices_no_nulls);
  }
}

template <typename ValuesType>
struct TakeRandomTest {
  static void Test(const std::shared_ptr<DataType>& type) {
    auto rand = random::RandomArrayGenerator(kRandomSeed);
    const int64_t values_length = 64 * 16 + 1;
    const int64_t indices_length = 64 * 4 + 1;
    for (const auto null_probability : {0.0, 0.001, 0.05, 0.25, 0.95, 0.999, 1.0}) {
      auto values = rand.ArrayOf(type, values_length, null_probability);
      CheckTakeRandom<ValuesType, Int8Type>(values, indices_length, null_probability,
                                            &rand);
      CheckTakeRandom<ValuesType, Int16Type>(values, indices_length, null_probability,
                                             &rand);
      CheckTakeRandom<ValuesType, Int32Type>(values, indices_length, null_probability,
                                             &rand);
      CheckTakeRandom<ValuesType, Int64Type>(values, indices_length, null_probability,
                                             &rand);
      CheckTakeRandom<ValuesType, UInt8Type>(values, indices_length, null_probability,
                                             &rand);
      CheckTakeRandom<ValuesType, UInt16Type>(values, indices_length, null_probability,
                                              &rand);
      CheckTakeRandom<ValuesType, UInt32Type>(values, indices_length, null_probability,
                                              &rand);
      CheckTakeRandom<ValuesType, UInt64Type>(values, indices_length, null_probability,
                                              &rand);
      // Sliced values array
      if (values_length > 2) {
        values = values->Slice(1, values_length - 2);
        CheckTakeRandom<ValuesType, UInt64Type>(values, indices_length, null_probability,
                                                &rand);
      }
    }
  }
};

TEST(TestFilter, PrimitiveRandom) { TestRandomPrimitiveCTypes<FilterRandomTest>(); }

TEST(TestFilter, RandomBoolean) { FilterRandomTest<>::Test(boolean()); }

TEST(TestFilter, RandomString) {
  FilterRandomTest<>::Test(utf8());
  FilterRandomTest<>::Test(large_utf8());
}

TEST(TestFilter, RandomFixedSizeBinary) {
  FilterRandomTest<>::Test(fixed_size_binary(0));
  FilterRandomTest<>::Test(fixed_size_binary(16));
}

TEST(TestTake, PrimitiveRandom) { TestRandomPrimitiveCTypes<TakeRandomTest>(); }

TEST(TestTake, RandomBoolean) { TakeRandomTest<BooleanType>::Test(boolean()); }

TEST(TestTake, RandomString) {
  TakeRandomTest<StringType>::Test(utf8());
  TakeRandomTest<LargeStringType>::Test(large_utf8());
}

TEST(TestTake, RandomFixedSizeBinary) {
  TakeRandomTest<FixedSizeBinaryType>::Test(fixed_size_binary(0));
  TakeRandomTest<FixedSizeBinaryType>::Test(fixed_size_binary(16));
}

// ----------------------------------------------------------------------
// DropNull tests

void AssertDropNullArrays(const std::shared_ptr<Array>& values,
                          const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> actual, DropNull(*values));
  ValidateOutput(actual);
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

Status DropNullJSON(const std::shared_ptr<DataType>& type, const std::string& values,
                    std::shared_ptr<Array>* out) {
  return DropNull(*ArrayFromJSON(type, values)).Value(out);
}

void CheckDropNull(const std::shared_ptr<DataType>& type, const std::string& values,
                   const std::string& expected) {
  std::shared_ptr<Array> actual;

  ASSERT_OK(DropNullJSON(type, values, &actual));
  ValidateOutput(actual);
  AssertArraysEqual(*ArrayFromJSON(type, expected), *actual, /*verbose=*/true);
}

struct TestDropNullKernel : public ::testing::Test {
  void TestNoValidityBitmapButUnknownNullCount(const std::shared_ptr<Array>& values) {
    ASSERT_EQ(values->null_count(), 0);
    auto expected = (*DropNull(values)).make_array();

    auto new_values = MakeArray(values->data()->Copy());
    new_values->data()->buffers[0].reset();
    new_values->data()->null_count = kUnknownNullCount;
    auto result = (*DropNull(new_values)).make_array();
    AssertArraysEqual(*expected, *result);
  }

  void TestNoValidityBitmapButUnknownNullCount(const std::shared_ptr<DataType>& type,
                                               const std::string& values) {
    TestNoValidityBitmapButUnknownNullCount(ArrayFromJSON(type, values));
  }
};

TEST_F(TestDropNullKernel, DropNull) {
  CheckDropNull(null(), "[null, null, null]", "[]");
  CheckDropNull(null(), "[null]", "[]");
}

TEST_F(TestDropNullKernel, DropNullBoolean) {
  CheckDropNull(boolean(), "[true, false, true]", "[true, false, true]");
  CheckDropNull(boolean(), "[null, false, true]", "[false, true]");
  CheckDropNull(boolean(), "[]", "[]");
  CheckDropNull(boolean(), "[null, null]", "[]");

  TestNoValidityBitmapButUnknownNullCount(boolean(), "[true, false, true]");
}

template <typename ArrowType>
struct TestDropNullKernelTyped : public TestDropNullKernel {
  TestDropNullKernelTyped() : rng_(seed_) {}

  std::shared_ptr<Int32Array> Offsets(int32_t length, int32_t slice_count) {
    return checked_pointer_cast<Int32Array>(rng_.Offsets(slice_count, 0, length));
  }

  // Slice `array` into multiple chunks along `offsets`
  ArrayVector Slices(const std::shared_ptr<Array>& array,
                     const std::shared_ptr<Int32Array>& offsets) {
    ArrayVector slices(offsets->length() - 1);
    for (int64_t i = 0; i != static_cast<int64_t>(slices.size()); ++i) {
      slices[i] =
          array->Slice(offsets->Value(i), offsets->Value(i + 1) - offsets->Value(i));
    }
    return slices;
  }

  random::SeedType seed_ = 0xdeadbeef;
  random::RandomArrayGenerator rng_;
};

template <typename ArrowType>
class TestDropNullKernelWithNumeric : public TestDropNullKernelTyped<ArrowType> {
 protected:
  void AssertDropNull(const std::string& values, const std::string& expected) {
    CheckDropNull(type_singleton(), values, expected);
  }

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_SUITE(TestDropNullKernelWithNumeric, NumericArrowTypes);
TYPED_TEST(TestDropNullKernelWithNumeric, DropNullNumeric) {
  this->AssertDropNull("[7, 8, 9]", "[7, 8, 9]");
  this->AssertDropNull("[null, 8, 9]", "[8, 9]");
  this->AssertDropNull("[null, null, null]", "[]");
}

template <typename TypeClass>
class TestDropNullKernelWithString : public TestDropNullKernelTyped<TypeClass> {
 public:
  std::shared_ptr<DataType> value_type() {
    return TypeTraits<TypeClass>::type_singleton();
  }

  void AssertDropNull(const std::string& values, const std::string& expected) {
    CheckDropNull(value_type(), values, expected);
  }

  void AssertDropNullDictionary(const std::string& dictionary_values,
                                const std::string& dictionary_indices,
                                const std::string& expected_indices) {
    auto dict = ArrayFromJSON(value_type(), dictionary_values);
    auto type = dictionary(int8(), value_type());
    ASSERT_OK_AND_ASSIGN(auto values,
                         DictionaryArray::FromArrays(
                             type, ArrayFromJSON(int8(), dictionary_indices), dict));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), expected_indices), dict));
    AssertDropNullArrays(values, expected);
  }
};

TYPED_TEST_SUITE(TestDropNullKernelWithString, BaseBinaryArrowTypes);

TYPED_TEST(TestDropNullKernelWithString, DropNullString) {
  this->AssertDropNull(R"(["a", "b", "c"])", R"(["a", "b", "c"])");
  this->AssertDropNull(R"([null, "b", "c"])", "[\"b\", \"c\"]");
  this->AssertDropNull(R"(["a", "b", null])", R"(["a", "b"])");

  this->TestNoValidityBitmapButUnknownNullCount(this->value_type(), R"(["a", "b", "c"])");
}

TYPED_TEST(TestDropNullKernelWithString, DropNullDictionary) {
  auto dict = R"(["a", "b", "c", "d", "e"])";
  this->AssertDropNullDictionary(dict, "[3, 4, 2]", "[3, 4, 2]");
  this->AssertDropNullDictionary(dict, "[null, 4, 2]", "[4, 2]");
}

class TestDropNullKernelFSB : public TestDropNullKernelTyped<FixedSizeBinaryType> {
 public:
  std::shared_ptr<DataType> value_type() { return fixed_size_binary(3); }

  void AssertDropNull(const std::string& values, const std::string& expected) {
    CheckDropNull(value_type(), values, expected);
  }
};

TEST_F(TestDropNullKernelFSB, DropNullFixedSizeBinary) {
  this->AssertDropNull(R"(["aaa", "bbb", "ccc"])", R"(["aaa", "bbb", "ccc"])");
  this->AssertDropNull(R"([null, "bbb", "ccc"])", "[\"bbb\", \"ccc\"]");

  this->TestNoValidityBitmapButUnknownNullCount(this->value_type(),
                                                R"(["aaa", "bbb", "ccc"])");
}

class TestDropNullKernelWithList : public TestDropNullKernelTyped<ListType> {};

TEST_F(TestDropNullKernelWithList, DropNullListInt32) {
  std::string list_json = "[[], [1,2], null, [3]]";
  CheckDropNull(list(int32()), list_json, "[[], [1,2], [3]]");
  this->TestNoValidityBitmapButUnknownNullCount(list(int32()), "[[], [1,2], [3]]");
}

TEST_F(TestDropNullKernelWithList, DropNullListListInt32) {
  std::string list_json = R"([
    [],
    [[1], [2, null, 2], []],
    null,
    [[3, null], null]
  ])";
  auto type = list(list(int32()));
  CheckDropNull(type, list_json, R"([
    [],
    [[1], [2, null, 2], []],
    [[3, null], null]
  ])");

  this->TestNoValidityBitmapButUnknownNullCount(type,
                                                "[[[1], [2, null, 2], []], [[3, null]]]");
}

class TestDropNullKernelWithLargeList : public TestDropNullKernelTyped<LargeListType> {};

TEST_F(TestDropNullKernelWithLargeList, DropNullLargeListInt32) {
  std::string list_json = "[[], [1,2], null, [3]]";
  CheckDropNull(large_list(int32()), list_json, "[[], [1,2],  [3]]");

  this->TestNoValidityBitmapButUnknownNullCount(
      fixed_size_list(int32(), 3), "[[1, null, 3], [4, 5, 6], [7, 8, null]]");
}

class TestDropNullKernelWithFixedSizeList
    : public TestDropNullKernelTyped<FixedSizeListType> {};

TEST_F(TestDropNullKernelWithFixedSizeList, DropNullFixedSizeListInt32) {
  std::string list_json = "[null, [1, null, 3], [4, 5, 6], [7, 8, null]]";
  CheckDropNull(fixed_size_list(int32(), 3), list_json,
                "[[1, null, 3], [4, 5, 6], [7, 8, null]]");

  this->TestNoValidityBitmapButUnknownNullCount(
      fixed_size_list(int32(), 3), "[[1, null, 3], [4, 5, 6], [7, 8, null]]");
}

class TestDropNullKernelWithMap : public TestDropNullKernelTyped<MapType> {};

TEST_F(TestDropNullKernelWithMap, DropNullMapStringToInt32) {
  std::string map_json = R"([
    [["joe", 0], ["mark", null]],
    null,
    [["cap", 8]],
    []
  ])";
  std::string expected_json = R"([
    [["joe", 0], ["mark", null]],
    [["cap", 8]],
    []
  ])";
  CheckDropNull(map(utf8(), int32()), map_json, expected_json);
}

class TestDropNullKernelWithStruct : public TestDropNullKernelTyped<StructType> {};

TEST_F(TestDropNullKernelWithStruct, DropNullStruct) {
  auto struct_type = struct_({field("a", int32()), field("b", utf8())});
  auto struct_json = R"([
    null,
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])";
  auto expected_struct_json = R"([
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])";
  CheckDropNull(struct_type, struct_json, expected_struct_json);
  this->TestNoValidityBitmapButUnknownNullCount(struct_type, expected_struct_json);
}

class TestDropNullKernelWithUnion : public TestDropNullKernelTyped<UnionType> {};

TEST_F(TestDropNullKernelWithUnion, DropNullUnion) {
  auto union_type = dense_union({field("a", int32()), field("b", utf8())}, {2, 5});
  auto union_json = R"([
      [2, null],
      [2, 222],
      [5, "hello"],
      [5, "eh"],
      [2, null],
      [2, 111],
      [5, null]
    ])";
  CheckDropNull(union_type, union_json, union_json);
}

class TestDropNullKernelWithRecordBatch : public TestDropNullKernelTyped<RecordBatch> {
 public:
  void AssertDropNull(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
                      const std::string& expected_batch) {
    std::shared_ptr<RecordBatch> actual;

    ASSERT_OK(this->DoDropNull(schm, batch_json, &actual));
    ValidateOutput(actual);
    ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(schm, expected_batch), *actual);
  }

  Status DoDropNull(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
                    std::shared_ptr<RecordBatch>* out) {
    auto batch = RecordBatchFromJSON(schm, batch_json);
    ARROW_ASSIGN_OR_RAISE(Datum out_datum, DropNull(batch));
    *out = out_datum.record_batch();
    return Status::OK();
  }
};

TEST_F(TestDropNullKernelWithRecordBatch, DropNullRecordBatch) {
  std::vector<std::shared_ptr<Field>> fields = {field("a", int32()), field("b", utf8())};
  auto schm = schema(fields);

  auto batch_json = R"([
    {"a": null, "b": "yo"},
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])";
  this->AssertDropNull(schm, batch_json, R"([
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])");

  batch_json = R"([
    {"a": null, "b": "yo"},
    {"a": 1, "b": null},
    {"a": null, "b": "hello"},
    {"a": 4, "b": null}
  ])";
  this->AssertDropNull(schm, batch_json, R"([])");
  this->AssertDropNull(schm, R"([])", R"([])");
}

class TestDropNullKernelWithChunkedArray : public TestDropNullKernelTyped<ChunkedArray> {
 public:
  TestDropNullKernelWithChunkedArray()
      : sizes_({0, 1, 2, 4, 16, 31, 1234}),
        null_probabilities_({0.0, 0.1, 0.5, 0.9, 1.0}) {}

  void AssertDropNull(const std::shared_ptr<DataType>& type,
                      const std::vector<std::string>& values,
                      const std::vector<std::string>& expected) {
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK(this->DoDropNull(type, values, &actual));
    ValidateOutput(actual);

    AssertChunkedEqual(*ChunkedArrayFromJSON(type, expected), *actual);
  }

  Status DoDropNull(const std::shared_ptr<DataType>& type,
                    const std::vector<std::string>& values,
                    std::shared_ptr<ChunkedArray>* out) {
    ARROW_ASSIGN_OR_RAISE(Datum out_datum, DropNull(ChunkedArrayFromJSON(type, values)));
    *out = out_datum.chunked_array();
    return Status::OK();
  }

  template <typename ArrayFactory>
  void CheckDropNullWithSlices(ArrayFactory&& factory) {
    for (auto size : this->sizes_) {
      for (auto null_probability : this->null_probabilities_) {
        std::shared_ptr<Array> concatenated_array;
        std::shared_ptr<ChunkedArray> chunked_array;
        factory(size, null_probability, &chunked_array, &concatenated_array);

        ASSERT_OK_AND_ASSIGN(auto out_datum, DropNull(chunked_array));
        auto actual_chunked_array = out_datum.chunked_array();
        ASSERT_OK_AND_ASSIGN(auto actual, Concatenate(actual_chunked_array->chunks()));

        ASSERT_OK_AND_ASSIGN(out_datum, DropNull(*concatenated_array));
        auto expected = out_datum.make_array();

        AssertArraysEqual(*expected, *actual);
      }
    }
  }

  std::vector<int32_t> sizes_;
  std::vector<double> null_probabilities_;
};

TEST_F(TestDropNullKernelWithChunkedArray, DropNullChunkedArray) {
  this->AssertDropNull(int8(), {"[]"}, {"[]"});
  this->AssertDropNull(int8(), {"[null]", "[8, null]"}, {"[8]"});

  this->AssertDropNull(int8(), {"[null]", "[null, null]"}, {"[]"});
  this->AssertDropNull(int8(), {"[7]", "[8, 9]"}, {"[7]", "[8, 9]"});
  this->AssertDropNull(int8(), {"[]", "[]"}, {"[]", "[]"});
}

TEST_F(TestDropNullKernelWithChunkedArray, DropNullChunkedArrayWithSlices) {
  // With Null Arrays
  this->CheckDropNullWithSlices([this](int32_t size, double null_probability,
                                       std::shared_ptr<ChunkedArray>* out_chunked_array,
                                       std::shared_ptr<Array>* out_concatenated_array) {
    auto array = std::make_shared<NullArray>(size);
    auto offsets = this->Offsets(size, 3);
    auto slices = this->Slices(array, offsets);
    *out_chunked_array = std::make_shared<ChunkedArray>(std::move(slices));

    ASSERT_OK_AND_ASSIGN(*out_concatenated_array,
                         Concatenate((*out_chunked_array)->chunks()));
  });
  // Without Null Arrays
  this->CheckDropNullWithSlices([this](int32_t size, double null_probability,
                                       std::shared_ptr<ChunkedArray>* out_chunked_array,
                                       std::shared_ptr<Array>* out_concatenated_array) {
    auto array = this->rng_.ArrayOf(int16(), size, null_probability);
    auto offsets = this->Offsets(size, 3);
    auto slices = this->Slices(array, offsets);
    *out_chunked_array = std::make_shared<ChunkedArray>(std::move(slices));

    ASSERT_OK_AND_ASSIGN(*out_concatenated_array,
                         Concatenate((*out_chunked_array)->chunks()));
  });
}

class TestDropNullKernelWithTable : public TestDropNullKernelTyped<Table> {
 public:
  TestDropNullKernelWithTable()
      : sizes_({0, 1, 4, 31, 1234}), null_probabilities_({0.0, 0.1, 0.5, 0.9, 1.0}) {}

  void AssertDropNull(const std::shared_ptr<Schema>& schm,
                      const std::vector<std::string>& table_json,
                      const std::vector<std::string>& expected_table) {
    std::shared_ptr<Table> actual;
    ASSERT_OK(this->DoDropNull(schm, table_json, &actual));
    ValidateOutput(actual);
    ASSERT_TABLES_EQUAL(*TableFromJSON(schm, expected_table), *actual);
  }

  Status DoDropNull(const std::shared_ptr<Schema>& schm,
                    const std::vector<std::string>& values, std::shared_ptr<Table>* out) {
    ARROW_ASSIGN_OR_RAISE(Datum out_datum, DropNull(TableFromJSON(schm, values)));
    *out = out_datum.table();
    return Status::OK();
  }

  template <typename ArrayFactory>
  void CheckDropNullWithSlices(ArrayFactory&& factory) {
    for (auto size : this->sizes_) {
      for (auto null_probability : this->null_probabilities_) {
        std::shared_ptr<Table> table_w_slices;
        std::shared_ptr<Table> table_wo_slices;

        factory(size, null_probability, &table_w_slices, &table_wo_slices);

        ASSERT_OK_AND_ASSIGN(auto out_datum, DropNull(table_w_slices));
        ValidateOutput(out_datum);
        auto actual = out_datum.table();

        ASSERT_OK_AND_ASSIGN(out_datum, DropNull(table_wo_slices));
        ValidateOutput(out_datum);
        auto expected = out_datum.table();
        if (actual->num_rows() > 0) {
          ASSERT_TRUE(actual->num_rows() == expected->num_rows());
          for (int index = 0; index < actual->num_columns(); index++) {
            ASSERT_OK_AND_ASSIGN(auto actual_col,
                                 Concatenate(actual->column(index)->chunks()));
            ASSERT_OK_AND_ASSIGN(auto expected_col,
                                 Concatenate(expected->column(index)->chunks()));
            AssertArraysEqual(*actual_col, *expected_col);
          }
        }
      }
    }
  }

  std::vector<int32_t> sizes_;
  std::vector<double> null_probabilities_;
};

TEST_F(TestDropNullKernelWithTable, DropNullTable) {
  std::vector<std::shared_ptr<Field>> fields = {field("a", int32()), field("b", utf8())};
  auto schm = schema(fields);

  {
    std::vector<std::string> table_json = {R"([
      {"a": null, "b": "yo"},
      {"a": 1, "b": ""}
    ])",
                                           R"([
      {"a": 2, "b": "hello"},
      {"a": 4, "b": "eh"}
    ])"};
    std::vector<std::string> expected_table_json = {R"([
      {"a": 1, "b": ""}
    ])",
                                                    R"([
      {"a": 2, "b": "hello"},
      {"a": 4, "b": "eh"}
    ])"};
    this->AssertDropNull(schm, table_json, expected_table_json);
  }
  {
    std::vector<std::string> table_json = {R"([
      {"a": null, "b": "yo"},
      {"a": 1, "b": null}
    ])",
                                           R"([
      {"a": 2, "b": null},
      {"a": null, "b": "eh"}
    ])"};
    std::shared_ptr<Table> actual;
    ASSERT_OK(this->DoDropNull(schm, table_json, &actual));
    AssertSchemaEqual(schm, actual->schema());
    ASSERT_EQ(actual->num_rows(), 0);
  }
}

TEST_F(TestDropNullKernelWithTable, DropNullTableWithSlices) {
  // With Null Arrays
  this->CheckDropNullWithSlices([this](int32_t size, double null_probability,
                                       std::shared_ptr<Table>* out_table_w_slices,
                                       std::shared_ptr<Table>* out_table_wo_slices) {
    FieldVector fields = {field("a", int32()), field("b", utf8())};
    auto schm = schema(fields);
    ASSERT_OK_AND_ASSIGN(auto col_a, MakeArrayOfNull(int32(), size));
    ASSERT_OK_AND_ASSIGN(auto col_b, MakeArrayOfNull(utf8(), size));

    // Compute random chunkings of columns `a` and `b`
    auto slices_a = this->Slices(col_a, this->Offsets(size, 3));
    auto slices_b = this->Slices(col_b, this->Offsets(size, 3));

    ChunkedArrayVector table_content_w_slices{
        std::make_shared<ChunkedArray>(std::move(slices_a)),
        std::make_shared<ChunkedArray>(std::move(slices_b))};
    *out_table_w_slices = Table::Make(schm, std::move(table_content_w_slices), size);

    ChunkedArrayVector table_content_wo_slices{std::make_shared<ChunkedArray>(col_a),
                                               std::make_shared<ChunkedArray>(col_b)};
    *out_table_wo_slices = Table::Make(schm, std::move(table_content_wo_slices), size);
  });

  // Without Null Arrays
  this->CheckDropNullWithSlices([this](int32_t size, double null_probability,
                                       std::shared_ptr<Table>* out_table_w_slices,
                                       std::shared_ptr<Table>* out_table_wo_slices) {
    FieldVector fields = {field("a", int32()), field("b", utf8())};
    auto schm = schema(fields);
    auto col_a = this->rng_.ArrayOf(int32(), size, null_probability);
    auto col_b = this->rng_.ArrayOf(utf8(), size, null_probability);

    // Compute random chunkings of columns `a` and `b`
    auto slices_a = this->Slices(col_a, this->Offsets(size, 3));
    auto slices_b = this->Slices(col_b, this->Offsets(size, 3));

    ChunkedArrayVector table_content_w_slices{
        std::make_shared<ChunkedArray>(std::move(slices_a)),
        std::make_shared<ChunkedArray>(std::move(slices_b))};
    *out_table_w_slices = Table::Make(schm, std::move(table_content_w_slices), size);

    ChunkedArrayVector table_content_wo_slices{std::make_shared<ChunkedArray>(col_a),
                                               std::make_shared<ChunkedArray>(col_b)};
    *out_table_wo_slices = Table::Make(schm, std::move(table_content_wo_slices), size);
  });
}

TEST(TestIndicesNonZero, IndicesNonZero) {
  Datum actual;
  std::shared_ptr<Array> result;

  for (const auto& type : NumericTypes()) {
    ARROW_SCOPED_TRACE("Input type = ", type->ToString());

    ASSERT_OK_AND_ASSIGN(
        actual,
        CallFunction("indices_nonzero", {ArrayFromJSON(type, "[null, 50, 0, 10]")}));
    result = actual.make_array();
    AssertArraysEqual(*ArrayFromJSON(uint64(), "[1, 3]"), *result, /*verbose*/ true);

    // empty
    ASSERT_OK_AND_ASSIGN(actual,
                         CallFunction("indices_nonzero", {ArrayFromJSON(type, "[]")}));
    result = actual.make_array();
    AssertArraysEqual(*ArrayFromJSON(uint64(), "[]"), *result, /*verbose*/ true);

    // chunked
    ChunkedArray chunked_arr(
        {ArrayFromJSON(type, "[1, 0, 3]"), ArrayFromJSON(type, "[4, 0, 6]")});
    ASSERT_OK_AND_ASSIGN(
        actual, CallFunction("indices_nonzero", {static_cast<Datum>(chunked_arr)}));
    AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 2, 3, 5]"), *actual.make_array(),
                      /*verbose*/ true);

    // empty chunked
    ChunkedArray chunked_arr_empty({ArrayFromJSON(type, "[1, 0, 3]"),
                                    ArrayFromJSON(type, "[]"),
                                    ArrayFromJSON(type, "[4, 0, 6]")});
    ASSERT_OK_AND_ASSIGN(
        actual, CallFunction("indices_nonzero", {static_cast<Datum>(chunked_arr_empty)}));
    AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 2, 3, 5]"), *actual.make_array(),
                      /*verbose*/ true);
  }
}

TEST(TestIndicesNonZero, IndicesNonZeroBoolean) {
  Datum actual;
  std::shared_ptr<Array> result;

  // boool
  ASSERT_OK_AND_ASSIGN(
      actual, CallFunction("indices_nonzero",
                           {ArrayFromJSON(boolean(), "[null, true, false, true]")}));
  result = actual.make_array();
  AssertArraysEqual(*result, *ArrayFromJSON(uint64(), "[1, 3]"), /*verbose*/ true);
}

TEST(TestIndicesNonZero, IndicesNonZeroDecimal) {
  Datum actual;
  std::shared_ptr<Array> result;

  for (const auto& decimal_factory : {decimal128, decimal256}) {
    ASSERT_OK_AND_ASSIGN(
        actual, CallFunction("indices_nonzero",
                             {DecimalArrayFromJSON(decimal_factory(2, -2),
                                                   R"(["12E2",null,"0","0"])")}));
    result = actual.make_array();
    AssertArraysEqual(*ArrayFromJSON(uint64(), "[0]"), *result, /*verbose*/ true);

    ASSERT_OK_AND_ASSIGN(
        actual,
        CallFunction(
            "indices_nonzero",
            {DecimalArrayFromJSON(
                decimal_factory(6, 9),
                R"(["765483.999999999","0.000000000",null,"-987645.000000001"])")}));
    result = actual.make_array();
    AssertArraysEqual(*ArrayFromJSON(uint64(), "[0, 3]"), *result, /*verbose*/ true);
  }
}

}  // namespace compute
}  // namespace arrow
