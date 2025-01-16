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

#include "arrow/array/concatenate.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util_internal.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/logging.h"

namespace arrow::compute {

namespace {

using SmallSignedIntegerTypes = ::testing::Types<Int8Type, Int16Type>;

}  // namespace

// ----------------------------------------------------------------------
// InversePermutation tests

namespace {

Result<Datum> InversePermutation(const Datum& indices, int64_t max_index,
                                 std::shared_ptr<DataType> output_type) {
  InversePermutationOptions options{max_index, std::move(output_type)};
  return InversePermutation(indices, options);
}

void AssertInversePermutation(const Datum& indices, int64_t max_index,
                              const std::shared_ptr<DataType>& output_type,
                              const Datum& expected, bool validity_must_be_null) {
  ASSERT_OK_AND_ASSIGN(auto result, InversePermutation(indices, max_index, output_type));
  ValidateOutput(result);
  ASSERT_EQ(indices.kind(), result.kind());
  std::shared_ptr<Array> result_array;
  if (result.is_array()) {
    result_array = result.make_array();
  } else {
    ASSERT_TRUE(result.is_chunked_array());
    ASSERT_OK_AND_ASSIGN(result_array, Concatenate(result.chunked_array()->chunks()));
  }
  AssertDatumsEqual(expected, result_array);
  if (validity_must_be_null) {
    ASSERT_FALSE(result_array->data()->HasValidityBitmap());
  }
}

template <typename InputFunc>
void DoTestInversePermutationForInputTypes(
    const std::vector<std::shared_ptr<DataType>>& input_types, InputFunc&& input,
    int64_t max_index, const std::shared_ptr<DataType>& output_type,
    const Datum& expected, bool validity_must_be_null = false) {
  for (const auto& input_type : input_types) {
    ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
    ASSERT_OK_AND_ASSIGN(auto indices, input(input_type));
    AssertInversePermutation(indices, max_index, output_type, expected,
                             validity_must_be_null);
  }
}

template <typename InputFunc>
void DoTestInversePermutationForInputOutputTypes(
    const std::vector<std::shared_ptr<DataType>>& input_types,
    const std::vector<std::shared_ptr<DataType>>& output_types, InputFunc&& input,
    int64_t max_index, const std::string& expected_str, bool validity_must_be_null) {
  for (const auto& output_type : output_types) {
    ARROW_SCOPED_TRACE("Output type: " + output_type->ToString());
    auto expected = ArrayFromJSON(output_type, expected_str);
    DoTestInversePermutationForInputTypes(input_types, std::forward<InputFunc>(input),
                                          max_index, output_type, expected,
                                          validity_must_be_null);
  }
}

void TestInversePermutationForInputOutputTypes(
    const std::vector<std::shared_ptr<DataType>>& input_types,
    const std::vector<std::shared_ptr<DataType>>& output_types,
    const std::vector<std::string>& indices_chunked_str, int64_t max_index,
    const std::string& expected_str, bool validity_must_be_null) {
  {
    ARROW_SCOPED_TRACE("Array");
    DoTestInversePermutationForInputOutputTypes(
        input_types, output_types,
        [&](const std::shared_ptr<DataType>& input_type) -> Result<Datum> {
          auto chunked = ChunkedArrayFromJSON(input_type, indices_chunked_str);
          return Concatenate(chunked->chunks());
        },
        max_index, expected_str, validity_must_be_null);
  }
  {
    ARROW_SCOPED_TRACE("Chunked");
    DoTestInversePermutationForInputOutputTypes(
        input_types, output_types,
        [&](const std::shared_ptr<DataType>& input_type) -> Result<Datum> {
          return ChunkedArrayFromJSON(input_type, indices_chunked_str);
        },
        max_index, expected_str, validity_must_be_null);
  }
}

void TestInversePermutation(const std::vector<std::string>& indices_chunked_str,
                            int64_t max_index, const std::string& expected_str,
                            bool validity_must_be_null = false) {
  TestInversePermutationForInputOutputTypes(SignedIntTypes(), SignedIntTypes(),
                                            indices_chunked_str, max_index, expected_str,
                                            validity_must_be_null);
}

}  // namespace

TEST(InversePermutation, InvalidOutputType) {
  {
    ARROW_SCOPED_TRACE("Output type unsigned");
    auto indices = ArrayFromJSON(int32(), "[]");
    ASSERT_RAISES_WITH_MESSAGE(
        TypeError,
        "Type error: Output type of inverse_permutation must be signed integer, got "
        "uint32",
        InversePermutation(indices, /*max_index=*/0, /*output_type=*/uint32()));
  }
  {
    ARROW_SCOPED_TRACE("Output type float");
    auto indices = ArrayFromJSON(int32(), "[]");
    ASSERT_RAISES_WITH_MESSAGE(
        TypeError,
        "Type error: Output type of inverse_permutation must be signed integer, got "
        "float",
        InversePermutation(indices, /*max_index=*/0, /*output_type=*/float32()));
  }
  {
    ARROW_SCOPED_TRACE("Output type string");
    auto indices = ArrayFromJSON(int32(), "[]");
    ASSERT_RAISES_WITH_MESSAGE(
        TypeError,
        "Type error: Output type of inverse_permutation must be signed integer, got "
        "string",
        InversePermutation(indices, /*max_index=*/0, /*output_type=*/utf8()));
  }
}

TEST(InversePermutation, DefaultOptions) {
  {
    ARROW_SCOPED_TRACE("Default options values");
    InversePermutationOptions options;
    ASSERT_EQ(options.max_index, -1);
    ASSERT_EQ(options.output_type, nullptr);
  }
  {
    ARROW_SCOPED_TRACE("Default options semantics");
    for (const auto& input_type : SignedIntTypes()) {
      ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
      auto indices = ArrayFromJSON(input_type, "[0]");
      ASSERT_OK_AND_ASSIGN(Datum result, InversePermutation(indices));
      AssertDatumsEqual(indices, result);
    }
  }
}

TEST(InversePermutation, InvalidIndex) {
  {
    ARROW_SCOPED_TRACE("Negative index");
    auto indices = ArrayFromJSON(int32(), "[-1]");
    ASSERT_RAISES_WITH_MESSAGE(IndexError, "Index error: Index out of bounds: -1",
                               InversePermutation(indices));
  }
  {
    ARROW_SCOPED_TRACE("Exceeds max_index");
    auto indices = ArrayFromJSON(int32(), "[42]");
    ASSERT_RAISES_WITH_MESSAGE(
        IndexError, "Index error: Index out of bounds: 42",
        InversePermutation(indices, /*max_index=*/1, /*output_type=*/int32()));
  }
}

TEST(InversePermutation, Basic) {
  {
    ARROW_SCOPED_TRACE("Basic");
    std::vector<std::string> indices_chunked{
        "[]", "[9, 7, 5, 3, 1]", "[0]", "[2, 4, 6]", "[8]", "[]"};
    int64_t max_index = 9;
    auto expected = "[5, 4, 6, 3, 7, 2, 8, 1, 9, 0]";
    TestInversePermutation(indices_chunked, max_index, expected,
                           /*validity_must_be_null=*/true);
  }
  {
    ARROW_SCOPED_TRACE("Basic with nulls");
    std::vector<std::string> indices_chunked{
        "[]", "[9, 7, 5, 3, 1]", "[null]", "[null, null, null]", "[null]", "[]"};
    int64_t max_index = 9;
    auto expected = "[null, 4, null, 3, null, 2, null, 1, null, 0]";
    TestInversePermutation(indices_chunked, max_index, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output greater than input");
    std::vector<std::string> indices_chunked{"[]", "[1]", "[]", "[2]"};
    int64_t max_index = 6;
    auto expected = "[null, 0, 1, null, null, null, null]";
    TestInversePermutation(indices_chunked, max_index, expected);
  }
  {
    ARROW_SCOPED_TRACE("Input all null");
    std::vector<std::string> indices_chunked{"[]", "[null]", "[]", "[null]"};
    int64_t max_index = 1;
    auto expected = "[null, null]";
    TestInversePermutation(indices_chunked, max_index, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty input output null");
    std::vector<std::string> indices_chunked{"[]", "[]", "[]", "[]"};
    int64_t max_index = 6;
    auto expected = "[null, null, null, null, null, null, null]";
    TestInversePermutation(indices_chunked, max_index, expected);
  }
  {
    ARROW_SCOPED_TRACE("Input duplicated indices");
    std::vector<std::string> indices_chunked{"[]", "[1, 2]", "[3, 1, 2, 3, 1]",
                                             "[]", "[2]",    "[3]"};
    int64_t max_index = 4;
    auto expected = "[null, 6, 7, 8, null]";
    TestInversePermutation(indices_chunked, max_index, expected);
  }
}

template <typename ArrowType>
class TestInversePermutationSmallOutputType : public ::testing::Test {
 protected:
  using CType = typename TypeTraits<ArrowType>::CType;

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_SUITE(TestInversePermutationSmallOutputType, SmallSignedIntegerTypes);

TYPED_TEST(TestInversePermutationSmallOutputType, JustEnoughOutputType) {
  auto output_type = this->type_singleton();
  int64_t input_length =
      static_cast<int64_t>(std::numeric_limits<typename TestFixture::CType>::max());
  auto expected =
      ArrayFromJSON(output_type, "[" + std::to_string(input_length - 1) + "]");
  DoTestInversePermutationForInputTypes(
      SignedIntTypes(),
      [&](const std::shared_ptr<DataType>& input_type) -> Result<Datum> {
        return ConstantArrayGenerator::Zeroes(input_length, input_type);
      },
      /*max_index=*/0, output_type, expected);
}

TYPED_TEST(TestInversePermutationSmallOutputType, InsufficientOutputType) {
  auto output_type = this->type_singleton();
  int64_t input_length =
      static_cast<int64_t>(std::numeric_limits<typename TestFixture::CType>::max()) + 1;
  for (const auto& input_type : SignedIntTypes()) {
    ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
    auto indices = ConstantArrayGenerator::Zeroes(input_length, input_type);
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid,
        "Invalid: Output type " + output_type->ToString() +
            " of inverse_permutation is insufficient to store indices of length " +
            std::to_string(input_length),
        InversePermutation(indices, /*max_index=*/0, output_type));
  }
}

// ----------------------------------------------------------------------
// Scatter tests
//
// Shorthand notation:
//
//   A = Array
//   C = ChunkedArray

namespace {

Result<Datum> Scatter(const Datum& values, const Datum& indices, int64_t max_index) {
  ScatterOptions options{max_index};
  ARROW_ASSIGN_OR_RAISE(Datum result, Scatter(values, indices, options));
  ValidateOutput(result);
  return result;
}

void AssertScatterAAA(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<Array>& indices, int64_t max_index,
                      const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(Datum result, Scatter(values, indices, max_index));
  AssertDatumsEqual(expected, result);
}

void AssertScatterCAC(const std::shared_ptr<ChunkedArray>& values,
                      const std::shared_ptr<Array>& indices, int64_t max_index,
                      const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(Datum result, Scatter(values, indices, max_index));
  ASSERT_TRUE(result.is_chunked_array());
  ASSERT_OK_AND_ASSIGN(auto result_array, Concatenate(result.chunked_array()->chunks()));
  AssertDatumsEqual(expected, result_array);
}

void AssertScatterACC(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<ChunkedArray>& indices, int64_t max_index,
                      const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(Datum result, Scatter(values, indices, max_index));
  ASSERT_TRUE(result.is_chunked_array());
  ASSERT_OK_AND_ASSIGN(auto result_array, Concatenate(result.chunked_array()->chunks()));
  AssertDatumsEqual(expected, result_array);
}

void AssertScatterCCC(const std::shared_ptr<ChunkedArray>& values,
                      const std::shared_ptr<ChunkedArray>& indices, int64_t max_index,
                      const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(Datum result, Scatter(values, indices, max_index));
  ASSERT_TRUE(result.is_chunked_array());
  ASSERT_OK_AND_ASSIGN(auto result_array, Concatenate(result.chunked_array()->chunks()));
  AssertDatumsEqual(expected, result_array);
}

void DoTestScatterAAA(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<Array>& indices, int64_t max_index,
                      const std::shared_ptr<Array>& expected) {
  AssertScatterAAA(values, indices, max_index, expected);
}

/// The following helper functions are based on the invariant:
/// Scatter([V, V], [I, I'], 2 * (m + 1) - 1) == Concat(E, E)
///
/// where
///   V = values
///   I = indices
///   m = max_index
///   I' = I + (m + 1)
///   E = Scatter(V, I, m)

/// Make indices suffix I' = I + (m + 1).
Result<std::shared_ptr<Array>> MakeIndicesSuffix(const std::shared_ptr<Array>& indices,
                                                 int64_t max_index) {
  ARROW_ASSIGN_OR_RAISE(auto m_plus_one, MakeScalar(indices->type(), max_index + 1));
  ARROW_ASSIGN_OR_RAISE(auto indices_plus_m_plus_one, Add(indices, m_plus_one));
  return indices_plus_m_plus_one.make_array();
}

void DoTestScatterCACWithArrays(const std::shared_ptr<Array>& values,
                                const std::shared_ptr<Array>& indices, int64_t max_index,
                                const std::shared_ptr<Array>& expected) {
  auto chunked_values2 = std::make_shared<ChunkedArray>(ArrayVector{values, values});

  auto indices_prefix = indices;
  ASSERT_OK_AND_ASSIGN(auto indices_suffix, MakeIndicesSuffix(indices, max_index));
  ASSERT_OK_AND_ASSIGN(auto concat_indices2,
                       Concatenate({indices_prefix, indices_suffix}));

  ASSERT_OK_AND_ASSIGN(auto concat_expected2,
                       Concatenate(ArrayVector{expected, expected}));

  AssertScatterCAC(chunked_values2, concat_indices2, (max_index + 1) * 2 - 1,
                   concat_expected2);
}

void DoTestScatterACCWithArrays(const std::shared_ptr<Array>& values,
                                const std::shared_ptr<Array>& indices, int64_t max_index,
                                const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(auto concat_values2, Concatenate(ArrayVector{values, values}));

  auto indices_prefix = indices;
  ASSERT_OK_AND_ASSIGN(auto indices_suffix, MakeIndicesSuffix(indices, max_index));
  auto chunked_indices2 =
      std::make_shared<ChunkedArray>(ArrayVector{indices_prefix, indices_suffix});

  ASSERT_OK_AND_ASSIGN(auto concat_expected2,
                       Concatenate(ArrayVector{expected, expected}));

  AssertScatterACC(concat_values2, chunked_indices2, (max_index + 1) * 2 - 1,
                   concat_expected2);
}

void DoTestScatterCCCWithArrays(const std::shared_ptr<Array>& values,
                                const std::shared_ptr<Array>& indices, int64_t max_index,
                                const std::shared_ptr<Array>& expected) {
  auto chunked_values2 = std::make_shared<ChunkedArray>(ArrayVector{values, values});

  auto indices_prefix = indices;
  ASSERT_OK_AND_ASSIGN(auto indices_suffix, MakeIndicesSuffix(indices, max_index));
  auto chunked_indices2 =
      std::make_shared<ChunkedArray>(ArrayVector{indices_prefix, indices_suffix});

  ASSERT_OK_AND_ASSIGN(auto concat_expected2,
                       Concatenate(ArrayVector{expected, expected}));

  AssertScatterCCC(chunked_values2, chunked_indices2, (max_index + 1) * 2 - 1,
                   concat_expected2);
}

void DoTestScatterForIndicesTypes(
    const std::vector<std::shared_ptr<DataType>>& indices_types,
    const std::shared_ptr<Array>& values, const std::shared_ptr<Array>& indices,
    int64_t max_index, const std::shared_ptr<Array>& expected) {
  for (const auto& indices_type : indices_types) {
    ARROW_SCOPED_TRACE("Indices type: " + indices_type->ToString());
    ASSERT_OK_AND_ASSIGN(auto casted, Cast(indices, indices_type));
    ASSERT_TRUE(casted.is_array());
    auto casted_indices = casted.make_array();
    {
      ARROW_SCOPED_TRACE("AAA");
      DoTestScatterAAA(values, casted_indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("CAA");
      DoTestScatterCACWithArrays(values, casted_indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("ACA");
      DoTestScatterACCWithArrays(values, casted_indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("CCA");
      DoTestScatterCCCWithArrays(values, casted_indices, max_index, expected);
    }
  }
}

void DoTestScatter(const std::shared_ptr<Array>& values,
                   const std::shared_ptr<Array>& indices, int64_t max_index,
                   const std::shared_ptr<Array>& expected) {
  DoTestScatterForIndicesTypes(SignedIntTypes(), values, indices, max_index, expected);
}

void TestScatter(const std::shared_ptr<DataType>& value_type,
                 const std::string& values_str, const std::string& indices_str,
                 int64_t max_index, const std::string& expected_str) {
  auto values = ArrayFromJSON(value_type, values_str);
  auto indices = ArrayFromJSON(int8(), indices_str);
  auto expected = ArrayFromJSON(value_type, expected_str);
  DoTestScatter(values, indices, max_index, expected);
}

}  // namespace

TEST(Scatter, Invalid) {
  {
    ARROW_SCOPED_TRACE("Length mismatch");
    auto values = ArrayFromJSON(int32(), "[0, 1]");
    auto indices = ArrayFromJSON(int32(), "[0]");
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid,
        "Invalid: Input and indices of scatter must have the same length, got 2 and 1",
        Scatter(values, indices));
  }
  {
    ARROW_SCOPED_TRACE("Invalid input type");
    auto values = ArrayFromJSON(int32(), "[0]");
    {
      ARROW_SCOPED_TRACE("uint32");
      auto indices = ArrayFromJSON(uint32(), R"([0])");
      ASSERT_RAISES_WITH_MESSAGE(
          TypeError,
          "Type error: Indices of scatter must be of signed integer type, got uint32",
          Scatter(values, indices));
    }
    {
      ARROW_SCOPED_TRACE("string");
      auto indices = ArrayFromJSON(utf8(), R"(["a"])");
      ASSERT_RAISES_WITH_MESSAGE(
          TypeError,
          "Type error: Indices of scatter must be of signed integer type, got string",
          Scatter(values, indices));
    }
  }
}

TEST(Scatter, DefaultOptions) {
  {
    ARROW_SCOPED_TRACE("Default options values");
    ScatterOptions options;
    ASSERT_EQ(options.max_index, -1);
  }
  {
    ARROW_SCOPED_TRACE("Default options semantics");
    auto values = ArrayFromJSON(utf8(), R"(["a"])");
    for (const auto& indices_type : SignedIntTypes()) {
      ARROW_SCOPED_TRACE("Indices type: " + indices_type->ToString());
      auto indices = ArrayFromJSON(indices_type, "[0]");
      ASSERT_OK_AND_ASSIGN(Datum result, Scatter(values, indices));
      AssertDatumsEqual(values, result);
    }
  }
}

TEST(Scatter, InvalidIndex) {
  {
    ARROW_SCOPED_TRACE("Negative index");
    auto values = ArrayFromJSON(utf8(), R"(["a"])");
    auto indices = ArrayFromJSON(int32(), "[-1]");
    ASSERT_RAISES_WITH_MESSAGE(IndexError, "Index error: Index out of bounds: -1",
                               Scatter(values, indices));
  }
  {
    ARROW_SCOPED_TRACE("Exceeds max_index");
    auto values = ArrayFromJSON(utf8(), R"(["a"])");
    auto indices = ArrayFromJSON(int32(), "[42]");
    ASSERT_RAISES_WITH_MESSAGE(IndexError, "Index error: Index out of bounds: 42",
                               Scatter(values, indices, /*max_index=*/1));
  }
}

template <typename ArrowType>
class TestScatterSmallIndicesTypes : public ::testing::Test {
 protected:
  using CType = typename TypeTraits<ArrowType>::CType;

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_SUITE(TestScatterSmallIndicesTypes, SmallSignedIntegerTypes);

TYPED_TEST(TestScatterSmallIndicesTypes, MaxIntegerIndex) {
  auto values = ArrayFromJSON(utf8(), R"(["a"])");
  auto indices_type = this->type_singleton();
  int64_t max_integer =
      static_cast<int64_t>(std::numeric_limits<typename TestFixture::CType>::max());
  auto indices = ArrayFromJSON(indices_type, "[" + std::to_string(max_integer - 1) + "]");
  ASSERT_OK_AND_ASSIGN(auto expected_prefix_nulls,
                       MakeArrayOfNull(utf8(), max_integer - 1));
  auto expected_suffix_value = values;
  ASSERT_OK_AND_ASSIGN(auto expected,
                       Concatenate({expected_prefix_nulls, expected_suffix_value}));
  DoTestScatterAAA(values, indices, /*max_index=*/max_integer - 1, expected);
}

TEST(Scatter, Boolean) {
  {
    ARROW_SCOPED_TRACE("Basic");
    auto values = "[true, false, true, true, false, false, true, true, true, false]";
    auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
    int64_t max_index = 9;
    auto expected = "[false, true, true, true, false, false, true, true, false, true]";
    TestScatter(boolean(), values, indices, max_index, expected);
  }
  {
    ARROW_SCOPED_TRACE("Values with nulls");
    auto values = "[true, false, null, true, true, false, false, null, null, true]";
    auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
    int64_t max_index = 9;
    auto expected = "[true, null, null, false, false, true, true, null, false, true]";
    TestScatter(boolean(), values, indices, max_index, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices with nulls");
    auto values = "[true, false, true, true, false, false, true, true, true, false]";
    auto indices = "[9, null, 7, null, 5, null, 3, null, 1, null]";
    int64_t max_index = 9;
    auto expected = "[null, true, null, true, null, false, null, true, null, true]";
    TestScatter(boolean(), values, indices, max_index, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output greater than input");
    auto values = "[true, true, true, false, false, false]";
    auto indices = "[0, 3, 6, 1, 4, 7]";
    int64_t max_index = 8;
    auto expected = "[true, false, null, true, false, null, true, false, null]";
    TestScatter(boolean(), values, indices, max_index, expected);
  }
  {
    ARROW_SCOPED_TRACE("Values all null");
    auto values = "[null, null]";
    auto indices = "[0, 1]";
    int64_t max_index = 1;
    auto expected = "[null, null]";
    TestScatter(boolean(), values, indices, max_index, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices all null");
    auto values = "[true, false]";
    auto indices = "[null, null]";
    int64_t max_index = 1;
    auto expected = "[null, null]";
    TestScatter(boolean(), values, indices, max_index, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty input output null");
    auto values = "[]";
    auto indices = "[]";
    int64_t max_index = 1;
    auto expected = "[null, null]";
    TestScatter(boolean(), values, indices, max_index, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices duplicated indices");
    auto values = "[true, false, null, null]";
    auto indices = "[0, 1, 0, 1]";
    int64_t max_index = 3;
    auto expected = "[null, null, null, null]";
    TestScatter(boolean(), values, indices, max_index, expected);
  }
}

TEST(Scatter, Numeric) {
  for (const auto& value_type : NumericTypes()) {
    ARROW_SCOPED_TRACE(value_type->ToString());
    {
      ARROW_SCOPED_TRACE("Basic");
      auto values = "[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]";
      auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
      int64_t max_index = 9;
      auto expected = "[19, 18, 17, 16, 15, 14, 13, 12, 11, 10]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Values with nulls");
      auto values = "[null, 11, null, 13, null, 15, null, 17, null, 19]";
      auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
      int64_t max_index = 9;
      auto expected = "[19, null, 17, null, 15, null, 13, null, 11, null]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Indices with nulls");
      auto values = "[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]";
      auto indices = "[9, null, 7, null, 5, null, 3, null, 1, null]";
      int64_t max_index = 9;
      auto expected = "[null, 18, null, 16, null, 14, null, 12, null, 10]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Output greater than input");
      auto values = "[0, 0, 0, 1, 1, 1]";
      auto indices = "[0, 3, 6, 1, 4, 7]";
      int64_t max_index = 8;
      auto expected = "[0, 1, null, 0, 1, null, 0, 1, null]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Values all null");
      auto values = "[null, null]";
      auto indices = "[0, 1]";
      int64_t max_index = 1;
      auto expected = "[null, null]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Indices all null");
      auto values = "[0, 1]";
      auto indices = "[null, null]";
      int64_t max_index = 1;
      auto expected = "[null, null]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Empty input output null");
      auto values = "[]";
      auto indices = "[]";
      int64_t max_index = 1;
      auto expected = "[null, null]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Indices duplicated indices");
      auto values = "[1, 0, null, null]";
      auto indices = "[0, 1, 0, 1]";
      int64_t max_index = 3;
      auto expected = "[null, null, null, null]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
  }
}

TEST(Scatter, Binary) {
  for (const auto& value_type : BaseBinaryTypes()) {
    ARROW_SCOPED_TRACE(value_type->ToString());
    {
      ARROW_SCOPED_TRACE("Basic");
      auto values = R"(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])";
      auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
      int64_t max_index = 9;
      auto expected = R"(["j", "i", "h", "g", "f", "e", "d", "c", "b", "a"])";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Values with nulls");
      auto values = R"([null, "b", null, "d", null, "f", null, "h", null, "j"])";
      auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
      int64_t max_index = 9;
      auto expected = R"(["j", null, "h", null, "f", null, "d", null, "b", null])";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Indices with nulls");
      auto values = R"(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])";
      auto indices = "[9, null, 7, null, 5, null, 3, null, 1, null]";
      int64_t max_index = 9;
      auto expected = R"([null, "i", null, "g", null, "e", null, "c", null, "a"])";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Output greater than input");
      auto values = R"(["a", "a", "a", "b", "b", "b"])";
      auto indices = "[0, 3, 6, 1, 4, 7]";
      int64_t max_index = 8;
      auto expected = R"(["a", "b", null, "a", "b", null, "a", "b", null])";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Values all null");
      auto values = "[null, null]";
      auto indices = "[0, 1]";
      int64_t max_index = 1;
      auto expected = "[null, null]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Indices all null");
      auto values = R"(["a", "b"])";
      auto indices = "[null, null]";
      int64_t max_index = 1;
      auto expected = "[null, null]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Empty input output null");
      auto values = "[]";
      auto indices = "[]";
      int64_t max_index = 1;
      auto expected = "[null, null]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
    {
      ARROW_SCOPED_TRACE("Indices duplicated indices");
      auto values = R"(["a", "b", null, null])";
      auto indices = "[0, 1, 0, 1]";
      int64_t max_index = 3;
      auto expected = "[null, null, null, null]";
      TestScatter(value_type, values, indices, max_index, expected);
    }
  }
}

}  // namespace arrow::compute
