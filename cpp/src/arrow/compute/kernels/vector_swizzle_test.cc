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
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/logging.h"

namespace arrow::compute {

namespace {

static const std::vector<std::shared_ptr<DataType>> kSignedIntegerTypes = {
    int8(), int16(), int32(), int64()};

static const std::vector<std::shared_ptr<DataType>> kIntegerTypes = {
    int8(), uint8(), int16(), uint16(), int32(), uint32(), int64(), uint64()};

static const std::vector<std::shared_ptr<DataType>> kNumericTypes = {
    uint8(), int8(),   uint16(), int16(),   uint32(),
    int32(), uint64(), int64(),  float32(), float64()};

static const std::vector<std::shared_ptr<DataType>> kNumericAndBaseBinaryTypes = {
    uint8(), int8(),    uint16(),  int16(),  uint32(), int32(),        uint64(),
    int64(), float32(), float64(), binary(), utf8(),   large_binary(), large_utf8()};

using SmallOutputTypes = ::testing::Types<UInt8Type, UInt16Type, Int8Type, Int16Type>;

}  // namespace

// ----------------------------------------------------------------------
// InversePermutation tests

namespace {

Result<Datum> InversePermutation(const Datum& indices, int64_t output_length,
                                 std::shared_ptr<DataType> output_type) {
  InversePermutationOptions options{output_length, std::move(output_type)};
  return InversePermutation(indices, options);
}

void AssertInversePermutation(const Datum& indices, int64_t output_length,
                              const std::shared_ptr<DataType>& output_type,
                              const Datum& expected, bool validity_must_be_null) {
  ASSERT_OK_AND_ASSIGN(auto result,
                       InversePermutation(indices, output_length, output_type));
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
    int64_t output_length, const std::shared_ptr<DataType>& output_type,
    const Datum& expected, bool validity_must_be_null = false) {
  for (const auto& input_type : input_types) {
    ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
    auto indices = input(input_type);
    AssertInversePermutation(indices, output_length, output_type, expected,
                             validity_must_be_null);
  }
}

template <typename InputFunc>
void DoTestInversePermutationForInputOutputTypes(
    const std::vector<std::shared_ptr<DataType>>& input_types,
    const std::vector<std::shared_ptr<DataType>>& output_types, InputFunc&& input,
    int64_t output_length, const std::string& expected_str, bool validity_must_be_null) {
  for (const auto& output_type : kIntegerTypes) {
    ARROW_SCOPED_TRACE("Output type: " + output_type->ToString());
    auto expected = ArrayFromJSON(output_type, expected_str);
    DoTestInversePermutationForInputTypes(input_types, std::forward<InputFunc>(input),
                                          output_length, output_type, expected,
                                          validity_must_be_null);
  }
}

void TestInversePermutationForInputOutputTypes(
    const std::vector<std::shared_ptr<DataType>>& input_types,
    const std::vector<std::shared_ptr<DataType>>& output_types,
    const std::string& indices_str, const std::vector<std::string>& indices_chunked_str,
    int64_t output_length, const std::string& expected_str, bool validity_must_be_null) {
  {
    ARROW_SCOPED_TRACE("Array");
    DoTestInversePermutationForInputOutputTypes(
        input_types, output_types,
        [&](const std::shared_ptr<DataType>& input_type) {
          return ArrayFromJSON(input_type, indices_str);
        },
        output_length, expected_str, validity_must_be_null);
  }
  {
    ARROW_SCOPED_TRACE("Chunked");
    DoTestInversePermutationForInputOutputTypes(
        input_types, output_types,
        [&](const std::shared_ptr<DataType>& input_type) {
          return ChunkedArrayFromJSON(input_type, indices_chunked_str);
        },
        output_length, expected_str, validity_must_be_null);
  }
}

void TestInversePermutationSigned(const std::string& indices_str,
                                  const std::vector<std::string>& indices_chunked_str,
                                  int64_t output_length, const std::string& expected_str,
                                  bool validity_must_be_null = false) {
  TestInversePermutationForInputOutputTypes(
      kSignedIntegerTypes, kIntegerTypes, indices_str, indices_chunked_str, output_length,
      expected_str, validity_must_be_null);
}

void TestInversePermutation(const std::string& indices_str,
                            const std::vector<std::string>& indices_chunked_str,
                            int64_t output_length, const std::string& expected_str,
                            bool validity_must_be_null = false) {
  TestInversePermutationForInputOutputTypes(kIntegerTypes, kIntegerTypes, indices_str,
                                            indices_chunked_str, output_length,
                                            expected_str, validity_must_be_null);
}

}  // namespace

TEST(InversePermutation, InvalidOutputType) {
  {
    ARROW_SCOPED_TRACE("Output type float");
    auto indices = ArrayFromJSON(int32(), "[]");
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: Output type of inverse_permutation must be integer, got float",
        InversePermutation(indices, /*output_length=*/0, /*output_type=*/float32()));
  }
  {
    ARROW_SCOPED_TRACE("Output type string");
    auto indices = ArrayFromJSON(int32(), "[]");
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid,
        "Invalid: Output type of inverse_permutation must be integer, got string",
        InversePermutation(indices, /*output_length=*/0, /*output_type=*/utf8()));
  }
}

TEST(InversePermutation, DefaultOptions) {
  {
    ARROW_SCOPED_TRACE("Default options values");
    InversePermutationOptions options;
    ASSERT_EQ(options.output_length, -1);
    ASSERT_EQ(options.output_type, nullptr);
  }
  {
    ARROW_SCOPED_TRACE("Default options semantics");
    for (const auto& input_type : kIntegerTypes) {
      ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
      auto indices = ArrayFromJSON(input_type, "[0]");
      ASSERT_OK_AND_ASSIGN(Datum result, InversePermutation(indices));
      AssertDatumsEqual(indices, result);
    }
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

TYPED_TEST_SUITE(TestInversePermutationSmallOutputType, SmallOutputTypes);

TYPED_TEST(TestInversePermutationSmallOutputType, JustEnoughOutputType) {
  auto output_type = this->type_singleton();
  int64_t input_length =
      static_cast<int64_t>(std::numeric_limits<typename TestFixture::CType>::max());
  auto expected =
      ArrayFromJSON(output_type, "[" + std::to_string(input_length - 1) + "]");
  DoTestInversePermutationForInputTypes(
      kIntegerTypes,
      [&](const std::shared_ptr<DataType>& input_type) {
        return ConstantArrayGenerator::Zeroes(input_length, input_type);
      },
      /*output_length=*/1, output_type, expected);
}

TYPED_TEST(TestInversePermutationSmallOutputType, InsufficientOutputType) {
  auto output_type = this->type_singleton();
  int64_t input_length =
      static_cast<int64_t>(std::numeric_limits<typename TestFixture::CType>::max()) + 1;
  for (const auto& input_type : kIntegerTypes) {
    ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
    auto indices = ConstantArrayGenerator::Zeroes(input_length, input_type);
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid,
        "Invalid: Output type " + output_type->ToString() +
            " of inverse_permutation is insufficient to store indices of length " +
            std::to_string(input_length),
        InversePermutation(indices, /*output_length=*/1, output_type));
  }
}

TEST(InversePermutation, Basic) {
  {
    ARROW_SCOPED_TRACE("Basic");
    auto indices = "[9, 7, 5, 3, 1, 0, 2, 4, 6, 8]";
    std::vector<std::string> indices_chunked{
        "[]", "[9, 7, 5, 3, 1]", "[0]", "[2, 4, 6]", "[8]", "[]"};
    int64_t output_length = 10;
    auto expected = "[5, 4, 6, 3, 7, 2, 8, 1, 9, 0]";
    TestInversePermutation(indices, indices_chunked, output_length, expected,
                           /*validity_must_be_null=*/true);
  }
  {
    ARROW_SCOPED_TRACE("Basic with nulls");
    auto indices = "[9, 7, 5, 3, 1, null, null, null, null, null]";
    std::vector<std::string> indices_chunked{
        "[]", "[9, 7, 5, 3, 1]", "[null]", "[null, null, null]", "[null]", "[]"};
    int64_t output_length = 10;
    auto expected = "[null, 4, null, 3, null, 2, null, 1, null, 0]";
    TestInversePermutation(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Basic with negatives");
    auto indices = "[9, 7, 5, 3, 1, -1, -2, -3, -4, -5]";
    std::vector<std::string> indices_chunked{
        "[]", "[9, 7, 5, 3, 1]", "[-1]", "[-2, -3, -4]", "[-5]", "[]"};
    int64_t output_length = 10;
    auto expected = "[null, 4, null, 3, null, 2, null, 1, null, 0]";
    TestInversePermutationSigned(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty output");
    auto indices = "[1, 2]";
    std::vector<std::string> indices_chunked{"[]", "[1]", "[]", "[]", "[2]", "[]"};
    int64_t output_length = 0;
    auto expected = "[]";
    TestInversePermutation(indices, indices_chunked, output_length, expected,
                           /*validity_must_be_null=*/true);
  }
  {
    ARROW_SCOPED_TRACE("Output less than input");
    auto indices = "[1, 0]";
    std::vector<std::string> indices_chunked{"[]", "[]", "[]", "[1, 0]"};
    int64_t output_length = 1;
    auto expected = "[1]";
    TestInversePermutation(indices, indices_chunked, output_length, expected,
                           /*validity_must_be_null=*/true);
  }
  {
    ARROW_SCOPED_TRACE("Output greater than input");
    auto indices = "[1, 2]";
    std::vector<std::string> indices_chunked{"[]", "[1]", "[]", "[2]"};
    int64_t output_length = 7;
    auto expected = "[null, 0, 1, null, null, null, null]";
    TestInversePermutation(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Input all null");
    auto indices = "[null, null]";
    std::vector<std::string> indices_chunked{"[]", "[null]", "[]", "[null]"};
    int64_t output_length = 2;
    auto expected = "[null, null]";
    TestInversePermutation(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output all null");
    auto indices = "[2, 3]";
    std::vector<std::string> indices_chunked{"[]", "[2]", "[]", "[3]"};
    int64_t output_length = 2;
    auto expected = "[null, null]";
    TestInversePermutation(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty input output null");
    auto indices = "[]";
    std::vector<std::string> indices_chunked{"[]", "[]", "[]", "[]"};
    int64_t output_length = 7;
    auto expected = "[null, null, null, null, null, null, null]";
    TestInversePermutation(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Input duplicated indices");
    auto indices = "[1, 2, 3, 1, 2, 3, 1, 2, 3]";
    std::vector<std::string> indices_chunked{"[]", "[1, 2]", "[3, 1, 2, 3, 1]",
                                             "[]", "[2]",    "[3]"};
    int64_t output_length = 5;
    auto expected = "[null, 6, 7, 8, null]";
    TestInversePermutation(indices, indices_chunked, output_length, expected);
  }
}

// ----------------------------------------------------------------------
// Permute tests
//
// Shorthand notation:
//
//   A = Array
//   C = ChunkedArray

namespace {

Result<Datum> Permute(const Datum& values, const Datum& indices, int64_t output_length) {
  PermuteOptions options{output_length};
  return Permute(values, indices, options);
}

void AssertPermuteAAA(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<Array>& indices, int64_t output_length,
                      const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(Datum result, Permute(values, indices, output_length));
  AssertDatumsEqual(expected, result);
}

void AssertPermuteCAC(const std::shared_ptr<ChunkedArray>& values,
                      const std::shared_ptr<Array>& indices, int64_t output_length,
                      const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(Datum result, Permute(values, indices, output_length));
  ASSERT_TRUE(result.is_chunked_array());
  ASSERT_OK_AND_ASSIGN(auto result_array, Concatenate(result.chunked_array()->chunks()));
  AssertDatumsEqual(expected, result_array);
}

void AssertPermuteACC(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<ChunkedArray>& indices, int64_t output_length,
                      const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(Datum result, Permute(values, indices, output_length));
  ASSERT_TRUE(result.is_chunked_array());
  ASSERT_OK_AND_ASSIGN(auto result_array, Concatenate(result.chunked_array()->chunks()));
  AssertDatumsEqual(expected, result_array);
}

void AssertPermuteCCC(const std::shared_ptr<ChunkedArray>& values,
                      const std::shared_ptr<ChunkedArray>& indices, int64_t output_length,
                      const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(Datum result, Permute(values, indices, output_length));
  ASSERT_TRUE(result.is_chunked_array());
  ASSERT_OK_AND_ASSIGN(auto result_array, Concatenate(result.chunked_array()->chunks()));
  AssertDatumsEqual(expected, result_array);
}

void DoTestPermuteAAA(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<Array>& indices, int64_t output_length,
                      const std::shared_ptr<Array>& expected) {
  AssertPermuteAAA(values, indices, output_length, expected);
}

/// The following helper functions are based on the invariant:
/// Permute([V, V], [I', I''], 2 * l) == Concat(E, E)
///
/// where
///   V = values
///   I = indices
///   l = output_length
///   I' = ReplaceWithMask(I, i >= l, null)
///   I'' = ReplaceWithMask(I, i < 0, null) + l
///   E = Permute(V, I, l)

/// Make indices prefix I' = ReplaceWithMask(I, i >= l, null).
Result<std::shared_ptr<Array>> MakeIndicesPrefix(const std::shared_ptr<Array>& indices,
                                                 int64_t output_length) {
  ARROW_ASSIGN_OR_RAISE(auto l, MakeScalar(indices->type(), output_length));
  ARROW_ASSIGN_OR_RAISE(auto ge_than_l, CallFunction("greater_equal", {indices, l}));
  ARROW_ASSIGN_OR_RAISE(auto all_null,
                        MakeArrayOfNull(indices->type(), indices->length()));
  ARROW_ASSIGN_OR_RAISE(auto prefix, ReplaceWithMask(indices, ge_than_l, all_null));
  return prefix.make_array();
}

/// Make indices suffix I'' = ReplaceWithMask(I, i < 0, null) + l.
Result<std::shared_ptr<Array>> MakeIndicesSuffix(const std::shared_ptr<Array>& indices,
                                                 int64_t output_length) {
  ARROW_ASSIGN_OR_RAISE(auto zero, MakeScalar(indices->type(), 0));
  ARROW_ASSIGN_OR_RAISE(auto negative, CallFunction("less", {indices, zero}));
  ARROW_ASSIGN_OR_RAISE(auto all_null,
                        MakeArrayOfNull(indices->type(), indices->length()));
  ARROW_ASSIGN_OR_RAISE(auto replaced, ReplaceWithMask(indices, negative, all_null));
  ARROW_ASSIGN_OR_RAISE(auto l, MakeScalar(indices->type(), output_length));
  ARROW_ASSIGN_OR_RAISE(auto suffix, Add(replaced, l));
  return suffix.make_array();
}

void DoTestPermuteCACWithArrays(const std::shared_ptr<Array>& values,
                                const std::shared_ptr<Array>& indices,
                                int64_t output_length,
                                const std::shared_ptr<Array>& expected) {
  auto chunked_values2 = std::make_shared<ChunkedArray>(ArrayVector{values, values});

  ASSERT_OK_AND_ASSIGN(auto indices_prefix, MakeIndicesPrefix(indices, output_length));
  ASSERT_OK_AND_ASSIGN(auto indices_suffix, MakeIndicesSuffix(indices, output_length));
  ASSERT_OK_AND_ASSIGN(auto concat_indices2,
                       Concatenate({indices_prefix, indices_suffix}));

  ASSERT_OK_AND_ASSIGN(auto concat_expected2,
                       Concatenate(ArrayVector{expected, expected}));

  AssertPermuteCAC(chunked_values2, concat_indices2, output_length * 2, concat_expected2);
}

void DoTestPermuteACCWithArrays(const std::shared_ptr<Array>& values,
                                const std::shared_ptr<Array>& indices,
                                int64_t output_length,
                                const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(auto concat_values2, Concatenate(ArrayVector{values, values}));

  ASSERT_OK_AND_ASSIGN(auto indices_prefix, MakeIndicesPrefix(indices, output_length));
  ASSERT_OK_AND_ASSIGN(auto indices_suffix, MakeIndicesSuffix(indices, output_length));
  auto chunked_indices2 =
      std::make_shared<ChunkedArray>(ArrayVector{indices_prefix, indices_suffix});

  ASSERT_OK_AND_ASSIGN(auto concat_expected2,
                       Concatenate(ArrayVector{expected, expected}));

  AssertPermuteACC(concat_values2, chunked_indices2, output_length * 2, concat_expected2);
}

void DoTestPermuteCCCWithArrays(const std::shared_ptr<Array>& values,
                                const std::shared_ptr<Array>& indices,
                                int64_t output_length,
                                const std::shared_ptr<Array>& expected) {
  auto chunked_values2 = std::make_shared<ChunkedArray>(ArrayVector{values, values});

  ASSERT_OK_AND_ASSIGN(auto indices_prefix, MakeIndicesPrefix(indices, output_length));
  ASSERT_OK_AND_ASSIGN(auto indices_suffix, MakeIndicesSuffix(indices, output_length));
  auto chunked_indices2 =
      std::make_shared<ChunkedArray>(ArrayVector{indices_prefix, indices_suffix});

  ASSERT_OK_AND_ASSIGN(auto concat_expected2,
                       Concatenate(ArrayVector{expected, expected}));

  AssertPermuteCCC(chunked_values2, chunked_indices2, output_length * 2,
                   concat_expected2);
}

void DoTestPermuteForIndicesTypes(
    const std::vector<std::shared_ptr<DataType>>& indices_types,
    const std::shared_ptr<Array>& values, const std::shared_ptr<Array>& indices,
    int64_t output_length, const std::shared_ptr<Array>& expected) {
  for (const auto& indices_type : indices_types) {
    ARROW_SCOPED_TRACE("Indices type: " + indices_type->ToString());
    ASSERT_OK_AND_ASSIGN(auto casted, Cast(indices, indices_type));
    ASSERT_TRUE(casted.is_array());
    auto casted_indices = casted.make_array();
    {
      ARROW_SCOPED_TRACE("AAA");
      DoTestPermuteAAA(values, casted_indices, output_length, expected);
    }
    {
      ARROW_SCOPED_TRACE("CAA");
      DoTestPermuteCACWithArrays(values, casted_indices, output_length, expected);
    }
    {
      ARROW_SCOPED_TRACE("ACA");
      DoTestPermuteACCWithArrays(values, casted_indices, output_length, expected);
    }
    {
      ARROW_SCOPED_TRACE("CCA");
      DoTestPermuteCCCWithArrays(values, casted_indices, output_length, expected);
    }
  }
}

void DoTestPermuteSignedIndices(const std::shared_ptr<Array>& values,
                                const std::shared_ptr<Array>& indices,
                                int64_t output_length,
                                const std::shared_ptr<Array>& expected) {
  DoTestPermuteForIndicesTypes(kSignedIntegerTypes, values, indices, output_length,
                               expected);
}

void DoTestPermute(const std::shared_ptr<Array>& values,
                   const std::shared_ptr<Array>& indices, int64_t output_length,
                   const std::shared_ptr<Array>& expected) {
  DoTestPermuteForIndicesTypes(kIntegerTypes, values, indices, output_length, expected);
}

}  // namespace

TEST(Permute, Invalid) {
  {
    ARROW_SCOPED_TRACE("Length mismatch");
    auto values = ArrayFromJSON(int32(), "[0, 1]");
    auto indices = ArrayFromJSON(int32(), "[0]");
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid,
        "Invalid: Input and indices of permute must have the same length, got 2 and 1",
        Permute(values, indices));
  }
  {
    ARROW_SCOPED_TRACE("Invalid input type");
    auto values = ArrayFromJSON(int32(), "[0]");
    auto indices = ArrayFromJSON(utf8(), R"(["a"])");
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: Indices of permute must be of integer type, got string",
        Permute(values, indices));
  }
}

TEST(Permute, DefaultOptions) {
  {
    ARROW_SCOPED_TRACE("Default options values");
    PermuteOptions options;
    ASSERT_EQ(options.output_length, -1);
  }
  {
    ARROW_SCOPED_TRACE("Default options semantics");
    auto values = ArrayFromJSON(utf8(), R"(["a"])");
    for (const auto& indices_type : kIntegerTypes) {
      ARROW_SCOPED_TRACE("Indices type: " + indices_type->ToString());
      auto indices = ArrayFromJSON(indices_type, "[0]");
      ASSERT_OK_AND_ASSIGN(Datum result, Permute(values, indices));
      AssertDatumsEqual(values, result);
    }
  }
}

template <typename ArrowType>
class TestPermuteSmallIndicesTypes : public ::testing::Test {
 protected:
  using CType = typename TypeTraits<ArrowType>::CType;

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_SUITE(TestPermuteSmallIndicesTypes, SmallOutputTypes);

TYPED_TEST(TestPermuteSmallIndicesTypes, MaxIntegerIndex) {
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
  DoTestPermuteAAA(values, indices, /*output_length=*/max_integer, expected);
}

template <typename ArrowType>
class TestPermuteTyped : public ::testing::Test {
 protected:
  virtual std::shared_ptr<DataType> values_type() const {
    if constexpr (is_parameter_free_type<ArrowType>::value) {
      return TypeTraits<ArrowType>::type_singleton();
    } else {
      EXPECT_TRUE(false) << "values_type() must be overridden for parameterized types";
      return nullptr;
    }
  }

  void TestPermuteSignedIndices(const std::string& values_str,
                                const std::string& indices_str, int64_t output_length,
                                const std::string& expected_str) {
    TestPermute(DoTestPermuteSignedIndices, values_str, indices_str, output_length,
                expected_str);
  }

  void TestPermute(const std::string& values_str, const std::string& indices_str,
                   int64_t output_length, const std::string& expected_str) {
    TestPermute(DoTestPermute, values_str, indices_str, output_length, expected_str);
  }

 private:
  template <typename DoTestFunc>
  void TestPermute(DoTestFunc&& func, const std::string& values_str,
                   const std::string& indices_str, int64_t output_length,
                   const std::string& expected_str) {
    auto values = ArrayFromJSON(values_type(), values_str);
    auto indices = ArrayFromJSON(int8(), indices_str);
    auto expected = ArrayFromJSON(values_type(), expected_str);
    func(values, indices, output_length, expected);
  }
};

class TestPermuteBoolean : public TestPermuteTyped<BooleanType> {};

TEST_F(TestPermuteBoolean, Basic) {
  {
    ARROW_SCOPED_TRACE("Basic");
    auto values = "[true, false, true, true, false, false, true, true, true, false]";
    auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
    int64_t output_length = 10;
    auto expected = "[false, true, true, true, false, false, true, true, false, true]";
    TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Values with nulls");
    auto values = "[true, false, null, true, true, false, false, null, null, true]";
    auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
    int64_t output_length = 10;
    auto expected = "[true, null, null, false, false, true, true, null, false, true]";
    TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices with nulls");
    auto values = "[true, false, true, true, false, false, true, true, true, false]";
    auto indices = "[9, null, 7, null, 5, null, 3, null, 1, null]";
    int64_t output_length = 10;
    auto expected = "[null, true, null, true, null, false, null, true, null, true]";
    TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices with negatives");
    auto values = "[true, false, true, true, false, false, true, true, true, false]";
    auto indices = "[9, -1, 7, -2, 5, -3, 3, -4, 1, -5]";
    int64_t output_length = 10;
    auto expected = "[null, true, null, true, null, false, null, true, null, true]";
    TestPermuteSignedIndices(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty output");
    auto values = "[true, false]";
    auto indices = "[0, 1]";
    int64_t output_length = 0;
    auto expected = "[]";
    TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output less than input");
    auto values = "[false, true]";
    auto indices = "[1, 0]";
    int64_t output_length = 1;
    auto expected = "[true]";
    TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output greater than input");
    auto values = "[true, true, true, false, false, false]";
    auto indices = "[0, 3, 6, 1, 4, 7]";
    int64_t output_length = 9;
    auto expected = "[true, false, null, true, false, null, true, false, null]";
    TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Values all null");
    auto values = "[null, null]";
    auto indices = "[0, 1]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices all null");
    auto values = "[true, false]";
    auto indices = "[null, null]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output all null");
    auto values = "[true, false]";
    auto indices = "[2, 3]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty input output null");
    auto values = "[]";
    auto indices = "[]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices duplicated indices");
    auto values = "[true, false, null, null]";
    auto indices = "[0, 1, 0, 1]";
    int64_t output_length = 4;
    auto expected = "[null, null, null, null]";
    TestPermute(values, indices, output_length, expected);
  }
}

template <typename ArrowType>
class TestPermuteNumeric : public TestPermuteTyped<ArrowType> {};

TYPED_TEST_SUITE(TestPermuteNumeric, NumericArrowTypes);

TYPED_TEST(TestPermuteNumeric, Basic) {
  {
    ARROW_SCOPED_TRACE("Basic");
    auto values = "[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]";
    auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
    int64_t output_length = 10;
    auto expected = "[19, 18, 17, 16, 15, 14, 13, 12, 11, 10]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Values with nulls");
    auto values = "[null, 11, null, 13, null, 15, null, 17, null, 19]";
    auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
    int64_t output_length = 10;
    auto expected = "[19, null, 17, null, 15, null, 13, null, 11, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices with nulls");
    auto values = "[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]";
    auto indices = "[9, null, 7, null, 5, null, 3, null, 1, null]";
    int64_t output_length = 10;
    auto expected = "[null, 18, null, 16, null, 14, null, 12, null, 10]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices with negatives");
    auto values = "[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]";
    auto indices = "[9, -1, 7, -2, 5, -3, 3, -4, 1, -5]";
    int64_t output_length = 10;
    auto expected = "[null, 18, null, 16, null, 14, null, 12, null, 10]";
    this->TestPermuteSignedIndices(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty output");
    auto values = "[1, 0]";
    auto indices = "[0, 1]";
    int64_t output_length = 0;
    auto expected = "[]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output less than input");
    auto values = "[1, 0]";
    auto indices = "[1, 0]";
    int64_t output_length = 1;
    auto expected = "[0]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output greater than input");
    auto values = "[0, 0, 0, 1, 1, 1]";
    auto indices = "[0, 3, 6, 1, 4, 7]";
    int64_t output_length = 9;
    auto expected = "[0, 1, null, 0, 1, null, 0, 1, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Values all null");
    auto values = "[null, null]";
    auto indices = "[0, 1]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices all null");
    auto values = "[0, 1]";
    auto indices = "[null, null]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output all null");
    auto values = "[0, 1]";
    auto indices = "[2, 3]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty input output null");
    auto values = "[]";
    auto indices = "[]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices duplicated indices");
    auto values = "[1, 0, null, null]";
    auto indices = "[0, 1, 0, 1]";
    int64_t output_length = 4;
    auto expected = "[null, null, null, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
}

template <typename ArrowType>
class TestPermuteString : public TestPermuteTyped<ArrowType> {};

TYPED_TEST_SUITE(TestPermuteString, BaseBinaryArrowTypes);

TYPED_TEST(TestPermuteString, Basic) {
  {
    ARROW_SCOPED_TRACE("Basic");
    auto values = R"(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])";
    auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
    int64_t output_length = 10;
    auto expected = R"(["j", "i", "h", "g", "f", "e", "d", "c", "b", "a"])";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Values with nulls");
    auto values = R"([null, "b", null, "d", null, "f", null, "h", null, "j"])";
    auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
    int64_t output_length = 10;
    auto expected = R"(["j", null, "h", null, "f", null, "d", null, "b", null])";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices with nulls");
    auto values = R"(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])";
    auto indices = "[9, null, 7, null, 5, null, 3, null, 1, null]";
    int64_t output_length = 10;
    auto expected = R"([null, "i", null, "g", null, "e", null, "c", null, "a"])";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices with negatives");
    auto values = R"(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])";
    auto indices = "[9, -1, 7, -2, 5, -3, 3, -4, 1, -5]";
    int64_t output_length = 10;
    auto expected = R"([null, "i", null, "g", null, "e", null, "c", null, "a"])";
    this->TestPermuteSignedIndices(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty output");
    auto values = R"(["b", "a"])";
    auto indices = "[0, 1]";
    int64_t output_length = 0;
    auto expected = "[]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output less than input");
    auto values = R"(["b", "a"])";
    auto indices = "[1, 0]";
    int64_t output_length = 1;
    auto expected = R"(["a"])";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output greater than input");
    auto values = R"(["a", "a", "a", "b", "b", "b"])";
    auto indices = "[0, 3, 6, 1, 4, 7]";
    int64_t output_length = 9;
    auto expected = R"(["a", "b", null, "a", "b", null, "a", "b", null])";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Values all null");
    auto values = "[null, null]";
    auto indices = "[0, 1]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices all null");
    auto values = R"(["a", "b"])";
    auto indices = "[null, null]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output all null");
    auto values = R"(["a", "b"])";
    auto indices = "[2, 3]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty input output null");
    auto values = "[]";
    auto indices = "[]";
    int64_t output_length = 2;
    auto expected = "[null, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Indices duplicated indices");
    auto values = R"(["a", "b", null, null])";
    auto indices = "[0, 1, 0, 1]";
    int64_t output_length = 4;
    auto expected = "[null, null, null, null]";
    this->TestPermute(values, indices, output_length, expected);
  }
}

// ----------------------------------------------------------------------
// Test Permute using a hypothetical if-else special form.
// Also demonstrate how Permute can serve as a building block of implementing special
// forms.

namespace {

/// Execute an if-else expression using regular evaluation, as a reference.
Result<Datum> ExecuteIfElseByExpr(const Expression& cond, const Expression& if_true,
                                  const Expression& if_false,
                                  const std::shared_ptr<Schema>& schema,
                                  const ExecBatch& input) {
  auto if_else = call("if_else", {cond, if_true, if_false});
  ARROW_ASSIGN_OR_RAISE(auto bound, if_else.Bind(*schema));
  return ExecuteScalarExpression(bound, input);
}

/// Execute an if-else expression in a special form fashion, in which Permute is used as a
/// building block.
Result<Datum> ExecuteIfElseByPermute(const Expression& cond, const Expression& if_true,
                                     const Expression& if_false,
                                     const std::shared_ptr<Schema>& schema,
                                     const ExecBatch& input) {
  for (const auto& column : input.values) {
    DCHECK(column.is_array());
  }

  ARROW_ASSIGN_OR_RAISE(auto input_rb, input.ToRecordBatch(schema));

  // 1. Evaluate "cond", getting a boolean array as a mask to branches.
  ARROW_ASSIGN_OR_RAISE(auto bound_cond, cond.Bind(*schema));
  ARROW_ASSIGN_OR_RAISE(auto cond_datum, ExecuteScalarExpression(bound_cond, input));

  // 2. Get indices of "true"s from the mask as the selection vector.
  ARROW_ASSIGN_OR_RAISE(auto sel_if_true_datum,
                        CallFunction("indices_nonzero", {cond_datum}));
  DCHECK(sel_if_true_datum.is_array());
  auto sel_if_true_array = sel_if_true_datum.make_array();

  // 3. Take the "true" rows from input.
  ARROW_ASSIGN_OR_RAISE(auto if_true_input_datum,
                        CallFunction("take", {input_rb, sel_if_true_datum}));

  // 4. Get indices of "false"es from the mask as the selection vector - by first
  // inverting the mask and then getting the non-zero's indices.
  ARROW_ASSIGN_OR_RAISE(auto invert_cond_datum, CallFunction("invert", {cond_datum}));
  ARROW_ASSIGN_OR_RAISE(auto sel_if_false_datum,
                        CallFunction("indices_nonzero", {invert_cond_datum}));
  DCHECK(sel_if_false_datum.is_array());
  auto sel_if_false_array = sel_if_false_datum.make_array();

  // 5. Take the "false" rows from input.
  ARROW_ASSIGN_OR_RAISE(auto if_false_input_datum,
                        CallFunction("take", {input_rb, sel_if_false_datum}));

  DCHECK_EQ(if_true_input_datum.kind(), Datum::RECORD_BATCH);
  auto if_true_input_batch = ExecBatch(*if_true_input_datum.record_batch());

  DCHECK_EQ(if_false_input_datum.kind(), Datum::RECORD_BATCH);
  auto if_false_input_batch = ExecBatch(*if_false_input_datum.record_batch());

  // 6. Evaluate "true" branch on the "true" rows.
  ARROW_ASSIGN_OR_RAISE(auto bound_if_true, if_true.Bind(*schema));
  ARROW_ASSIGN_OR_RAISE(auto if_true_result_datum,
                        ExecuteScalarExpression(bound_if_true, if_true_input_batch));
  DCHECK(if_true_result_datum.is_array());
  auto if_true_result_array = if_true_result_datum.make_array();

  // 7. Evaluate "false" branch on the "false" rows.
  ARROW_ASSIGN_OR_RAISE(auto bound_if_false, if_false.Bind(*schema));
  ARROW_ASSIGN_OR_RAISE(auto if_false_result_datum,
                        ExecuteScalarExpression(bound_if_false, if_false_input_batch));
  DCHECK(if_false_result_datum.is_array());
  auto if_false_result_array = if_false_result_datum.make_array();

  // 8. Combine the "true"/"false" results/selection vectors into chunked arrays.
  auto result_ca = std::make_shared<ChunkedArray>(
      ArrayVector{if_true_result_array, if_false_result_array});
  auto sel_ca =
      std::make_shared<ChunkedArray>(ArrayVector{sel_if_true_array, sel_if_false_array});

  // 9. Finally, permute the "true"/"false" results to their original positions in the
  // input (according to the selection vectors). Note we didn't handle the rows with nulls
  // in the mask, because Permute will fill nulls for these rows and this is equal to the
  // null handling policy of if-else, which is pretty nice.
  return Permute(/*values=*/result_ca, /*indices=*/sel_ca,
                 /*output_length=*/input.length);
}

void DoTestIfElse(const Expression& cond, const Expression& if_true,
                  const Expression& if_false, const std::shared_ptr<Schema>& schema,
                  const ExecBatch& input) {
  ASSERT_OK_AND_ASSIGN(Datum result_by_expr,
                       ExecuteIfElseByExpr(cond, if_true, if_false, schema, input));
  ASSERT_TRUE(result_by_expr.is_array());
  ASSERT_OK_AND_ASSIGN(Datum result_by_permute,
                       ExecuteIfElseByPermute(cond, if_true, if_false, schema, input));
  // Permute will output chunked array because we input values and indices as chunked
  // arrays consisting of each branches. We don't care the shape of the output when
  // comparing the results - only contents, so we concatenate the chunked array.
  ASSERT_TRUE(result_by_permute.is_chunked_array());
  ASSERT_OK_AND_ASSIGN(auto result_by_permute_concat,
                       Concatenate(result_by_permute.chunked_array()->chunks()));

  AssertDatumsEqual(result_by_expr, result_by_permute_concat);
}

void DoTestIfElse(const Expression& cond, const Expression& if_true,
                  const Expression& if_false, const std::shared_ptr<Schema>& schema,
                  const ExecBatch& input, const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(Datum result,
                       ExecuteIfElseByPermute(cond, if_true, if_false, schema, input));
  ASSERT_TRUE(result.is_chunked_array());
  ASSERT_OK_AND_ASSIGN(auto result_concat, Concatenate(result.chunked_array()->chunks()));

  AssertDatumsEqual(expected, result_concat);
}

}  // namespace

TEST(Permute, IfElse) {
  {
    ARROW_SCOPED_TRACE("if (b != 0) then a / b else b");
    auto cond = call("not_equal", {field_ref("b"), literal(0)});
    auto if_true = call("divide", {field_ref("a"), field_ref("b")});
    auto if_false = field_ref("b");
    auto schema = arrow::schema({field("a", int32()), field("b", int32())});
    auto rb = RecordBatchFromJSON(schema, R"([
        [1, 1],
        [2, 1],
        [3, 0],
        [4, 1],
        [5, 1]
      ])");
    auto input = ExecBatch(*rb);

    {
      ARROW_SCOPED_TRACE("Regular evaluation");
      ASSERT_RAISES_WITH_MESSAGE(
          Invalid, "Invalid: divide by zero",
          ExecuteIfElseByExpr(cond, if_true, if_false, schema, input));
    }

    {
      ARROW_SCOPED_TRACE("Special form");
      auto expected = ArrayFromJSON(int32(), "[1, 2, 0, 4, 5]");
      DoTestIfElse(cond, if_true, if_false, schema, input, expected);
    }
  }
  {
    ARROW_SCOPED_TRACE("if (a > b) then a else b");
    auto cond = call("greater", {field_ref("a"), field_ref("b")});
    auto if_true = field_ref("a");
    auto if_false = field_ref("b");
    constexpr int64_t length = 5;
    for (const auto& type : kNumericTypes) {
      ARROW_SCOPED_TRACE("Type " + type->ToString());
      auto schema = arrow::schema({field("a", type), field("b", type)});
      auto big = ArrayFromJSON(type, "[1, 2, 3, 4, 5]");
      auto small = ArrayFromJSON(type, "[0, 1, 2, 3, 4]");
      {
        ARROW_SCOPED_TRACE("All true");
        auto input =
            ExecBatch(*RecordBatch::Make(schema, length, {/*a=*/big, /*b=*/small}));
        DoTestIfElse(cond, if_true, if_false, schema, input);
      }
      {
        ARROW_SCOPED_TRACE("All false");
        auto input =
            ExecBatch(*RecordBatch::Make(schema, length, {/*a=*/small, /*b=*/big}));
        DoTestIfElse(cond, if_true, if_false, schema, input);
      }
    }
    {
      ARROW_SCOPED_TRACE("Random");
      auto rng = random::RandomArrayGenerator(42);
      constexpr int64_t length = 1024;
      constexpr int repeat = 10;
      for (const auto& type : kNumericAndBaseBinaryTypes) {
        ARROW_SCOPED_TRACE("Type " + type->ToString());
        auto schema = arrow::schema({field("a", type), field("b", type)});
        for (int i = 0; i < repeat; ++i) {
          auto a = rng.ArrayOf(type, length, /*null_probability=*/0.2);
          auto b = rng.ArrayOf(type, length, /*null_probability=*/0.2);
          auto input =
              ExecBatch(*RecordBatch::Make(schema, length, {std::move(a), std::move(b)}));
          DoTestIfElse(cond, if_true, if_false, schema, input);
        }
      }
    }
  }
}

}  // namespace arrow::compute
