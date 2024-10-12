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

namespace arrow::compute {

namespace {

static const std::vector<std::shared_ptr<DataType>> kIntegerTypes = {
    int8(), uint8(), int16(), uint16(), int32(), uint32(), int64(), uint64()};

using SmallOutputTypes = ::testing::Types<UInt8Type, UInt16Type, Int8Type, Int16Type>;

}  // namespace

// ----------------------------------------------------------------------
// ReverseIndices tests

namespace {

Result<Datum> ReverseIndices(const Datum& indices, int64_t output_length,
                             std::shared_ptr<DataType> output_type) {
  ReverseIndicesOptions options{output_length, std::move(output_type)};
  return ReverseIndices(indices, options);
}

void AssertReverseIndices(const Datum& indices, int64_t output_length,
                          const std::shared_ptr<DataType>& output_type,
                          const Datum& expected, bool validity_must_be_null) {
  ASSERT_OK_AND_ASSIGN(auto result, ReverseIndices(indices, output_length, output_type));
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
void DoTestReverseIndicesForAllInputTypes(InputFunc&& input, int64_t output_length,
                                          const std::shared_ptr<DataType>& output_type,
                                          const Datum& expected,
                                          bool validity_must_be_null = false) {
  for (const auto& input_type : kIntegerTypes) {
    ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
    auto indices = input(input_type);
    AssertReverseIndices(indices, output_length, output_type, expected,
                         validity_must_be_null);
  }
}

template <typename InputFunc>
void DoTestReverseIndicesForAllInputOutputTypes(InputFunc&& input, int64_t output_length,
                                                const std::string& expected_str,
                                                bool validity_must_be_null) {
  for (const auto& output_type : kIntegerTypes) {
    ARROW_SCOPED_TRACE("Output type: " + output_type->ToString());
    auto expected = ArrayFromJSON(output_type, expected_str);
    DoTestReverseIndicesForAllInputTypes(std::forward<InputFunc>(input), output_length,
                                         output_type, expected, validity_must_be_null);
  }
}

void TestReverseIndices(const std::string& indices_str,
                        const std::vector<std::string>& indices_chunked_str,
                        int64_t output_length, const std::string& expected_str,
                        bool validity_must_be_null = false) {
  {
    ARROW_SCOPED_TRACE("Array");
    DoTestReverseIndicesForAllInputOutputTypes(
        [&](const std::shared_ptr<DataType>& input_type) {
          return ArrayFromJSON(input_type, indices_str);
        },
        output_length, expected_str, validity_must_be_null);
  }
  {
    ARROW_SCOPED_TRACE("Chunked");
    DoTestReverseIndicesForAllInputOutputTypes(
        [&](const std::shared_ptr<DataType>& input_type) {
          return ChunkedArrayFromJSON(input_type, indices_chunked_str);
        },
        output_length, expected_str, validity_must_be_null);
  }
}

}  // namespace

TEST(ReverseIndices, InvalidOutputType) {
  {
    ARROW_SCOPED_TRACE("Output type float");
    auto indices = ArrayFromJSON(int32(), "[]");
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: Output type of reverse_indices must be integer, got float",
        ReverseIndices(indices, /*output_length=*/0, /*output_type=*/float32()));
  }
  {
    ARROW_SCOPED_TRACE("Output type string");
    auto indices = ArrayFromJSON(int32(), "[]");
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: Output type of reverse_indices must be integer, got string",
        ReverseIndices(indices, /*output_length=*/0, /*output_type=*/utf8()));
  }
}

TEST(ReverseIndices, DefaultOptions) {
  {
    ARROW_SCOPED_TRACE("Default options values");
    ReverseIndicesOptions options;
    ASSERT_EQ(options.output_length, -1);
    ASSERT_EQ(options.output_type, nullptr);
  }
  {
    ARROW_SCOPED_TRACE("Default options semantics");
    for (const auto& input_type : kIntegerTypes) {
      ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
      auto indices = ArrayFromJSON(input_type, "[0]");
      ASSERT_OK_AND_ASSIGN(Datum result, ReverseIndices(indices));
      AssertDatumsEqual(indices, result);
    }
  }
}

template <typename ArrowType>
class TestReverseIndicesSmallOutputType : public ::testing::Test {
 protected:
  using CType = typename TypeTraits<ArrowType>::CType;

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }

  void JustEnoughOutputType() {
    auto output_type = type_singleton();
    int64_t input_length = static_cast<int64_t>(std::numeric_limits<CType>::max());
    auto expected =
        ArrayFromJSON(output_type, "[" + std::to_string(input_length - 1) + "]");
    DoTestReverseIndicesForAllInputTypes(
        [&](const std::shared_ptr<DataType>& input_type) {
          return ConstantArrayGenerator::Zeroes(input_length, input_type);
        },
        /*output_length=*/1, output_type, expected);
  }

  void InsufficientOutputType() {
    auto output_type = type_singleton();
    int64_t input_length = static_cast<int64_t>(std::numeric_limits<CType>::max()) + 1;
    for (const auto& input_type : kIntegerTypes) {
      ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
      auto indices = ConstantArrayGenerator::Zeroes(input_length, int64());
      ASSERT_RAISES_WITH_MESSAGE(
          Invalid,
          "Invalid: Output type " + output_type->ToString() +
              " of reverse_indices is insufficient to store indices of length " +
              std::to_string(input_length),
          ReverseIndices(indices, /*output_length=*/1, output_type));
    }
  }
};

TYPED_TEST_SUITE(TestReverseIndicesSmallOutputType, SmallOutputTypes);

TYPED_TEST(TestReverseIndicesSmallOutputType, JustEnoughOutputType) {
  this->JustEnoughOutputType();
}

TYPED_TEST(TestReverseIndicesSmallOutputType, InsufficientOutputType) {
  this->InsufficientOutputType();
}

TEST(ReverseIndices, Basic) {
  {
    ARROW_SCOPED_TRACE("Basic");
    auto indices = "[9, 7, 5, 3, 1, 0, 2, 4, 6, 8]";
    std::vector<std::string> indices_chunked{
        "[]", "[9, 7, 5, 3, 1]", "[0]", "[2, 4, 6]", "[8]", "[]"};
    int64_t output_length = 10;
    auto expected = "[5, 4, 6, 3, 7, 2, 8, 1, 9, 0]";
    TestReverseIndices(indices, indices_chunked, output_length, expected,
                       /*validity_must_be_null=*/true);
  }
  {
    ARROW_SCOPED_TRACE("Empty output");
    auto indices = "[1, 2]";
    std::vector<std::string> indices_chunked{"[]", "[1]", "[]", "[]", "[2]", "[]"};
    int64_t output_length = 0;
    auto expected = "[]";
    TestReverseIndices(indices, indices_chunked, output_length, expected,
                       /*validity_must_be_null=*/true);
  }
  {
    ARROW_SCOPED_TRACE("Output less than input");
    auto indices = "[1, 0]";
    std::vector<std::string> indices_chunked{"[]", "[]", "[]", "[1, 0]"};
    int64_t output_length = 1;
    auto expected = "[1]";
    TestReverseIndices(indices, indices_chunked, output_length, expected,
                       /*validity_must_be_null=*/true);
  }
  {
    ARROW_SCOPED_TRACE("Output greater than input");
    auto indices = "[1, 2]";
    std::vector<std::string> indices_chunked{"[]", "[1]", "[]", "[2]"};
    int64_t output_length = 7;
    auto expected = "[null, 0, 1, null, null, null, null]";
    TestReverseIndices(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Input all null");
    auto indices = "[null, null]";
    std::vector<std::string> indices_chunked{"[]", "[null]", "[]", "[null]"};
    int64_t output_length = 1;
    auto expected = "[null]";
    TestReverseIndices(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output all null");
    auto indices = "[1, 2]";
    std::vector<std::string> indices_chunked{"[]", "[1]", "[]", "[2]"};
    int64_t output_length = 1;
    auto expected = "[null]";
    TestReverseIndices(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty input output null");
    auto indices = "[]";
    std::vector<std::string> indices_chunked{"[]", "[]", "[]", "[]"};
    int64_t output_length = 7;
    auto expected = "[null, null, null, null, null, null, null]";
    TestReverseIndices(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Input duplicated indices");
    auto indices = "[1, 2, 3, 1, 2, 3, 1, 2, 3]";
    std::vector<std::string> indices_chunked{"[]", "[1, 2]", "[3, 1, 2, 3, 1]",
                                             "[]", "[2]",    "[3]"};
    int64_t output_length = 5;
    auto expected = "[null, 6, 7, 8, null]";
    TestReverseIndices(indices, indices_chunked, output_length, expected);
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
/// PermuteXXA([V, V], [I', I''], 2 * l) == Concat(E, E)
///
/// where
///   V = values
///   I = indices
///   l = output_length
///   I' = ReplaceWithMask(I, i >= l, null)
///   I'' = I + l
///   E = PermuteAAA(V, I, l)

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

/// Make indices suffix I'' = I + l.
Result<std::shared_ptr<Array>> MakeIndicesSuffix(const std::shared_ptr<Array>& indices,
                                                 int64_t output_length) {
  ARROW_ASSIGN_OR_RAISE(auto l, MakeScalar(indices->type(), output_length));
  ARROW_ASSIGN_OR_RAISE(auto suffix, Add(indices, l));
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

void TestPermute(const std::shared_ptr<DataType>& values_type,
                 const std::string& values_str, const std::string& indices_str,
                 int64_t output_length, const std::string& expected_str) {
  auto values = ArrayFromJSON(values_type, values_str);
  auto expected = ArrayFromJSON(values_type, expected_str);
  for (const auto& indices_type : kIntegerTypes) {
    ARROW_SCOPED_TRACE("Indices type: " + indices_type->ToString());
    auto indices = ArrayFromJSON(indices_type, indices_str);
    {
      ARROW_SCOPED_TRACE("AAA");
      DoTestPermuteAAA(values, indices, output_length, expected);
    }
    {
      ARROW_SCOPED_TRACE("CAA");
      DoTestPermuteCACWithArrays(values, indices, output_length, expected);
    }
    {
      ARROW_SCOPED_TRACE("ACA");
      DoTestPermuteACCWithArrays(values, indices, output_length, expected);
    }
    {
      ARROW_SCOPED_TRACE("CCA");
      DoTestPermuteCCCWithArrays(values, indices, output_length, expected);
    }
  }
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

  void MaxIntegerIndex() {
    auto values = ArrayFromJSON(utf8(), R"(["a"])");
    auto indices_type = type_singleton();
    int64_t max_integer = static_cast<int64_t>(std::numeric_limits<CType>::max());
    auto indices =
        ArrayFromJSON(indices_type, "[" + std::to_string(max_integer - 1) + "]");
    ASSERT_OK_AND_ASSIGN(auto expected_prefix_nulls,
                         MakeArrayOfNull(utf8(), max_integer - 1));
    auto expected_suffix_value = values;
    ASSERT_OK_AND_ASSIGN(auto expected,
                         Concatenate({expected_prefix_nulls, expected_suffix_value}));
    DoTestPermuteAAA(values, indices, /*output_length=*/max_integer, expected);
  }
};

TYPED_TEST_SUITE(TestPermuteSmallIndicesTypes, SmallOutputTypes);

TYPED_TEST(TestPermuteSmallIndicesTypes, MaxIntegerIndex) { this->MaxIntegerIndex(); }

TEST(Permute, Basic) {
  {
    ARROW_SCOPED_TRACE("Basic");
    auto values = "[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]";
    auto indices = "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]";
    int64_t output_length = 10;
    auto expected = "[19, 18, 17, 16, 15, 14, 13, 12, 11, 10]";
    TestPermute(int64(), values, indices, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output greater than input");
    auto values = "[0, 0, 0, 1, 1, 1]";
    auto indices = "[0, 3, 6, 1, 4, 7]";
    int64_t output_length = 9;
    auto expected = "[0, 1, null, 0, 1, null, 0, 1, null]";
    TestPermute(int64(), values, indices, output_length, expected);
  }
}

};  // namespace arrow::compute
