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
#include <cstdint>
#include <cstdio>
#include <functional>
#include <iosfwd>
#include <locale>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_compat.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace compute {

// ----------------------------------------------------------------------
// IsIn tests

void CheckIsIn(const std::shared_ptr<DataType>& type, const std::string& input_json,
               const std::string& value_set_json, const std::string& expected_json,
               bool skip_nulls = false) {
  auto input = ArrayFromJSON(type, input_json);
  auto value_set = ArrayFromJSON(type, value_set_json);
  auto expected = ArrayFromJSON(boolean(), expected_json);

  ASSERT_OK_AND_ASSIGN(Datum actual_datum,
                       IsIn(input, SetLookupOptions(value_set, skip_nulls)));
  std::shared_ptr<Array> actual = actual_datum.make_array();
  ValidateOutput(actual_datum);
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

void CheckIsInChunked(const std::shared_ptr<ChunkedArray>& input,
                      const std::shared_ptr<ChunkedArray>& value_set,
                      const std::shared_ptr<ChunkedArray>& expected,
                      bool skip_nulls = false) {
  ASSERT_OK_AND_ASSIGN(Datum actual_datum,
                       IsIn(input, SetLookupOptions(value_set, skip_nulls)));
  auto actual = actual_datum.chunked_array();
  ValidateOutput(actual_datum);
  AssertChunkedEqual(*expected, *actual);
}

void CheckIsInDictionary(const std::shared_ptr<DataType>& type,
                         const std::shared_ptr<DataType>& index_type,
                         const std::string& input_dictionary_json,
                         const std::string& input_index_json,
                         const std::string& value_set_json,
                         const std::string& expected_json, bool skip_nulls = false) {
  auto dict_type = dictionary(index_type, type);
  auto indices = ArrayFromJSON(index_type, input_index_json);
  auto dict = ArrayFromJSON(type, input_dictionary_json);

  ASSERT_OK_AND_ASSIGN(auto input, DictionaryArray::FromArrays(dict_type, indices, dict));
  auto value_set = ArrayFromJSON(type, value_set_json);
  auto expected = ArrayFromJSON(boolean(), expected_json);

  ASSERT_OK_AND_ASSIGN(Datum actual_datum,
                       IsIn(input, SetLookupOptions(value_set, skip_nulls)));
  std::shared_ptr<Array> actual = actual_datum.make_array();
  ValidateOutput(actual_datum);
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

class TestIsInKernel : public ::testing::Test {};

TEST_F(TestIsInKernel, CallBinary) {
  auto input = ArrayFromJSON(int8(), "[0, 1, 2, 3, 4, 5, 6, 7, 8]");
  auto value_set = ArrayFromJSON(int8(), "[2, 3, 5, 7]");
  ASSERT_RAISES(Invalid, CallFunction("is_in", {input, value_set}));

  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction("is_in_meta_binary", {input, value_set}));
  auto expected = ArrayFromJSON(boolean(), ("[false, false, true, true, false,"
                                            "true, false, true, false]"));
  AssertArraysEqual(*expected, *out.make_array());
}

TEST_F(TestIsInKernel, ImplicitlyCastValueSet) {
  auto input = ArrayFromJSON(int8(), "[0, 1, 2, 3, 4, 5, 6, 7, 8]");

  SetLookupOptions opts{ArrayFromJSON(int32(), "[2, 3, 5, 7]")};
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction("is_in", {input}, &opts));

  auto expected = ArrayFromJSON(boolean(), ("[false, false, true, true, false,"
                                            "true, false, true, false]"));
  AssertArraysEqual(*expected, *out.make_array());

  // fails; value_set cannot be cast to int8
  opts = SetLookupOptions{ArrayFromJSON(float32(), "[2.5, 3.1, 5.0]")};
  ASSERT_RAISES(Invalid, CallFunction("is_in", {input}, &opts));
}

template <typename Type>
class TestIsInKernelPrimitive : public ::testing::Test {};

template <typename Type>
class TestIsInKernelBinary : public ::testing::Test {};

using PrimitiveTypes = ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type,
                                        Int32Type, UInt32Type, Int64Type, UInt64Type,
                                        FloatType, DoubleType, Date32Type, Date64Type>;

TYPED_TEST_SUITE(TestIsInKernelPrimitive, PrimitiveTypes);

TYPED_TEST(TestIsInKernelPrimitive, IsIn) {
  auto type = TypeTraits<TypeParam>::type_singleton();

  // No Nulls
  CheckIsIn(type, "[0, 1, 2, 3, 2]", "[2, 1]", "[false, true, true, false, true]");

  // Nulls in left array
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, 1]", "[false, true, true, false, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, 1]", "[false, true, true, false, true]",
            /*skip_nulls=*/true);

  // Nulls in right array
  CheckIsIn(type, "[0, 1, 2, 3, 2]", "[2, null, 1]", "[false, true, true, false, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, "[0, 1, 2, 3, 2]", "[2, null, 1]", "[false, true, true, false, true]",
            /*skip_nulls=*/true);

  // Nulls in both the arrays
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, null, 1]", "[true, true, true, false, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, null, 1]",
            "[false, true, true, false, true]", /*skip_nulls=*/true);

  // Duplicates in right array
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[null, 2, 2, null, 1, 1]",
            "[true, true, true, false, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[null, 2, 2, null, 1, 1]",
            "[false, true, true, false, true]", /*skip_nulls=*/true);

  // Empty Arrays
  CheckIsIn(type, "[]", "[]", "[]");
}

TEST_F(TestIsInKernel, NullType) {
  auto type = null();

  CheckIsIn(type, "[null, null, null]", "[null]", "[true, true, true]");
  CheckIsIn(type, "[null, null, null]", "[]", "[false, false, false]");
  CheckIsIn(type, "[]", "[]", "[]");

  CheckIsIn(type, "[null, null]", "[null]", "[false, false]", /*skip_nulls=*/true);
  CheckIsIn(type, "[null, null]", "[]", "[false, false]", /*skip_nulls=*/true);

  // Duplicates in right array
  CheckIsIn(type, "[null, null, null]", "[null, null]", "[true, true, true]");
  CheckIsIn(type, "[null, null]", "[null, null]", "[false, false]", /*skip_nulls=*/true);
}

TEST_F(TestIsInKernel, TimeTimestamp) {
  for (const auto& type :
       {time32(TimeUnit::SECOND), time64(TimeUnit::NANO), timestamp(TimeUnit::MICRO)}) {
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, true, false, true, true]", /*skip_nulls=*/false);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, false, false, true, true]", /*skip_nulls=*/true);

    // Duplicates in right array
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, true, false, true, true]", /*skip_nulls=*/false);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, false, false, true, true]", /*skip_nulls=*/true);
  }
}

TEST_F(TestIsInKernel, Boolean) {
  auto type = boolean();

  CheckIsIn(type, "[true, false, null, true, false]", "[false]",
            "[false, true, false, false, true]", /*skip_nulls=*/false);
  CheckIsIn(type, "[true, false, null, true, false]", "[false]",
            "[false, true, false, false, true]", /*skip_nulls=*/true);

  CheckIsIn(type, "[true, false, null, true, false]", "[false, null]",
            "[false, true, true, false, true]", /*skip_nulls=*/false);
  CheckIsIn(type, "[true, false, null, true, false]", "[false, null]",
            "[false, true, false, false, true]", /*skip_nulls=*/true);

  // Duplicates in right array
  CheckIsIn(type, "[true, false, null, true, false]", "[null, false, false, null]",
            "[false, true, true, false, true]", /*skip_nulls=*/false);
  CheckIsIn(type, "[true, false, null, true, false]", "[null, false, false, null]",
            "[false, true, false, false, true]", /*skip_nulls=*/true);
}

TYPED_TEST_SUITE(TestIsInKernelBinary, BaseBinaryArrowTypes);

TYPED_TEST(TestIsInKernelBinary, Binary) {
  auto type = TypeTraits<TypeParam>::type_singleton();

  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", ""])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", ""])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/true);

  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", "", null])",
            "[true, true, false, true, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", "", null])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/true);

  // Duplicates in right array
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])",
            R"([null, "aaa", "aaa", "", "", null])", "[true, true, false, true, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])",
            R"([null, "aaa", "aaa", "", "", null])", "[true, true, false, false, true]",
            /*skip_nulls=*/true);
}

TEST_F(TestIsInKernel, FixedSizeBinary) {
  auto type = fixed_size_binary(3);

  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb"])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb"])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/true);

  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb", null])",
            "[true, true, false, true, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb", null])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/true);

  // Duplicates in right array
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])",
            R"(["aaa", null, "aaa", "bbb", "bbb", null])",
            "[true, true, false, true, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])",
            R"(["aaa", null, "aaa", "bbb", "bbb", null])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/true);
}

TEST_F(TestIsInKernel, Decimal) {
  auto type = decimal(3, 1);

  CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])", R"(["12.3", "78.9"])",
            "[true, false, true, false, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])", R"(["12.3", "78.9"])",
            "[true, false, true, false, true]",
            /*skip_nulls=*/true);

  CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
            R"(["12.3", "78.9", null])", "[true, false, true, true, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
            R"(["12.3", "78.9", null])", "[true, false, true, false, true]",
            /*skip_nulls=*/true);

  // Duplicates in right array
  CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
            R"([null, "12.3", "12.3", "78.9", "78.9", null])",
            "[true, false, true, true, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
            R"([null, "12.3", "12.3", "78.9", "78.9", null])",
            "[true, false, true, false, true]",
            /*skip_nulls=*/true);
}

TEST_F(TestIsInKernel, DictionaryArray) {
  for (auto index_ty : all_dictionary_index_types()) {
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 2, null, 0]",
                        /*value_set_json=*/R"(["A", "B", "C"])",
                        /*expected_json=*/"[true, true, false, true]",
                        /*skip_nulls=*/false);
    CheckIsInDictionary(/*type=*/float32(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/"[4.1, -1.0, 42, 9.8]",
                        /*input_index_json=*/"[1, 2, null, 0]",
                        /*value_set_json=*/"[4.1, 42, -1.0]",
                        /*expected_json=*/"[true, true, false, true]",
                        /*skip_nulls=*/false);

    // With nulls and skip_nulls=false
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[true, false, true, true, true]",
                        /*skip_nulls=*/false);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[true, false, true, true, true]",
                        /*skip_nulls=*/false);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A"])",
                        /*expected_json=*/"[false, false, false, true, false]",
                        /*skip_nulls=*/false);

    // With nulls and skip_nulls=true
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[true, false, false, true, true]",
                        /*skip_nulls=*/true);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[false, false, false, true, false]",
                        /*skip_nulls=*/true);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A"])",
                        /*expected_json=*/"[false, false, false, true, false]",
                        /*skip_nulls=*/true);

    // With duplicates in value_set
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 2, null, 0]",
                        /*value_set_json=*/R"(["A", "A", "B", "A", "B", "C"])",
                        /*expected_json=*/"[true, true, false, true]",
                        /*skip_nulls=*/false);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "C", "B", "A", null, null, "B"])",
                        /*expected_json=*/"[true, false, true, true, true]",
                        /*skip_nulls=*/false);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "C", "B", "A", null, null, "B"])",
                        /*expected_json=*/"[true, false, false, true, true]",
                        /*skip_nulls=*/true);
  }
}

TEST_F(TestIsInKernel, ChunkedArrayInvoke) {
  auto input = ChunkedArrayFromJSON(
      utf8(), {R"(["abc", "def", "", "abc", "jkl"])", R"(["def", null, "abc", "zzz"])"});
  // No null in value_set
  auto value_set = ChunkedArrayFromJSON(utf8(), {R"(["", "def"])", R"(["abc"])"});
  auto expected = ChunkedArrayFromJSON(
      boolean(), {"[true, true, true, true, false]", "[true, false, true, false]"});

  CheckIsInChunked(input, value_set, expected, /*skip_nulls=*/false);
  CheckIsInChunked(input, value_set, expected, /*skip_nulls=*/true);

  value_set = ChunkedArrayFromJSON(utf8(), {R"(["", "def"])", R"([null])"});
  expected = ChunkedArrayFromJSON(
      boolean(), {"[false, true, true, false, false]", "[true, true, false, false]"});
  CheckIsInChunked(input, value_set, expected, /*skip_nulls=*/false);
  expected = ChunkedArrayFromJSON(
      boolean(), {"[false, true, true, false, false]", "[true, false, false, false]"});
  CheckIsInChunked(input, value_set, expected, /*skip_nulls=*/true);

  // Duplicates in value_set
  value_set =
      ChunkedArrayFromJSON(utf8(), {R"(["", null, "", "def"])", R"(["def", null])"});
  expected = ChunkedArrayFromJSON(
      boolean(), {"[false, true, true, false, false]", "[true, true, false, false]"});
  CheckIsInChunked(input, value_set, expected, /*skip_nulls=*/false);
  expected = ChunkedArrayFromJSON(
      boolean(), {"[false, true, true, false, false]", "[true, false, false, false]"});
  CheckIsInChunked(input, value_set, expected, /*skip_nulls=*/true);
}

// ----------------------------------------------------------------------
// IndexIn tests

class TestIndexInKernel : public ::testing::Test {
 public:
  void CheckIndexIn(const std::shared_ptr<DataType>& type, const std::string& input_json,
                    const std::string& value_set_json, const std::string& expected_json,
                    bool skip_nulls = false) {
    std::shared_ptr<Array> input = ArrayFromJSON(type, input_json);
    std::shared_ptr<Array> value_set = ArrayFromJSON(type, value_set_json);
    std::shared_ptr<Array> expected = ArrayFromJSON(int32(), expected_json);

    SetLookupOptions options(value_set, skip_nulls);
    ASSERT_OK_AND_ASSIGN(Datum actual_datum, IndexIn(input, options));
    std::shared_ptr<Array> actual = actual_datum.make_array();
    ValidateOutput(actual_datum);
    AssertArraysEqual(*expected, *actual, /*verbose=*/true);
  }

  void CheckIndexInChunked(const std::shared_ptr<ChunkedArray>& input,
                           const std::shared_ptr<ChunkedArray>& value_set,
                           const std::shared_ptr<ChunkedArray>& expected,
                           bool skip_nulls) {
    ASSERT_OK_AND_ASSIGN(Datum actual,
                         IndexIn(input, SetLookupOptions(value_set, skip_nulls)));
    ASSERT_EQ(Datum::CHUNKED_ARRAY, actual.kind());
    ValidateOutput(actual);
    AssertChunkedEqual(*expected, *actual.chunked_array());
  }

  void CheckIndexInDictionary(const std::shared_ptr<DataType>& type,
                              const std::shared_ptr<DataType>& index_type,
                              const std::string& input_dictionary_json,
                              const std::string& input_index_json,
                              const std::string& value_set_json,
                              const std::string& expected_json, bool skip_nulls = false) {
    auto dict_type = dictionary(index_type, type);
    auto indices = ArrayFromJSON(index_type, input_index_json);
    auto dict = ArrayFromJSON(type, input_dictionary_json);

    ASSERT_OK_AND_ASSIGN(auto input,
                         DictionaryArray::FromArrays(dict_type, indices, dict));
    auto value_set = ArrayFromJSON(type, value_set_json);
    auto expected = ArrayFromJSON(int32(), expected_json);

    SetLookupOptions options(value_set, skip_nulls);
    ASSERT_OK_AND_ASSIGN(Datum actual_datum, IndexIn(input, options));
    std::shared_ptr<Array> actual = actual_datum.make_array();
    ValidateOutput(actual_datum);
    AssertArraysEqual(*expected, *actual, /*verbose=*/true);
  }
};

TEST_F(TestIndexInKernel, CallBinary) {
  auto input = ArrayFromJSON(int8(), "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
  auto value_set = ArrayFromJSON(int8(), "[2, 3, 5, 7]");
  ASSERT_RAISES(Invalid, CallFunction("index_in", {input, value_set}));

  ASSERT_OK_AND_ASSIGN(Datum out,
                       CallFunction("index_in_meta_binary", {input, value_set}));
  auto expected = ArrayFromJSON(int32(), ("[null, null, 0, 1, null, 2, null, 3, null,"
                                          " null, null]"));
  AssertArraysEqual(*expected, *out.make_array());
}

template <typename Type>
class TestIndexInKernelPrimitive : public TestIndexInKernel {};

using PrimitiveDictionaries =
    ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type, UInt32Type,
                     Int64Type, UInt64Type, FloatType, DoubleType, Date32Type,
                     Date64Type>;

TYPED_TEST_SUITE(TestIndexInKernelPrimitive, PrimitiveDictionaries);

TYPED_TEST(TestIndexInKernelPrimitive, IndexIn) {
  auto type = TypeTraits<TypeParam>::type_singleton();

  // No Nulls
  this->CheckIndexIn(type,
                     /* input= */ "[2, 1, 2, 1, 2, 3]",
                     /* value_set= */ "[2, 1, 3]",
                     /* expected= */ "[0, 1, 0, 1, 0, 2]");

  // Haystack array all null
  this->CheckIndexIn(type,
                     /* input= */ "[null, null, null, null, null, null]",
                     /* value_set= */ "[2, 1, 3]",
                     /* expected= */ "[null, null, null, null, null, null]");

  // Needles array all null
  this->CheckIndexIn(type,
                     /* input= */ "[2, 1, 2, 1, 2, 3]",
                     /* value_set= */ "[null]",
                     /* expected= */ "[null, null, null, null, null, null]");

  // Both arrays all null
  this->CheckIndexIn(type,
                     /* input= */ "[null, null, null, null]",
                     /* value_set= */ "[null]",
                     /* expected= */ "[0, 0, 0, 0]");

  // Duplicates in value_set
  this->CheckIndexIn(type,
                     /* input= */ "[2, 1, 2, 1, 2, 3]",
                     /* value_set= */ "[2, 2, 1, 1, 1, 3, 3]",
                     /* expected= */ "[0, 2, 0, 2, 0, 5]");

  // Duplicates and nulls in value_set
  this->CheckIndexIn(type,
                     /* input= */ "[2, 1, 2, 1, 2, 3]",
                     /* value_set= */ "[2, 2, null, null, 1, 1, 1, 3, 3]",
                     /* expected= */ "[0, 4, 0, 4, 0, 7]");

  // No Match
  this->CheckIndexIn(type,
                     /* input= */ "[2, null, 7, 3, 8]",
                     /* value_set= */ "[2, null, 6, 3]",
                     /* expected= */ "[0, 1, null, 3, null]");

  // Empty Arrays
  this->CheckIndexIn(type, "[]", "[]", "[]");
}

TYPED_TEST(TestIndexInKernelPrimitive, SkipNulls) {
  auto type = TypeTraits<TypeParam>::type_singleton();

  // No nulls in value_set
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 3]",
                     /*expected=*/"[null, 0, null, 1, null]",
                     /*skip_nulls=*/false);
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 3]",
                     /*expected=*/"[null, 0, null, 1, null]",
                     /*skip_nulls=*/true);
  // Same with duplicates in value_set
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 1, 3, 3]",
                     /*expected=*/"[null, 0, null, 2, null]",
                     /*skip_nulls=*/false);
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 1, 3, 3]",
                     /*expected=*/"[null, 0, null, 2, null]",
                     /*skip_nulls=*/true);

  // Nulls in value_set
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, null, 3]",
                     /*expected=*/"[null, 0, null, 2, 1]",
                     /*skip_nulls=*/false);
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 1, null, null, 3, 3]",
                     /*expected=*/"[null, 0, null, 4, null]",
                     /*skip_nulls=*/true);
  // Same with duplicates in value_set
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 1, null, null, 3, 3]",
                     /*expected=*/"[null, 0, null, 4, 2]",
                     /*skip_nulls=*/false);
}

TEST_F(TestIndexInKernel, NullType) {
  CheckIndexIn(null(), "[null, null, null]", "[null]", "[0, 0, 0]");
  CheckIndexIn(null(), "[null, null, null]", "[]", "[null, null, null]");
  CheckIndexIn(null(), "[]", "[null, null]", "[]");
  CheckIndexIn(null(), "[]", "[]", "[]");

  CheckIndexIn(null(), "[null, null]", "[null]", "[null, null]", /*skip_nulls=*/true);
  CheckIndexIn(null(), "[null, null]", "[]", "[null, null]", /*skip_nulls=*/true);
}

TEST_F(TestIndexInKernel, TimeTimestamp) {
  CheckIndexIn(time32(TimeUnit::SECOND),
               /* input= */ "[1, null, 5, 1, 2]",
               /* value_set= */ "[2, 1, null]",
               /* expected= */ "[1, 2, null, 1, 0]");

  // Duplicates in value_set
  CheckIndexIn(time32(TimeUnit::SECOND),
               /* input= */ "[1, null, 5, 1, 2]",
               /* value_set= */ "[2, 2, 1, 1, null, null]",
               /* expected= */ "[2, 4, null, 2, 0]");

  // Needles array has no nulls
  CheckIndexIn(time32(TimeUnit::SECOND),
               /* input= */ "[2, null, 5, 1]",
               /* value_set= */ "[2, 1]",
               /* expected= */ "[0, null, null, 1]");

  // No match
  CheckIndexIn(time32(TimeUnit::SECOND), "[3, null, 5, 3]", "[2, 1]",
               "[null, null, null, null]");

  // Empty arrays
  CheckIndexIn(time32(TimeUnit::SECOND), "[]", "[]", "[]");

  CheckIndexIn(time64(TimeUnit::NANO), "[2, null, 2, 1]", "[2, null, 1]", "[0, 1, 0, 2]");

  CheckIndexIn(timestamp(TimeUnit::NANO), "[2, null, 2, 1]", "[2, null, 1]",
               "[0, 1, 0, 2]");

  // Empty input array
  CheckIndexIn(timestamp(TimeUnit::NANO), "[]", "[2, null, 1]", "[]");

  // Empty value_set array
  CheckIndexIn(timestamp(TimeUnit::NANO), "[2, null, 1]", "[]", "[null, null, null]");

  // Both array are all null
  CheckIndexIn(time32(TimeUnit::SECOND), "[null, null, null, null]", "[null]",
               "[0, 0, 0, 0]");
}

TEST_F(TestIndexInKernel, Boolean) {
  CheckIndexIn(boolean(),
               /* input= */ "[false, null, false, true]",
               /* value_set= */ "[null, false, true]",
               /* expected= */ "[1, 0, 1, 2]");

  CheckIndexIn(boolean(), "[false, null, false, true]", "[false, true, null]",
               "[0, 2, 0, 1]");

  // Duplicates in value_set
  CheckIndexIn(boolean(), "[false, null, false, true]",
               "[false, false, true, true, null, null]", "[0, 4, 0, 2]");

  // No Nulls
  CheckIndexIn(boolean(), "[true, true, false, true]", "[false, true]", "[1, 1, 0, 1]");

  CheckIndexIn(boolean(), "[false, true, false, true]", "[true]", "[null, 0, null, 0]");

  // No match
  CheckIndexIn(boolean(), "[true, true, true, true]", "[false]",
               "[null, null, null, null]");

  // Nulls in input array
  CheckIndexIn(boolean(), "[null, null, null, null]", "[true]",
               "[null, null, null, null]");

  // Nulls in value_set array
  CheckIndexIn(boolean(), "[true, true, false, true]", "[null]",
               "[null, null, null, null]");

  // Both array have Nulls
  CheckIndexIn(boolean(), "[null, null, null, null]", "[null]", "[0, 0, 0, 0]");
}

template <typename Type>
class TestIndexInKernelBinary : public TestIndexInKernel {};

TYPED_TEST_SUITE(TestIndexInKernelBinary, BaseBinaryArrowTypes);

TYPED_TEST(TestIndexInKernelBinary, Binary) {
  auto type = TypeTraits<TypeParam>::type_singleton();
  this->CheckIndexIn(type, R"(["foo", null, "bar", "foo"])", R"(["foo", null, "bar"])",
                     R"([0, 1, 2, 0])");

  // Duplicates in value_set
  this->CheckIndexIn(type, R"(["foo", null, "bar", "foo"])",
                     R"(["foo", "foo", null, null, "bar", "bar"])", R"([0, 2, 4, 0])");

  // No match
  this->CheckIndexIn(type,
                     /* input= */ R"(["foo", null, "bar", "foo"])",
                     /* value_set= */ R"(["baz", "bazzz"])",
                     /* expected= */ R"([null, null, null, null])");

  // Nulls in input array
  this->CheckIndexIn(type,
                     /* input= */ R"([null, null, null, null])",
                     /* value_set= */ R"(["foo", "bar"])",
                     /* expected= */ R"([null, null, null, null])");

  // Nulls in value_set array
  this->CheckIndexIn(type, R"(["foo", "bar", "foo"])", R"([null])",
                     R"([null, null, null])");

  // Both array have Nulls
  this->CheckIndexIn(type,
                     /* input= */ R"([null, null, null, null])",
                     /* value_set= */ R"([null])",
                     /* expected= */ R"([0, 0, 0, 0])");

  // Empty arrays
  this->CheckIndexIn(type, R"([])", R"([])", R"([])");

  // Empty input array
  this->CheckIndexIn(type, R"([])", R"(["foo", null, "bar"])", "[]");

  // Empty value_set array
  this->CheckIndexIn(type, R"(["foo", null, "bar", "foo"])", "[]",
                     R"([null, null, null, null])");
}

TEST_F(TestIndexInKernel, BinaryResizeTable) {
  const int32_t kTotalValues = 10000;
#if !defined(ARROW_VALGRIND)
  const int32_t kRepeats = 10;
#else
  // Mitigate Valgrind's slowness
  const int32_t kRepeats = 3;
#endif

  const int32_t kBufSize = 20;

  Int32Builder expected_builder;
  StringBuilder input_builder;
  ASSERT_OK(expected_builder.Resize(kTotalValues * kRepeats));
  ASSERT_OK(input_builder.Resize(kTotalValues * kRepeats));
  ASSERT_OK(input_builder.ReserveData(kBufSize * kTotalValues * kRepeats));

  for (int32_t i = 0; i < kTotalValues * kRepeats; i++) {
    int32_t index = i % kTotalValues;

    char buf[kBufSize] = "test";
    ASSERT_GE(snprintf(buf + 4, sizeof(buf) - 4, "%d", index), 0);

    input_builder.UnsafeAppend(util::string_view(buf));
    expected_builder.UnsafeAppend(index);
  }

  std::shared_ptr<Array> input, value_set, expected;
  ASSERT_OK(input_builder.Finish(&input));
  value_set = input->Slice(0, kTotalValues);
  ASSERT_OK(expected_builder.Finish(&expected));

  ASSERT_OK_AND_ASSIGN(Datum actual_datum, IndexIn(input, value_set));
  std::shared_ptr<Array> actual = actual_datum.make_array();
  ASSERT_ARRAYS_EQUAL(*expected, *actual);
}

TEST_F(TestIndexInKernel, FixedSizeBinary) {
  CheckIndexIn(fixed_size_binary(3),
               /*input=*/R"(["bbb", null, "ddd", "aaa", "ccc", "aaa"])",
               /*value_set=*/R"(["aaa", null, "bbb", "ccc"])",
               /*expected=*/R"([2, 1, null, 0, 3, 0])");
  CheckIndexIn(fixed_size_binary(3),
               /*input=*/R"(["bbb", null, "ddd", "aaa", "ccc", "aaa"])",
               /*value_set=*/R"(["aaa", null, "bbb", "ccc"])",
               /*expected=*/R"([2, null, null, 0, 3, 0])",
               /*skip_nulls=*/true);

  CheckIndexIn(fixed_size_binary(3),
               /*input=*/R"(["bbb", null, "ddd", "aaa", "ccc", "aaa"])",
               /*value_set=*/R"(["aaa", "bbb", "ccc"])",
               /*expected=*/R"([1, null, null, 0, 2, 0])");
  CheckIndexIn(fixed_size_binary(3),
               /*input=*/R"(["bbb", null, "ddd", "aaa", "ccc", "aaa"])",
               /*value_set=*/R"(["aaa", "bbb", "ccc"])",
               /*expected=*/R"([1, null, null, 0, 2, 0])",
               /*skip_nulls=*/true);

  // Duplicates in value_set
  CheckIndexIn(fixed_size_binary(3),
               /*input=*/R"(["bbb", null, "ddd", "aaa", "ccc", "aaa"])",
               /*value_set=*/R"(["aaa", "aaa", null, null, "bbb", "bbb", "ccc"])",
               /*expected=*/R"([4, 2, null, 0, 6, 0])");
  CheckIndexIn(fixed_size_binary(3),
               /*input=*/R"(["bbb", null, "ddd", "aaa", "ccc", "aaa"])",
               /*value_set=*/R"(["aaa", "aaa", null, null, "bbb", "bbb", "ccc"])",
               /*expected=*/R"([4, null, null, 0, 6, 0])",
               /*skip_nulls=*/true);

  // Empty input array
  CheckIndexIn(fixed_size_binary(5), R"([])", R"(["bbbbb", null, "aaaaa", "ccccc"])",
               R"([])");

  // Empty value_set array
  CheckIndexIn(fixed_size_binary(5), R"(["bbbbb", null, "bbbbb"])", R"([])",
               R"([null, null, null])");

  // Empty arrays
  CheckIndexIn(fixed_size_binary(0), R"([])", R"([])", R"([])");
}

TEST_F(TestIndexInKernel, MonthDayNanoInterval) {
  auto type = month_day_nano_interval();

  CheckIndexIn(type,
               /*input=*/R"([[5, -1, 5], null, [4, 5, 6], [5, -1, 5], [1, 2, 3]])",
               /*value_set=*/R"([null, [4, 5, 6], [5, -1, 5]])",
               /*expected=*/R"([2, 0, 1, 2, null])",
               /*skip_nulls=*/false);

  // Duplicates in value_set
  CheckIndexIn(
      type,
      /*input=*/R"([[7, 8, 0], null, [0, 0, 0], [7, 8, 0], [0, 0, 1]])",
      /*value_set=*/R"([null, null, [0, 0, 0], [0, 0, 0], [7, 8, 0], [7, 8, 0]])",
      /*expected=*/R"([4, 0, 2, 4, null])",
      /*skip_nulls=*/false);
}

TEST_F(TestIndexInKernel, Decimal) {
  auto type = decimal(2, 0);

  CheckIndexIn(type,
               /*input=*/R"(["12", null, "11", "12", "13"])",
               /*value_set=*/R"([null, "11", "12"])",
               /*expected=*/R"([2, 0, 1, 2, null])",
               /*skip_nulls=*/false);
  CheckIndexIn(type,
               /*input=*/R"(["12", null, "11", "12", "13"])",
               /*value_set=*/R"([null, "11", "12"])",
               /*expected=*/R"([2, null, 1, 2, null])",
               /*skip_nulls=*/true);

  CheckIndexIn(type,
               /*input=*/R"(["12", null, "11", "12", "13"])",
               /*value_set=*/R"(["11", "12"])",
               /*expected=*/R"([1, null, 0, 1, null])",
               /*skip_nulls=*/false);
  CheckIndexIn(type,
               /*input=*/R"(["12", null, "11", "12", "13"])",
               /*value_set=*/R"(["11", "12"])",
               /*expected=*/R"([1, null, 0, 1, null])",
               /*skip_nulls=*/true);

  // Duplicates in value_set
  CheckIndexIn(type,
               /*input=*/R"(["12", null, "11", "12", "13"])",
               /*value_set=*/R"([null, null, "11", "11", "12", "12"])",
               /*expected=*/R"([4, 0, 2, 4, null])",
               /*skip_nulls=*/false);
  CheckIndexIn(type,
               /*input=*/R"(["12", null, "11", "12", "13"])",
               /*value_set=*/R"([null, null, "11", "11", "12", "12"])",
               /*expected=*/R"([4, null, 2, 4, null])",
               /*skip_nulls=*/true);
}

TEST_F(TestIndexInKernel, DictionaryArray) {
  for (auto index_ty : all_dictionary_index_types()) {
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                           /*input_index_json=*/"[1, 2, null, 0]",
                           /*value_set_json=*/R"(["A", "B", "C"])",
                           /*expected_json=*/"[1, 2, null, 0]",
                           /*skip_nulls=*/false);
    CheckIndexInDictionary(/*type=*/float32(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/"[4.1, -1.0, 42, 9.8]",
                           /*input_index_json=*/"[1, 2, null, 0]",
                           /*value_set_json=*/"[4.1, 42, -1.0]",
                           /*expected_json=*/"[2, 1, null, 0]",
                           /*skip_nulls=*/false);

    // With nulls and skip_nulls=false
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A", null])",
                           /*expected_json=*/"[1, null, 3, 2, 1]",
                           /*skip_nulls=*/false);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A", null])",
                           /*expected_json=*/"[3, null, 3, 2, 3]",
                           /*skip_nulls=*/false);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A"])",
                           /*expected_json=*/"[null, null, null, 2, null]",
                           /*skip_nulls=*/false);

    // With nulls and skip_nulls=true
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A", null])",
                           /*expected_json=*/"[1, null, null, 2, 1]",
                           /*skip_nulls=*/true);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A", null])",
                           /*expected_json=*/"[null, null, null, 2, null]",
                           /*skip_nulls=*/true);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A"])",
                           /*expected_json=*/"[null, null, null, 2, null]",
                           /*skip_nulls=*/true);

    // With duplicates in value_set
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                           /*input_index_json=*/"[1, 2, null, 0]",
                           /*value_set_json=*/R"(["A", "A", "B", "B", "C", "C"])",
                           /*expected_json=*/"[2, 4, null, 0]",
                           /*skip_nulls=*/false);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "C", "B", "B", "A", "A", null])",
                           /*expected_json=*/"[6, null, 6, 4, 6]",
                           /*skip_nulls=*/false);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "C", "B", "B", "A", "A", null])",
                           /*expected_json=*/"[null, null, null, 4, null]",
                           /*skip_nulls=*/true);
  }
}

TEST_F(TestIndexInKernel, ChunkedArrayInvoke) {
  auto input = ChunkedArrayFromJSON(utf8(), {R"(["abc", "def", "ghi", "abc", "jkl"])",
                                             R"(["def", null, "abc", "zzz"])"});
  // No null in value_set
  auto value_set = ChunkedArrayFromJSON(utf8(), {R"(["ghi", "def"])", R"(["abc"])"});
  auto expected =
      ChunkedArrayFromJSON(int32(), {"[2, 1, 0, 2, null]", "[1, null, 2, null]"});

  CheckIndexInChunked(input, value_set, expected, /*skip_nulls=*/false);
  CheckIndexInChunked(input, value_set, expected, /*skip_nulls=*/true);

  // Null in value_set
  value_set = ChunkedArrayFromJSON(utf8(), {R"(["ghi", "def"])", R"([null, "abc"])"});
  expected = ChunkedArrayFromJSON(int32(), {"[3, 1, 0, 3, null]", "[1, 2, 3, null]"});
  CheckIndexInChunked(input, value_set, expected, /*skip_nulls=*/false);
  expected = ChunkedArrayFromJSON(int32(), {"[3, 1, 0, 3, null]", "[1, null, 3, null]"});
  CheckIndexInChunked(input, value_set, expected, /*skip_nulls=*/true);

  // Duplicates in value_set
  value_set = ChunkedArrayFromJSON(
      utf8(), {R"(["ghi", "ghi", "def"])", R"(["def", null, null, "abc"])"});
  expected = ChunkedArrayFromJSON(int32(), {"[6, 2, 0, 6, null]", "[2, 4, 6, null]"});
  CheckIndexInChunked(input, value_set, expected, /*skip_nulls=*/false);
  expected = ChunkedArrayFromJSON(int32(), {"[6, 2, 0, 6, null]", "[2, null, 6, null]"});
  CheckIndexInChunked(input, value_set, expected, /*skip_nulls=*/true);
}

TEST(TestSetLookup, DispatchBest) {
  for (std::string name : {"is_in", "index_in"}) {
    CheckDispatchBest(name, {int32()}, {int32()});
    CheckDispatchBest(name, {dictionary(int32(), utf8())}, {utf8()});
  }
}

TEST(TestSetLookup, IsInWithImplicitCasts) {
  SetLookupOptions opts{ArrayFromJSON(utf8(), R"(["b", null])")};
  CheckScalarUnary("is_in",
                   ArrayFromJSON(dictionary(int32(), utf8()), R"(["a", "b", "c", null])"),
                   ArrayFromJSON(boolean(), "[0, 1, 0, 1]"), &opts);
}

}  // namespace compute
}  // namespace arrow
