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
#include "arrow/compute/kernels/test_util_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_compat.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace compute {

// ----------------------------------------------------------------------
// IsIn tests

void CheckIsIn(const std::shared_ptr<Array> input,
               const std::shared_ptr<Array>& value_set, const std::string& expected_json,
               SetLookupOptions::NullMatchingBehavior null_matching_behavior =
                   SetLookupOptions::MATCH) {
  auto expected = ArrayFromJSON(boolean(), expected_json);

  ASSERT_OK_AND_ASSIGN(Datum actual_datum,
                       IsIn(input, SetLookupOptions(value_set, null_matching_behavior)));
  std::shared_ptr<Array> actual = actual_datum.make_array();
  ValidateOutput(actual_datum);
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

void CheckIsIn(const std::shared_ptr<DataType>& type, const std::string& input_json,
               const std::string& value_set_json, const std::string& expected_json,
               SetLookupOptions::NullMatchingBehavior null_matching_behavior =
                   SetLookupOptions::MATCH) {
  auto input = ArrayFromJSON(type, input_json);
  auto value_set = ArrayFromJSON(type, value_set_json);
  CheckIsIn(input, value_set, expected_json, null_matching_behavior);
}

void CheckIsInChunked(const std::shared_ptr<ChunkedArray>& input,
                      const std::shared_ptr<ChunkedArray>& value_set,
                      const std::shared_ptr<ChunkedArray>& expected,
                      SetLookupOptions::NullMatchingBehavior null_matching_behavior =
                          SetLookupOptions::MATCH) {
  ASSERT_OK_AND_ASSIGN(Datum actual_datum,
                       IsIn(input, SetLookupOptions(value_set, null_matching_behavior)));
  auto actual = actual_datum.chunked_array();
  ValidateOutput(actual_datum);

  // Output contiguous in a single chunk
  ASSERT_EQ(1, actual->num_chunks());
  ASSERT_TRUE(actual->Equals(*expected));
}

void CheckIsInDictionary(const std::shared_ptr<DataType>& type,
                         const std::shared_ptr<DataType>& index_type,
                         const std::string& input_dictionary_json,
                         const std::string& input_index_json,
                         const std::string& value_set_json,
                         const std::string& expected_json,
                         SetLookupOptions::NullMatchingBehavior null_matching_behavior =
                             SetLookupOptions::MATCH) {
  auto dict_type = dictionary(index_type, type);
  auto indices = ArrayFromJSON(index_type, input_index_json);
  auto dict = ArrayFromJSON(type, input_dictionary_json);

  ASSERT_OK_AND_ASSIGN(auto input, DictionaryArray::FromArrays(dict_type, indices, dict));
  auto value_set = ArrayFromJSON(type, value_set_json);
  auto expected = ArrayFromJSON(boolean(), expected_json);

  ASSERT_OK_AND_ASSIGN(Datum actual_datum,
                       IsIn(input, SetLookupOptions(value_set, null_matching_behavior)));
  std::shared_ptr<Array> actual = actual_datum.make_array();
  ValidateOutput(actual_datum);
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

void CheckIsIn(const std::shared_ptr<Array> input,
               const std::shared_ptr<Array>& value_set, const std::string& expected_json,
               bool skip_nulls) {
  auto expected = ArrayFromJSON(boolean(), expected_json);

  ASSERT_OK_AND_ASSIGN(Datum actual_datum,
                       IsIn(input, SetLookupOptions(value_set, skip_nulls)));
  std::shared_ptr<Array> actual = actual_datum.make_array();
  ValidateOutput(actual_datum);
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

void CheckIsIn(const std::shared_ptr<DataType>& type, const std::string& input_json,
               const std::string& value_set_json, const std::string& expected_json,
               bool skip_nulls) {
  auto input = ArrayFromJSON(type, input_json);
  auto value_set = ArrayFromJSON(type, value_set_json);
  CheckIsIn(input, value_set, expected_json, skip_nulls);
}

void CheckIsInChunked(const std::shared_ptr<ChunkedArray>& input,
                      const std::shared_ptr<ChunkedArray>& value_set,
                      const std::shared_ptr<ChunkedArray>& expected, bool skip_nulls) {
  ASSERT_OK_AND_ASSIGN(Datum actual_datum,
                       IsIn(input, SetLookupOptions(value_set, skip_nulls)));
  auto actual = actual_datum.chunked_array();
  ValidateOutput(actual_datum);

  // Output contiguous in a single chunk
  ASSERT_EQ(1, actual->num_chunks());
  ASSERT_TRUE(actual->Equals(*expected));
}

void CheckIsInDictionary(const std::shared_ptr<DataType>& type,
                         const std::shared_ptr<DataType>& index_type,
                         const std::string& input_dictionary_json,
                         const std::string& input_index_json,
                         const std::string& value_set_json,
                         const std::string& expected_json, bool skip_nulls) {
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

  // value_set cannot be casted to int8, but int8 is castable to float
  CheckIsIn(input, ArrayFromJSON(float32(), "[1.0, 2.5, 3.1, 5.0]"),
            "[false, true, false, false, false, true, false, false, false]");

  // Allow implicit casts between binary types...
  CheckIsIn(ArrayFromJSON(binary(), R"(["aaa", "bb", "ccc", null, "bbb"])"),
            ArrayFromJSON(fixed_size_binary(3), R"(["aaa", "bbb"])"),
            "[true, false, false, false, true]");
  CheckIsIn(ArrayFromJSON(fixed_size_binary(3), R"(["aaa", "bbb", "ccc", null, "bbb"])"),
            ArrayFromJSON(binary(), R"(["aa", "bbb"])"),
            "[false, true, false, false, true]");
  CheckIsIn(ArrayFromJSON(utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"),
            ArrayFromJSON(large_utf8(), R"(["aaa", "bbb"])"),
            "[true, true, false, false, true]");
  CheckIsIn(ArrayFromJSON(large_utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"),
            ArrayFromJSON(utf8(), R"(["aaa", "bbb"])"),
            "[true, true, false, false, true]");

  // But explicitly deny implicit casts from non-binary to utf8 to
  // avoid surprises
  ASSERT_RAISES(TypeError,
                IsIn(ArrayFromJSON(utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"),
                     SetLookupOptions(ArrayFromJSON(float64(), "[1.0, 2.0]"))));
  ASSERT_RAISES(TypeError, IsIn(ArrayFromJSON(float64(), "[1.0, 2.0]"),
                                SetLookupOptions(ArrayFromJSON(
                                    utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"))));

  ASSERT_RAISES(TypeError,
                IsIn(ArrayFromJSON(large_utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"),
                     SetLookupOptions(ArrayFromJSON(float64(), "[1.0, 2.0]"))));
  ASSERT_RAISES(TypeError,
                IsIn(ArrayFromJSON(float64(), "[1.0, 2.0]"),
                     SetLookupOptions(ArrayFromJSON(
                         large_utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"))));
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
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, 1]", "[false, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, 1]", "[false, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, 1]", "[null, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, 1]", "[null, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  // Nulls in right array
  CheckIsIn(type, "[0, 1, 2, 3, 2]", "[2, null, 1]", "[false, true, true, false, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, "[0, 1, 2, 3, 2]", "[2, null, 1]", "[false, true, true, false, true]",
            /*skip_nulls=*/true);
  CheckIsIn(type, "[0, 1, 2, 3, 2]", "[2, null, 1]", "[false, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, "[0, 1, 2, 3, 2]", "[2, null, 1]", "[false, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, "[0, 1, 2, 3, 2]", "[2, null, 1]", "[false, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, "[0, 1, 2, 3, 2]", "[2, null, 1]", "[null, true, true, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  // Nulls in both the arrays
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, null, 1]", "[true, true, true, false, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, null, 1]",
            "[false, true, true, false, true]", /*skip_nulls=*/true);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, null, 1]", "[true, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, null, 1]",
            "[false, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, null, 1]", "[null, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[2, null, 1]", "[null, true, true, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  // Duplicates in right array
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[null, 2, 2, null, 1, 1]",
            "[true, true, true, false, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[null, 2, 2, null, 1, 1]",
            "[false, true, true, false, true]", /*skip_nulls=*/true);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[null, 2, 2, null, 1, 1]",
            "[true, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[null, 2, 2, null, 1, 1]",
            "[false, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[null, 2, 2, null, 1, 1]",
            "[null, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, "[null, 1, 2, 3, 2]", "[null, 2, 2, null, 1, 1]",
            "[null, true, true, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  // Empty Arrays
  CheckIsIn(type, "[]", "[]", "[]");
}

TEST_F(TestIsInKernel, NullType) {
  auto type = null();

  CheckIsIn(type, "[null, null, null]", "[null]", "[true, true, true]");
  CheckIsIn(type, "[null, null, null]", "[]", "[false, false, false]");
  CheckIsIn(type, "[]", "[]", "[]");

  CheckIsIn(type, "[null, null]", "[null]", "[false, false]", /*skip_nulls=*/true);
  CheckIsIn(type, "[null, null]", "[null]", "[false, false]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, "[null, null]", "[null]", "[null, null]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, "[null, null]", "[null]", "[null, null]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  CheckIsIn(type, "[null, null]", "[]", "[false, false]", /*skip_nulls=*/true);
  CheckIsIn(type, "[null, null]", "[]", "[false, false]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, "[null, null]", "[]", "[null, null]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, "[null, null]", "[]", "[null, null]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  // Duplicates in right array
  CheckIsIn(type, "[null, null, null]", "[null, null]", "[true, true, true]");
  CheckIsIn(type, "[null, null]", "[null, null]", "[false, false]", /*skip_nulls=*/true);
  CheckIsIn(type, "[null, null]", "[null, null]", "[false, false]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, "[null, null]", "[null, null]", "[null, null]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, "[null, null]", "[null, null]", "[null, null]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);
}

TEST_F(TestIsInKernel, TimeTimestamp) {
  for (const auto& type :
       {time32(TimeUnit::SECOND), time64(TimeUnit::NANO), timestamp(TimeUnit::MICRO),
        timestamp(TimeUnit::NANO, "UTC")}) {
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, true, false, true, true]", /*skip_nulls=*/false);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, false, false, true, true]", /*skip_nulls=*/true);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, true, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, false, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, null, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, null, null, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

    // Duplicates in right array
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, true, false, true, true]", /*skip_nulls=*/false);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, false, false, true, true]", /*skip_nulls=*/true);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, true, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, false, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, null, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, null, null, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);
  }

  // Disallow mixing timezone-aware and timezone-naive values
  ASSERT_RAISES(TypeError, IsIn(ArrayFromJSON(timestamp(TimeUnit::SECOND), "[0, 1, 2]"),
                                SetLookupOptions(ArrayFromJSON(
                                    timestamp(TimeUnit::SECOND, "UTC"), "[0, 2]"))));
  ASSERT_RAISES(
      TypeError,
      IsIn(ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), "[0, 1, 2]"),
           SetLookupOptions(ArrayFromJSON(timestamp(TimeUnit::SECOND), "[0, 2]"))));
  // However, mixed timezones are allowed (underlying value is UTC)
  CheckIsIn(ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), "[0, 1, 2]"),
            ArrayFromJSON(timestamp(TimeUnit::SECOND, "America/New_York"), "[0, 2]"),
            "[true, false, true]");
}

TEST_F(TestIsInKernel, TimeDuration) {
  for (const auto& type : DurationTypes()) {
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, true, false, true, true]", /*skip_nulls=*/false);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, false, false, true, true]", /*skip_nulls=*/true);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, true, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, false, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, null, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, null]",
              "[true, null, null, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

    // Duplicates in right array
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, true, false, true, true]", /*skip_nulls=*/false);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, false, false, true, true]", /*skip_nulls=*/true);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, true, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, false, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, null, false, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
    CheckIsIn(type, "[1, null, 5, 1, 2]", "[2, 1, 1, null, 2]",
              "[true, null, null, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);
  }

  // Different units, cast value_set to values will fail, then cast values to value_set
  CheckIsIn(ArrayFromJSON(duration(TimeUnit::SECOND), "[0, 1, 2]"),
            ArrayFromJSON(duration(TimeUnit::MILLI), "[1, 2, 2000]"),
            "[false, false, true]");

  // Different units, cast value_set to values
  CheckIsIn(ArrayFromJSON(duration(TimeUnit::MILLI), "[0, 1, 2000]"),
            ArrayFromJSON(duration(TimeUnit::SECOND), "[0, 2]"), "[true, false, true]");
}

TEST_F(TestIsInKernel, Boolean) {
  auto type = boolean();

  CheckIsIn(type, "[true, false, null, true, false]", "[false]",
            "[false, true, false, false, true]", /*skip_nulls=*/false);
  CheckIsIn(type, "[true, false, null, true, false]", "[false]",
            "[false, true, false, false, true]", /*skip_nulls=*/true);
  CheckIsIn(type, "[true, false, null, true, false]", "[false]",
            "[false, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, "[true, false, null, true, false]", "[false]",
            "[false, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, "[true, false, null, true, false]", "[false]",
            "[false, true, null, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, "[true, false, null, true, false]", "[false]",
            "[false, true, null, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  CheckIsIn(type, "[true, false, null, true, false]", "[false, null]",
            "[false, true, true, false, true]", /*skip_nulls=*/false);
  CheckIsIn(type, "[true, false, null, true, false]", "[false, null]",
            "[false, true, false, false, true]", /*skip_nulls=*/true);
  CheckIsIn(type, "[true, false, null, true, false]", "[false, null]",
            "[false, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, "[true, false, null, true, false]", "[false, null]",
            "[false, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, "[true, false, null, true, false]", "[false, null]",
            "[false, true, null, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, "[true, false, null, true, false]", "[false, null]",
            "[null, true, null, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  // Duplicates in right array
  CheckIsIn(type, "[true, false, null, true, false]", "[null, false, false, null]",
            "[false, true, true, false, true]", /*skip_nulls=*/false);
  CheckIsIn(type, "[true, false, null, true, false]", "[null, false, false, null]",
            "[false, true, false, false, true]", /*skip_nulls=*/true);
  CheckIsIn(type, "[true, false, null, true, false]", "[null, false, false, null]",
            "[false, true, true, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, "[true, false, null, true, false]", "[null, false, false, null]",
            "[false, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, "[true, false, null, true, false]", "[null, false, false, null]",
            "[false, true, null, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, "[true, false, null, true, false]", "[null, false, false, null]",
            "[null, true, null, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);
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
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", ""])",
            "[true, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", ""])",
            "[true, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", ""])",
            "[true, true, false, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", ""])",
            "[true, true, false, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", "", null])",
            "[true, true, false, true, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", "", null])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/true);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", "", null])",
            "[true, true, false, true, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", "", null])",
            "[true, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", "", null])",
            "[true, true, false, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])", R"(["aaa", "", null])",
            "[true, true, null, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  // Duplicates in right array
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])",
            R"([null, "aaa", "aaa", "", "", null])", "[true, true, false, true, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])",
            R"([null, "aaa", "aaa", "", "", null])", "[true, true, false, false, true]",
            /*skip_nulls=*/true);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])",
            R"([null, "aaa", "aaa", "", "", null])", "[true, true, false, true, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])",
            R"([null, "aaa", "aaa", "", "", null])", "[true, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])",
            R"([null, "aaa", "aaa", "", "", null])", "[true, true, false, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, R"(["aaa", "", "cc", null, ""])",
            R"([null, "aaa", "aaa", "", "", null])", "[true, true, null, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);
}

TEST_F(TestIsInKernel, FixedSizeBinary) {
  auto type = fixed_size_binary(3);

  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb"])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb"])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/true);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb"])",
            "[true, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb"])",
            "[true, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb"])",
            "[true, true, false, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb"])",
            "[true, true, false, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb", null])",
            "[true, true, false, true, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb", null])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/true);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb", null])",
            "[true, true, false, true, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb", null])",
            "[true, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb", null])",
            "[true, true, false, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])", R"(["aaa", "bbb", null])",
            "[true, true, null, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  // Duplicates in right array
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])",
            R"(["aaa", null, "aaa", "bbb", "bbb", null])",
            "[true, true, false, true, true]",
            /*skip_nulls=*/false);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])",
            R"(["aaa", null, "aaa", "bbb", "bbb", null])",
            "[true, true, false, false, true]",
            /*skip_nulls=*/true);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])",
            R"(["aaa", null, "aaa", "bbb", "bbb", null])",
            "[true, true, false, true, true]",
            /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])",
            R"(["aaa", null, "aaa", "bbb", "bbb", null])",
            "[true, true, false, false, true]",
            /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])",
            R"(["aaa", null, "aaa", "bbb", "bbb", null])",
            "[true, true, false, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  CheckIsIn(type, R"(["aaa", "bbb", "ccc", null, "bbb"])",
            R"(["aaa", null, "aaa", "bbb", "bbb", null])",
            "[true, true, null, null, true]",
            /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  ASSERT_RAISES(Invalid,
                IsIn(ArrayFromJSON(fixed_size_binary(3), R"(["abc"])"),
                     SetLookupOptions(ArrayFromJSON(fixed_size_binary(2), R"(["ab"])"))));
}

TEST_F(TestIsInKernel, Decimal) {
  for (auto type : {decimal128(3, 1), decimal256(3, 1)}) {
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])", R"(["12.3", "78.9"])",
              "[true, false, true, false, true]",
              /*skip_nulls=*/false);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])", R"(["12.3", "78.9"])",
              "[true, false, true, false, true]",
              /*skip_nulls=*/true);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])", R"(["12.3", "78.9"])",
              "[true, false, true, false, true]",
              /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])", R"(["12.3", "78.9"])",
              "[true, false, true, false, true]",
              /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])", R"(["12.3", "78.9"])",
              "[true, false, true, null, true]",
              /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])", R"(["12.3", "78.9"])",
              "[true, false, true, null, true]",
              /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"(["12.3", "78.9", null])", "[true, false, true, true, true]",
              /*skip_nulls=*/false);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"(["12.3", "78.9", null])", "[true, false, true, false, true]",
              /*skip_nulls=*/true);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"(["12.3", "78.9", null])", "[true, false, true, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"(["12.3", "78.9", null])", "[true, false, true, false, true]",
              /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"(["12.3", "78.9", null])", "[true, false, true, null, true]",
              /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"(["12.3", "78.9", null])", "[true, null, true, null, true]",
              /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

    // Duplicates in right array
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"([null, "12.3", "12.3", "78.9", "78.9", null])",
              "[true, false, true, true, true]",
              /*skip_nulls=*/false);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"([null, "12.3", "12.3", "78.9", "78.9", null])",
              "[true, false, true, false, true]",
              /*skip_nulls=*/true);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"([null, "12.3", "12.3", "78.9", "78.9", null])",
              "[true, false, true, true, true]",
              /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"([null, "12.3", "12.3", "78.9", "78.9", null])",
              "[true, false, true, false, true]",
              /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"([null, "12.3", "12.3", "78.9", "78.9", null])",
              "[true, false, true, null, true]",
              /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
    CheckIsIn(type, R"(["12.3", "45.6", "78.9", null, "12.3"])",
              R"([null, "12.3", "12.3", "78.9", "78.9", null])",
              "[true, null, true, null, true]",
              /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

    CheckIsIn(ArrayFromJSON(decimal128(4, 2), R"(["12.30", "45.60", "78.90"])"),
              ArrayFromJSON(type, R"(["12.3", "78.9"])"), "[true, false, true]");
  }
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
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 2, null, 0]",
                        /*value_set_json=*/R"(["A", "B", "C"])",
                        /*expected_json=*/"[true, true, false, true]",
                        /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsInDictionary(/*type=*/float32(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/"[4.1, -1.0, 42, 9.8]",
                        /*input_index_json=*/"[1, 2, null, 0]",
                        /*value_set_json=*/"[4.1, 42, -1.0]",
                        /*expected_json=*/"[true, true, false, true]",
                        /*null_matching_behavior=*/SetLookupOptions::MATCH);

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
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[true, false, true, true, true]",
                        /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[true, false, true, true, true]",
                        /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A"])",
                        /*expected_json=*/"[false, false, false, true, false]",
                        /*null_matching_behavior=*/SetLookupOptions::MATCH);

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
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[true, false, false, true, true]",
                        /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[false, false, false, true, false]",
                        /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A"])",
                        /*expected_json=*/"[false, false, false, true, false]",
                        /*null_matching_behavior=*/SetLookupOptions::SKIP);

    // With nulls and null_matching_behavior=EMIT_NULL
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[true, false, null, true, true]",
                        /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[null, false, null, true, null]",
                        /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A"])",
                        /*expected_json=*/"[null, false, null, true, null]",
                        /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);

    // With nulls and null_matching_behavior=INCONCLUSIVE
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[true, null, null, true, true]",
                        /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A", null])",
                        /*expected_json=*/"[null, null, null, true, null]",
                        /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "B", "A"])",
                        /*expected_json=*/"[null, false, null, true, null]",
                        /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

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
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 2, null, 0]",
                        /*value_set_json=*/R"(["A", "A", "B", "A", "B", "C"])",
                        /*expected_json=*/"[true, true, false, true]",
                        /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "C", "B", "A", null, null, "B"])",
                        /*expected_json=*/"[true, false, true, true, true]",
                        /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "C", "B", "A", null, null, "B"])",
                        /*expected_json=*/"[true, false, false, true, true]",
                        /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "C", "B", "A", null, null, "B"])",
                        /*expected_json=*/"[true, false, null, true, true]",
                        /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
    CheckIsInDictionary(/*type=*/utf8(),
                        /*index_type=*/index_ty,
                        /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                        /*input_index_json=*/"[1, 3, null, 0, 1]",
                        /*value_set_json=*/R"(["C", "C", "B", "A", null, null, "B"])",
                        /*expected_json=*/"[true, null, null, true, true]",
                        /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);
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
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::SKIP);
  expected = ChunkedArrayFromJSON(
      boolean(), {"[true, true, true, true, false]", "[true, null, true, false]"});
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  expected = ChunkedArrayFromJSON(
      boolean(), {"[true, true, true, true, false]", "[true, null, true, false]"});
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  value_set = ChunkedArrayFromJSON(utf8(), {R"(["", "def"])", R"([null])"});
  expected = ChunkedArrayFromJSON(
      boolean(), {"[false, true, true, false, false]", "[true, true, false, false]"});
  CheckIsInChunked(input, value_set, expected, /*skip_nulls=*/false);
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::MATCH);
  expected = ChunkedArrayFromJSON(
      boolean(), {"[false, true, true, false, false]", "[true, false, false, false]"});
  CheckIsInChunked(input, value_set, expected, /*skip_nulls=*/true);
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::SKIP);
  expected = ChunkedArrayFromJSON(
      boolean(), {"[false, true, true, false, false]", "[true, null, false, false]"});
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  expected = ChunkedArrayFromJSON(
      boolean(), {"[null, true, true, null, null]", "[true, null, null, null]"});
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);

  // Duplicates in value_set
  value_set =
      ChunkedArrayFromJSON(utf8(), {R"(["", null, "", "def"])", R"(["def", null])"});
  expected = ChunkedArrayFromJSON(
      boolean(), {"[false, true, true, false, false]", "[true, true, false, false]"});
  CheckIsInChunked(input, value_set, expected, /*skip_nulls=*/false);
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::MATCH);
  expected = ChunkedArrayFromJSON(
      boolean(), {"[false, true, true, false, false]", "[true, false, false, false]"});
  CheckIsInChunked(input, value_set, expected, /*skip_nulls=*/true);
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::SKIP);
  expected = ChunkedArrayFromJSON(
      boolean(), {"[false, true, true, false, false]", "[true, null, false, false]"});
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::EMIT_NULL);
  expected = ChunkedArrayFromJSON(
      boolean(), {"[null, true, true, null, null]", "[true, null, null, null]"});
  CheckIsInChunked(input, value_set, expected,
                   /*null_matching_behavior=*/SetLookupOptions::INCONCLUSIVE);
}

// ----------------------------------------------------------------------
// IndexIn tests

class TestIndexInKernel : public ::testing::Test {
 public:
  void CheckIndexIn(const std::shared_ptr<Array>& input,
                    const std::shared_ptr<Array>& value_set,
                    const std::string& expected_json,
                    SetLookupOptions::NullMatchingBehavior null_matching_behavior =
                        SetLookupOptions::MATCH) {
    std::shared_ptr<Array> expected = ArrayFromJSON(int32(), expected_json);

    SetLookupOptions options(value_set, null_matching_behavior);
    ASSERT_OK_AND_ASSIGN(Datum actual_datum, IndexIn(input, options));
    std::shared_ptr<Array> actual = actual_datum.make_array();
    ValidateOutput(actual_datum);
    AssertArraysEqual(*expected, *actual, /*verbose=*/true);
  }

  void CheckIndexIn(const std::shared_ptr<DataType>& type, const std::string& input_json,
                    const std::string& value_set_json, const std::string& expected_json,
                    SetLookupOptions::NullMatchingBehavior null_matching_behavior =
                        SetLookupOptions::MATCH) {
    std::shared_ptr<Array> input = ArrayFromJSON(type, input_json);
    std::shared_ptr<Array> value_set = ArrayFromJSON(type, value_set_json);
    return CheckIndexIn(input, value_set, expected_json, null_matching_behavior);
  }

  void CheckIndexInChunked(const std::shared_ptr<ChunkedArray>& input,
                           const std::shared_ptr<ChunkedArray>& value_set,
                           const std::shared_ptr<ChunkedArray>& expected,
                           SetLookupOptions::NullMatchingBehavior null_matching_behavior =
                               SetLookupOptions::MATCH) {
    ASSERT_OK_AND_ASSIGN(
        Datum actual,
        IndexIn(input, SetLookupOptions(value_set, null_matching_behavior)));
    ASSERT_EQ(Datum::CHUNKED_ARRAY, actual.kind());
    ValidateOutput(actual);

    auto actual_chunked = actual.chunked_array();

    // Output contiguous in a single chunk
    ASSERT_EQ(1, actual_chunked->num_chunks());
    ASSERT_TRUE(actual_chunked->Equals(*expected));
  }

  void CheckIndexInDictionary(
      const std::shared_ptr<DataType>& type, const std::shared_ptr<DataType>& index_type,
      const std::string& input_dictionary_json, const std::string& input_index_json,
      const std::string& value_set_json, const std::string& expected_json,
      SetLookupOptions::NullMatchingBehavior null_matching_behavior =
          SetLookupOptions::MATCH) {
    auto dict_type = dictionary(index_type, type);
    auto indices = ArrayFromJSON(index_type, input_index_json);
    auto dict = ArrayFromJSON(type, input_dictionary_json);

    ASSERT_OK_AND_ASSIGN(auto input,
                         DictionaryArray::FromArrays(dict_type, indices, dict));
    auto value_set = ArrayFromJSON(type, value_set_json);
    auto expected = ArrayFromJSON(int32(), expected_json);

    SetLookupOptions options(value_set, null_matching_behavior);
    ASSERT_OK_AND_ASSIGN(Datum actual_datum, IndexIn(input, options));
    std::shared_ptr<Array> actual = actual_datum.make_array();
    ValidateOutput(actual_datum);
    AssertArraysEqual(*expected, *actual, /*verbose=*/true);
  }

  void CheckIndexIn(const std::shared_ptr<Array>& input,
                    const std::shared_ptr<Array>& value_set,
                    const std::string& expected_json, bool skip_nulls) {
    std::shared_ptr<Array> expected = ArrayFromJSON(int32(), expected_json);

    SetLookupOptions options(value_set, skip_nulls);
    ASSERT_OK_AND_ASSIGN(Datum actual_datum, IndexIn(input, options));
    std::shared_ptr<Array> actual = actual_datum.make_array();
    ValidateOutput(actual_datum);
    AssertArraysEqual(*expected, *actual, /*verbose=*/true);
  }

  void CheckIndexIn(const std::shared_ptr<DataType>& type, const std::string& input_json,
                    const std::string& value_set_json, const std::string& expected_json,
                    bool skip_nulls) {
    std::shared_ptr<Array> input = ArrayFromJSON(type, input_json);
    std::shared_ptr<Array> value_set = ArrayFromJSON(type, value_set_json);
    return CheckIndexIn(input, value_set, expected_json, skip_nulls);
  }

  void CheckIndexInChunked(const std::shared_ptr<ChunkedArray>& input,
                           const std::shared_ptr<ChunkedArray>& value_set,
                           const std::shared_ptr<ChunkedArray>& expected,
                           bool skip_nulls) {
    ASSERT_OK_AND_ASSIGN(Datum actual,
                         IndexIn(input, SetLookupOptions(value_set, skip_nulls)));
    ASSERT_EQ(Datum::CHUNKED_ARRAY, actual.kind());
    ValidateOutput(actual);

    auto actual_chunked = actual.chunked_array();

    // Output contiguous in a single chunk
    ASSERT_EQ(1, actual_chunked->num_chunks());
    ASSERT_TRUE(actual_chunked->Equals(*expected));
  }

  void CheckIndexInDictionary(const std::shared_ptr<DataType>& type,
                              const std::shared_ptr<DataType>& index_type,
                              const std::string& input_dictionary_json,
                              const std::string& input_index_json,
                              const std::string& value_set_json,
                              const std::string& expected_json, bool skip_nulls) {
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
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 3]",
                     /*expected=*/"[null, 0, null, 1, null]",
                     /*null_matching_behavior=*/SetLookupOptions::MATCH);
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 3]",
                     /*expected=*/"[null, 0, null, 1, null]",
                     /*null_matching_behavior=*/SetLookupOptions::SKIP);
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
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 1, 3, 3]",
                     /*expected=*/"[null, 0, null, 2, null]",
                     /*null_matching_behavior=*/SetLookupOptions::MATCH);
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 1, 3, 3]",
                     /*expected=*/"[null, 0, null, 2, null]",
                     /*null_matching_behavior=*/SetLookupOptions::SKIP);

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
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, null, 3]",
                     /*expected=*/"[null, 0, null, 2, 1]",
                     /*null_matching_behavior=*/SetLookupOptions::MATCH);
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 1, null, null, 3, 3]",
                     /*expected=*/"[null, 0, null, 4, null]",
                     /*null_matching_behavior=*/SetLookupOptions::SKIP);
  // Same with duplicates in value_set
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 1, null, null, 3, 3]",
                     /*expected=*/"[null, 0, null, 4, 2]",
                     /*skip_nulls=*/false);
  this->CheckIndexIn(type,
                     /*input=*/"[0, 1, 2, 3, null]",
                     /*value_set=*/"[1, 1, null, null, 3, 3]",
                     /*expected=*/"[null, 0, null, 4, 2]",
                     /*null_matching_behavior=*/SetLookupOptions::MATCH);
}

TEST_F(TestIndexInKernel, NullType) {
  CheckIndexIn(null(), "[null, null, null]", "[null]", "[0, 0, 0]");
  CheckIndexIn(null(), "[null, null, null]", "[]", "[null, null, null]");
  CheckIndexIn(null(), "[]", "[null, null]", "[]");
  CheckIndexIn(null(), "[]", "[]", "[]");

  CheckIndexIn(null(), "[null, null]", "[null]", "[null, null]", /*skip_nulls=*/true);
  CheckIndexIn(null(), "[null, null]", "[]", "[null, null]", /*skip_nulls=*/true);
  CheckIndexIn(null(), "[null, null]", "[null]", "[null, null]",
               /*null_matching_behavior=*/SetLookupOptions::SKIP);
  CheckIndexIn(null(), "[null, null]", "[]", "[null, null]",
               /*null_matching_behavior=*/SetLookupOptions::SKIP);
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

  CheckIndexIn(timestamp(TimeUnit::SECOND, "UTC"), "[2, null, 2, 1]", "[2, null, 1]",
               "[0, 1, 0, 2]");

  // Empty input array
  CheckIndexIn(timestamp(TimeUnit::NANO), "[]", "[2, null, 1]", "[]");

  // Empty value_set array
  CheckIndexIn(timestamp(TimeUnit::NANO), "[2, null, 1]", "[]", "[null, null, null]");

  // Both array are all null
  CheckIndexIn(time32(TimeUnit::SECOND), "[null, null, null, null]", "[null]",
               "[0, 0, 0, 0]");

  // Disallow mixing timezone-aware and timezone-naive values
  ASSERT_RAISES(TypeError,
                IndexIn(ArrayFromJSON(timestamp(TimeUnit::SECOND), "[0, 1, 2]"),
                        SetLookupOptions(ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"),
                                                       "[0, 2]"))));
  ASSERT_RAISES(
      TypeError,
      IndexIn(ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), "[0, 1, 2]"),
              SetLookupOptions(ArrayFromJSON(timestamp(TimeUnit::SECOND), "[0, 2]"))));
  // However, mixed timezones are allowed (underlying value is UTC)
  CheckIndexIn(ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), "[0, 1, 2]"),
               ArrayFromJSON(timestamp(TimeUnit::SECOND, "America/New_York"), "[0, 2]"),
               "[0, null, 1]");
}

TEST_F(TestIndexInKernel, TimeDuration) {
  CheckIndexIn(duration(TimeUnit::SECOND),
               /* input= */ "[1, null, 5, 1, 2]",
               /* value_set= */ "[2, 1, null]",
               /* expected= */ "[1, 2, null, 1, 0]");

  // Duplicates in value_set
  CheckIndexIn(duration(TimeUnit::SECOND),
               /* input= */ "[1, null, 5, 1, 2]",
               /* value_set= */ "[2, 2, 1, 1, null, null]",
               /* expected= */ "[2, 4, null, 2, 0]");

  // Needles array has no nulls
  CheckIndexIn(duration(TimeUnit::SECOND),
               /* input= */ "[2, null, 5, 1]",
               /* value_set= */ "[2, 1]",
               /* expected= */ "[0, null, null, 1]");

  // No match
  CheckIndexIn(duration(TimeUnit::SECOND), "[3, null, 5, 3]", "[2, 1]",
               "[null, null, null, null]");

  // Empty arrays
  CheckIndexIn(duration(TimeUnit::SECOND), "[]", "[]", "[]");

  CheckIndexIn(duration(TimeUnit::NANO), "[2, null, 2, 1]", "[2, null, 1]",
               "[0, 1, 0, 2]");

  CheckIndexIn(duration(TimeUnit::NANO), "[2, null, 2, 1]", "[2, null, 1]",
               "[0, 1, 0, 2]");

  // Empty input array
  CheckIndexIn(duration(TimeUnit::NANO), "[]", "[2, null, 1]", "[]");

  // Empty value_set array
  CheckIndexIn(duration(TimeUnit::NANO), "[2, null, 1]", "[]", "[null, null, null]");

  // Both array are all null
  CheckIndexIn(duration(TimeUnit::SECOND), "[null, null, null, null]", "[null]",
               "[0, 0, 0, 0]");

  // Different units, cast value_set to values will fail, then cast values to value_set
  CheckIndexIn(ArrayFromJSON(duration(TimeUnit::SECOND), "[0, 1, 2]"),
               ArrayFromJSON(duration(TimeUnit::MILLI), "[1, 2, 2000]"),
               "[null, null, 2]");

  // Different units, cast value_set to values
  CheckIndexIn(ArrayFromJSON(duration(TimeUnit::MILLI), "[0, 1, 2000]"),
               ArrayFromJSON(duration(TimeUnit::SECOND), "[0, 2]"), "[0, null, 1]");
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

TEST_F(TestIndexInKernel, ImplicitlyCastValueSet) {
  auto input = ArrayFromJSON(int8(), "[0, 1, 2, 3, 4, 5, 6, 7, 8]");

  SetLookupOptions opts{ArrayFromJSON(int32(), "[2, 3, 5, 7]")};
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction("index_in", {input}, &opts));

  auto expected = ArrayFromJSON(int32(), ("[null, null, 0, 1, null,"
                                          "2, null, 3, null]"));
  AssertArraysEqual(*expected, *out.make_array());

  // Although value_set cannot be cast to int8, but int8 is castable to float
  CheckIndexIn(input, ArrayFromJSON(float32(), "[1.0, 2.5, 3.1, 5.0]"),
               "[null, 0, null, null, null, 3, null, null, null]");

  // Allow implicit casts between binary types...
  CheckIndexIn(ArrayFromJSON(binary(), R"(["aaa", "bb", "ccc", null, "bbb"])"),
               ArrayFromJSON(fixed_size_binary(3), R"(["aaa", "bbb"])"),
               "[0, null, null, null, 1]");
  CheckIndexIn(
      ArrayFromJSON(fixed_size_binary(3), R"(["aaa", "bbb", "ccc", null, "bbb"])"),
      ArrayFromJSON(binary(), R"(["aa", "bbb"])"), "[null, 1, null, null, 1]");
  CheckIndexIn(ArrayFromJSON(utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"),
               ArrayFromJSON(large_utf8(), R"(["aaa", "bbb"])"), "[0, 1, null, null, 1]");
  CheckIndexIn(ArrayFromJSON(large_utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"),
               ArrayFromJSON(utf8(), R"(["aaa", "bbb"])"), "[0, 1, null, null, 1]");
  // But explicitly deny implicit casts from non-binary to utf8 to
  // avoid surprises
  ASSERT_RAISES(TypeError,
                IndexIn(ArrayFromJSON(utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"),
                        SetLookupOptions(ArrayFromJSON(float64(), "[1.0, 2.0]"))));
  ASSERT_RAISES(TypeError,
                IndexIn(ArrayFromJSON(float64(), "[1.0, 2.0]"),
                        SetLookupOptions(ArrayFromJSON(
                            utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"))));

  ASSERT_RAISES(
      TypeError,
      IndexIn(ArrayFromJSON(large_utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"),
              SetLookupOptions(ArrayFromJSON(float64(), "[1.0, 2.0]"))));
  ASSERT_RAISES(TypeError,
                IndexIn(ArrayFromJSON(float64(), "[1.0, 2.0]"),
                        SetLookupOptions(ArrayFromJSON(
                            large_utf8(), R"(["aaa", "bbb", "ccc", null, "bbb"])"))));
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

    input_builder.UnsafeAppend(std::string_view(buf));
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
               /*value_set=*/R"(["aaa", null, "bbb", "ccc"])",
               /*expected=*/R"([2, null, null, 0, 3, 0])",
               /*null_matching_behavior=*/SetLookupOptions::SKIP);

  CheckIndexIn(fixed_size_binary(3),
               /*input=*/R"(["bbb", null, "ddd", "aaa", "ccc", "aaa"])",
               /*value_set=*/R"(["aaa", "bbb", "ccc"])",
               /*expected=*/R"([1, null, null, 0, 2, 0])");
  CheckIndexIn(fixed_size_binary(3),
               /*input=*/R"(["bbb", null, "ddd", "aaa", "ccc", "aaa"])",
               /*value_set=*/R"(["aaa", "bbb", "ccc"])",
               /*expected=*/R"([1, null, null, 0, 2, 0])",
               /*skip_nulls=*/true);
  CheckIndexIn(fixed_size_binary(3),
               /*input=*/R"(["bbb", null, "ddd", "aaa", "ccc", "aaa"])",
               /*value_set=*/R"(["aaa", "bbb", "ccc"])",
               /*expected=*/R"([1, null, null, 0, 2, 0])",
               /*null_matching_behavior=*/SetLookupOptions::SKIP);

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
  CheckIndexIn(fixed_size_binary(3),
               /*input=*/R"(["bbb", null, "ddd", "aaa", "ccc", "aaa"])",
               /*value_set=*/R"(["aaa", "aaa", null, null, "bbb", "bbb", "ccc"])",
               /*expected=*/R"([4, null, null, 0, 6, 0])",
               /*null_matching_behavior=*/SetLookupOptions::SKIP);

  // Empty input array
  CheckIndexIn(fixed_size_binary(5), R"([])", R"(["bbbbb", null, "aaaaa", "ccccc"])",
               R"([])");

  // Empty value_set array
  CheckIndexIn(fixed_size_binary(5), R"(["bbbbb", null, "bbbbb"])", R"([])",
               R"([null, null, null])");

  // Empty arrays
  CheckIndexIn(fixed_size_binary(0), R"([])", R"([])", R"([])");

  ASSERT_RAISES(
      Invalid,
      IndexIn(ArrayFromJSON(fixed_size_binary(3), R"(["abc"])"),
              SetLookupOptions(ArrayFromJSON(fixed_size_binary(2), R"(["ab"])"))));
}

TEST_F(TestIndexInKernel, MonthDayNanoInterval) {
  auto type = month_day_nano_interval();

  CheckIndexIn(type,
               /*input=*/R"([[5, -1, 5], null, [4, 5, 6], [5, -1, 5], [1, 2, 3]])",
               /*value_set=*/R"([null, [4, 5, 6], [5, -1, 5]])",
               /*expected=*/R"([2, 0, 1, 2, null])",
               /*skip_nulls=*/false);
  CheckIndexIn(type,
               /*input=*/R"([[5, -1, 5], null, [4, 5, 6], [5, -1, 5], [1, 2, 3]])",
               /*value_set=*/R"([null, [4, 5, 6], [5, -1, 5]])",
               /*expected=*/R"([2, 0, 1, 2, null])",
               /*null_matching_behavior=*/SetLookupOptions::MATCH);

  // Duplicates in value_set
  CheckIndexIn(
      type,
      /*input=*/R"([[7, 8, 0], null, [0, 0, 0], [7, 8, 0], [0, 0, 1]])",
      /*value_set=*/R"([null, null, [0, 0, 0], [0, 0, 0], [7, 8, 0], [7, 8, 0]])",
      /*expected=*/R"([4, 0, 2, 4, null])",
      /*skip_nulls=*/false);
  CheckIndexIn(
      type,
      /*input=*/R"([[7, 8, 0], null, [0, 0, 0], [7, 8, 0], [0, 0, 1]])",
      /*value_set=*/R"([null, null, [0, 0, 0], [0, 0, 0], [7, 8, 0], [7, 8, 0]])",
      /*expected=*/R"([4, 0, 2, 4, null])",
      /*null_matching_behavior=*/SetLookupOptions::MATCH);
}

TEST_F(TestIndexInKernel, Decimal) {
  for (const auto& type : {decimal128(2, 0), decimal256(2, 0)}) {
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
                 /*value_set=*/R"([null, "11", "12"])",
                 /*expected=*/R"([2, 0, 1, 2, null])",
                 /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIndexIn(type,
                 /*input=*/R"(["12", null, "11", "12", "13"])",
                 /*value_set=*/R"([null, "11", "12"])",
                 /*expected=*/R"([2, null, 1, 2, null])",
                 /*null_matching_behavior=*/SetLookupOptions::SKIP);

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
    CheckIndexIn(type,
                 /*input=*/R"(["12", null, "11", "12", "13"])",
                 /*value_set=*/R"(["11", "12"])",
                 /*expected=*/R"([1, null, 0, 1, null])",
                 /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIndexIn(type,
                 /*input=*/R"(["12", null, "11", "12", "13"])",
                 /*value_set=*/R"(["11", "12"])",
                 /*expected=*/R"([1, null, 0, 1, null])",
                 /*null_matching_behavior=*/SetLookupOptions::SKIP);

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
    CheckIndexIn(type,
                 /*input=*/R"(["12", null, "11", "12", "13"])",
                 /*value_set=*/R"([null, "11", "12"])",
                 /*expected=*/R"([2, 0, 1, 2, null])",
                 /*skip_nulls=*/false);
    CheckIndexIn(type,
                 /*input=*/R"(["12", null, "11", "12", "13"])",
                 /*value_set=*/R"([null, null, "11", "11", "12", "12"])",
                 /*expected=*/R"([4, 0, 2, 4, null])",
                 /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIndexIn(type,
                 /*input=*/R"(["12", null, "11", "12", "13"])",
                 /*value_set=*/R"([null, null, "11", "11", "12", "12"])",
                 /*expected=*/R"([4, null, 2, 4, null])",
                 /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIndexIn(type,
                 /*input=*/R"(["12", null, "11", "12", "13"])",
                 /*value_set=*/R"([null, "11", "12"])",
                 /*expected=*/R"([2, 0, 1, 2, null])",
                 /*null_matching_behavior=*/SetLookupOptions::MATCH);

    CheckIndexIn(
        ArrayFromJSON(decimal256(3, 1), R"(["12.0", null, "11.0", "12.0", "13.0"])"),
        ArrayFromJSON(type, R"([null, "11", "12"])"), R"([2, 0, 1, 2, null])");
  }
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
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                           /*input_index_json=*/"[1, 2, null, 0]",
                           /*value_set_json=*/R"(["A", "B", "C"])",
                           /*expected_json=*/"[1, 2, null, 0]",
                           /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIndexInDictionary(/*type=*/float32(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/"[4.1, -1.0, 42, 9.8]",
                           /*input_index_json=*/"[1, 2, null, 0]",
                           /*value_set_json=*/"[4.1, 42, -1.0]",
                           /*expected_json=*/"[2, 1, null, 0]",
                           /*null_matching_behavior=*/SetLookupOptions::MATCH);

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
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A", null])",
                           /*expected_json=*/"[1, null, 3, 2, 1]",
                           /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A", null])",
                           /*expected_json=*/"[3, null, 3, 2, 3]",
                           /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A"])",
                           /*expected_json=*/"[null, null, null, 2, null]",
                           /*null_matching_behavior=*/SetLookupOptions::MATCH);

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
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A", null])",
                           /*expected_json=*/"[1, null, null, 2, 1]",
                           /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A", null])",
                           /*expected_json=*/"[null, null, null, 2, null]",
                           /*null_matching_behavior=*/SetLookupOptions::SKIP);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "B", "A"])",
                           /*expected_json=*/"[null, null, null, 2, null]",
                           /*null_matching_behavior=*/SetLookupOptions::SKIP);

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
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", "B", "C", "D"])",
                           /*input_index_json=*/"[1, 2, null, 0]",
                           /*value_set_json=*/R"(["A", "A", "B", "B", "C", "C"])",
                           /*expected_json=*/"[2, 4, null, 0]",
                           /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "C", "B", "B", "A", "A", null])",
                           /*expected_json=*/"[6, null, 6, 4, 6]",
                           /*null_matching_behavior=*/SetLookupOptions::MATCH);
    CheckIndexInDictionary(/*type=*/utf8(),
                           /*index_type=*/index_ty,
                           /*input_dictionary_json=*/R"(["A", null, "C", "D"])",
                           /*input_index_json=*/"[1, 3, null, 0, 1]",
                           /*value_set_json=*/R"(["C", "C", "B", "B", "A", "A", null])",
                           /*expected_json=*/"[null, null, null, 4, null]",
                           /*null_matching_behavior=*/SetLookupOptions::SKIP);
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
  CheckIndexInChunked(input, value_set, expected,
                      /*null_matching_behavior=*/SetLookupOptions::MATCH);
  CheckIndexInChunked(input, value_set, expected,
                      /*null_matching_behavior=*/SetLookupOptions::SKIP);

  // Null in value_set
  value_set = ChunkedArrayFromJSON(utf8(), {R"(["ghi", "def"])", R"([null, "abc"])"});
  expected = ChunkedArrayFromJSON(int32(), {"[3, 1, 0, 3, null]", "[1, 2, 3, null]"});
  CheckIndexInChunked(input, value_set, expected, /*skip_nulls=*/false);
  CheckIndexInChunked(input, value_set, expected,
                      /*null_matching_behavior=*/SetLookupOptions::MATCH);
  expected = ChunkedArrayFromJSON(int32(), {"[3, 1, 0, 3, null]", "[1, null, 3, null]"});
  CheckIndexInChunked(input, value_set, expected, /*skip_nulls=*/true);
  CheckIndexInChunked(input, value_set, expected,
                      /*null_matching_behavior=*/SetLookupOptions::SKIP);

  // Duplicates in value_set
  value_set = ChunkedArrayFromJSON(
      utf8(), {R"(["ghi", "ghi", "def"])", R"(["def", null, null, "abc"])"});
  expected = ChunkedArrayFromJSON(int32(), {"[6, 2, 0, 6, null]", "[2, 4, 6, null]"});
  CheckIndexInChunked(input, value_set, expected, /*skip_nulls=*/false);
  CheckIndexInChunked(input, value_set, expected,
                      /*null_matching_behavior=*/SetLookupOptions::MATCH);
  expected = ChunkedArrayFromJSON(int32(), {"[6, 2, 0, 6, null]", "[2, null, 6, null]"});
  CheckIndexInChunked(input, value_set, expected, /*skip_nulls=*/true);
  CheckIndexInChunked(input, value_set, expected,
                      /*null_matching_behavior=*/SetLookupOptions::SKIP);
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
