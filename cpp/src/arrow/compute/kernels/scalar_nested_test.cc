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

#include "arrow/chunked_array.h"
#include "arrow/compute/api.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/test_util_internal.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace compute {

static std::shared_ptr<DataType> GetOffsetType(const DataType& type) {
  switch (type.id()) {
    case Type::LIST:
    case Type::LIST_VIEW:
      return int32();
    case Type::LARGE_LIST:
    case Type::LARGE_LIST_VIEW:
      return int64();
    default:
      Unreachable("Unexpected type");
  }
}

TEST(TestScalarNested, ListValueLength) {
  for (auto ty : {list(int32()), large_list(int32()), list_view(int32()),
                  large_list_view(int32())}) {
    CheckScalarUnary("list_value_length", ty, "[[0, null, 1], null, [2, 3], []]",
                     GetOffsetType(*ty), "[3, null, 2, 0]");
  }

  CheckScalarUnary("list_value_length", fixed_size_list(int32(), 3),
                   "[[0, null, 1], null, [2, 3, 4], [1, 2, null]]", int32(),
                   "[3, null, 3, 3]");
}

TEST(TestScalarNested, ListElementNonFixedListWithNulls) {
  auto sample = "[[7, 5, 81], [6, null, 4, 7, 8], [3, 12, 2, 0], [1, 9], null]";
  for (auto ty : NumericTypes()) {
    for (auto list_type :
         {list(ty), large_list(ty), list_view(ty), large_list_view(ty)}) {
      auto input = ArrayFromJSON(list_type, sample);
      auto null_input = ArrayFromJSON(list_type, "[null]");
      for (auto index_type : IntTypes()) {
        auto index = ScalarFromJSON(index_type, "1");
        auto expected = ArrayFromJSON(ty, "[5, null, 12, 9, null]");
        auto expected_null = ArrayFromJSON(ty, "[null]");
        CheckScalar("list_element", {input, index}, expected);
        CheckScalar("list_element", {null_input, index}, expected_null);
      }
    }
  }
}

TEST(TestScalarNested, ListElementFixedList) {
  auto sample = "[[7, 5, 81], [6, 4, 8], [3, 12, 2], [1, 43, 87]]";
  for (auto ty : NumericTypes()) {
    auto input = ArrayFromJSON(fixed_size_list(ty, 3), sample);
    for (auto index_type : IntTypes()) {
      auto index = ScalarFromJSON(index_type, "0");
      auto expected = ArrayFromJSON(ty, "[7, 6, 3, 1]");
      CheckScalar("list_element", {input, index}, expected);
      index = ScalarFromJSON(index_type, "1");
      expected = ArrayFromJSON(ty, "[5, 4, 12, 43]");
      CheckScalar("list_element", {input, index}, expected);
      index = ScalarFromJSON(index_type, "2");
      expected = ArrayFromJSON(ty, "[81, 8, 2, 87]");
      CheckScalar("list_element", {input, index}, expected);
      index = ScalarFromJSON(index_type, "3");
      EXPECT_THAT(CallFunction("list_element", {input, index}),
                  Raises(StatusCode::Invalid));
    }
  }
}

TEST(TestScalarNested, ListElementInvalid) {
  auto input_array = ArrayFromJSON(list(float32()), "[[0.1, 1.1], [0.2, 1.2]]");
  auto input_scalar = ScalarFromJSON(list(float32()), "[0.1, 0.2]");

  // invalid index: null
  auto index = ScalarFromJSON(int32(), "null");
  EXPECT_THAT(CallFunction("list_element", {input_array, index}),
              Raises(StatusCode::Invalid));
  EXPECT_THAT(CallFunction("list_element", {input_scalar, index}),
              Raises(StatusCode::Invalid));

  // invalid index: < 0
  index = ScalarFromJSON(int32(), "-1");
  EXPECT_THAT(CallFunction("list_element", {input_array, index}),
              Raises(StatusCode::Invalid));
  EXPECT_THAT(CallFunction("list_element", {input_scalar, index}),
              Raises(StatusCode::Invalid));

  // invalid index: >= list.length
  index = ScalarFromJSON(int32(), "2");
  EXPECT_THAT(CallFunction("list_element", {input_array, index}),
              Raises(StatusCode::Invalid));
  EXPECT_THAT(CallFunction("list_element", {input_scalar, index}),
              Raises(StatusCode::Invalid));

  // invalid input
  input_array = ArrayFromJSON(list(float32()), "[[41, 6, 93], [], [2]]");
  input_scalar = ScalarFromJSON(list(float32()), "[]");
  index = ScalarFromJSON(int32(), "0");
  EXPECT_THAT(CallFunction("list_element", {input_array, index}),
              Raises(StatusCode::Invalid));
  EXPECT_THAT(CallFunction("list_element", {input_scalar, index}),
              Raises(StatusCode::Invalid));
}

void CheckListContains(Datum lists, Datum value, const std::string& expected) {
  CheckScalar("list_contains", {std::move(lists), std::move(value)},
              ArrayFromJSON(boolean(), expected));
}

TEST(TestScalarNested, ListContains) {
  auto sample = "[[7, 5, 81], [6, null, 4, 7, 8], [], [5], null, [null]]";
  for (auto ty : NumericTypes()) {
    for (auto list_type :
         {list(ty), large_list(ty), list_view(ty), large_list_view(ty)}) {
      auto input = ArrayFromJSON(list_type, sample);
      CheckListContains(input, ScalarFromJSON(ty, "5"),
                        "[true, false, false, true, null, false]");
      CheckListContains(input, ScalarFromJSON(ty, "7"),
                        "[true, true, false, false, null, false]");
      CheckListContains(input, ScalarFromJSON(ty, "null"),
                        "[false, true, false, false, null, true]");
      CheckListContains(ArrayFromJSON(list_type, "[]"), ScalarFromJSON(ty, "5"), "[]");
    }
  }

  auto input =
      ArrayFromJSON(list(utf8()), R"([["a", "b"], ["a", "c"], ["b", "c", "d"]])");
  CheckListContains(input, ScalarFromJSON(utf8(), R"("a")"), "[true, true, false]");
  CheckListContains(input->Slice(1), ScalarFromJSON(utf8(), R"("b")"), "[false, true]");

  // All-null lists contain no non-null value
  CheckListContains(ArrayFromJSON(list(int64()), "[[null, null]]"),
                    ScalarFromJSON(int64(), "1"), "[false]");
}

TEST(TestScalarNested, ListContainsLongLists) {
  // Matches and nulls beyond the first bitmap words, at unaligned offsets
  std::string values = "[";
  for (int i = 0; i < 300; ++i) {
    values += i == 0 ? "" : ", ";
    values += i == 100 ? "null" : i == 290 ? "7" : "1";
  }
  values += "]";
  ASSERT_OK_AND_ASSIGN(auto input,
                       ListArray::FromArrays(*ArrayFromJSON(int32(), "[0, 3, 150, 300]"),
                                             *ArrayFromJSON(int32(), values)));
  CheckListContains(input, ScalarFromJSON(int32(), "7"), "[false, false, true]");
  CheckListContains(input, ScalarFromJSON(int32(), "null"), "[false, true, false]");
  CheckListContains(input, ArrayFromJSON(int32(), "[7, 7, null]"),
                    "[false, false, false]");
  CheckListContains(input, ArrayFromJSON(int32(), "[1, null, 7]"), "[true, true, true]");
  // The null value's slot holds 0, which must not match
  CheckListContains(input, ScalarFromJSON(int32(), "0"), "[false, false, false]");
}

TEST(TestScalarNested, ListContainsManyShortLists) {
  // Short lists at every bit alignment, with null lists and null values
  std::string lists = "[";
  std::string contains_5 = "[";
  std::string contains_null = "[";
  std::string contains_0 = "[";
  for (int i = 0; i < 200; ++i) {
    const std::string sep = i == 0 ? "" : ", ";
    const bool is_null = i % 11 == 0;
    lists += sep + (is_null      ? "null"
                    : i % 7 == 0 ? "[5, 1]"
                    : i % 3 == 0 ? "[null, 2, 3]"
                                 : "[1]");
    contains_5 += sep + (is_null ? "null" : i % 7 == 0 ? "true" : "false");
    contains_null += sep + (is_null                    ? "null"
                            : i % 7 != 0 && i % 3 == 0 ? "true"
                                                       : "false");
    contains_0 += sep + (is_null ? "null" : "false");
  }
  auto input = ArrayFromJSON(list(int32()), lists + "]");
  CheckListContains(input, ScalarFromJSON(int32(), "5"), contains_5 + "]");
  CheckListContains(input, ScalarFromJSON(int32(), "null"), contains_null + "]");
  // Null values' slots hold 0, which must not match
  CheckListContains(input, ScalarFromJSON(int32(), "0"), contains_0 + "]");
}

TEST(TestScalarNested, ListContainsNull) {
  // A null value matches lists holding a null
  auto input = ArrayFromJSON(
      list(int32()), "[[1, null], [null], [1, 2], [1, 1], [2, 1, null, 1], [], null]");
  for (auto value : {ScalarFromJSON(int32(), "null"), MakeNullScalar(null()),
                     ScalarFromJSON(int64(), "null")}) {
    CheckListContains(input, value, "[true, true, false, false, true, false, null]");
  }
  CheckListContains(ArrayFromJSON(list(int32()), "[[]]"), ScalarFromJSON(int32(), "null"),
                    "[false]");

  input = ArrayFromJSON(list(utf8()), R"([["x", null], [null], ["x", "y"], [], null])");
  CheckListContains(input, ScalarFromJSON(utf8(), "null"),
                    "[true, true, false, false, null]");

  input = ArrayFromJSON(list(list(int32())),
                        "[[[1], null], [null], [[1], [null]], [], null]");
  CheckListContains(input, MakeNullScalar(list(int32())),
                    "[true, true, false, false, null]");

  input = ArrayFromJSON(list(null()), "[[null], [], null]");
  CheckListContains(input, MakeNullScalar(null()), "[true, false, null]");

  // Dictionary values are also null where their dictionary is
  ASSERT_OK_AND_ASSIGN(
      input, ListArray::FromArrays(
                 *ArrayFromJSON(int32(), "[0, 1, 2]"),
                 *DictArrayFromJSON(dictionary(int8(), int64()), "[0, 1]", "[null, 1]")));
  for (auto value : {ScalarFromJSON(int64(), "null"), MakeNullScalar(null())}) {
    CheckListContains(input, value, "[true, false]");
  }
  CheckListContains(input, ArrayFromJSON(int64(), "[null, null]"), "[true, false]");
  ASSERT_OK_AND_ASSIGN(auto list_scalar, input->GetScalar(0));
  CheckListContains(list_scalar, ArrayFromJSON(int64(), "[null, 1]"), "[true, false]");
  // Also of a null-typed dictionary, both of values and of queries
  auto null_dict = DictArrayFromJSON(dictionary(int8(), null()), "[0, 0]", "[null]");
  ASSERT_OK_AND_ASSIGN(
      input, ListArray::FromArrays(*ArrayFromJSON(int32(), "[0, 2, 2]"), *null_dict));
  CheckListContains(input, MakeNullScalar(null()), "[true, false]");
  ASSERT_OK_AND_ASSIGN(list_scalar, input->GetScalar(0));
  CheckListContains(list_scalar, ArrayFromJSON(int64(), "[null, null]"), "[true, true]");
  CheckListContains(ArrayFromJSON(list(struct_({field("a", int64())})), "[[null], []]"),
                    null_dict, "[true, false]");
}

TEST(TestScalarNested, ListContainsNaN) {
  // Unlike "equal", a NaN value matches NaN list values
  for (auto ty : {float32(), float64()}) {
    auto input = ArrayFromJSON(list(ty), "[[1.5, null], [NaN], [1.5, NaN], [], null]");
    for (auto value_ty : {float32(), float64()}) {
      CheckListContains(input, ScalarFromJSON(value_ty, "NaN"),
                        "[false, true, true, false, null]");
      CheckListContains(input, ScalarFromJSON(value_ty, "1.5"),
                        "[true, false, true, false, null]");
    }
  }
  CheckListContains(ArrayFromJSON(list(int64()), "[[1], []]"),
                    ScalarFromJSON(float64(), "NaN"), "[false, false]");

  auto input =
      ArrayFromJSON(list(float16()), "[[1.5, null], [NaN], [1.5, NaN], [], null]");
  CheckListContains(input, ScalarFromJSON(float16(), "NaN"),
                    "[false, true, true, false, null]");
  CheckListContains(ArrayFromJSON(list(float32()), "[[1.5], [NaN]]"),
                    ScalarFromJSON(float16(), "NaN"), "[false, true]");
  // Null queries compare float16 as float32 too
  input = ArrayFromJSON(list(float16()), "[[1], [null]]");
  CheckListContains(input, ArrayFromJSON(float32(), "[null, null]"), "[false, true]");
  CheckListContains(input, ScalarFromJSON(float32(), "null"), "[false, true]");
  CheckListContains(ArrayFromJSON(list(float32()), "[[1], [null]]"),
                    ArrayFromJSON(float16(), "[null, null]"), "[false, true]");
  CheckListContains(ScalarFromJSON(list(float16()), "[1, null]"),
                    ArrayFromJSON(float32(), "[null, null]"), "[true, true]");
  // Also in dictionaries, both of values and of queries
  auto f16_dict =
      DictArrayFromJSON(dictionary(int8(), float16()), "[0, 1, 2]", "[NaN, 1, null]");
  ASSERT_OK_AND_ASSIGN(
      input, ListArray::FromArrays(*ArrayFromJSON(int32(), "[0, 1, 2, 3]"), *f16_dict));
  CheckListContains(input, ScalarFromJSON(float32(), "NaN"), "[true, false, false]");
  CheckListContains(input, ArrayFromJSON(float32(), "[1, 1, null]"),
                    "[false, true, true]");
  CheckListContains(ArrayFromJSON(list(float32()), "[[NaN], [1], [null]]"), f16_dict,
                    "[true, true, true]");
  ASSERT_OK_AND_ASSIGN(auto f16_nan, f16_dict->GetScalar(0));
  CheckListContains(ArrayFromJSON(list(float32()), "[[NaN], [1], [null]]"), f16_nan,
                    "[true, false, false]");

  // NaNs with different payloads (0x7ff8000000000001, 0x7ff8000000000002) match too
  ASSERT_OK_AND_ASSIGN(auto nans,
                       ArrayFromJSON(uint64(), "[9221120237041090561]")->View(float64()));
  ASSERT_OK_AND_ASSIGN(
      auto queries, ArrayFromJSON(uint64(), "[9221120237041090562, 4607182418800017408]")
                        ->View(float64()));  // [NaN, 1.0]
  ASSERT_OK_AND_ASSIGN(
      input, ListArray::FromArrays(*ArrayFromJSON(int32(), "[0, 1, 1]"), *nans));
  CheckListContains(input, queries, "[true, false]");
  ASSERT_OK_AND_ASSIGN(auto nan_query, queries->GetScalar(0));
  CheckListContains(input, nan_query, "[true, false]");
  ASSERT_OK_AND_ASSIGN(auto nan_list, input->GetScalar(0));
  CheckListContains(nan_list, queries, "[true, false]");
}

TEST(TestScalarNested, ListContainsValueTypes) {
  auto check = [](const std::shared_ptr<DataType>& ty, const std::string& x,
                  const std::string& y) {
    auto input = ArrayFromJSON(
        list(ty), "[[" + x + ", null], [" + y + "], [" + x + ", " + y + "], [], null]");
    CheckListContains(input, ScalarFromJSON(ty, x), "[true, false, true, false, null]");
    CheckListContains(input, ScalarFromJSON(ty, y), "[false, true, true, false, null]");
    CheckListContains(
        input,
        ArrayFromJSON(ty, "[" + x + ", " + y + ", " + y + ", " + x + ", " + x + "]"),
        "[true, true, true, false, null]");
    // A single list is searched for each query
    CheckListContains(ScalarFromJSON(list(ty), "[" + x + ", null]"),
                      ArrayFromJSON(ty, "[" + x + ", " + y + ", null]"),
                      "[true, false, true]");
  };
  check(utf8(), R"("x")", R"("y")");
  check(large_binary(), R"("x")", R"("y")");
  check(binary_view(), R"("x")", R"("y")");
  check(fixed_size_binary(1), R"("x")", R"("y")");
  // String views compare with other string layouts, unlike in "equal"
  CheckListContains(ArrayFromJSON(list(utf8()), R"([["x"], ["y"]])"),
                    ScalarFromJSON(utf8_view(), R"("x")"), "[true, false]");
  CheckListContains(ArrayFromJSON(list(binary_view()), R"([["x"], ["y"]])"),
                    ArrayFromJSON(large_binary(), R"(["y", "y"])"), "[false, true]");
  check(boolean(), "true", "false");
  check(int16(), "1", "2");
  check(float16(), "1.5", "2.5");
  check(date32(), "18262", "18628");
  check(decimal128(38, 2), R"("1.50")", R"("2.50")");
  check(decimal256(40, 2), R"("1.50")", R"("2.50")");
  check(timestamp(TimeUnit::NANO), "1", "2");
  check(duration(TimeUnit::MILLI), "1", "2");

  // Zero-width values all equal each other
  auto input =
      ArrayFromJSON(list(fixed_size_binary(0)), R"([["", null], [null], [], null])");
  CheckListContains(input, ScalarFromJSON(fixed_size_binary(0), R"("")"),
                    "[true, false, false, null]");
  CheckListContains(input, ArrayFromJSON(fixed_size_binary(0), R"(["", "", "", ""])"),
                    "[true, false, false, null]");
  CheckListContains(ScalarFromJSON(list(fixed_size_binary(0)), R"(["", null])"),
                    ArrayFromJSON(fixed_size_binary(0), R"(["", null])"), "[true, true]");

  // Values of other widths never match, even with a common prefix
  for (const auto& list_type :
       {list(fixed_size_binary(1)), list_view(fixed_size_binary(1))}) {
    input = ArrayFromJSON(list_type, R"([["a"], ["b", null], null])");
    CheckListContains(input, ScalarFromJSON(fixed_size_binary(2), R"("ab")"),
                      "[false, false, null]");
    CheckListContains(input, ArrayFromJSON(fixed_size_binary(2), R"(["ab", null, "ab"])"),
                      "[false, true, null]");
  }
  CheckListContains(ScalarFromJSON(list(fixed_size_binary(2)), R"(["ab", null])"),
                    ArrayFromJSON(fixed_size_binary(1), R"(["a", null])"),
                    "[false, true]");
}

TEST(TestScalarNested, ListContainsSignedZero) {
  // Like "equal", -0.0 equals 0.0
  auto input = ArrayFromJSON(list(float64()), "[[0.0], [-0.0], [1.0], null]");
  CheckListContains(input, ScalarFromJSON(float64(), "-0.0"),
                    "[true, true, false, null]");
  CheckListContains(input, ArrayFromJSON(float64(), "[-0.0, 0.0, 0.0, 0.0]"),
                    "[true, true, false, null]");
  CheckListContains(ScalarFromJSON(list(float64()), "[-0.0, NaN]"),
                    ArrayFromJSON(float64(), "[0.0, -0.0, NaN, 1.0]"),
                    "[true, true, true, false]");
}

TEST(TestScalarNested, ListContainsImplicitCast) {
  auto input = ArrayFromJSON(list(int64()),
                             "[[2, 2, 3, null, null], null, [], [null], [1], [0, -1]]");
  CheckListContains(input, ScalarFromJSON(float64(), "2.0"),
                    "[true, null, false, false, false, false]");
  CheckListContains(input, ScalarFromJSON(float64(), "1.5"),
                    "[false, null, false, false, false, false]");

  // No overflow of the list values
  CheckListContains(ArrayFromJSON(list(int8()), "[[44, 1], null, [], [null]]"),
                    ScalarFromJSON(int64(), "300"), "[false, null, false, false]");
  CheckListContains(ArrayFromJSON(list(float64()), "[[1.0, 2.0], [3.0], null, []]"),
                    ScalarFromJSON(int64(), "1"), "[true, false, null, false]");
  CheckListContains(ArrayFromJSON(list(decimal128(38, 2)), R"([["1.50"], ["2.50"]])"),
                    ScalarFromJSON(decimal128(3, 2), R"("1.50")"), "[true, false]");

  // Values of null lists are not cast: 2^53 + 1 isn't a float64
  auto values = ArrayFromJSON(int64(), "[9007199254740993, 1]");
  auto validity = ArrayFromJSON(boolean(), "[false, true]")->data()->buffers[1];
  ASSERT_OK_AND_ASSIGN(
      auto lists, ListArray::FromArrays(*ArrayFromJSON(int32(), "[0, 1, 2]"), *values,
                                        default_memory_pool(), validity));
  ASSERT_OK_AND_ASSIGN(auto fixed_size_lists,
                       FixedSizeListArray::FromArrays(values, 1, validity));
  for (Datum input : {Datum(lists), Datum(fixed_size_lists)}) {
    CheckListContains(input, ScalarFromJSON(float64(), "1"), "[null, true]");
    CheckListContains(input, ArrayFromJSON(float64(), "[1, null]"), "[null, false]");
  }
  // Unlike values of non-null lists
  EXPECT_THAT(CallFunction("list_contains",
                           {ArrayFromJSON(list(int64()), "[[9007199254740993], null]"),
                            ScalarFromJSON(float64(), "1")}),
              Raises(StatusCode::Invalid));

  // Queries are cast like for "equal", even those of null lists
  auto floats = list(float64());
  for (Datum lists : {Datum(ArrayFromJSON(floats, "[[1], null]")),
                      Datum(ArrayFromJSON(floats, "[null, null]")),
                      Datum(ScalarFromJSON(floats, "null"))}) {
    for (Datum queries : {Datum(ArrayFromJSON(int64(), "[9007199254740993, 1]")),
                          Datum(ScalarFromJSON(int64(), "9007199254740993"))}) {
      EXPECT_THAT(CallFunction("list_contains", {lists, queries}),
                  Raises(StatusCode::Invalid));
    }
  }
}

TEST(TestScalarNested, ListContainsChunked) {
  auto array = ArrayFromJSON(list(int32()), "[[1, 2], [3], null, [], [2, null]]");
  auto input = std::make_shared<ChunkedArray>(
      ArrayVector{array->Slice(3), array->Slice(0, 0), array->Slice(1, 2)});
  ASSERT_OK_AND_ASSIGN(
      Datum result, CallFunction("list_contains", {input, ScalarFromJSON(int32(), "2")}));
  AssertDatumsEqual(
      ChunkedArrayFromJSON(boolean(), {"[false, true]", "[]", "[false, null]"}), result,
      /*verbose=*/true);
}

TEST(TestScalarNested, ListContainsFixedSizeList) {
  auto input = ArrayFromJSON(fixed_size_list(int32(), 2),
                             "[[1, 2], [3, null], null, [2, 4], [null, null]]");
  CheckListContains(input, ScalarFromJSON(int32(), "2"),
                    "[true, false, null, true, false]");
  CheckListContains(input->Slice(2), ScalarFromJSON(int32(), "4"), "[null, true, false]");
}

TEST(TestScalarNested, ListContainsListViewOutOfOrder) {
  // Views: [3, 4], [1, 2], [2, 3], [4], []
  ASSERT_OK_AND_ASSIGN(
      auto input, ListViewArray::FromArrays(*ArrayFromJSON(int32(), "[2, 0, 1, 3, 0]"),
                                            *ArrayFromJSON(int32(), "[2, 2, 2, 1, 0]"),
                                            *ArrayFromJSON(int32(), "[1, 2, 3, 4]")));
  CheckListContains(input, ScalarFromJSON(int32(), "3"),
                    "[true, false, true, false, false]");
  CheckListContains(input, ScalarFromJSON(int32(), "1"),
                    "[false, true, false, false, false]");
  CheckListContains(input, ArrayFromJSON(int32(), "[3, 1, 1, 4, 1]"),
                    "[true, true, false, true, false]");
  // Only the child values referenced by the (sliced) views are compared
  CheckListContains(input->Slice(2), ScalarFromJSON(int32(), "3"),
                    "[true, false, false]");
  CheckListContains(input->Slice(3), ScalarFromJSON(int32(), "1"), "[false, false]");
  CheckListContains(input->Slice(4), ScalarFromJSON(int32(), "1"), "[false]");

  // Child values outside every view are not cast: 2^53 + 1 isn't a float64
  ASSERT_OK_AND_ASSIGN(
      input, ListViewArray::FromArrays(
                 *ArrayFromJSON(int32(), "[0, 2]"), *ArrayFromJSON(int32(), "[1, 1]"),
                 *ArrayFromJSON(int64(), "[1, 9007199254740993, 3, 9007199254740993]")));
  CheckListContains(input, ScalarFromJSON(float64(), "1"), "[true, false]");
  CheckListContains(input, ArrayFromJSON(float64(), "[3, 3]"), "[false, true]");
  auto views = ArrayFromJSON(list_view(int64()), "[[1, 2], null, [], [3, null]]");
  CheckListContains(views, ScalarFromJSON(float64(), "3"), "[false, null, false, true]");
  CheckListContains(views, ArrayFromJSON(float64(), "[2, 1, 1, null]"),
                    "[true, null, false, true]");
}

TEST(TestScalarNested, ListContainsArrayOfValues) {
  // Each list is searched for the value at the same index
  for (auto list_type : {list(int32()), large_list(int32()), list_view(int32()),
                         large_list_view(int32())}) {
    CheckListContains(
        ArrayFromJSON(list_type, "[[1, 2], [3, null], [], null, [4, 5], [null], [6]]"),
        ArrayFromJSON(int32(), "[2, null, 1, 3, 4, null, 7]"),
        "[true, true, false, null, true, true, false]");
  }
  CheckListContains(ArrayFromJSON(fixed_size_list(int32(), 2),
                                  "[[1, 2], [3, null], null, [4, 5], [null, 6]]"),
                    ArrayFromJSON(int32(), "[2, null, 1, 5, 7]"),
                    "[true, true, null, true, false]");
  CheckListContains(ArrayFromJSON(list(int64()), "[[1, 2], [3], [4]]"),
                    ArrayFromJSON(float64(), "[2.0, 3.5, 4.0]"), "[true, false, true]");
  CheckListContains(ArrayFromJSON(list(float64()), "[[1.5, NaN], [NaN], [1.5], [null]]"),
                    ArrayFromJSON(float64(), "[NaN, 1.5, NaN, NaN]"),
                    "[true, false, false, false]");
  CheckListContains(ArrayFromJSON(list(int32()), "[[1, null], [2], []]"),
                    ArrayFromJSON(null(), "[null, null, null]"), "[true, false, false]");

  // A scalar list is searched for each value
  CheckListContains(ScalarFromJSON(list(int32()), "[1, null]"),
                    ArrayFromJSON(int32(), "[1, 2, null]"), "[true, false, true]");
  CheckListContains(ScalarFromJSON(fixed_size_list(int32(), 2), "[1, 2]"),
                    ArrayFromJSON(int32(), "[2, 3]"), "[true, false]");
  CheckListContains(ScalarFromJSON(list(int32()), "null"),
                    ArrayFromJSON(int32(), "[1, 2]"), "[null, null]");
  CheckListContains(ScalarFromJSON(list(int64()), "[1, 2]"),
                    ArrayFromJSON(float64(), "[2.0, 2.5, null]"), "[true, false, false]");
  CheckListContains(ScalarFromJSON(list(utf8()), R"(["a", null])"),
                    ArrayFromJSON(null(), "[null, null]"), "[true, true]");
  CheckListContains(ScalarFromJSON(list(utf8()), R"(["a"])"),
                    ArrayFromJSON(utf8(), "[null, null]"), "[false, false]");
}

TEST(TestScalarNested, ListContainsInvalid) {
  auto input = ArrayFromJSON(list(int32()), "[[1, 2], [3]]");
  // Null values must be comparable too
  for (Datum value :
       {Datum(ArrayFromJSON(utf8(), R"(["a", "b"])")),
        Datum(ScalarFromJSON(utf8(), R"("a")")), Datum(ScalarFromJSON(boolean(), "true")),
        Datum(ScalarFromJSON(utf8(), "null")),
        Datum(ArrayFromJSON(utf8(), "[null, null]"))}) {
    EXPECT_THAT(CallFunction("list_contains", {input, value}),
                Raises(StatusCode::NotImplemented));
    EXPECT_THAT(
        CallFunction("list_contains", {ScalarFromJSON(list(int32()), "[1]"),
                                       ArrayFromJSON(value.type(), "[null, null]")}),
        Raises(StatusCode::NotImplemented));
  }
  // Also for null lists, whose value type must be comparable all the same
  for (Datum lists : {Datum(ArrayFromJSON(list(int32()), "[null, null]")),
                      Datum(ScalarFromJSON(list(int32()), "null"))}) {
    for (auto queries : {ArrayFromJSON(utf8(), R"(["a", "b"])"),
                         ArrayFromJSON(utf8(), "[null, null]")}) {
      EXPECT_THAT(CallFunction("list_contains", {lists, queries}),
                  Raises(StatusCode::NotImplemented));
    }
  }
  EXPECT_THAT(CallFunction("list_contains", {ArrayFromJSON(list(int32()), "[]"),
                                             ArrayFromJSON(utf8(), "[]")}),
              Raises(StatusCode::NotImplemented));
  // Lists, unions and run-end encoded values only compare with null queries, which
  // only need validity. Other queries raise, also for null lists.
  auto union_type = sparse_union({field("i", int64()), field("s", utf8())}, {0, 1});
  auto ree_type = run_end_encoded(int32(), int64());
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> ree_queries,
                       RunEndEncodedArray::Make(2, ArrayFromJSON(int32(), "[2]"),
                                                ArrayFromJSON(int64(), "[1]")));
  ASSERT_OK_AND_ASSIGN(auto null_ree_queries, MakeArrayOfNull(ree_type, 2));
  for (const auto& [queries, null_queries] :
       {std::pair(ArrayFromJSON(list(int32()), "[[1], null]"),
                  ArrayFromJSON(list(int32()), "[null, null]")),
        std::pair(ArrayFromJSON(union_type, R"([[0, 1], [1, "a"]])"),
                  ArrayFromJSON(union_type, "[[0, null], [1, null]]")),
        std::pair(ree_queries, null_ree_queries)}) {
    ARROW_SCOPED_TRACE(*queries->type());
    ASSERT_OK_AND_ASSIGN(auto null_lists, MakeArrayOfNull(list(queries->type()), 2));
    ASSERT_OK_AND_ASSIGN(
        auto empty_and_null,
        ListArray::FromArrays(
            *ArrayFromJSON(int32(), "[0, 0, 0]"), *null_queries->Slice(0, 0),
            default_memory_pool(),
            ArrayFromJSON(boolean(), "[true, false]")->data()->buffers[1]));
    for (Datum lists : {Datum(null_lists), Datum(MakeNullScalar(list(queries->type()))),
                        Datum(empty_and_null)}) {
      EXPECT_THAT(CallFunction("list_contains", {lists, queries}),
                  Raises(StatusCode::NotImplemented));
    }
    CheckListContains(null_lists, null_queries, "[null, null]");
    CheckListContains(MakeNullScalar(list(queries->type())), null_queries,
                      "[null, null]");
    CheckListContains(empty_and_null, null_queries, "[false, null]");
  }
  // Like "equal", timestamps with and without a time zone don't compare
  for (auto value : {"1", "null"}) {
    EXPECT_THAT(
        CallFunction("list_contains",
                     {ArrayFromJSON(list(timestamp(TimeUnit::SECOND, "UTC")), "[[1]]"),
                      ScalarFromJSON(timestamp(TimeUnit::SECOND), value)}),
        Raises(StatusCode::Invalid));
  }
  // Like "equal", timestamps don't compare with durations, which don't cast to them,
  // also in dictionaries
  auto timestamps = list(timestamp(TimeUnit::SECOND));
  for (Datum lists : {Datum(ArrayFromJSON(timestamps, "[[1], null]")),
                      Datum(ScalarFromJSON(timestamps, "null"))}) {
    for (auto queries : {"[1, null]", "[null, null]"}) {
      auto durations = ArrayFromJSON(duration(TimeUnit::SECOND), queries);
      ASSERT_OK_AND_ASSIGN(Datum encoded, DictionaryEncode(durations));
      for (Datum value : {Datum(durations), encoded}) {
        EXPECT_THAT(CallFunction("list_contains", {lists, value}),
                    Raises(StatusCode::NotImplemented));
      }
    }
  }
}

using VarLenListLikeTypeFactory =
    std::shared_ptr<DataType> (*)(std::shared_ptr<DataType>);
static const VarLenListLikeTypeFactory kVarLenListTypeFactories[] = {
    list,
    large_list,
    list_view,
    large_list_view,
};

TEST(TestScalarNested, ListSliceVariableOutput) {
  const auto value_types = {float32(), int32()};
  for (auto value_type : value_types) {
    for (auto list_type_factory : kVarLenListTypeFactories) {
      ListSliceOptions args(/*start=*/0, /*stop=*/2, /*step=*/1,
                            /*return_fixed_size_list=*/false);
      auto list_ty = list_type_factory(value_type);
      auto input = ArrayFromJSON(list_ty, "[[1, 2, 3], [4, 5], [6], null]");
      auto expected = ArrayFromJSON(list_ty, "[[1, 2], [4, 5], [6], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      args.start = 1;
      expected = ArrayFromJSON(list_ty, "[[2], [5], [], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      args.start = 2;
      args.stop = 4;
      expected = ArrayFromJSON(list_ty, "[[3], [], [], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      args.start = 1;
      args.stop = std::nullopt;
      expected = ArrayFromJSON(list_ty, "[[2, 3], [5], [], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      args.start = 0;
      args.stop = 4;
      args.step = 2;
      expected = ArrayFromJSON(list_ty, "[[1, 3], [4], [6], null]");
    }
  }

  // Verify passing `return_fixed_size_list=false` with fixed size input
  // returns variable size even if stop is beyond list_size
  ListSliceOptions args(/*start=*/0, /*stop=*/2, /*step=*/1,
                        /*return_fixed_size_list=*/false);
  auto input = ArrayFromJSON(fixed_size_list(int32(), 1), "[[1]]");
  auto expected = ArrayFromJSON(list(int32()), "[[1]]");
  CheckScalarUnary("list_slice", input, expected, &args);

  args.start = 0;
  args.stop = 0;
  auto input_empty = ArrayFromJSON(list(int32()), "[[1, 2, 3], [4, 5], null]");
  auto expected_empty = ArrayFromJSON(list(int32()), "[[], [], null]");
  CheckScalarUnary("list_slice", input_empty, expected_empty, &args);
}

TEST(TestScalarNested, ListSliceFixedOutput) {
  const auto value_types = {float32(), int32()};
  for (auto value_type : value_types) {
    const char* kVarLenListJSON = "[[1, 2, 3], [4, 5], [6], null]";
    const char* kFixedSizeListJSON = "[[1, 2, 3], [4, 5, null], [6, null, null], null]";
    std::vector<std::shared_ptr<Array>> inputs;
    for (auto list_type_factory : kVarLenListTypeFactories) {
      inputs.push_back(ArrayFromJSON(list_type_factory(value_type), kVarLenListJSON));
    }
    inputs.push_back(ArrayFromJSON(fixed_size_list(value_type, 3), kFixedSizeListJSON));
    for (auto input : inputs) {
      ListSliceOptions args(/*start=*/0, /*stop=*/2, /*step=*/1,
                            /*return_fixed_size_list=*/true);
      auto expected = ArrayFromJSON(fixed_size_list(value_type, 2),
                                    "[[1, 2], [4, 5], [6, null], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      args.start = 1;
      expected =
          ArrayFromJSON(fixed_size_list(value_type, 1), "[[2], [5], [null], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      args.start = 2;
      args.stop = 4;
      expected = ArrayFromJSON(fixed_size_list(value_type, 2),
                               "[[3, null], [null, null], [null, null], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      args.start = 1;
      args.stop = std::nullopt;
      expected = ArrayFromJSON(fixed_size_list(value_type, 2),
                               "[[2, 3], [5, null], [null, null], null]");
      if (input->type()->id() == Type::FIXED_SIZE_LIST) {
        CheckScalarUnary("list_slice", input, expected, &args);
      } else {
        EXPECT_RAISES_WITH_MESSAGE_THAT(
            Invalid,
            ::testing::HasSubstr("Unable to produce FixedSizeListArray from "
                                 "non-FixedSizeListArray without `stop` being set."),
            CallFunction("list_slice", {input}, &args));
      }

      args.start = 3;
      args.stop = std::nullopt;
      expected = ArrayFromJSON(fixed_size_list(value_type, 0), "[[], [], [], null]");
      if (input->type()->id() == Type::FIXED_SIZE_LIST) {
        CheckScalarUnary("list_slice", input, expected, &args);
      }

      args.start = 0;
      args.stop = 4;
      args.step = 2;
      expected = ArrayFromJSON(fixed_size_list(value_type, 2),
                               "[[1, 3], [4, null], [6, null], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      // More checks for step slicing start/stop/step combinations
      args.start = 1;
      args.stop = 3;
      args.step = 2;
      expected =
          ArrayFromJSON(fixed_size_list(value_type, 1), "[[2], [5], [null], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      args.start = 2;
      expected =
          ArrayFromJSON(fixed_size_list(value_type, 1), "[[3], [null], [null], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      args.start = 0;
      args.stop = 2;
      args.step = 3;
      expected = ArrayFromJSON(fixed_size_list(value_type, 1), "[[1], [4], [6], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      args.start = 0;
      args.stop = 5;
      args.step = 2;
      expected = ArrayFromJSON(fixed_size_list(value_type, 3),
                               "[[1, 3, null], [4, null, null], [6, null, null], null]");
      CheckScalarUnary("list_slice", input, expected, &args);

      args.start = 0;
      args.stop = 6;
      args.step = 3;
      expected = ArrayFromJSON(fixed_size_list(value_type, 2),
                               "[[1, null], [4, null], [6, null], null]");
      CheckScalarUnary("list_slice", input, expected, &args);
    }
  }
}

TEST(TestScalarNested, ListSliceChildArrayOffset) {
  auto offsets = ArrayFromJSON(int32(), "[0, 1, 3]");
  auto data = ArrayFromJSON(int8(), "[0, 1, 2, 3, 4]");
  auto slice = data->Slice(2);

  // [[2], [3, 4]] with offset of 2 for values.
  ASSERT_OK_AND_ASSIGN(auto input, ListArray::FromArrays(*offsets, *slice));
  ASSERT_EQ(input->offset(), 0);
  ASSERT_EQ(input->values()->offset(), 2);

  ListSliceOptions args(/*start=*/0, /*stop=*/3, /*step=*/1,
                        /*return_fixed_size_list=*/false);
  auto expected = ArrayFromJSON(list(int8()), "[[2], [3, 4]]");
  CheckScalarUnary("list_slice", input, expected, &args);

  args.return_fixed_size_list = true;
  expected = ArrayFromJSON(fixed_size_list(int8(), 3), "[[2, null, null], [3, 4, null]]");
  CheckScalarUnary("list_slice", input, expected, &args);
}

TEST(TestScalarNested, ListSliceOutputEqualsInputType) {
  const char* kVarLenListJSON = "[[1, 2, 3], [4, 5], [6, null], null]";
  const char* kFixedLenListJSON = "[[1, 2], [4, 5], [6, null], null]";
  // Default is to return same type as the one passed in.
  std::vector<std::shared_ptr<Array>> inputs;
  for (auto list_type_factory : kVarLenListTypeFactories) {
    inputs.push_back(ArrayFromJSON(list_type_factory(int8()), kVarLenListJSON));
  }
  inputs.push_back(ArrayFromJSON(fixed_size_list(int8(), 2), kFixedLenListJSON));
  for (auto input : inputs) {
    ListSliceOptions args(/*start=*/0, /*stop=*/2, /*step=*/1);
    auto expected = ArrayFromJSON(input->type(), "[[1, 2], [4, 5], [6, null], null]");
    CheckScalarUnary("list_slice", input, expected, &args);
  }
}

TEST(TestScalarNested, ListSliceBadParameters) {
  auto input = ArrayFromJSON(list(int32()), "[[1]]");

  // negative start
  ListSliceOptions args(/*start=*/-1, /*stop=*/1, /*step=*/1,
                        /*return_fixed_size_list=*/true);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "`start`(-1) should be greater than or equal to 0 and not greater than "
          "`stop`(1)"),
      CallFunction("list_slice", {input}, &args));
  // start greater than stop
  args.start = 1;
  args.stop = 0;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "`start`(1) should be greater than or equal to 0 and not greater than "
          "`stop`(0)"),
      CallFunction("list_slice", {input}, &args));
  // stop not set and FixedSizeList requested with variable sized input
  args.stop = std::nullopt;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Invalid: Unable to produce FixedSizeListArray from "
                           "non-FixedSizeListArray without `stop` being set."),
      CallFunction("list_slice", {input}, &args));
  // Catch step must be >= 1
  args.start = 0;
  args.stop = 2;
  args.step = 0;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("`step` must be greater than or equal to 1, got: 0"),
      CallFunction("list_slice", {input}, &args));
}

TEST(TestScalarNested, StructField) {
  StructFieldOptions trivial;
  StructFieldOptions extract0({0});
  StructFieldOptions extract20({2, 0});
  StructFieldOptions invalid1({-1});
  StructFieldOptions invalid2({2, 4});
  StructFieldOptions invalid3({3});
  StructFieldOptions invalid4({0, 1});

  // Test using FieldRefs
  StructFieldOptions extract0_field_ref_path(FieldRef(FieldPath({0})));
  StructFieldOptions extract0_field_ref_name(FieldRef("a"));
  ASSERT_OK_AND_ASSIGN(auto field_ref, FieldRef::FromDotPath(".c.d"));
  StructFieldOptions extract20_field_ref_nest(field_ref);

  FieldVector fields = {field("a", int32()), field("b", utf8()),
                        field("c", struct_({
                                       field("d", int64()),
                                       field("e", float64()),
                                   }))};
  {
    auto arr = ArrayFromJSON(struct_(fields), R"([
      [1, "a", [10, 10.0]],
      [null, "b", [11, 11.0]],
      [3, null, [12, 12.0]],
      null
    ])");
    CheckScalar("struct_field", {arr}, arr, &trivial);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, 3, null]"),
                &extract0);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int64(), "[10, 11, 12, null]"),
                &extract20);

    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, 3, null]"),
                &extract0_field_ref_path);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, 3, null]"),
                &extract0_field_ref_name);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int64(), "[10, 11, 12, null]"),
                &extract20_field_ref_nest);

    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid1));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("No match for FieldRef"),
                                    CallFunction("struct_field", {arr}, &invalid2));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid3));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("No match for FieldRef"),
                                    CallFunction("struct_field", {arr}, &invalid4));
  }
  {
    auto ty = dense_union(fields, {2, 5, 8});
    auto arr = ArrayFromJSON(ty, R"([
      [2, 1],
      [5, "foo"],
      [8, null],
      [8, [10, 10.0]]
    ])");
    CheckScalar("struct_field", {arr}, arr, &trivial);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, null, null]"),
                &extract0);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int64(), "[null, null, null, 10]"),
                &extract20);

    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, null, null]"),
                &extract0_field_ref_path);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, null, null]"),
                &extract0_field_ref_name);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int64(), "[null, null, null, 10]"),
                &extract20_field_ref_nest);

    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid1));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("No match for FieldRef"),
                                    CallFunction("struct_field", {arr}, &invalid2));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid3));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("No match for FieldRef"),
                                    CallFunction("struct_field", {arr}, &invalid4));

    // Test edge cases for union representation
    auto ints = ArrayFromJSON(fields[0]->type(), "[null, 2, 3]");
    auto strs = ArrayFromJSON(fields[1]->type(), R"([null, "bar"])");
    auto nested = ArrayFromJSON(fields[2]->type(), R"([null, [10, 10.0]])");
    auto type_ids = ArrayFromJSON(int8(), "[2, 5, 8, 2, 5, 8]")->data()->buffers[1];
    auto offsets = ArrayFromJSON(int32(), "[0, 0, 0, 1, 1, 1]")->data()->buffers[1];

    arr = std::make_shared<DenseUnionArray>(ty, /*length=*/6,
                                            ArrayVector{ints, strs, nested}, type_ids,
                                            offsets, /*offset=*/0);
    // Sliced parent
    CheckScalar("struct_field", {arr->Slice(3, 3)},
                ArrayFromJSON(int32(), "[2, null, null]"), &extract0);
    // Sliced child
    arr = std::make_shared<DenseUnionArray>(ty, /*length=*/6,
                                            ArrayVector{ints->Slice(1, 2), strs, nested},
                                            type_ids, offsets, /*offset=*/0);
    CheckScalar("struct_field", {arr},
                ArrayFromJSON(int32(), "[2, null, null, 3, null, null]"), &extract0);
    // Sliced parent + sliced child
    CheckScalar("struct_field", {arr->Slice(3, 3)},
                ArrayFromJSON(int32(), "[3, null, null]"), &extract0);
  }
  {
    // The underlying implementation is tested directly/more thoroughly in
    // array_union_test.cc.
    auto arr = ArrayFromJSON(sparse_union(fields, {2, 5, 8}), R"([
      [2, 1],
      [5, "foo"],
      [8, null],
      [8, [10, 10.0]]
    ])");
    CheckScalar("struct_field", {arr}, arr, &trivial);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, null, null]"),
                &extract0);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int64(), "[null, null, null, 10]"),
                &extract20);

    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, null, null]"),
                &extract0_field_ref_path);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int32(), "[1, null, null, null]"),
                &extract0_field_ref_name);
    CheckScalar("struct_field", {arr}, ArrayFromJSON(int64(), "[null, null, null, 10]"),
                &extract20_field_ref_nest);

    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid1));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("No match for FieldRef"),
                                    CallFunction("struct_field", {arr}, &invalid2));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("out-of-bounds field reference"),
                                    CallFunction("struct_field", {arr}, &invalid3));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("No match for FieldRef"),
                                    CallFunction("struct_field", {arr}, &invalid4));
  }
  {
    auto arr = ArrayFromJSON(int32(), "[0, 1, 2, 3]");
    ASSERT_RAISES(NotImplemented, CallFunction("struct_field", {arr}, &trivial));
    ASSERT_RAISES(NotImplemented, CallFunction("struct_field", {arr}, &extract0));
  }
}

void CheckMapLookupWithDifferentOptions(const std::shared_ptr<Array>& map,
                                        const std::shared_ptr<Scalar>& query_key,
                                        const std::shared_ptr<Array>& expected_all,
                                        const std::shared_ptr<Array>& expected_first,
                                        const std::shared_ptr<Array>& expected_last) {
  MapLookupOptions all_matches(query_key, MapLookupOptions::ALL);
  MapLookupOptions first_matches(query_key, MapLookupOptions::FIRST);
  MapLookupOptions last_matches(query_key, MapLookupOptions::LAST);

  CheckScalar("map_lookup", {map}, expected_all, &all_matches);
  CheckScalar("map_lookup", {map}, expected_first, &first_matches);
  CheckScalar("map_lookup", {map}, expected_last, &last_matches);
}

class TestMapLookupKernel : public ::testing::Test {};

TEST_F(TestMapLookupKernel, BooleanKey) {
  auto true_scalar = ScalarFromJSON(boolean(), R"(true)");
  auto map_type = map(boolean(), int32());
  const char* input = R"(
    [
      [
        [true, 99], [false, 1], [false, 2], [true, null], [false, 5],
        [true, 8]
      ],
      null,
      [
        [false, null], [true, 67], [false, 101], [false, 1], [false, null],
        [false, 9], [true, 80]
      ],
      [],
      [
        [false, 1], [false, 2], [false, 3], [false, 4]
      ],
      [
        [true, 9], [true, 2], [true, 5], [true, 8]
      ]
    ]
  )";
  auto map_array = ArrayFromJSON(map_type, input);
  auto map_array_tweaked = TweakValidityBit(map_array, 5, false);

  auto expected_all = ArrayFromJSON(list(int32()), R"(
    [[99, null, 8], null, [67, 80], null, null, null ])");
  auto expected_first = ArrayFromJSON(int32(), "[99, null, 67, null, null, null]");
  auto expected_last = ArrayFromJSON(int32(), "[8, null, 80, null, null, null]");

  CheckMapLookupWithDifferentOptions(map_array_tweaked, true_scalar, expected_all,
                                     expected_first, expected_last);
}

TEST_F(TestMapLookupKernel, MonthDayNanoIntervalKeys) {
  auto key_type = month_day_nano_interval();
  auto map_type = map(key_type, utf8());
  auto key_scalar = ScalarFromJSON(month_day_nano_interval(), R"([1, 2, -3])");
  const char* input = R"(
    [
      [
        [[-9, -10, 11], "zero"], [[1, 2, -3], "first_one"], [[11, -12, 0], "two"],
        [[1, 2, -3], null], [[-7, -8, -9], "three"], [[1, 2, -3], "second_one"],
        [[1, 2, -3], "last_one"]
      ],
      null,
      [
        [[-5, 6, 7], "zero_hero"], [[15, 16, 2], "almost_six"],
        [[1, 2, -3], "the_dumb_one"], [[-7, -8, -9], "eleven"],
        [[1, 2, -3], "the_chosen_one"], [[-5, 6, 7], "meaning of life"],
        [[1, 2, -3], "just_one"], [[1, 2, -3], "no more ones!"]
      ],
      [
        [[-5, 6, 7], "this"], [[-13, 14, -1], "has"], [[11, -12, 0], "no"],
        [[15, 16, 2], "keys"]
      ],
      [
        [[1, 2, -3], "this"], [[1, 2, -3], "should"], [[1, 2, -3], "also"],
        [[1, 2, -3], "be"], [[1, 2, -3], "null"]
      ],
      []
    ]
  )";
  auto map_array = ArrayFromJSON(map_type, input);
  auto map_array_tweaked = TweakValidityBit(map_array, 4, false);

  auto expected_first =
      ArrayFromJSON(utf8(), R"(["first_one", null, "the_dumb_one", null, null, null])");
  auto expected_last =
      ArrayFromJSON(utf8(), R"(["last_one", null, "no more ones!", null, null, null])");
  auto expected_all = ArrayFromJSON(list(utf8()),
                                    R"([
                                          ["first_one", null, "second_one", "last_one"],
                                          null,
                                          ["the_dumb_one", "the_chosen_one", "just_one", "no more ones!"],
                                          null,
                                          null,
                                          null
                                        ]
                                      )");

  CheckMapLookupWithDifferentOptions(map_array_tweaked, key_scalar, expected_all,
                                     expected_first, expected_last);
}

TEST_F(TestMapLookupKernel, FixedSizeBinary) {
  auto key_type = fixed_size_binary(6);
  auto map_type = map(key_type, int32());
  auto sheesh_scalar = ScalarFromJSON(key_type, R"("sheesh")");
  const char* input = R"(
      [
        [
          ["sheesh", 99], ["yooloo", 1], ["yaaaay", 2], ["sheesh", null], ["no way", 5],
          ["sheesh", 8]
        ],
        null,
        [
          ["hmm,mm", null], ["sheesh", 67], ["snaccc", 101], ["awwwww", 1], ["dapdap", null],
          ["yooloo", 9], ["sheesh", 80]
        ],
        [],
        [
          ["nopeno", 1], ["nonono", 2], ["sheess", 3], ["here!!", 4]
        ],
        [
          ["sheesh", 9], ["sheesh", 2], ["sheesh", 5], ["sheesh", 8]
        ]
      ]
    )";
  auto map_array = ArrayFromJSON(map_type, input);
  auto map_array_tweaked = TweakValidityBit(map_array, 5, false);

  auto expected_all = ArrayFromJSON(list(int32()), R"(
    [[99, null, 8], null, [67, 80], null, null, null ])");
  auto expected_first = ArrayFromJSON(int32(), "[99, null, 67, null, null, null]");
  auto expected_last = ArrayFromJSON(int32(), "[8, null, 80, null, null, null]");

  CheckMapLookupWithDifferentOptions(map_array_tweaked, sheesh_scalar, expected_all,
                                     expected_first, expected_last);
}

TEST_F(TestMapLookupKernel, Errors) {
  auto map_type = map(int32(), utf8());
  const char* input = R"(
    [
      [
        [0, "zero"], [1, "first one"], [2, "two"], [1, null], [3, "three"], [1, "second one"],
        [1, "last one"]
      ],
      null,
      [
        [0, "zero hero"], [9, "almost six"], [1, "the dumb one"], [7, "eleven"],
        [1, "the chosen one"], [42, "meaning of life?"], [1, "just_one"],
        [1, "no more ones!"]
      ],
      [
        [4, "this"], [6, "has"], [8, "no"], [2, "ones"]
      ],
      [
        [1, "this"], [1, "should"], [1, "also"], [1, "be"], [1, "null"]
      ],
      []
    ])";
  auto map_array = ArrayFromJSON(map_type, input);
  auto query_key_int16 = MakeScalar(int16(), 1).ValueOrDie();
  FieldVector fields = {field("a", int32()), field("b", utf8()),
                        field("c", struct_({
                                       field("d", int64()),
                                       field("e", float64()),
                                   }))};
  auto unsupported_scalar = ScalarFromJSON(struct_(fields), R"([1, "a", [10, 10.0]])");

  MapLookupOptions unsupported(unsupported_scalar, MapLookupOptions::FIRST);
  MapLookupOptions all(query_key_int16, MapLookupOptions::ALL);
  MapLookupOptions first(query_key_int16, MapLookupOptions::FIRST);
  MapLookupOptions last(query_key_int16, MapLookupOptions::LAST);
  MapLookupOptions empty_key(nullptr, MapLookupOptions::FIRST);
  MapLookupOptions null_key(MakeNullScalar(int32()), MapLookupOptions::FIRST);

  for (auto option : {unsupported, all, first, last}) {
    ASSERT_RAISES(TypeError, CallFunction("map_lookup", {map_array}, &option));
  }

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("key can't be empty"),
                                  CallFunction("map_lookup", {map_array}, &empty_key));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("key can't be null"),
                                  CallFunction("map_lookup", {map_array}, &null_key));
}

template <typename KeyType>
class TestMapLookupIntegralKeys : public ::testing ::Test {
 protected:
  std::shared_ptr<DataType> type_singleton() const {
    std::shared_ptr<DataType> type = default_type_instance<KeyType>();
    return map(type, utf8());
  }
};

TYPED_TEST_SUITE(TestMapLookupIntegralKeys, PhysicalIntegralArrowTypes);

TYPED_TEST(TestMapLookupIntegralKeys, StringItems) {
  auto map_type = this->type_singleton();
  const char* input = R"(
    [
      [
        [0, "zero"], [1, "first_one"], [2, "two"], [1, null], [3, "three"], [1, "second_one"],
        [1, "last_one"]
      ],
      null,
      [
        [0, "zero_hero"], [9, "almost_six"], [1, "the_dumb_one"], [7, "eleven"],
        [1, "the_chosen_one"], [42, "meaning of life?"], [1, "just_one"],
        [1, "no more ones!"]
      ],
      [
        [4, "this"], [6, "has"], [8, "no"], [2, "ones"]
      ],
      [
        [1, "this"], [1, "should"], [1, "also"], [1, "be"], [1, "null"]
      ],
      []
    ])";
  auto map_array = ArrayFromJSON(map_type, input);
  auto map_array_tweaked = TweakValidityBit(map_array, 4, false);

  auto expected_all = ArrayFromJSON(list(utf8()), R"(
                          [
                            ["first_one", null, "second_one", "last_one"],
                            null,
                            ["the_dumb_one", "the_chosen_one", "just_one", "no more ones!"],
                            null,
                            null,
                            null
                          ])");
  auto expected_first =
      ArrayFromJSON(utf8(), R"(["first_one", null, "the_dumb_one", null, null, null])");
  auto expected_last =
      ArrayFromJSON(utf8(), R"(["last_one", null, "no more ones!", null, null, null])");

  CheckMapLookupWithDifferentOptions(
      map_array_tweaked, MakeScalar(default_type_instance<TypeParam>(), 1).ValueOrDie(),
      expected_all, expected_first, expected_last);
}
template <typename KeyType>
class TestMapLookupDecimalKeys : public ::testing ::Test {
 protected:
  std::shared_ptr<DataType> type_singleton() const {
    return std::make_shared<KeyType>(/*precision=*/5,
                                     /*scale=*/4);
  }
};

TYPED_TEST_SUITE(TestMapLookupDecimalKeys, DecimalArrowTypes);

TYPED_TEST(TestMapLookupDecimalKeys, StringItems) {
  auto type = this->type_singleton();
  auto map_type = map(type, utf8());
  auto key_scalar = DecimalScalarFromJSON(type, R"("1.2345")");
  const char* input = R"(
    [
      [
        ["0.8923", "zero"], ["1.2345", "first_one"], ["2.7001", "two"],
        ["1.2345", null], ["3.2234", "three"], ["1.2345", "second_one"],
        ["1.2345", "last_one"]
      ],
      null,
      [
        ["0.0012", "zero_hero"], ["9.0093", "almost_six"], ["1.2345", "the_dumb_one"],
        ["7.6587", "eleven"], ["1.2345", "the_chosen_one"], ["4.2000", "meaning of life"],
        ["1.2345", "just_one"], ["1.2345", "no more ones!"]
      ],
      [
        ["4.8794", "this"], ["6.2345", "has"], ["8.6649", "no"], ["0.0122", "ones"]
      ],
      [
        ["1.2345", "this"], ["1.2345", "should"], ["1.2345", "also"], ["1.2345", "be"], ["1.2345", "null"]
      ],
      []
    ]
  )";
  auto map_array = ArrayFromJSON(map_type, input);
  auto map_array_tweaked = TweakValidityBit(map_array, 4, false);

  auto expected_first =
      ArrayFromJSON(utf8(), R"(["first_one", null, "the_dumb_one", null, null, null])");
  auto expected_last =
      ArrayFromJSON(utf8(), R"(["last_one", null, "no more ones!", null, null, null])");
  auto expected_all = ArrayFromJSON(list(utf8()),
                                    R"([
                                        ["first_one", null, "second_one", "last_one"],
                                        null,
                                        ["the_dumb_one", "the_chosen_one", "just_one", "no more ones!"],
                                        null,
                                        null,
                                        null
                                      ]
                                    )");
  CheckMapLookupWithDifferentOptions(map_array_tweaked, key_scalar, expected_all,
                                     expected_first, expected_last);
}

template <typename KeyType>
class TestMapLookupBinaryKeys : public ::testing ::Test {
 protected:
  std::shared_ptr<DataType> type_singleton() const {
    return TypeTraits<KeyType>::type_singleton();
  }
};

TYPED_TEST_SUITE(TestMapLookupBinaryKeys, BaseBinaryArrowTypes);

TYPED_TEST(TestMapLookupBinaryKeys, IntegralItems) {
  auto key_type = this->type_singleton();
  auto sheesh_scalar = ScalarFromJSON(key_type, R"("sheesh")");
  auto map_type = map(key_type, int32());
  const char* input = R"(
      [
        [
          ["sheesh", 99], ["yolo", 1], ["yay", 2], ["sheesh", null], ["no way!", 5],
          ["sheesh", 8]
        ],
        null,
        [
          ["hmm", null], ["sheesh", 67], ["snacc", 101], ["awesome", 1], ["dap", null],
          ["yolo", 9], ["sheesh", 80]
        ],
        [],
        [
          ["nope", 1], ["no", 2], ["sheeshes", 3], ["here!", 4]
        ],
        [
          ["sheesh", 9], ["sheesh", 2], ["sheesh", 5], ["sheesh", 8]
        ]
      ]
    )";
  auto map_array = ArrayFromJSON(map_type, input);
  auto map_array_tweaked = TweakValidityBit(map_array, 5, false);

  auto expected_all = ArrayFromJSON(list(int32()), R"(
    [[99, null, 8], null, [67, 80], null, null, null ])");
  auto expected_first = ArrayFromJSON(int32(), "[99, null, 67, null, null, null]");
  auto expected_last = ArrayFromJSON(int32(), "[8, null, 80, null, null, null]");

  CheckMapLookupWithDifferentOptions(map_array_tweaked, sheesh_scalar, expected_all,
                                     expected_first, expected_last);
}

struct {
  Result<Datum> operator()(std::vector<Datum> args) {
    return CallFunction("make_struct", args);
  }

  template <typename... Options>
  Result<Datum> operator()(std::vector<Datum> args, std::vector<std::string> field_names,
                           Options... options) {
    MakeStructOptions opts{field_names, options...};
    return CallFunction("make_struct", args, &opts);
  }
} MakeStructor;

TEST(MakeStruct, Scalar) {
  auto i32 = MakeScalar(1);
  auto f64 = MakeScalar(2.5);
  auto str = MakeScalar("yo");

  EXPECT_THAT(MakeStructor({i32, f64, str}, {"i", "f", "s"}),
              ResultWith(Datum(*StructScalar::Make({i32, f64, str}, {"i", "f", "s"}))));

  // Names default to field_index
  EXPECT_THAT(MakeStructor({i32, f64, str}),
              ResultWith(Datum(*StructScalar::Make({i32, f64, str}, {"0", "1", "2"}))));

  // Three field names but one input value
  EXPECT_THAT(MakeStructor({str}, {"i", "f", "s"}), Raises(StatusCode::Invalid));

  // ARROW-16757: No input values yields empty struct array of length 1
  ScalarVector value;
  auto empty_scalar = std::make_shared<StructScalar>(value, struct_({}));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> empty_result,
                       MakeArrayFromScalar(*empty_scalar, 0));
  ASSERT_OK_AND_ASSIGN(Datum empty_actual,
                       CallFunction("make_struct", std::vector<Datum>({})));
  AssertDatumsEqual(Datum(empty_result), empty_actual);
}

TEST(MakeStruct, Array) {
  std::vector<std::string> field_names{"i", "s"};

  auto i32 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto str = ArrayFromJSON(utf8(), R"(["aa", "aa", "aa"])");

  EXPECT_THAT(MakeStructor({i32, str}, {"i", "s"}),
              ResultWith(Datum(*StructArray::Make({i32, str}, field_names))));

  // Scalars are broadcast to the length of the arrays
  EXPECT_THAT(MakeStructor({i32, MakeScalar("aa")}, {"i", "s"}),
              ResultWith(Datum(*StructArray::Make({i32, str}, field_names))));

  // Array length mismatch
  EXPECT_THAT(MakeStructor({i32->Slice(1), str}, field_names),
              Raises(StatusCode::Invalid));
}

TEST(MakeStruct, NullableMetadataPassedThru) {
  auto i32 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto str = ArrayFromJSON(utf8(), R"(["aa", "aa", "aa"])");

  std::vector<std::string> field_names{"i", "s"};
  std::vector<bool> nullability{true, false};
  std::vector<std::shared_ptr<const KeyValueMetadata>> metadata = {
      key_value_metadata({"a", "b"}, {"ALPHA", "BRAVO"}), nullptr};

  ASSERT_OK_AND_ASSIGN(auto proj,
                       MakeStructor({i32, str}, field_names, nullability, metadata));

  AssertTypeEqual(*proj.type(), StructType({
                                    field("i", int32(), /*nullable=*/true, metadata[0]),
                                    field("s", utf8(), /*nullable=*/false, nullptr),
                                }));

  // error: projecting an array containing nulls with nullable=false
  EXPECT_THAT(MakeStructor({i32, ArrayFromJSON(utf8(), R"(["aa", null, "aa"])")},
                           field_names, nullability, metadata),
              Raises(StatusCode::Invalid));
}

TEST(MakeStruct, ChunkedArray) {
  std::vector<std::string> field_names{"i", "s"};

  auto i32_0 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto i32_1 = ArrayFromJSON(int32(), "[]");
  auto i32_2 = ArrayFromJSON(int32(), "[32, 0]");

  auto str_0 = ArrayFromJSON(utf8(), R"(["aa", "aa", "aa"])");
  auto str_1 = ArrayFromJSON(utf8(), "[]");
  auto str_2 = ArrayFromJSON(utf8(), R"(["aa", "aa"])");

  ASSERT_OK_AND_ASSIGN(auto i32, ChunkedArray::Make({i32_0, i32_1, i32_2}));
  ASSERT_OK_AND_ASSIGN(auto str, ChunkedArray::Make({str_0, str_1, str_2}));

  ASSERT_OK_AND_ASSIGN(auto expected_0, StructArray::Make({i32_0, str_0}, field_names));
  ASSERT_OK_AND_ASSIGN(auto expected_1, StructArray::Make({i32_1, str_1}, field_names));
  ASSERT_OK_AND_ASSIGN(auto expected_2, StructArray::Make({i32_2, str_2}, field_names));
  ASSERT_OK_AND_ASSIGN(Datum expected,
                       ChunkedArray::Make({expected_0, expected_1, expected_2}));

  ASSERT_OK_AND_EQ(expected, MakeStructor({i32, str}, field_names));

  // Scalars are broadcast to the length of the arrays
  ASSERT_OK_AND_EQ(expected, MakeStructor({i32, MakeScalar("aa")}, field_names));

  // Array length mismatch
  ASSERT_RAISES(Invalid, MakeStructor({i32->Slice(1), str}, field_names));
}

TEST(MakeStruct, ChunkedArrayDifferentChunking) {
  std::vector<std::string> field_names{"i", "s"};

  auto i32_0 = ArrayFromJSON(int32(), "[42, 13, 7]");
  auto i32_1 = ArrayFromJSON(int32(), "[]");
  auto i32_2 = ArrayFromJSON(int32(), "[32, 0]");

  auto str_0 = ArrayFromJSON(utf8(), R"(["aa", "aa"])");
  auto str_1 = ArrayFromJSON(utf8(), R"(["aa"])");
  auto str_2 = ArrayFromJSON(utf8(), R"([])");
  auto str_3 = ArrayFromJSON(utf8(), R"(["aa", "aa"])");

  ASSERT_OK_AND_ASSIGN(auto i32, ChunkedArray::Make({i32_0, i32_1, i32_2}));
  ASSERT_OK_AND_ASSIGN(auto str, ChunkedArray::Make({str_0, str_1, str_2, str_3}));

  std::vector<ArrayVector> expected_rechunked =
      ::arrow::internal::RechunkArraysConsistently({i32->chunks(), str->chunks()});
  ASSERT_EQ(expected_rechunked[0].size(), expected_rechunked[1].size());

  ArrayVector expected_chunks(expected_rechunked[0].size());
  for (size_t i = 0; i < expected_chunks.size(); ++i) {
    ASSERT_OK_AND_ASSIGN(expected_chunks[i], StructArray::Make({expected_rechunked[0][i],
                                                                expected_rechunked[1][i]},
                                                               field_names));
  }

  ASSERT_OK_AND_ASSIGN(Datum expected, ChunkedArray::Make(expected_chunks));

  ASSERT_OK_AND_EQ(expected, MakeStructor({i32, str}, field_names));

  // Scalars are broadcast to the length of the arrays
  ASSERT_OK_AND_EQ(expected, MakeStructor({i32, MakeScalar("aa")}, field_names));

  // Array length mismatch
  ASSERT_RAISES(Invalid, MakeStructor({i32->Slice(1), str}, field_names));
}

}  // namespace compute
}  // namespace arrow
