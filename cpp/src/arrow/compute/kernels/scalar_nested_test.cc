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
#include "arrow/compute/kernels/test_util.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace compute {

static std::shared_ptr<DataType> GetOffsetType(const DataType& type) {
  return type.id() == Type::LIST ? int32() : int64();
}

TEST(TestScalarNested, ListValueLength) {
  for (auto ty : {list(int32()), large_list(int32())}) {
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
    for (auto list_type : {list(ty), large_list(ty)}) {
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

TEST(TestScalarNested, ListSliceVariableOutput) {
  const auto value_types = {float32(), int32()};
  for (auto value_type : value_types) {
    auto input = ArrayFromJSON(list(value_type), "[[1, 2, 3], [4, 5], [6], null]");
    ListSliceOptions args(/*start=*/0, /*stop=*/2, /*step=*/1,
                          /*return_fixed_size_list=*/false);
    auto expected = ArrayFromJSON(list(value_type), "[[1, 2], [4, 5], [6], null]");
    CheckScalarUnary("list_slice", input, expected, &args);

    args.start = 1;
    expected = ArrayFromJSON(list(value_type), "[[2], [5], [], null]");
    CheckScalarUnary("list_slice", input, expected, &args);

    args.start = 2;
    args.stop = 4;
    expected = ArrayFromJSON(list(value_type), "[[3], [], [], null]");
    CheckScalarUnary("list_slice", input, expected, &args);

    args.start = 1;
    args.stop = std::nullopt;
    expected = ArrayFromJSON(list(value_type), "[[2, 3], [5], [], null]");
    CheckScalarUnary("list_slice", input, expected, &args);

    args.start = 0;
    args.stop = 4;
    args.step = 2;
    expected = ArrayFromJSON(list(value_type), "[[1, 3], [4], [6], null]");
  }

  // Verify passing `return_fixed_size_list=false` with fixed size input
  // returns variable size even if stop is beyond list_size
  ListSliceOptions args(/*start=*/0, /*stop=*/2, /*step=*/1,
                        /*return_fixed_size_list=*/false);
  auto input = ArrayFromJSON(fixed_size_list(int32(), 1), "[[1]]");
  auto expected = ArrayFromJSON(list(int32()), "[[1]]");
  CheckScalarUnary("list_slice", input, expected, &args);
}

TEST(TestScalarNested, ListSliceFixedOutput) {
  const auto value_types = {float32(), int32()};
  for (auto value_type : value_types) {
    auto inputs = {ArrayFromJSON(list(value_type), "[[1, 2, 3], [4, 5], [6], null]"),
                   ArrayFromJSON(fixed_size_list(value_type, 3),
                                 "[[1, 2, 3], [4, 5, null], [6, null, null], null]")};
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
            NotImplemented,
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

  ListSliceOptions args(/*start=*/0, /*stop=*/2, /*step=*/1,
                        /*return_fixed_size_list=*/false);
  auto expected = ArrayFromJSON(list(int8()), "[[2], [3, 4]]");
  CheckScalarUnary("list_slice", input, expected, &args);

  args.return_fixed_size_list = true;
  expected = ArrayFromJSON(fixed_size_list(int8(), 2), "[[2, null], [3, 4]]");
  CheckScalarUnary("list_slice", input, expected, &args);
}

TEST(TestScalarNested, ListSliceOutputEqualsInputType) {
  // Default is to return same type as the one passed in.
  auto inputs = {
      ArrayFromJSON(list(int8()), "[[1, 2, 3], [4, 5], [6, null], null]"),
      ArrayFromJSON(large_list(int8()), "[[1, 2, 3], [4, 5], [6, null], null]"),
      ArrayFromJSON(fixed_size_list(int8(), 2), "[[1, 2], [4, 5], [6, null], null]")};
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
          "`start`(-1) should be greater than 0 and smaller than `stop`(1)"),
      CallFunction("list_slice", {input}, &args));
  // start greater than stop
  args.start = 1;
  args.stop = 0;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "`start`(1) should be greater than 0 and smaller than `stop`(0)"),
      CallFunction("list_slice", {input}, &args));
  // start same as stop
  args.stop = args.start;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "`start`(1) should be greater than 0 and smaller than `stop`(1)"),
      CallFunction("list_slice", {input}, &args));
  // stop not set and FixedSizeList requested with variable sized input
  args.stop = std::nullopt;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      NotImplemented,
      ::testing::HasSubstr("NotImplemented: Unable to produce FixedSizeListArray from "
                           "non-FixedSizeListArray without "
                           "`stop` being set."),
      CallFunction("list_slice", {input}, &args));
  // Catch step must be >= 1
  args.start = 0;
  args.stop = 2;
  args.step = 0;
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("`step` must be >= 1, got: 0"),
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
